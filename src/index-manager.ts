/**
 * SmolDB Index Manager
 * Handles primary and secondary indexes with binary persistence
 */

import type {
  DocumentLocation,
  PrimaryIndex,
  SecondaryIndex,
  QueryFilter,
  Document,
} from './types';
import {
  INDEX_HEADER_SIZE,
  INDEX_FILE_MAGIC,
  FORMAT_VERSION,
} from './constants';
import {
  encodeString,
  decodeString,
  writeUint32,
  readUint32,
  writeUint64,
  readUint64,
  writeUint16,
  readUint16,
  serializeIndexValue,
  getNestedValue,
  matchesFilter,
} from './utils';
import { IndexCorruptedError, InvalidFileFormatError } from './errors';
import type { StorageEngine } from './storage';

export class IndexManager {
  private primaryIndex: PrimaryIndex = new Map();
  private secondaryIndexes: SecondaryIndex = new Map();
  private dirty = false;

  constructor(public readonly indexPath: string) {}

  /**
   * Check if a document exists
   */
  has(id: string): boolean {
    return this.primaryIndex.has(id);
  }

  /**
   * Get document location
   */
  getLocation(id: string): DocumentLocation | undefined {
    return this.primaryIndex.get(id);
  }

  /**
   * Get all document IDs
   */
  getAllIds(): string[] {
    return Array.from(this.primaryIndex.keys());
  }

  /**
   * Get the primary index (for compaction)
   */
  getPrimaryIndex(): PrimaryIndex {
    return this.primaryIndex;
  }

  /**
   * Get count of documents
   */
  get size(): number {
    return this.primaryIndex.size;
  }

  /**
   * Add a document to the index
   */
  addDocument(id: string, location: DocumentLocation, data: Document): void {
    this.primaryIndex.set(id, location);

    // Update secondary indexes
    for (const [fieldPath, valueMap] of this.secondaryIndexes) {
      const value = getNestedValue(data, fieldPath);
      if (value !== undefined) {
        const serialized = serializeIndexValue(value);
        let ids = valueMap.get(serialized);
        if (!ids) {
          ids = new Set();
          valueMap.set(serialized, ids);
        }
        ids.add(id);
      }
    }

    this.dirty = true;
  }

  /**
   * Update document location and secondary indexes
   */
  updateDocument(id: string, location: DocumentLocation, newData: Document, oldData?: Document): void {
    this.primaryIndex.set(id, location);

    // Update secondary indexes
    for (const [fieldPath, valueMap] of this.secondaryIndexes) {
      // Remove from old value's set if we have old data
      if (oldData) {
        const oldValue = getNestedValue(oldData, fieldPath);
        if (oldValue !== undefined) {
          const oldSerialized = serializeIndexValue(oldValue);
          const oldIds = valueMap.get(oldSerialized);
          if (oldIds) {
            oldIds.delete(id);
            if (oldIds.size === 0) {
              valueMap.delete(oldSerialized);
            }
          }
        }
      }

      // Add to new value's set
      const newValue = getNestedValue(newData, fieldPath);
      if (newValue !== undefined) {
        const newSerialized = serializeIndexValue(newValue);
        let ids = valueMap.get(newSerialized);
        if (!ids) {
          ids = new Set();
          valueMap.set(newSerialized, ids);
        }
        ids.add(id);
      }
    }

    this.dirty = true;
  }

  /**
   * Update only the location (after compaction)
   */
  updateLocation(id: string, location: DocumentLocation): void {
    this.primaryIndex.set(id, location);
    this.dirty = true;
  }

  /**
   * Remove a document from the index
   */
  removeDocument(id: string, data?: Document): void {
    this.primaryIndex.delete(id);

    // Remove from secondary indexes if we have the data
    if (data) {
      for (const [fieldPath, valueMap] of this.secondaryIndexes) {
        const value = getNestedValue(data, fieldPath);
        if (value !== undefined) {
          const serialized = serializeIndexValue(value);
          const ids = valueMap.get(serialized);
          if (ids) {
            ids.delete(id);
            if (ids.size === 0) {
              valueMap.delete(serialized);
            }
          }
        }
      }
    }

    this.dirty = true;
  }

  /**
   * Create a secondary index on a field path
   */
  async createSecondaryIndex(fieldPath: string, storage: StorageEngine): Promise<void> {
    if (this.secondaryIndexes.has(fieldPath)) {
      return; // Already exists
    }

    const valueMap = new Map<string, Set<string>>();
    this.secondaryIndexes.set(fieldPath, valueMap);

    // Build index from existing documents
    for (const [id, location] of this.primaryIndex) {
      const data = await storage.read(location);
      const value = getNestedValue(data, fieldPath);
      if (value !== undefined) {
        const serialized = serializeIndexValue(value);
        let ids = valueMap.get(serialized);
        if (!ids) {
          ids = new Set();
          valueMap.set(serialized, ids);
        }
        ids.add(id);
      }
    }

    this.dirty = true;
  }

  /**
   * Get indexed field paths
   */
  getIndexedFields(): string[] {
    return Array.from(this.secondaryIndexes.keys());
  }

  /**
   * Query documents using filter
   * Uses secondary indexes when available
   */
  async query(
    filter: QueryFilter,
    storage: StorageEngine,
    reader?: (id: string, location: DocumentLocation) => Promise<Document>
  ): Promise<Document[]> {
    const results: Document[] = [];
    let candidateIds: Set<string> | null = null;
    let fullyCoveredByIndexes = true;

    // Try to use secondary indexes
    for (const [field, value] of Object.entries(filter)) {
      const fieldIndex = this.secondaryIndexes.get(field);
      if (fieldIndex) {
        const serialized = serializeIndexValue(value);
        const matchingIds = fieldIndex.get(serialized);

        if (matchingIds) {
          if (candidateIds === null) {
            candidateIds = new Set(matchingIds);
          } else {
            // Intersect
            candidateIds = new Set([...candidateIds].filter((id) => matchingIds.has(id)));
          }
        } else {
          // No matches for this indexed value
          return [];
        }
      }

      if (!fieldIndex) {
        fullyCoveredByIndexes = false;
      }
    }

    // If no indexed fields matched, use full scan
    if (candidateIds === null) {
      candidateIds = new Set(this.primaryIndex.keys());
      fullyCoveredByIndexes = false;
    }

    // Fetch and filter documents
    for (const id of candidateIds) {
      const location = this.primaryIndex.get(id);
      if (!location) continue;

      const doc = reader ? await reader(id, location) : await storage.read(location);

      if (fullyCoveredByIndexes) {
        results.push(doc);
      } else if (matchesFilter(doc, filter)) {
        results.push(doc);
      }
    }

    return results;
  }

  /**
   * Return matching document IDs.
   *
   * If the filter is fully covered by secondary indexes, this avoids reading any documents.
   * Otherwise it will read the reduced candidate set and validate via matchesFilter.
   */
  async queryIds(
    filter: QueryFilter,
    storage: StorageEngine,
    reader?: (id: string, location: DocumentLocation) => Promise<Document>
  ): Promise<string[]> {
    let candidateIds: Set<string> | null = null;
    let fullyCoveredByIndexes = true;

    for (const [field, value] of Object.entries(filter)) {
      const fieldIndex = this.secondaryIndexes.get(field);
      if (fieldIndex) {
        const serialized = serializeIndexValue(value);
        const matchingIds = fieldIndex.get(serialized);

        if (matchingIds) {
          if (candidateIds === null) {
            candidateIds = new Set(matchingIds);
          } else {
            candidateIds = new Set([...candidateIds].filter((id) => matchingIds.has(id)));
          }
        } else {
          return [];
        }
      } else {
        fullyCoveredByIndexes = false;
      }
    }

    if (candidateIds === null) {
      candidateIds = new Set(this.primaryIndex.keys());
      fullyCoveredByIndexes = false;
    }

    if (fullyCoveredByIndexes) {
      return Array.from(candidateIds);
    }

    const ids: string[] = [];
    for (const id of candidateIds) {
      const location = this.primaryIndex.get(id);
      if (!location) continue;

      const doc = reader ? await reader(id, location) : await storage.read(location);
      if (matchesFilter(doc, filter)) {
        ids.push(id);
      }
    }

    return ids;
  }

  /**
   * Count matching documents.
   *
   * If the filter is fully covered by secondary indexes, this avoids reading any documents.
   */
  async count(
    filter: QueryFilter,
    storage: StorageEngine,
    reader?: (id: string, location: DocumentLocation) => Promise<Document>
  ): Promise<number> {
    let candidateIds: Set<string> | null = null;
    let fullyCoveredByIndexes = true;

    for (const [field, value] of Object.entries(filter)) {
      const fieldIndex = this.secondaryIndexes.get(field);
      if (fieldIndex) {
        const serialized = serializeIndexValue(value);
        const matchingIds = fieldIndex.get(serialized);

        if (matchingIds) {
          if (candidateIds === null) {
            candidateIds = new Set(matchingIds);
          } else {
            candidateIds = new Set([...candidateIds].filter((id) => matchingIds.has(id)));
          }
        } else {
          return 0;
        }
      } else {
        fullyCoveredByIndexes = false;
      }
    }

    if (candidateIds === null) {
      candidateIds = new Set(this.primaryIndex.keys());
      fullyCoveredByIndexes = false;
    }

    if (fullyCoveredByIndexes) {
      return candidateIds.size;
    }

    let total = 0;
    for (const id of candidateIds) {
      const location = this.primaryIndex.get(id);
      if (!location) continue;

      const doc = reader ? await reader(id, location) : await storage.read(location);
      if (matchesFilter(doc, filter)) {
        total++;
      }
    }

    return total;
  }

  /**
   * Load index from file
   */
  async load(): Promise<void> {
    const file = Bun.file(this.indexPath);
    if (!(await file.exists())) {
      return; // No index file, start fresh
    }

    const buffer = await file.arrayBuffer();
    const bytes = new Uint8Array(buffer);

    if (bytes.length < INDEX_HEADER_SIZE) {
      throw new IndexCorruptedError(this.indexPath);
    }

    // Read header
    const magic = readUint32(bytes, 0);
    if (magic !== INDEX_FILE_MAGIC) {
      throw new InvalidFileFormatError(
        this.indexPath,
        `Invalid magic: expected 0x${INDEX_FILE_MAGIC.toString(16)}, got 0x${magic.toString(16)}`
      );
    }

    const version = readUint32(bytes, 4);
    if (version !== FORMAT_VERSION) {
      throw new InvalidFileFormatError(this.indexPath, `Unsupported version: ${version}`);
    }

    const secondaryIndexCount = readUint16(bytes, 8);
    const primaryIndexCount = readUint32(bytes, 10);
    const primaryIndexOffset = readUint32(bytes, 14);
    const secondaryIndexOffset = readUint32(bytes, 18);

    // Read primary index
    let offset = primaryIndexOffset;
    for (let i = 0; i < primaryIndexCount; i++) {
      const idLength = readUint16(bytes, offset);
      offset += 2;

      const id = decodeString(bytes.slice(offset, offset + idLength));
      offset += idLength;

      const docOffset = readUint64(bytes, offset);
      offset += 8;

      const length = readUint32(bytes, offset);
      offset += 4;

      const slabSize = readUint32(bytes, offset);
      offset += 4;

      const flags = readUint32(bytes, offset);
      offset += 4;

      this.primaryIndex.set(id, {
        offset: docOffset,
        length,
        slabSize,
        isBlob: (flags & 1) !== 0,
      });
    }

    // Read secondary indexes
    offset = secondaryIndexOffset;
    for (let i = 0; i < secondaryIndexCount; i++) {
      const fieldPathLength = readUint16(bytes, offset);
      offset += 2;

      const fieldPath = decodeString(bytes.slice(offset, offset + fieldPathLength));
      offset += fieldPathLength;

      const entryCount = readUint32(bytes, offset);
      offset += 4;

      const valueMap = new Map<string, Set<string>>();
      this.secondaryIndexes.set(fieldPath, valueMap);

      for (let j = 0; j < entryCount; j++) {
        const valueLength = readUint32(bytes, offset);
        offset += 4;

        const serializedValue = decodeString(bytes.slice(offset, offset + valueLength));
        offset += valueLength;

        const idCount = readUint32(bytes, offset);
        offset += 4;

        const ids = new Set<string>();
        for (let k = 0; k < idCount; k++) {
          const docIdLength = readUint16(bytes, offset);
          offset += 2;

          const docId = decodeString(bytes.slice(offset, offset + docIdLength));
          offset += docIdLength;

          ids.add(docId);
        }

        valueMap.set(serializedValue, ids);
      }
    }

    this.dirty = false;
  }

  /**
   * Persist index to file
   */
  async persist(): Promise<void> {
    // Calculate total size
    let size = INDEX_HEADER_SIZE;

    // Primary index size
    const primaryStart = size;
    for (const [id] of this.primaryIndex) {
      const idBytes = encodeString(id);
      size += 2 + idBytes.length + 8 + 4 + 4 + 4; // idLen + id + offset + length + slabSize + flags
    }

    // Secondary indexes size
    const secondaryStart = size;
    for (const [fieldPath, valueMap] of this.secondaryIndexes) {
      const fieldPathBytes = encodeString(fieldPath);
      size += 2 + fieldPathBytes.length + 4; // fieldPathLen + fieldPath + entryCount

      for (const [serializedValue, ids] of valueMap) {
        const valueBytes = encodeString(serializedValue);
        size += 4 + valueBytes.length + 4; // valueLen + value + idCount

        for (const id of ids) {
          const idBytes = encodeString(id);
          size += 2 + idBytes.length; // idLen + id
        }
      }
    }

    // Allocate buffer
    const buffer = new Uint8Array(size);
    let offset = 0;

    // Write header
    writeUint32(buffer, offset, INDEX_FILE_MAGIC);
    offset += 4;
    writeUint32(buffer, offset, FORMAT_VERSION);
    offset += 4;
    writeUint16(buffer, offset, this.secondaryIndexes.size);
    offset += 2;
    writeUint32(buffer, offset, this.primaryIndex.size);
    offset += 4;
    writeUint32(buffer, offset, primaryStart);
    offset += 4;
    writeUint32(buffer, offset, secondaryStart);
    offset += 4;

    // Pad header to INDEX_HEADER_SIZE
    offset = INDEX_HEADER_SIZE;

    // Write primary index
    for (const [id, location] of this.primaryIndex) {
      const idBytes = encodeString(id);
      writeUint16(buffer, offset, idBytes.length);
      offset += 2;

      buffer.set(idBytes, offset);
      offset += idBytes.length;

      writeUint64(buffer, offset, location.offset);
      offset += 8;

      writeUint32(buffer, offset, location.length);
      offset += 4;

      writeUint32(buffer, offset, location.slabSize);
      offset += 4;

      writeUint32(buffer, offset, location.isBlob ? 1 : 0);
      offset += 4;
    }

    // Write secondary indexes
    for (const [fieldPath, valueMap] of this.secondaryIndexes) {
      const fieldPathBytes = encodeString(fieldPath);
      writeUint16(buffer, offset, fieldPathBytes.length);
      offset += 2;

      buffer.set(fieldPathBytes, offset);
      offset += fieldPathBytes.length;

      writeUint32(buffer, offset, valueMap.size);
      offset += 4;

      for (const [serializedValue, ids] of valueMap) {
        const valueBytes = encodeString(serializedValue);
        writeUint32(buffer, offset, valueBytes.length);
        offset += 4;

        buffer.set(valueBytes, offset);
        offset += valueBytes.length;

        writeUint32(buffer, offset, ids.size);
        offset += 4;

        for (const id of ids) {
          const idBytes = encodeString(id);
          writeUint16(buffer, offset, idBytes.length);
          offset += 2;

          buffer.set(idBytes, offset);
          offset += idBytes.length;
        }
      }
    }

    await Bun.write(this.indexPath, buffer);
    this.dirty = false;
  }

  /**
   * Check if index needs to be persisted
   */
  isDirty(): boolean {
    return this.dirty;
  }

  /**
   * Replace primary index (after compaction)
   */
  replacePrimaryIndex(newIndex: PrimaryIndex): void {
    this.primaryIndex = newIndex;
    this.dirty = true;
  }

  /**
   * Clear all indexes
   */
  clear(): void {
    this.primaryIndex.clear();
    this.secondaryIndexes.clear();
    this.dirty = true;
  }
}
