/**
 * SmolDB Collection
 * Provides CRUD operations for a single collection
 */

import type { Document, QueryFilter, SmolDBOptions, CompactionResult } from './types';
import { SHARED_STATE_SIZE } from './types';
import { FILE_EXTENSIONS, BLOB_DIR, DEFAULT_BLOB_THRESHOLD, DEFAULT_GC_TRIGGER_RATIO } from './constants';
import { StorageEngine } from './storage';
import { IndexManager } from './index-manager';
import { DuplicateIdError, DocumentNotFoundError, NotInitializedError } from './errors';
import { readdir } from 'node:fs/promises';

export class Collection {
  private storage: StorageEngine;
  private indexManager: IndexManager;
  private initialized = false;
  private sharedBuffer: SharedArrayBuffer;
  private readonly cacheSize: number;
  private cache: Map<string, Document> | null = null;

  constructor(
    private readonly basePath: string,
    private readonly name: string,
    sharedBuffer?: SharedArrayBuffer,
    options?: SmolDBOptions
  ) {
    this.sharedBuffer = sharedBuffer ?? new SharedArrayBuffer(SHARED_STATE_SIZE);

    this.cacheSize = Math.max(0, options?.cacheSize ?? 0);
    if (this.cacheSize > 0) {
      this.cache = new Map();
    }

    const dataPath = `${basePath}/${name}${FILE_EXTENSIONS.DATA}`;
    const indexPath = `${basePath}/${name}${FILE_EXTENSIONS.INDEX}`;
    const blobPath = `${basePath}/${BLOB_DIR}/${name}`;

    this.storage = new StorageEngine(
      dataPath,
      blobPath,
      this.sharedBuffer,
      options?.blobThreshold ?? DEFAULT_BLOB_THRESHOLD
    );
    this.indexManager = new IndexManager(indexPath);
  }

  /**
   * Get collection name
   */
  getName(): string {
    return this.name;
  }

  /**
   * Ensure collection is initialized
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return;

    // Try to load existing index
    const indexFile = Bun.file(this.indexManager.indexPath);
    if (await indexFile.exists()) {
      await this.indexManager.load();
    }

    await this.storage.init(this.indexManager.getPrimaryIndex());
    this.initialized = true;
  }

  /**
   * Insert a new document
   * @throws DuplicateIdError if document with same ID exists
   */
  async insert(id: string, data: Document): Promise<void> {
    await this.ensureInitialized();

    if (this.indexManager.has(id)) {
      throw new DuplicateIdError(id);
    }

    const location = await this.storage.write(id, data);
    this.indexManager.addDocument(id, location, data);
    this.cacheSet(id, data);
  }

  /**
   * Run multiple mutations under a single storage write lock.
   * Useful for bulk inserts/updates/deletes of many small documents.
   */
  async batch<T>(
    fn: (ops: {
      insert: (id: string, data: Document) => Promise<void>;
      update: (id: string, data: Document) => Promise<void>;
      upsert: (id: string, data: Document) => Promise<void>;
      delete: (id: string) => Promise<boolean>;
    }) => Promise<T>
  ): Promise<T> {
    await this.ensureInitialized();

    return this.storage.batch(async (storageOps) => {
      return fn({
        insert: async (id, data) => {
          if (this.indexManager.has(id)) {
            throw new DuplicateIdError(id);
          }
          const location = await storageOps.write(id, data);
          this.indexManager.addDocument(id, location, data);
          this.cacheSet(id, data);
        },
        update: async (id, data) => {
          const oldLocation = this.indexManager.getLocation(id);
          if (!oldLocation) {
            throw new DocumentNotFoundError(id);
          }
          const oldData = await this.storage.read(oldLocation);
          const newLocation = await storageOps.update(id, data, oldLocation);
          this.indexManager.updateDocument(id, newLocation, data, oldData);
          this.cacheSet(id, data);
        },
        upsert: async (id, data) => {
          const existingLocation = this.indexManager.getLocation(id);
          if (existingLocation) {
            const oldData = await this.storage.read(existingLocation);
            const newLocation = await storageOps.update(id, data, existingLocation);
            this.indexManager.updateDocument(id, newLocation, data, oldData);
            this.cacheSet(id, data);
          } else {
            const location = await storageOps.write(id, data);
            this.indexManager.addDocument(id, location, data);
            this.cacheSet(id, data);
          }
        },
        delete: async (id) => {
          const location = this.indexManager.getLocation(id);
          if (!location) return false;

          const data = await this.storage.read(location);
          await storageOps.delete(location);
          this.indexManager.removeDocument(id, data);
          this.cacheDelete(id);
          return true;
        },
      });
    });
  }

  /**
   * Bulk insert optimized for many small documents.
   * This is primarily intended for initial loads / benchmarks.
   */
  async insertMany(items: Array<[id: string, data: Document]>): Promise<void> {
    await this.ensureInitialized();
    if (items.length === 0) return;

    for (const [id] of items) {
      if (this.indexManager.has(id)) {
        throw new DuplicateIdError(id);
      }
    }

    const locations = await this.storage.writeMany(items);
    for (let i = 0; i < items.length; i++) {
      const [id, data] = items[i];
      const location = locations[i];
      this.indexManager.addDocument(id, location, data);
      this.cacheSet(id, data);
    }
  }

  /**
   * Reset collection storage and index (truncate data file + delete blobs).
   * Faster than deleting documents one by one when you want a truly empty collection.
   */
  async reset(): Promise<void> {
    await this.ensureInitialized();

    // Clear blobs for this collection
    const blobPath = `${this.basePath}/${BLOB_DIR}/${this.name}`;
    try {
      const blobFiles = await readdir(blobPath);
      for (const file of blobFiles) {
        await Bun.file(`${blobPath}/${file}`).delete();
      }
    } catch {}

    await this.storage.reset();
    this.indexManager.clear();
    await this.indexManager.persist();
    this.cacheClear();
  }

  /**
   * Get a document by ID
   * @returns Document or null if not found
   */
  async get(id: string): Promise<Document | null> {
    await this.ensureInitialized();

    const cached = this.cacheGet(id);
    if (cached) return cached;

    const location = this.indexManager.getLocation(id);
    if (!location) return null;

    const doc = await this.storage.read(location);
    this.cacheSet(id, doc);
    return doc;
  }

  /**
   * Update an existing document
   * @throws DocumentNotFoundError if document doesn't exist
   */
  async update(id: string, data: Document): Promise<void> {
    await this.ensureInitialized();

    const oldLocation = this.indexManager.getLocation(id);
    if (!oldLocation) {
      throw new DocumentNotFoundError(id);
    }

    // Get old data for secondary index update
    const oldData = await this.storage.read(oldLocation);

    const newLocation = await this.storage.update(id, data, oldLocation);
    this.indexManager.updateDocument(id, newLocation, data, oldData);
    this.cacheSet(id, data);
  }

  /**
   * Upsert a document (insert or update)
   */
  async upsert(id: string, data: Document): Promise<void> {
    await this.ensureInitialized();

    const existingLocation = this.indexManager.getLocation(id);
    if (existingLocation) {
      const oldData = await this.storage.read(existingLocation);
      const newLocation = await this.storage.update(id, data, existingLocation);
      this.indexManager.updateDocument(id, newLocation, data, oldData);
      this.cacheSet(id, data);
    } else {
      const location = await this.storage.write(id, data);
      this.indexManager.addDocument(id, location, data);
      this.cacheSet(id, data);
    }
  }

  /**
   * Delete a document
   * @returns true if deleted, false if not found
   */
  async delete(id: string): Promise<boolean> {
    await this.ensureInitialized();

    const location = this.indexManager.getLocation(id);
    if (!location) return false;

    // Get data for secondary index cleanup
    const data = await this.storage.read(location);

    await this.storage.delete(location);
    this.indexManager.removeDocument(id, data);

    this.cacheDelete(id);

    return true;
  }

  /**
   * Check if a document exists
   */
  async has(id: string): Promise<boolean> {
    await this.ensureInitialized();
    return this.indexManager.has(id);
  }

  /**
   * Find documents matching a filter
   */
  async find(filter: QueryFilter): Promise<Document[]> {
    await this.ensureInitialized();
    return this.indexManager.query(filter, this.storage, (id, location) => this.readWithCache(id, location));
  }

  /**
   * Find a single document matching a filter
   */
  async findOne(filter: QueryFilter): Promise<Document | null> {
    const results = await this.find(filter);
    return results[0] ?? null;
  }

  /**
   * Find matching document IDs.
   * If the filter is fully covered by secondary indexes, this avoids reading documents.
   */
  async findIds(filter: QueryFilter): Promise<string[]> {
    await this.ensureInitialized();
    return this.indexManager.queryIds(filter, this.storage, (id, location) => this.readWithCache(id, location));
  }

  /**
   * Get all documents
   */
  async getAll(): Promise<Document[]> {
    await this.ensureInitialized();

    const docs: Document[] = [];
    for (const id of this.indexManager.getAllIds()) {
      const doc = await this.get(id);
      if (doc) docs.push(doc);
    }
    return docs;
  }

  /**
   * Get all document IDs
   */
  async keys(): Promise<string[]> {
    await this.ensureInitialized();
    return this.indexManager.getAllIds();
  }

  /**
   * Get document count.
   *
   * - Without a filter: returns total documents.
   * - With a filter: uses secondary indexes when possible.
   */
  async count(filter?: QueryFilter): Promise<number> {
    await this.ensureInitialized();
    if (!filter || Object.keys(filter).length === 0) {
      return this.indexManager.size;
    }
    return this.indexManager.count(filter, this.storage, (id, location) => this.readWithCache(id, location));
  }

  /**
   * Create a secondary index on a field path
   */
  async createIndex(fieldPath: string): Promise<void> {
    await this.ensureInitialized();
    await this.indexManager.createSecondaryIndex(fieldPath, this.storage);
  }

  /**
   * Get indexed field paths
   */
  async getIndexes(): Promise<string[]> {
    await this.ensureInitialized();
    return this.indexManager.getIndexedFields();
  }

  /**
   * Compact the collection (reclaim deleted space)
   */
  async compact(): Promise<CompactionResult> {
    await this.ensureInitialized();

    const primaryIndex = this.indexManager.getPrimaryIndex();
    const result = await this.storage.compact(primaryIndex);

    // Update index with new locations
    for (const [id, location] of result.newLocations) {
      this.indexManager.updateLocation(id, location);
    }

    // Persist updated index
    await this.indexManager.persist();

    return {
      bytesFreed: result.bytesFreed,
      documentsCompacted: primaryIndex.size,
      newFileSize: this.storage.getStats().fileSize,
    };
  }

  /**
   * Persist the index to disk
   */
  async persistIndex(): Promise<void> {
    if (this.initialized && this.indexManager.isDirty()) {
      await this.indexManager.persist();
    }
  }

  /**
   * Get storage statistics
   */
  async getStats(): Promise<{
    documentCount: number;
    fileSize: number;
    liveDataSize: number;
    freeSlots: number;
    indexes: string[];
  }> {
    await this.ensureInitialized();

    const storageStats = this.storage.getStats();
    return {
      documentCount: storageStats.documentCount,
      fileSize: storageStats.fileSize,
      liveDataSize: storageStats.liveDataSize,
      freeSlots: storageStats.freeSlots,
      indexes: this.indexManager.getIndexedFields(),
    };
  }

  /**
   * Clear all documents in the collection
   */
  async clear(): Promise<void> {
    await this.ensureInitialized();

    const ids = this.indexManager.getAllIds();
    if (ids.length === 0) return;

    await this.storage.batch(async (ops) => {
      for (const id of ids) {
        const location = this.indexManager.getLocation(id);
        if (!location) continue;
        await ops.delete(location);
      }
    });

    this.indexManager.clear();
    this.cacheClear();
  }

  /**
   * Iterate over all documents
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<{ id: string; data: Document }> {
    await this.ensureInitialized();

    for (const id of this.indexManager.getAllIds()) {
      const location = this.indexManager.getLocation(id);
      if (location) {
        const data = await this.storage.read(location);
        yield { id, data };
      }
    }
  }

  /**
   * Close the collection
   */
  async close(): Promise<void> {
    await this.persistIndex();
    await this.storage.close();
    this.initialized = false;
    this.cacheClear();
  }

  /**
   * Get SharedArrayBuffer for GC worker
   */
  getSharedBuffer(): SharedArrayBuffer {
    return this.sharedBuffer;
  }

  private cacheGet(id: string): Document | null {
    if (!this.cache) return null;
    const value = this.cache.get(id);
    if (!value) return null;
    // Refresh recency
    this.cache.delete(id);
    this.cache.set(id, value);
    return value;
  }

  private cacheSet(id: string, doc: Document): void {
    if (!this.cache) return;
    if (this.cache.has(id)) {
      this.cache.delete(id);
    }
    this.cache.set(id, doc);

    if (this.cache.size > this.cacheSize) {
      const oldestKey = this.cache.keys().next().value as string | undefined;
      if (oldestKey !== undefined) {
        this.cache.delete(oldestKey);
      }
    }
  }

  private cacheDelete(id: string): void {
    this.cache?.delete(id);
  }

  private cacheClear(): void {
    this.cache?.clear();
  }

  private async readWithCache(id: string, location: { offset: number; length: number; slabSize: number; isBlob: boolean }): Promise<Document> {
    const cached = this.cacheGet(id);
    if (cached) return cached;
    const doc = await this.storage.read(location);
    this.cacheSet(id, doc);
    return doc;
  }
}
