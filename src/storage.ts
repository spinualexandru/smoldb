/**
 * SmolDB Storage Engine
 * Handles slab allocation, file I/O, and free list management
 */

import { mkdir, open } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import type {
  DocumentLocation,
  SlabAllocation,
  FreeSlot,
  DataFileHeader,
  SlotHeader,
  PrimaryIndex,
  BlobReference,
} from './types';
import { SlotFlags, SharedStateOffsets } from './types';
import {
  DATA_HEADER_SIZE,
  SLOT_HEADER_SIZE,
  DATA_FILE_MAGIC,
  FORMAT_VERSION,
  DEFAULT_BLOB_THRESHOLD,
  selectSlabSize,
} from './constants';
import {
  crc32,
  encodeString,
  decodeString,
  writeUint32,
  readUint32,
  writeUint64,
  readUint64,
  AsyncMutex,
} from './utils';
import {
  CorruptedDataError,
  InvalidFileFormatError,
  ChecksumMismatchError,
} from './errors';

export class StorageEngine {
  private fileHandle: FileHandle | null = null;
  private header: DataFileHeader | null = null;
  private freeList: Set<FreeSlot> = new Set();
  private writeLock = new AsyncMutex();
  private sharedView: Int32Array | null = null;

  private batchDepth = 0;
  private headerDirty = false;
  private sharedStateDirty = false;

  constructor(
    private readonly dataPath: string,
    private readonly blobPath: string,
    sharedBuffer?: SharedArrayBuffer,
    private readonly blobThreshold: number = DEFAULT_BLOB_THRESHOLD
  ) {
    if (sharedBuffer) {
      this.sharedView = new Int32Array(sharedBuffer);
    }
  }

  /**
   * Initialize the storage engine
   * If file exists, reads header; otherwise creates new file
   */
  async init(existingIndex?: PrimaryIndex): Promise<void> {
    const file = Bun.file(this.dataPath);
    const exists = await file.exists();

    if (exists) {
      // Open existing file and read header
      this.fileHandle = await this.openFile(this.dataPath, 'r+');
      await this.readHeader();

      // Rebuild free list from index if provided
      if (existingIndex) {
        const packedSize = this.calculatePackedSize(existingIndex);
        // If all live slots exactly account for the file size, there can be no free slots.
        if (packedSize !== this.header!.nextSlotOffset) {
          await this.rebuildFreeList(existingIndex);
        }
      }
    } else {
      // Create new file with header
      this.fileHandle = await this.openFile(this.dataPath, 'w+');
      await this.createNewFile();
    }

    // Ensure blob directory exists
    await mkdir(this.blobPath, { recursive: true });

    // Update shared state
    this.updateSharedState();
  }

  /**
   * Write a document to storage
   * Returns the location where it was written
   */
  async write(id: string, data: Record<string, unknown>): Promise<DocumentLocation> {
    const release = await this.writeLock.acquire();
    try {
      return await this.writeUnlocked(id, data);
    } finally {
      release();
    }
  }

  /**
   * Read a document from storage
   */
  async read(location: DocumentLocation): Promise<Record<string, unknown>> {
    if (location.isBlob) {
      return this.readBlob(location);
    }

    const slotData = await this.readSlot(location.offset, location.length);
    const jsonString = decodeString(slotData);
    return JSON.parse(jsonString);
  }

  /**
   * Update a document in storage
   * Tries in-place update first, relocates if needed
   */
  async update(
    id: string,
    data: Record<string, unknown>,
    oldLocation: DocumentLocation
  ): Promise<DocumentLocation> {
    const release = await this.writeLock.acquire();
    try {
      return await this.updateUnlocked(id, data, oldLocation);
    } finally {
      release();
    }
  }

  /**
   * Delete a document from storage
   */
  async delete(location: DocumentLocation): Promise<void> {
    const release = await this.writeLock.acquire();
    try {
      await this.deleteUnlocked(location);
    } finally {
      release();
    }
  }

  /**
   * Run multiple write operations under a single write lock and flush header/shared-state once.
   */
  async batch<T>(
    fn: (ops: {
      write: (id: string, data: Record<string, unknown>) => Promise<DocumentLocation>;
      update: (
        id: string,
        data: Record<string, unknown>,
        oldLocation: DocumentLocation
      ) => Promise<DocumentLocation>;
      delete: (location: DocumentLocation) => Promise<void>;
    }) => Promise<T>
  ): Promise<T> {
    const release = await this.writeLock.acquire();
    this.batchDepth++;

    try {
      const result = await fn({
        write: (id, data) => this.writeUnlocked(id, data),
        update: (id, data, oldLocation) => this.updateUnlocked(id, data, oldLocation),
        delete: (location) => this.deleteUnlocked(location),
      });

      return result;
    } finally {
      this.batchDepth--;
      if (this.batchDepth === 0) {
        await this.flushMetadata();
      }
      release();
    }
  }

  /**
   * Fast path for inserting many small documents.
   * Writes all slots as one contiguous append write.
   *
   * Returns locations in the same order as the input.
   */
  async writeMany(items: Array<[id: string, data: Record<string, unknown>]>): Promise<DocumentLocation[]> {
    if (items.length === 0) return [];

    const release = await this.writeLock.acquire();
    try {
      // If any doc would be a blob, fall back to regular batch (keeps semantics correct).
      for (const [, data] of items) {
        const jsonBytes = encodeString(JSON.stringify(data));
        if (jsonBytes.length > this.blobThreshold) {
          const locations: DocumentLocation[] = [];
          this.batchDepth++;
          try {
            for (const [id, doc] of items) {
              locations.push(await this.writeUnlocked(id, doc));
            }
          } finally {
            this.batchDepth--;
            if (this.batchDepth === 0) {
              await this.flushMetadata();
            }
          }
          return locations;
        }
      }

      const startOffset = this.header!.nextSlotOffset;
      let currentOffset = startOffset;

      const slotBuffers: Uint8Array[] = [];
      const locations: DocumentLocation[] = [];
      let totalBytes = 0;
      let totalLiveData = 0;

      for (const [, data] of items) {
        const jsonBytes = encodeString(JSON.stringify(data));
        const slabSize = selectSlabSize(jsonBytes.length);
        const slotBuffer = new Uint8Array(slabSize);

        const checksum = crc32(jsonBytes);
        writeUint32(slotBuffer, 0, SlotFlags.ACTIVE);
        writeUint32(slotBuffer, 4, jsonBytes.length);
        writeUint32(slotBuffer, 8, slabSize);
        writeUint32(slotBuffer, 12, checksum);
        slotBuffer.set(jsonBytes, SLOT_HEADER_SIZE);

        slotBuffers.push(slotBuffer);
        locations.push({
          offset: currentOffset,
          length: jsonBytes.length,
          slabSize,
          isBlob: false,
        });

        currentOffset += slabSize;
        totalBytes += slabSize;
        totalLiveData += jsonBytes.length;
      }

      const fullBuffer = new Uint8Array(totalBytes);
      let pos = 0;
      for (const chunk of slotBuffers) {
        fullBuffer.set(chunk, pos);
        pos += chunk.length;
      }

      await this.writeAtOffset(startOffset, fullBuffer);

      this.header!.nextSlotOffset = startOffset + totalBytes;
      this.header!.fileSize = this.header!.nextSlotOffset;
      this.header!.documentCount += items.length;
      this.header!.liveDataSize += totalLiveData;

      this.markMetadataDirty();
      await this.flushIfNotBatching();

      return locations;
    } finally {
      release();
    }
  }

  /**
   * Truncate the data file and reset metadata/free list.
   * Keeps the file format header intact.
   */
  async reset(): Promise<void> {
    const release = await this.writeLock.acquire();
    try {
      if (!this.fileHandle) {
        throw new Error('StorageEngine not initialized');
      }

      await this.fileHandle.truncate(0);

      this.freeList.clear();
      this.header = {
        magic: DATA_FILE_MAGIC,
        version: FORMAT_VERSION,
        fileSize: DATA_HEADER_SIZE,
        liveDataSize: 0,
        documentCount: 0,
        nextSlotOffset: DATA_HEADER_SIZE,
      };

      await this.writeHeader();
      this.updateSharedState();
      this.headerDirty = false;
      this.sharedStateDirty = false;
    } finally {
      release();
    }
  }

  /**
   * Get current storage statistics
   */
  getStats(): { fileSize: number; liveDataSize: number; documentCount: number; freeSlots: number } {
    return {
      fileSize: this.header?.fileSize ?? 0,
      liveDataSize: this.header?.liveDataSize ?? 0,
      documentCount: this.header?.documentCount ?? 0,
      freeSlots: this.freeList.size,
    };
  }

  /**
   * Close the storage engine
   */
  async close(): Promise<void> {
    if (this.fileHandle !== null) {
      await this.fileHandle.close();
      this.fileHandle = null;
    }
  }

  // ============ Private Methods ============

  private async openFile(path: string, mode: string): Promise<FileHandle> {
    return open(path, mode as 'r+' | 'w+' | 'a+');
  }

  private async createNewFile(): Promise<void> {
    this.header = {
      magic: DATA_FILE_MAGIC,
      version: FORMAT_VERSION,
      fileSize: DATA_HEADER_SIZE,
      liveDataSize: 0,
      documentCount: 0,
      nextSlotOffset: DATA_HEADER_SIZE,
    };

    await this.writeHeader();
  }

  private async readHeader(): Promise<void> {
    if (!this.fileHandle) {
      throw new Error('StorageEngine not initialized');
    }

    const headerBytes = new Uint8Array(DATA_HEADER_SIZE);
    const { bytesRead } = await this.fileHandle.read(headerBytes, 0, DATA_HEADER_SIZE, 0);
    if (bytesRead < DATA_HEADER_SIZE) {
      throw new InvalidFileFormatError(this.dataPath, 'File too small to contain a valid header');
    }

    const magic = readUint32(headerBytes, 0);
    if (magic !== DATA_FILE_MAGIC) {
      throw new InvalidFileFormatError(
        this.dataPath,
        `Invalid magic number: expected 0x${DATA_FILE_MAGIC.toString(16)}, got 0x${magic.toString(16)}`
      );
    }

    const version = readUint32(headerBytes, 4);
    if (version !== FORMAT_VERSION) {
      throw new InvalidFileFormatError(this.dataPath, `Unsupported version: ${version}`);
    }

    this.header = {
      magic,
      version,
      fileSize: readUint64(headerBytes, 8),
      liveDataSize: readUint64(headerBytes, 16),
      documentCount: readUint64(headerBytes, 24),
      nextSlotOffset: readUint64(headerBytes, 32),
    };
  }

  private async writeHeader(): Promise<void> {
    if (!this.header) return;

    const headerBytes = new Uint8Array(DATA_HEADER_SIZE);
    writeUint32(headerBytes, 0, this.header.magic);
    writeUint32(headerBytes, 4, this.header.version);
    writeUint64(headerBytes, 8, this.header.fileSize);
    writeUint64(headerBytes, 16, this.header.liveDataSize);
    writeUint64(headerBytes, 24, this.header.documentCount);
    writeUint64(headerBytes, 32, this.header.nextSlotOffset);

    await this.writeAtOffset(0, headerBytes);
  }

  private async allocateSlot(dataLength: number): Promise<SlabAllocation> {
    const requiredSize = selectSlabSize(dataLength);

    // Check free list for matching or larger slot
    for (const freeSlot of this.freeList) {
      if (freeSlot.slabSize >= requiredSize) {
        this.freeList.delete(freeSlot);
        return {
          offset: freeSlot.offset,
          slabSize: freeSlot.slabSize,
          isReused: true,
        };
      }
    }

    // Allocate new slot at end of file
    const offset = this.header!.nextSlotOffset;
    return {
      offset,
      slabSize: requiredSize,
      isReused: false,
    };
  }

  private async writeSlot(
    offset: number,
    data: Uint8Array,
    slabSize: number,
    isBlob: boolean
  ): Promise<void> {
    const checksum = crc32(data);
    const flags = SlotFlags.ACTIVE | (isBlob ? SlotFlags.BLOB : 0);

    // Create slot buffer (header + data + padding)
    const slotBuffer = new Uint8Array(slabSize);

    // Write slot header (16 bytes)
    writeUint32(slotBuffer, 0, flags);
    writeUint32(slotBuffer, 4, data.length);
    writeUint32(slotBuffer, 8, slabSize);
    writeUint32(slotBuffer, 12, checksum);

    // Write data after header
    slotBuffer.set(data, SLOT_HEADER_SIZE);

    // Write to file at offset
    await this.writeAtOffset(offset, slotBuffer);
  }

  private async readSlot(offset: number, length: number): Promise<Uint8Array> {
    if (!this.fileHandle) {
      throw new Error('StorageEngine not initialized');
    }

    const slotBytes = new Uint8Array(SLOT_HEADER_SIZE + length);
    const { bytesRead } = await this.fileHandle.read(slotBytes, 0, slotBytes.length, offset);
    if (bytesRead < slotBytes.length) {
      throw new CorruptedDataError('Unexpected end of file while reading slot', offset);
    }

    // Read and validate header
    const flags = readUint32(slotBytes, 0);
    const dataLength = readUint32(slotBytes, 4);
    const storedChecksum = readUint32(slotBytes, 12);

    if (!(flags & SlotFlags.ACTIVE)) {
      throw new CorruptedDataError('Slot is not active', offset);
    }

    if (dataLength !== length) {
      throw new CorruptedDataError(`Data length mismatch: expected ${length}, got ${dataLength}`, offset);
    }

    // Extract and validate data
    const data = slotBytes.slice(SLOT_HEADER_SIZE, SLOT_HEADER_SIZE + length);
    const computedChecksum = crc32(data);

    if (computedChecksum !== storedChecksum) {
      throw new ChecksumMismatchError(storedChecksum, computedChecksum, offset);
    }

    return data;
  }

  private async markSlotFree(offset: number): Promise<void> {
    if (!this.fileHandle) {
      throw new Error('StorageEngine not initialized');
    }

    // Read current slot header
    const headerBytes = new Uint8Array(SLOT_HEADER_SIZE);
    const { bytesRead } = await this.fileHandle.read(headerBytes, 0, SLOT_HEADER_SIZE, offset);
    if (bytesRead < SLOT_HEADER_SIZE) {
      throw new CorruptedDataError('Unexpected end of file while reading slot header', offset);
    }

    // Clear ACTIVE flag
    const currentFlags = readUint32(headerBytes, 0);
    writeUint32(headerBytes, 0, currentFlags & ~SlotFlags.ACTIVE);

    // Write back just the flags (first 4 bytes)
    await this.writeAtOffset(offset, headerBytes.slice(0, 4));
  }

  private async writeAtOffset(offset: number, data: Uint8Array): Promise<void> {
    if (!this.fileHandle) {
      throw new Error('StorageEngine not initialized');
    }

    // FileHandle.write performs a positional write and extends the file if needed.
    await this.fileHandle.write(data, 0, data.length, offset);
  }

  private markMetadataDirty(): void {
    this.headerDirty = true;
    this.sharedStateDirty = true;
  }

  private async flushMetadata(): Promise<void> {
    if (this.headerDirty) {
      await this.writeHeader();
      this.headerDirty = false;
    }
    if (this.sharedStateDirty) {
      this.updateSharedState();
      this.sharedStateDirty = false;
    }
  }

  private async flushIfNotBatching(): Promise<void> {
    if (this.batchDepth === 0) {
      await this.flushMetadata();
    }
  }

  private async writeUnlocked(id: string, data: Record<string, unknown>): Promise<DocumentLocation> {
    const jsonString = JSON.stringify(data);
    const jsonBytes = encodeString(jsonString);

    // Check if document should go to blob store
    if (jsonBytes.length > this.blobThreshold) {
      return this.writeBlobForInsertUnlocked(id, jsonBytes);
    }

    // Allocate slot
    const allocation = await this.allocateSlot(jsonBytes.length);

    // Write slot
    await this.writeSlot(allocation.offset, jsonBytes, allocation.slabSize, false);

    // Update header
    if (!allocation.isReused) {
      this.header!.nextSlotOffset = allocation.offset + allocation.slabSize;
      this.header!.fileSize = this.header!.nextSlotOffset;
    }
    this.header!.documentCount++;
    this.header!.liveDataSize += jsonBytes.length;
    this.markMetadataDirty();
    await this.flushIfNotBatching();

    return {
      offset: allocation.offset,
      length: jsonBytes.length,
      slabSize: allocation.slabSize,
      isBlob: false,
    };
  }

  private async updateUnlocked(
    id: string,
    data: Record<string, unknown>,
    oldLocation: DocumentLocation
  ): Promise<DocumentLocation> {
    const jsonString = JSON.stringify(data);
    const jsonBytes = encodeString(jsonString);

    const shouldBeBlob = jsonBytes.length > this.blobThreshold;

    if (oldLocation.isBlob) {
      const oldBlobRef = await this.readBlobReference(oldLocation.offset, oldLocation.length);
      const oldBlobSize = oldBlobRef.size;

      if (shouldBeBlob) {
        // Blob -> blob: overwrite blob file + update reference (in place if it fits)
        const newLocation = await this.updateBlobReferenceUnlocked(id, jsonBytes, oldLocation);
        this.header!.liveDataSize += jsonBytes.length - oldBlobSize;
        this.markMetadataDirty();
        await this.flushIfNotBatching();
        return newLocation;
      }

      // Blob -> inline: delete blob file, free old ref slot, write inline
      await this.deleteBlob(oldBlobRef.path);
      await this.markSlotFree(oldLocation.offset);
      this.freeList.add({ offset: oldLocation.offset, slabSize: oldLocation.slabSize });

      const allocation = await this.allocateSlot(jsonBytes.length);
      await this.writeSlot(allocation.offset, jsonBytes, allocation.slabSize, false);

      if (!allocation.isReused) {
        this.header!.nextSlotOffset = allocation.offset + allocation.slabSize;
        this.header!.fileSize = this.header!.nextSlotOffset;
      }

      this.header!.liveDataSize += jsonBytes.length - oldBlobSize;
      this.markMetadataDirty();
      await this.flushIfNotBatching();

      return {
        offset: allocation.offset,
        length: jsonBytes.length,
        slabSize: allocation.slabSize,
        isBlob: false,
      };
    }

    if (shouldBeBlob) {
      // Inline -> blob: free old slot, write blob ref
      await this.markSlotFree(oldLocation.offset);
      this.freeList.add({ offset: oldLocation.offset, slabSize: oldLocation.slabSize });

      const newLocation = await this.writeBlobForUpdateUnlocked(id, jsonBytes);
      this.header!.liveDataSize += jsonBytes.length - oldLocation.length;
      this.markMetadataDirty();
      await this.flushIfNotBatching();
      return newLocation;
    }

    // Inline -> inline: check if fits in current slab
    if (jsonBytes.length + SLOT_HEADER_SIZE <= oldLocation.slabSize) {
      await this.writeSlot(oldLocation.offset, jsonBytes, oldLocation.slabSize, false);
      this.header!.liveDataSize += jsonBytes.length - oldLocation.length;
      this.markMetadataDirty();
      await this.flushIfNotBatching();

      return {
        offset: oldLocation.offset,
        length: jsonBytes.length,
        slabSize: oldLocation.slabSize,
        isBlob: false,
      };
    }

    // Relocate: free old slot, allocate new
    await this.markSlotFree(oldLocation.offset);
    this.freeList.add({ offset: oldLocation.offset, slabSize: oldLocation.slabSize });

    const allocation = await this.allocateSlot(jsonBytes.length);
    await this.writeSlot(allocation.offset, jsonBytes, allocation.slabSize, false);

    if (!allocation.isReused) {
      this.header!.nextSlotOffset = allocation.offset + allocation.slabSize;
      this.header!.fileSize = this.header!.nextSlotOffset;
    }
    this.header!.liveDataSize += jsonBytes.length - oldLocation.length;
    this.markMetadataDirty();
    await this.flushIfNotBatching();

    return {
      offset: allocation.offset,
      length: jsonBytes.length,
      slabSize: allocation.slabSize,
      isBlob: false,
    };
  }

  private async deleteUnlocked(location: DocumentLocation): Promise<void> {
    if (location.isBlob) {
      const blobRef = await this.readBlobReference(location.offset, location.length);
      await this.deleteBlob(blobRef.path);
      this.header!.liveDataSize -= blobRef.size;
    } else {
      this.header!.liveDataSize -= location.length;
    }

    await this.markSlotFree(location.offset);
    this.freeList.add({ offset: location.offset, slabSize: location.slabSize });

    this.header!.documentCount--;
    this.markMetadataDirty();
    await this.flushIfNotBatching();
  }

  private async writeBlobFile(path: string, data: Uint8Array): Promise<BlobReference> {
    const blobFilePath = `${this.blobPath}/${path}`;
    await Bun.write(blobFilePath, data);

    return {
      path,
      size: data.length,
      checksum: crc32(data),
    };
  }

  private async writeBlobReferenceSlot(ref: BlobReference): Promise<{ location: DocumentLocation; refBytes: Uint8Array }>
  {
    const refJson = JSON.stringify(ref);
    const refBytes = encodeString(refJson);

    const allocation = await this.allocateSlot(refBytes.length);
    await this.writeSlot(allocation.offset, refBytes, allocation.slabSize, true);

    if (!allocation.isReused) {
      this.header!.nextSlotOffset = allocation.offset + allocation.slabSize;
      this.header!.fileSize = this.header!.nextSlotOffset;
    }

    return {
      refBytes,
      location: {
        offset: allocation.offset,
        length: refBytes.length,
        slabSize: allocation.slabSize,
        isBlob: true,
      },
    };
  }

  private async writeBlobForInsertUnlocked(id: string, data: Uint8Array): Promise<DocumentLocation> {
    const blobRef = await this.writeBlobFile(`${id}.blob`, data);
    const { location } = await this.writeBlobReferenceSlot(blobRef);

    this.header!.documentCount++;
    this.header!.liveDataSize += blobRef.size;
    this.markMetadataDirty();
    await this.flushIfNotBatching();

    return location;
  }

  private async writeBlobForUpdateUnlocked(id: string, data: Uint8Array): Promise<DocumentLocation> {
    const blobRef = await this.writeBlobFile(`${id}.blob`, data);
    const { location } = await this.writeBlobReferenceSlot(blobRef);
    // Caller adjusts liveDataSize by delta and keeps documentCount unchanged.
    return location;
  }

  private async updateBlobReferenceUnlocked(
    id: string,
    data: Uint8Array,
    oldLocation: DocumentLocation
  ): Promise<DocumentLocation> {
    const blobRef = await this.writeBlobFile(`${id}.blob`, data);
    const refJson = JSON.stringify(blobRef);
    const refBytes = encodeString(refJson);

    // Update in-place if it fits, otherwise relocate the reference slot.
    if (refBytes.length + SLOT_HEADER_SIZE <= oldLocation.slabSize) {
      await this.writeSlot(oldLocation.offset, refBytes, oldLocation.slabSize, true);
      return {
        offset: oldLocation.offset,
        length: refBytes.length,
        slabSize: oldLocation.slabSize,
        isBlob: true,
      };
    }

    await this.markSlotFree(oldLocation.offset);
    this.freeList.add({ offset: oldLocation.offset, slabSize: oldLocation.slabSize });

    const allocation = await this.allocateSlot(refBytes.length);
    await this.writeSlot(allocation.offset, refBytes, allocation.slabSize, true);

    if (!allocation.isReused) {
      this.header!.nextSlotOffset = allocation.offset + allocation.slabSize;
      this.header!.fileSize = this.header!.nextSlotOffset;
    }

    return {
      offset: allocation.offset,
      length: refBytes.length,
      slabSize: allocation.slabSize,
      isBlob: true,
    };
  }

  private async readBlob(location: DocumentLocation): Promise<Record<string, unknown>> {
    const blobRef = await this.readBlobReference(location.offset, location.length);
    const blobPath = `${this.blobPath}/${blobRef.path}`;
    const blobFile = Bun.file(blobPath);
    const blobData = await blobFile.arrayBuffer();
    const blobBytes = new Uint8Array(blobData);

    // Validate checksum
    const computedChecksum = crc32(blobBytes);
    if (computedChecksum !== blobRef.checksum) {
      throw new ChecksumMismatchError(blobRef.checksum, computedChecksum);
    }

    const jsonString = decodeString(blobBytes);
    return JSON.parse(jsonString);
  }

  private async readBlobReference(offset: number, length: number): Promise<BlobReference> {
    const data = await this.readSlot(offset, length);
    const jsonString = decodeString(data);
    return JSON.parse(jsonString);
  }

  private async deleteBlob(path: string): Promise<void> {
    const blobFilePath = `${this.blobPath}/${path}`;
    const file = Bun.file(blobFilePath);
    if (await file.exists()) {
      await file.delete();
    }
  }

  private async rebuildFreeList(index: PrimaryIndex): Promise<void> {
    if (!this.header || !this.fileHandle) return;

    // Build set of occupied offsets
    const occupiedOffsets = new Set<number>();
    for (const location of index.values()) {
      occupiedOffsets.add(location.offset);
    }

    let offset = DATA_HEADER_SIZE;
    const headerBuf = new Uint8Array(SLOT_HEADER_SIZE);

    while (offset < this.header.nextSlotOffset) {
      const { bytesRead } = await this.fileHandle.read(headerBuf, 0, SLOT_HEADER_SIZE, offset);
      if (bytesRead < SLOT_HEADER_SIZE) break;

      const flags = readUint32(headerBuf, 0);
      const slabSize = readUint32(headerBuf, 8);
      if (slabSize === 0) break;

      // If the slot isn't active or the offset isn't in the primary index, treat it as free.
      if (!(flags & SlotFlags.ACTIVE) || !occupiedOffsets.has(offset)) {
        this.freeList.add({ offset, slabSize });
      }

      offset += slabSize;
    }
  }

  private calculatePackedSize(index: PrimaryIndex): number {
    let total = DATA_HEADER_SIZE;
    for (const location of index.values()) {
      total += location.slabSize;
    }
    return total;
  }

  private updateSharedState(): void {
    if (!this.sharedView || !this.header) return;

    Atomics.store(this.sharedView, SharedStateOffsets.FILE_SIZE / 4, this.header.fileSize);
    Atomics.store(this.sharedView, SharedStateOffsets.LIVE_DATA_SIZE / 4, this.header.liveDataSize);
    Atomics.store(this.sharedView, SharedStateOffsets.DOC_COUNT / 4, this.header.documentCount);
  }

  /**
   * Compact the storage (called by GC worker)
   * Returns bytes freed
   */
  async compact(index: PrimaryIndex): Promise<{
    bytesFreed: number;
    newLocations: Map<string, DocumentLocation>;
  }> {
    const release = await this.writeLock.acquire();
    try {
      const tempPath = `${this.dataPath}.tmp`;
      const newLocations = new Map<string, DocumentLocation>();
      let newOffset = DATA_HEADER_SIZE;

      // Create temp file with header
      const tempHeader = new Uint8Array(DATA_HEADER_SIZE);
      writeUint32(tempHeader, 0, DATA_FILE_MAGIC);
      writeUint32(tempHeader, 4, FORMAT_VERSION);

      // Build new file content
      const chunks: Uint8Array[] = [tempHeader];
      let totalLiveData = 0;

      for (const [id, location] of index) {
        // Read slot data (either inline doc JSON, or blob reference JSON)
        const data = await this.readSlot(location.offset, location.length);

        const newSlabSize = selectSlabSize(data.length);
        const slotBuffer = new Uint8Array(newSlabSize);
        const checksum = crc32(data);

        const flags = SlotFlags.ACTIVE | (location.isBlob ? SlotFlags.BLOB : 0);
        writeUint32(slotBuffer, 0, flags);
        writeUint32(slotBuffer, 4, data.length);
        writeUint32(slotBuffer, 8, newSlabSize);
        writeUint32(slotBuffer, 12, checksum);
        slotBuffer.set(data, SLOT_HEADER_SIZE);

        chunks.push(slotBuffer);

        newLocations.set(id, {
          offset: newOffset,
          length: data.length,
          slabSize: newSlabSize,
          isBlob: location.isBlob,
        });

        newOffset += newSlabSize;
        if (location.isBlob) {
          const blobRefJson = decodeString(data);
          const blobRef = JSON.parse(blobRefJson) as BlobReference;
          totalLiveData += blobRef.size;
        } else {
          totalLiveData += data.length;
        }
      }

      // Update header in first chunk
      writeUint64(tempHeader, 8, newOffset); // fileSize
      writeUint64(tempHeader, 16, totalLiveData); // liveDataSize
      writeUint64(tempHeader, 24, index.size); // documentCount
      writeUint64(tempHeader, 32, newOffset); // nextSlotOffset

      // Concatenate all chunks
      const totalSize = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
      const fullBuffer = new Uint8Array(totalSize);
      let pos = 0;
      for (const chunk of chunks) {
        fullBuffer.set(chunk, pos);
        pos += chunk.length;
      }

      // Write temp file
      await Bun.write(tempPath, fullBuffer);

      // Atomic swap using shell
      const oldSize = this.header!.fileSize;
      await Bun.$`mv ${tempPath} ${this.dataPath}`;

      // Reopen file handle (rename replaces the inode)
      if (this.fileHandle) {
        await this.fileHandle.close();
      }
      this.fileHandle = await this.openFile(this.dataPath, 'r+');

      // Update internal state
      this.header = {
        magic: DATA_FILE_MAGIC,
        version: FORMAT_VERSION,
        fileSize: newOffset,
        liveDataSize: totalLiveData,
        documentCount: index.size,
        nextSlotOffset: newOffset,
      };

      this.freeList.clear();
      this.updateSharedState();

      return {
        bytesFreed: oldSize - newOffset,
        newLocations,
      };
    } finally {
      release();
    }
  }
}
