/**
 * SmolDB - A file-based NoSQL database for Bun.js
 */

import { mkdir, readdir } from 'node:fs/promises';
import type { SmolDBOptions, CompactionResult } from './types';
import { SHARED_STATE_SIZE, SharedStateOffsets, GCCommand } from './types';
import { FILE_EXTENSIONS, BLOB_DIR, DEFAULT_GC_TRIGGER_RATIO } from './constants';
import { Collection } from './collection';
import { NotInitializedError } from './errors';

export class SmolDB {
  private collections = new Map<string, Collection>();
  private gcWorker: Worker | null = null;
  private sharedBuffer: SharedArrayBuffer;
  private sharedView: Int32Array;
  private initialized = false;
  private gcCheckInterval: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly basePath: string,
    private readonly options: SmolDBOptions = {}
  ) {
    this.sharedBuffer = new SharedArrayBuffer(SHARED_STATE_SIZE);
    this.sharedView = new Int32Array(this.sharedBuffer);
  }

  /**
   * Initialize the database
   * Creates directory structure and loads existing collections
   */
  async init(): Promise<void> {
    if (this.initialized) return;

    // Ensure base directory exists
    await mkdir(this.basePath, { recursive: true });
    await mkdir(`${this.basePath}/${BLOB_DIR}`, { recursive: true });

    // Scan for existing collections
    try {
      const files = await readdir(this.basePath);
      for (const file of files) {
        if (file.endsWith(FILE_EXTENSIONS.DATA)) {
          const name = file.slice(0, -FILE_EXTENSIONS.DATA.length);
          // Load collection lazily - just register it
          const coll = new Collection(this.basePath, name, this.sharedBuffer, this.options);
          this.collections.set(name, coll);
        }
      }
    } catch {
      // Directory might not exist yet, that's fine
    }

    // Start GC worker if enabled
    if (this.options.gcEnabled !== false) {
      await this.startGCWorker();
    }

    this.initialized = true;
  }

  /**
   * Get or create a collection
   */
  collection(name: string): Collection {
    this.ensureInitialized();

    let coll = this.collections.get(name);
    if (!coll) {
      coll = new Collection(this.basePath, name, this.sharedBuffer, this.options);
      this.collections.set(name, coll);
    }
    return coll;
  }

  /**
   * List all collection names
   */
  listCollections(): string[] {
    this.ensureInitialized();
    return Array.from(this.collections.keys());
  }

  /**
   * Drop a collection (delete all data)
   */
  async dropCollection(name: string): Promise<boolean> {
    this.ensureInitialized();

    const coll = this.collections.get(name);
    if (!coll) return false;

    await coll.close();

    // Delete collection files
    const dataPath = `${this.basePath}/${name}${FILE_EXTENSIONS.DATA}`;
    const indexPath = `${this.basePath}/${name}${FILE_EXTENSIONS.INDEX}`;
    const blobPath = `${this.basePath}/${BLOB_DIR}/${name}`;

    try {
      await Bun.file(dataPath).delete();
    } catch {}

    try {
      await Bun.file(indexPath).delete();
    } catch {}

    // Delete blob directory
    try {
      const blobFiles = await readdir(blobPath);
      for (const file of blobFiles) {
        await Bun.file(`${blobPath}/${file}`).delete();
      }
      // Note: Bun doesn't have rmdir, but directory will be empty
    } catch {}

    this.collections.delete(name);
    return true;
  }

  /**
   * Compact all collections (reclaim deleted space)
   */
  async compact(): Promise<Map<string, CompactionResult>> {
    this.ensureInitialized();

    const results = new Map<string, CompactionResult>();
    for (const [name, coll] of this.collections) {
      const result = await coll.compact();
      results.set(name, result);
    }
    return results;
  }

  /**
   * Trigger background GC for all collections
   */
  triggerGC(): void {
    this.ensureInitialized();

    if (this.gcWorker) {
      Atomics.store(this.sharedView, SharedStateOffsets.COMMAND / 4, GCCommand.TRIGGER_GC);
      Atomics.notify(this.sharedView, SharedStateOffsets.COMMAND / 4);
    }
  }

  /**
   * Get GC status
   */
  getGCStatus(): { status: number; progress: number; lastBytesFreed: number } {
    return {
      status: Atomics.load(this.sharedView, SharedStateOffsets.GC_STATUS / 4),
      progress: Atomics.load(this.sharedView, SharedStateOffsets.GC_PROGRESS / 4),
      lastBytesFreed: Atomics.load(this.sharedView, SharedStateOffsets.GC_BYTES_FREED / 4),
    };
  }

  /**
   * Persist all indexes to disk
   */
  async persistAllIndexes(): Promise<void> {
    this.ensureInitialized();

    for (const coll of this.collections.values()) {
      await coll.persistIndex();
    }
  }

  /**
   * Get database statistics
   */
  async getStats(): Promise<{
    collections: number;
    totalDocuments: number;
    totalFileSize: number;
    totalLiveDataSize: number;
  }> {
    this.ensureInitialized();

    let totalDocuments = 0;
    let totalFileSize = 0;
    let totalLiveDataSize = 0;

    for (const coll of this.collections.values()) {
      const stats = await coll.getStats();
      totalDocuments += stats.documentCount;
      totalFileSize += stats.fileSize;
      totalLiveDataSize += stats.liveDataSize;
    }

    return {
      collections: this.collections.size,
      totalDocuments,
      totalFileSize,
      totalLiveDataSize,
    };
  }

  /**
   * Close the database
   */
  async close(): Promise<void> {
    if (!this.initialized) return;

    // Stop GC interval
    if (this.gcCheckInterval) {
      clearInterval(this.gcCheckInterval);
      this.gcCheckInterval = null;
    }

    // Signal GC worker to shutdown
    if (this.gcWorker) {
      Atomics.store(this.sharedView, SharedStateOffsets.COMMAND / 4, GCCommand.SHUTDOWN);
      Atomics.notify(this.sharedView, SharedStateOffsets.COMMAND / 4);
      this.gcWorker.terminate();
      this.gcWorker = null;
    }

    // Close all collections
    for (const coll of this.collections.values()) {
      await coll.close();
    }

    this.collections.clear();
    this.initialized = false;
  }

  // ============ Private Methods ============

  private ensureInitialized(): void {
    if (!this.initialized) {
      throw new NotInitializedError();
    }
  }

  private async startGCWorker(): Promise<void> {
    // Try to start the GC worker
    try {
      const workerPath = new URL('./gc-worker.ts', import.meta.url).pathname;
      this.gcWorker = new Worker(workerPath);

      this.gcWorker.onmessage = (event) => {
        const { type, error } = event.data;
        if (type === 'error') {
          console.error('[SmolDB GC Worker Error]', error);
        }
      };

      this.gcWorker.onerror = (error) => {
        console.error('[SmolDB GC Worker Error]', error);
      };

      // Send init message
      this.gcWorker.postMessage({
        type: 'init',
        basePath: this.basePath,
        sharedBuffer: this.sharedBuffer,
        gcTriggerRatio: this.options.gcTriggerRatio ?? DEFAULT_GC_TRIGGER_RATIO,
      });

      // Start periodic GC check
      const triggerRatio = this.options.gcTriggerRatio ?? DEFAULT_GC_TRIGGER_RATIO;
      this.gcCheckInterval = setInterval(() => {
        this.checkAndTriggerGC(triggerRatio);
      }, 60000); // Check every minute
    } catch {
      // Worker failed to start, GC will be manual only
      console.warn('[SmolDB] GC worker failed to start, manual compaction only');
    }
  }

  private checkAndTriggerGC(triggerRatio: number): void {
    const fileSize = Atomics.load(this.sharedView, SharedStateOffsets.FILE_SIZE / 4);
    const liveDataSize = Atomics.load(this.sharedView, SharedStateOffsets.LIVE_DATA_SIZE / 4);
    const gcStatus = Atomics.load(this.sharedView, SharedStateOffsets.GC_STATUS / 4);

    // Only trigger if not already running and ratio exceeded
    if (gcStatus === 0 && liveDataSize > 0 && fileSize / liveDataSize > triggerRatio) {
      this.triggerGC();
    }
  }
}
