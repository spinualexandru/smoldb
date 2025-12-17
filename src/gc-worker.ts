/**
 * SmolDB Garbage Collection Worker
 * Runs in a separate thread to compact collections without blocking main thread
 */

import { readdir } from 'node:fs/promises';
import { SharedStateOffsets, GCCommand, GCStatus } from './types';
import { FILE_EXTENSIONS, DEFAULT_GC_TRIGGER_RATIO } from './constants';
import { Collection } from './collection';

interface WorkerState {
  basePath: string;
  sharedBuffer: SharedArrayBuffer;
  sharedView: Int32Array;
  gcTriggerRatio: number;
  running: boolean;
}

let state: WorkerState | null = null;

/**
 * Initialize the worker with configuration
 */
function handleInit(data: {
  basePath: string;
  sharedBuffer: SharedArrayBuffer;
  gcTriggerRatio?: number;
}): void {
  state = {
    basePath: data.basePath,
    sharedBuffer: data.sharedBuffer,
    sharedView: new Int32Array(data.sharedBuffer),
    gcTriggerRatio: data.gcTriggerRatio ?? DEFAULT_GC_TRIGGER_RATIO,
    running: true,
  };

  // Start the worker loop
  workerLoop();
}

/**
 * Main worker loop - waits for commands
 */
async function workerLoop(): Promise<void> {
  if (!state) return;

  while (state.running) {
    // Wait for a command
    const result = Atomics.wait(
      state.sharedView,
      SharedStateOffsets.COMMAND / 4,
      GCCommand.NONE,
      5000 // Timeout after 5 seconds to check for auto-trigger
    );

    if (!state.running) break;

    const command = Atomics.load(state.sharedView, SharedStateOffsets.COMMAND / 4);

    switch (command) {
      case GCCommand.TRIGGER_GC:
        await runCompaction();
        // Reset command to none
        Atomics.store(state.sharedView, SharedStateOffsets.COMMAND / 4, GCCommand.NONE);
        break;

      case GCCommand.SHUTDOWN:
        state.running = false;
        break;

      default:
        // Check if auto-trigger conditions are met
        await checkAutoTrigger();
        break;
    }
  }
}

/**
 * Check if automatic GC should be triggered
 */
async function checkAutoTrigger(): Promise<void> {
  if (!state) return;

  const fileSize = Atomics.load(state.sharedView, SharedStateOffsets.FILE_SIZE / 4);
  const liveDataSize = Atomics.load(state.sharedView, SharedStateOffsets.LIVE_DATA_SIZE / 4);
  const gcStatus = Atomics.load(state.sharedView, SharedStateOffsets.GC_STATUS / 4);

  // Only trigger if not already running and ratio exceeded
  if (gcStatus === GCStatus.IDLE && liveDataSize > 0 && fileSize / liveDataSize > state.gcTriggerRatio) {
    await runCompaction();
  }
}

/**
 * Run compaction on all collections
 */
async function runCompaction(): Promise<void> {
  if (!state) return;

  // Set status to running
  Atomics.store(state.sharedView, SharedStateOffsets.GC_STATUS / 4, GCStatus.RUNNING);
  Atomics.store(state.sharedView, SharedStateOffsets.GC_PROGRESS / 4, 0);

  let totalBytesFreed = 0;

  try {
    // Find all collections
    const files = await readdir(state.basePath);
    const collections: string[] = [];

    for (const file of files) {
      if (file.endsWith(FILE_EXTENSIONS.DATA)) {
        collections.push(file.slice(0, -FILE_EXTENSIONS.DATA.length));
      }
    }

    if (collections.length === 0) {
      Atomics.store(state.sharedView, SharedStateOffsets.GC_STATUS / 4, GCStatus.COMPLETE);
      return;
    }

    // Compact each collection
    for (let i = 0; i < collections.length; i++) {
      const name = collections[i];

      try {
        // Create a temporary collection instance for compaction
        const coll = new Collection(state.basePath, name);
        const result = await coll.compact();
        totalBytesFreed += result.bytesFreed;
        await coll.close();
      } catch (error) {
        // Log error but continue with other collections
        self.postMessage({
          type: 'error',
          error: `Failed to compact collection ${name}: ${error}`,
        });
      }

      // Update progress
      const progress = Math.floor(((i + 1) / collections.length) * 100);
      Atomics.store(state.sharedView, SharedStateOffsets.GC_PROGRESS / 4, progress);
    }

    // Update bytes freed
    Atomics.store(state.sharedView, SharedStateOffsets.GC_BYTES_FREED / 4, totalBytesFreed);

    self.postMessage({
      type: 'complete',
      bytesFreed: totalBytesFreed,
      collectionsCompacted: collections.length,
    });
  } catch (error) {
    self.postMessage({
      type: 'error',
      error: `Compaction failed: ${error}`,
    });
  } finally {
    // Set status to complete (or idle)
    Atomics.store(state.sharedView, SharedStateOffsets.GC_STATUS / 4, GCStatus.IDLE);
    Atomics.store(state.sharedView, SharedStateOffsets.GC_PROGRESS / 4, 100);
  }
}

// Handle messages from main thread
self.onmessage = (event: MessageEvent) => {
  const { type, ...data } = event.data;

  switch (type) {
    case 'init':
      handleInit(data);
      break;

    default:
      self.postMessage({
        type: 'error',
        error: `Unknown message type: ${type}`,
      });
  }
};

// Handle worker errors
self.onerror = (error) => {
  console.error('[GC Worker Error]', error);
};
