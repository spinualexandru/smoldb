/**
 * SmolDB Type Definitions
 */

/** Document record location in the data file */
export interface DocumentLocation {
  /** Byte offset in data file */
  offset: number;
  /** Actual data length in bytes */
  length: number;
  /** Allocated slab size in bytes */
  slabSize: number;
  /** Whether stored in blob store (external file) */
  isBlob: boolean;
}

/** Slab allocation result */
export interface SlabAllocation {
  /** Byte offset where slot starts */
  offset: number;
  /** Total slab size allocated */
  slabSize: number;
  /** Whether this slot was reused from free list */
  isReused: boolean;
}

/** Free slot entry for reuse */
export interface FreeSlot {
  /** Byte offset of free slot */
  offset: number;
  /** Size of free slot */
  slabSize: number;
}

/** Data file header structure */
export interface DataFileHeader {
  /** Magic number for validation */
  magic: number;
  /** Format version */
  version: number;
  /** Total file size in bytes */
  fileSize: number;
  /** Size of live (non-deleted) data */
  liveDataSize: number;
  /** Number of documents */
  documentCount: number;
  /** Offset where next slot will be written */
  nextSlotOffset: number;
}

/** Slot header structure (16 bytes) */
export interface SlotHeader {
  /** Status flags (bit 0 = active, bit 1 = blob) */
  flags: number;
  /** Actual data length */
  dataLength: number;
  /** Allocated slab size */
  slabSize: number;
  /** CRC32 checksum of data */
  checksum: number;
}

/** Slot status flags */
export const SlotFlags = {
  /** Slot contains valid data */
  ACTIVE: 1 << 0,
  /** Slot contains blob reference (not inline JSON) */
  BLOB: 1 << 1,
} as const;

/** Collection options */
export interface CollectionOptions {
  /** Fields to index on creation */
  indexes?: string[];
}

/** Database options */
export interface SmolDBOptions {
  /** Enable background GC (default: true) */
  gcEnabled?: boolean;
  /** File size / live data ratio to trigger GC (default: 2.0) */
  gcTriggerRatio?: number;
  /** Size threshold in bytes for blob storage (default: 1MB) */
  blobThreshold?: number;
  /**
   * Optional in-memory document cache size (number of documents).
   * When > 0, SmolDB caches recently read/inserted documents per collection.
   */
  cacheSize?: number;
}

/** Query filter for find() */
export type QueryFilter = Record<string, unknown>;

/** Document type */
export type Document = Record<string, unknown>;

/** Primary index: Map<documentId, DocumentLocation> */
export type PrimaryIndex = Map<string, DocumentLocation>;

/** Secondary index: Map<fieldPath, Map<serializedValue, Set<documentId>>> */
export type SecondaryIndex = Map<string, Map<string, Set<string>>>;

/** Blob reference stored in data slot when document is in blob store */
export interface BlobReference {
  /** Relative path to blob file */
  path: string;
  /** Original document size */
  size: number;
  /** CRC32 checksum of blob content */
  checksum: number;
}

/** SharedArrayBuffer state offsets for GC worker communication */
export const SharedStateOffsets = {
  /** Current file size (uint32, bytes 0-3) */
  FILE_SIZE: 0,
  /** Live data size (uint32, bytes 4-7) */
  LIVE_DATA_SIZE: 4,
  /** Document count (uint32, bytes 8-11) */
  DOC_COUNT: 8,
  /** GC status: 0=idle, 1=running, 2=complete (uint32, bytes 12-15) */
  GC_STATUS: 12,
  /** GC progress 0-100 (uint32, bytes 16-19) */
  GC_PROGRESS: 16,
  /** Bytes freed in last run (uint32, bytes 20-23) */
  GC_BYTES_FREED: 20,
  /** Spinlock for synchronization (uint32, bytes 24-27) */
  LOCK: 24,
  /** Command: 0=none, 1=trigger_gc, 2=shutdown (uint32, bytes 28-31) */
  COMMAND: 28,
} as const;

/** GC command values */
export const GCCommand = {
  NONE: 0,
  TRIGGER_GC: 1,
  SHUTDOWN: 2,
} as const;

/** GC status values */
export const GCStatus = {
  IDLE: 0,
  RUNNING: 1,
  COMPLETE: 2,
} as const;

/** Size of SharedArrayBuffer for GC communication */
export const SHARED_STATE_SIZE = 64;

/** Compaction result */
export interface CompactionResult {
  /** Bytes freed by compaction */
  bytesFreed: number;
  /** Documents compacted */
  documentsCompacted: number;
  /** New file size */
  newFileSize: number;
}

/** GC worker initialization message */
export interface GCWorkerInitMessage {
  type: 'init';
  basePath: string;
  collectionName: string;
  sharedBuffer: SharedArrayBuffer;
}

/** GC worker message types */
export type GCWorkerMessage = GCWorkerInitMessage;
