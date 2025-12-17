/**
 * SmolDB Constants
 */

/** Slab bucket sizes in bytes */
export const SLAB_SIZES = {
  /** 1KB - small documents */
  TINY: 1024,
  /** 8KB - medium documents */
  SMALL: 8192,
  /** 64KB - larger documents */
  MEDIUM: 65536,
  /** 4KB alignment for huge documents */
  ALIGNMENT: 4096,
} as const;

/** Slot header size in bytes */
export const SLOT_HEADER_SIZE = 16;

/** Data file header size in bytes */
export const DATA_HEADER_SIZE = 64;

/** Index file header size in bytes */
export const INDEX_HEADER_SIZE = 64;

/** Threshold for blob storage (1MB) */
export const DEFAULT_BLOB_THRESHOLD = 1024 * 1024;

/** Default GC trigger ratio (file size / live data) */
export const DEFAULT_GC_TRIGGER_RATIO = 2.0;

/** Data file magic number: "SMOL" in ASCII (little-endian) */
export const DATA_FILE_MAGIC = 0x4c4f4d53;

/** Index file magic number: "SIDX" in ASCII (little-endian) */
export const INDEX_FILE_MAGIC = 0x58444953;

/** Current format version */
export const FORMAT_VERSION = 1;

/** File extensions */
export const FILE_EXTENSIONS = {
  DATA: '.data',
  INDEX: '.idx',
  TEMP: '.tmp',
  LOCK: '.lock',
} as const;

/** Blob directory name */
export const BLOB_DIR = 'blobs';

/**
 * Select the appropriate slab size for a given data length
 */
export function selectSlabSize(dataLength: number): number {
  const totalNeeded = dataLength + SLOT_HEADER_SIZE;

  if (totalNeeded <= SLAB_SIZES.TINY) return SLAB_SIZES.TINY;
  if (totalNeeded <= SLAB_SIZES.SMALL) return SLAB_SIZES.SMALL;
  if (totalNeeded <= SLAB_SIZES.MEDIUM) return SLAB_SIZES.MEDIUM;

  // For huge documents, align to 4KB boundary
  return Math.ceil(totalNeeded / SLAB_SIZES.ALIGNMENT) * SLAB_SIZES.ALIGNMENT;
}
