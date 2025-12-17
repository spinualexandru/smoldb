/**
 * SmolDB - A file-based NoSQL database for Bun.js
 *
 * Features:
 * - Slab-based storage with O(1) document access
 * - Secondary indexes for field queries
 * - Background garbage collection
 * - Automatic blob storage for large documents
 *
 * @example
 * ```typescript
 * import { SmolDB } from 'smoldb';
 *
 * const db = new SmolDB('./data');
 * await db.init();
 *
 * const users = db.collection('users');
 * await users.insert('user_1', { name: 'Alice', role: 'admin' });
 * await users.createIndex('role');
 *
 * const admins = await users.find({ role: 'admin' });
 * console.log(admins);
 *
 * await db.close();
 * ```
 */

// Main classes
export { SmolDB } from './src/smoldb';
export { Collection } from './src/collection';

// Types
export type {
  Document,
  QueryFilter,
  SmolDBOptions,
  CollectionOptions,
  CompactionResult,
  DocumentLocation,
} from './src/types';

// Errors
export {
  SmolDBError,
  DocumentNotFoundError,
  DuplicateIdError,
  CollectionNotFoundError,
  CorruptedDataError,
  IndexCorruptedError,
  InvalidFileFormatError,
  DocumentTooLargeError,
  NotInitializedError,
  ChecksumMismatchError,
} from './src/errors';

// Constants (for advanced usage)
export { SLAB_SIZES, DEFAULT_BLOB_THRESHOLD, DEFAULT_GC_TRIGGER_RATIO } from './src/constants';
