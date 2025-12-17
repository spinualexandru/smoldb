/**
 * SmolDB Custom Errors
 */

/** Base error class for SmolDB */
export class SmolDBError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SmolDBError';
  }
}

/** Document not found error */
export class DocumentNotFoundError extends SmolDBError {
  constructor(public readonly id: string) {
    super(`Document not found: ${id}`);
    this.name = 'DocumentNotFoundError';
  }
}

/** Collection not found error */
export class CollectionNotFoundError extends SmolDBError {
  constructor(public readonly collectionName: string) {
    super(`Collection not found: ${collectionName}`);
    this.name = 'CollectionNotFoundError';
  }
}

/** Duplicate document ID error */
export class DuplicateIdError extends SmolDBError {
  constructor(public readonly id: string) {
    super(`Document with ID already exists: ${id}`);
    this.name = 'DuplicateIdError';
  }
}

/** Data corruption detected */
export class CorruptedDataError extends SmolDBError {
  constructor(
    message: string,
    public readonly offset?: number
  ) {
    super(`Data corruption detected: ${message}${offset !== undefined ? ` at offset ${offset}` : ''}`);
    this.name = 'CorruptedDataError';
  }
}

/** Index file corruption detected */
export class IndexCorruptedError extends SmolDBError {
  constructor(public readonly indexPath: string) {
    super(`Index file corrupted: ${indexPath}`);
    this.name = 'IndexCorruptedError';
  }
}

/** Invalid file format (wrong magic number or version) */
export class InvalidFileFormatError extends SmolDBError {
  constructor(
    public readonly filePath: string,
    public readonly reason: string
  ) {
    super(`Invalid file format for ${filePath}: ${reason}`);
    this.name = 'InvalidFileFormatError';
  }
}

/** Document too large for inline storage */
export class DocumentTooLargeError extends SmolDBError {
  constructor(
    public readonly id: string,
    public readonly size: number,
    public readonly maxSize: number
  ) {
    super(`Document ${id} is too large (${size} bytes, max ${maxSize} bytes)`);
    this.name = 'DocumentTooLargeError';
  }
}

/** Database not initialized */
export class NotInitializedError extends SmolDBError {
  constructor() {
    super('Database not initialized. Call init() first.');
    this.name = 'NotInitializedError';
  }
}

/** Checksum mismatch error */
export class ChecksumMismatchError extends SmolDBError {
  constructor(
    public readonly expected: number,
    public readonly actual: number,
    public readonly offset?: number
  ) {
    super(
      `Checksum mismatch: expected 0x${expected.toString(16)}, got 0x${actual.toString(16)}` +
        (offset !== undefined ? ` at offset ${offset}` : '')
    );
    this.name = 'ChecksumMismatchError';
  }
}
