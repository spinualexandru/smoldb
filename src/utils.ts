/**
 * SmolDB Utility Functions
 */

/** Pre-computed CRC32 lookup table */
const CRC32_TABLE = new Uint32Array(256);

// Initialize CRC32 table
for (let i = 0; i < 256; i++) {
  let crc = i;
  for (let j = 0; j < 8; j++) {
    crc = crc & 1 ? (crc >>> 1) ^ 0xedb88320 : crc >>> 1;
  }
  CRC32_TABLE[i] = crc >>> 0;
}

/**
 * Calculate CRC32 checksum of a buffer
 */
export function crc32(data: Uint8Array): number {
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) {
    crc = CRC32_TABLE[(crc ^ data[i]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

/**
 * Encode a string to UTF-8 bytes
 */
export function encodeString(str: string): Uint8Array {
  return TEXT_ENCODER.encode(str);
}

/**
 * Decode UTF-8 bytes to a string
 */
export function decodeString(bytes: Uint8Array): string {
  return TEXT_DECODER.decode(bytes);
}

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

/**
 * Write a uint32 to a buffer at offset (little-endian)
 */
export function writeUint32(buffer: Uint8Array, offset: number, value: number): void {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 4);
  view.setUint32(0, value, true);
}

/**
 * Read a uint32 from a buffer at offset (little-endian)
 */
export function readUint32(buffer: Uint8Array, offset: number): number {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 4);
  return view.getUint32(0, true);
}

/**
 * Write a uint64 to a buffer at offset (little-endian)
 * Uses BigInt for 64-bit support
 */
export function writeUint64(buffer: Uint8Array, offset: number, value: number): void {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 8);
  view.setBigUint64(0, BigInt(value), true);
}

/**
 * Read a uint64 from a buffer at offset (little-endian)
 * Returns as number (safe for values up to 2^53)
 */
export function readUint64(buffer: Uint8Array, offset: number): number {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 8);
  return Number(view.getBigUint64(0, true));
}

/**
 * Write a uint16 to a buffer at offset (little-endian)
 */
export function writeUint16(buffer: Uint8Array, offset: number, value: number): void {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 2);
  view.setUint16(0, value, true);
}

/**
 * Read a uint16 from a buffer at offset (little-endian)
 */
export function readUint16(buffer: Uint8Array, offset: number): number {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 2);
  return view.getUint16(0, true);
}

/**
 * Serialize a value for secondary index storage
 * Returns a string representation that can be used as a Map key
 */
export function serializeIndexValue(value: unknown): string {
  if (value === null) return '\x00null';
  if (value === undefined) return '\x00undefined';
  if (typeof value === 'boolean') return `\x01${value ? '1' : '0'}`;
  if (typeof value === 'number') {
    // Use a format that preserves numeric ordering
    if (Number.isNaN(value)) return '\x02nan';
    if (!Number.isFinite(value)) return value > 0 ? '\x02+inf' : '\x02-inf';
    // Pad with zeros for consistent ordering
    const sign = value >= 0 ? '+' : '-';
    const abs = Math.abs(value);
    return `\x02${sign}${abs.toExponential(15)}`;
  }
  if (typeof value === 'string') return `\x03${value}`;
  // For objects/arrays, use JSON (not ideal for ordering, but works for equality)
  return `\x04${JSON.stringify(value)}`;
}

/**
 * Deserialize a value from secondary index storage
 */
export function deserializeIndexValue(serialized: string): unknown {
  const type = serialized.charCodeAt(0);
  const data = serialized.slice(1);

  switch (type) {
    case 0: // null/undefined
      return data === 'null' ? null : undefined;
    case 1: // boolean
      return data === '1';
    case 2: // number
      if (data === 'nan') return NaN;
      if (data === '+inf') return Infinity;
      if (data === '-inf') return -Infinity;
      return parseFloat(data.slice(1)); // Skip sign prefix
    case 3: // string
      return data;
    case 4: // object/array
      return JSON.parse(data);
    default:
      return data;
  }
}

/**
 * Get a nested value from an object using dot notation
 * e.g., getNestedValue({ user: { name: 'Alice' } }, 'user.name') => 'Alice'
 */
export function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.');
  let current: unknown = obj;

  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    if (typeof current !== 'object') return undefined;
    current = (current as Record<string, unknown>)[part];
  }

  return current;
}

/**
 * Check if a document matches a query filter
 */
export function matchesFilter(doc: Record<string, unknown>, filter: Record<string, unknown>): boolean {
  for (const [key, value] of Object.entries(filter)) {
    const docValue = getNestedValue(doc, key);

    // Deep equality check
    if (!deepEqual(docValue, value)) {
      return false;
    }
  }
  return true;
}

/**
 * Deep equality check for two values
 */
export function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || b === null) return false;
  if (typeof a !== typeof b) return false;

  if (typeof a === 'object') {
    if (Array.isArray(a) !== Array.isArray(b)) return false;

    if (Array.isArray(a) && Array.isArray(b)) {
      if (a.length !== b.length) return false;
      for (let i = 0; i < a.length; i++) {
        if (!deepEqual(a[i], b[i])) return false;
      }
      return true;
    }

    const aObj = a as Record<string, unknown>;
    const bObj = b as Record<string, unknown>;
    const aKeys = Object.keys(aObj);
    const bKeys = Object.keys(bObj);

    if (aKeys.length !== bKeys.length) return false;

    for (const key of aKeys) {
      if (!Object.prototype.hasOwnProperty.call(bObj, key)) return false;
      if (!deepEqual(aObj[key], bObj[key])) return false;
    }
    return true;
  }

  return false;
}

/**
 * Simple async mutex for write locking
 */
export class AsyncMutex {
  private locked = false;
  private queue: Array<() => void> = [];

  async acquire(): Promise<() => void> {
    return new Promise((resolve) => {
      const tryAcquire = () => {
        if (!this.locked) {
          this.locked = true;
          resolve(() => this.release());
        } else {
          this.queue.push(tryAcquire);
        }
      };
      tryAcquire();
    });
  }

  private release(): void {
    this.locked = false;
    const next = this.queue.shift();
    if (next) next();
  }
}
