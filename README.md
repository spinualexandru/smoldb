# SmolDB

A file-based NoSQL database for Bun.js with slab allocation, secondary indexes, and background garbage collection.

## Installation

```bash
bun install
```

## Quick Start

```typescript
import { SmolDB } from 'smoldb';

const db = new SmolDB('./data');
await db.init();

// Get a collection
const users = db.collection('users');

// CRUD operations
await users.insert('user_1', { name: 'Alice', role: 'admin' });
const user = await users.get('user_1');
await users.update('user_1', { name: 'Alice', role: 'superadmin' });
await users.delete('user_1');

// Secondary indexes for fast queries
await users.createIndex('role');
const admins = await users.find({ role: 'admin' });

// Cleanup
await db.close();
```

## Features

- **Slab-based storage**: Documents stored in size-bucketed slots (1KB, 8KB, 64KB)
- **O(1) document access**: Direct offset reads via persisted index
- **Secondary indexes**: Query by field values without full scans
- **In-place updates**: Small updates don't relocate documents
- **Batch writes & bulk insert**: Run many mutations under one write lock (`batch`) or append many small docs in one write (`insertMany`)
- **Optional in-memory cache**: LRU-ish per-collection cache for many-small-doc workloads (`cacheSize`)
- **Background GC**: Worker thread compaction reclaims deleted space
- **Blob storage**: Large documents (>1MB) automatically stored separately
- **Nested field queries**: Index and query `user.profile.country`
- **Index-only queries**: `findIds` and `count(filter)` can avoid reading documents when fully covered by indexes

## When to Use SmolDB

SmolDB is designed for specific use cases. Choose it when your application matches these patterns:

### Good Use Cases

| Use Case | Why SmolDB Works |
|----------|------------------|
| **Embedded databases** | No external server needed, just files |
| **Prototyping & MVPs** | Simple API, no schema setup required |
| **Desktop/Electron apps** | File-based storage, works offline |
| **CLI tools** | Lightweight, no daemon processes |
| **Configuration stores** | Persistent key-value with querying |
| **Session storage** | Fast lookups by session ID |
| **Caching layer** | Persist cache to disk between restarts |
| **Log aggregation** | Append-heavy workloads with periodic compaction |
| **Single-user apps** | No concurrent write conflicts |
| **Development/testing** | Easy to inspect, reset, and version control |

### When NOT to Use SmolDB

| Scenario | Better Alternative |
|----------|-------------------|
| **Concurrent writers** | Use a proper database with MVCC/locking |
| **Large datasets (>1M docs)** | Use SQLite, MongoDB, or PostgreSQL |
| **Complex queries** | Use SQL databases with proper query planners |
| **ACID transactions** | Use SQLite or PostgreSQL |
| **Real-time sync** | Use Firebase, Supabase, or CRDTs |
| **Multi-process access** | Use client-server databases |
| **Production web APIs** | Use PostgreSQL, MySQL, or MongoDB |

## Benchmarks & Comparisons

Benchmarks live under `benchmarks/` and write results to `benchmarks/results/*.json`.

The numbers below are from the latest committed runs on Linux x64 with Bun 1.3.4 (timestamp: 2025-12-17). Treat them as directional; they vary by SSD, filesystem, CPU governor, and dataset shape.

### Versus LowDB (write-through workloads)

SmolDB is faster on write-through operations (each op persists to disk) and on indexed counting. LowDB can still win on “bulk insert then flush once” patterns.

| Operation (10k-doc dataset where applicable) | SmolDB vs LowDB |
|---|---:|
| Write-through insert | ~4.81× faster |
| Write-through update | ~6.30× faster |
| Write-through delete | ~6.02× faster |
| Query: `count()` fully covered by indexes | ~1.69× faster |
| Cold start: load + single read (10,000 docs) | ~1.39× faster |
| Persist indexes / DB state | ~509× faster |

### Versus vanilla JSON file storage

The JSON baseline is “read whole file → JSON.parse → modify → JSON.stringify → write whole file”. SmolDB avoids whole-file rewrites by writing fixed-size slots and maintaining a binary index.

| Operation (10k-doc dataset where applicable) | SmolDB vs JSON |
|---|---:|
| Write-through insert | ~1.39× faster |
| Write-through update | ~1.88× faster |
| Query: indexed `count()` vs JSON scan | ~2.04× faster |
| Cold start: load + single read (10,000 docs) | ~1.45× faster |

### Interpreting the “Batch Insert” numbers

Some baselines can bulk-load into memory and flush once, so their “batch insert” can be extremely fast.

SmolDB provides two bulk paths:
- `collection.insertMany()` for fast append of many small documents (single contiguous write)
- `collection.batch()` to group mixed inserts/updates/deletes under a single storage lock and flush metadata once

If your workload is “build everything in memory then write once”, a plain JSON write will often win.

### The Sweet Spot

SmolDB excels when you have:
- **Hundreds to tens of thousands of documents** (not millions)
- **Many small documents** and a mix of reads/writes
- **Need for secondary indexes** without a full database
- **Single-process access** (CLI tools, desktop apps, scripts)
- **Preference for simplicity** over a full SQL engine

### Honest Limitations

1. **No multi-operation transactions**: `batch()` reduces overhead, but it is not an ACID transaction API.
2. **Single-process semantics**: No cross-process locking; don’t point multiple processes at the same DB.
3. **Index/query model**: Secondary indexes are equality-only (no ranges/ordering/joins).
4. **Memory usage**: Primary + secondary indexes are in RAM (fine for typical embedded workloads; not ideal for huge cardinality).
5. **Document reads are real I/O**: `find()` must read matching documents; use `findIds()` / `count(filter)` when you only need IDs/counts.

## API Reference

### SmolDB

```typescript
const db = new SmolDB(path: string, options?: SmolDBOptions);

interface SmolDBOptions {
  gcEnabled?: boolean;      // Enable background GC (default: true)
  gcTriggerRatio?: number;  // Trigger GC when fileSize/liveData > ratio (default: 2.0)
  blobThreshold?: number;   // Store docs larger than this as blobs (default: 1MB)
  cacheSize?: number;       // Optional per-collection document cache size (default: 0)
}

await db.init();                    // Initialize database
db.collection(name: string);        // Get or create collection
db.listCollections();               // List collection names
await db.dropCollection(name);      // Delete a collection
await db.compact();                 // Compact all collections
await db.persistAllIndexes();       // Save all indexes to disk
await db.getStats();                // Get database statistics
await db.close();                   // Close database
```

### Collection

```typescript
const coll = db.collection('users');

// CRUD
await coll.insert(id, document);    // Insert (throws if exists)
await coll.get(id);                 // Get by ID (null if not found)
await coll.update(id, document);    // Update (throws if not found)
await coll.upsert(id, document);    // Insert or update
await coll.delete(id);              // Delete (returns boolean)
await coll.has(id);                 // Check existence

// Querying
await coll.find({ field: value });  // Find by field (uses index if available)
await coll.findOne({ field: value });
await coll.findIds({ field: value }); // Return matching IDs (index-only when fully indexed)
await coll.getAll();                // Get all documents
await coll.keys();                  // Get all IDs
await coll.count();                 // Document count
await coll.count({ field: value }); // Count with filter (index-only when fully indexed)

// Indexes
await coll.createIndex('field');    // Create secondary index
await coll.createIndex('nested.field');
await coll.getIndexes();            // List indexed fields

// Maintenance
await coll.compact();               // Reclaim deleted space
await coll.clear();                 // Delete all documents
await coll.reset();                 // Fast truncate: clear storage + delete blobs
await coll.getStats();              // Collection statistics

// Bulk operations
await coll.insertMany([[id, doc], ...]); // Fast bulk insert for many small documents
await coll.batch(async (ops) => {
  await ops.insert('id1', { ... });
  await ops.update('id2', { ... });
  await ops.delete('id3');
});

// Iteration
for await (const { id, data } of coll) {
  console.log(id, data);
}
```

## Examples

See the `examples/` folder:

```bash
bun examples/basic-usage.ts      # CRUD operations
bun examples/secondary-indexes.ts # Querying with indexes
bun examples/nested-fields.ts    # Nested field indexing
bun examples/compaction.ts       # Space reclamation
bun examples/persistence.ts      # Data persistence
bun examples/http-api.ts         # REST API example
```

## Benchmarks

```bash
bun benchmarks/run.ts            # Run all benchmarks
bun benchmarks/bench-json.ts     # vs Vanilla JSON
bun benchmarks/bench-lowdb.ts    # vs LowDB
bun benchmarks/bench-scale.ts    # Scale tests
```

Results are saved to `benchmarks/results/*.json`.

## Architecture

```
Data File (.data)           Index File (.idx)
┌─────────────────┐         ┌─────────────────┐
│ Header (64B)    │         │ Header (64B)    │
├─────────────────┤         ├─────────────────┤
│ Slot 0 [1KB]    │◄────────│ id → offset     │
├─────────────────┤         │ id → offset     │
│ Slot 1 [8KB]    │◄────────│ ...             │
├─────────────────┤         ├─────────────────┤
│ Slot 2 [1KB]    │         │ Secondary Index │
│ (deleted)       │         │ field → values  │
├─────────────────┤         │ value → ids     │
│ Slot 3 [64KB]   │◄────────│ ...             │
└─────────────────┘         └─────────────────┘
```

- **Slab sizes**: 1KB → 8KB → 64KB → 4KB-aligned (huge)
- **Slot header**: 16 bytes (flags, length, slab size, CRC32)
- **Index**: Binary format for fast load/save

## License

MIT
