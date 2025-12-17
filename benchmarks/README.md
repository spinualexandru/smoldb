# SmolDB Benchmarks

Performance benchmarks comparing SmolDB against other JSON-based storage solutions.

## Quick Start

```bash
# Run all benchmarks
bun benchmarks/run.ts

# Run specific benchmark
bun benchmarks/run.ts json
bun benchmarks/run.ts lowdb
bun benchmarks/run.ts scale

# Run individual benchmark files directly
bun benchmarks/bench-json.ts
bun benchmarks/bench-lowdb.ts
bun benchmarks/bench-scale.ts
```

## Available Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `json` | SmolDB vs Vanilla JSON file storage |
| `lowdb` | SmolDB vs LowDB (popular JSON database) |
| `scale` | SmolDB performance at various scales (1K-50K docs) |

## Output

Each benchmark produces:
1. **Terminal output** - Summary table with ops/sec, timing stats
2. **JSON file** - Detailed results in `benchmarks/results/`

### JSON Output Format

```json
{
  "name": "SmolDB vs LowDB",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "system": {
    "platform": "linux",
    "arch": "x64",
    "runtime": "Bun 1.0.0"
  },
  "results": [
    {
      "name": "SmolDB: Single Insert",
      "operations": 1000,
      "totalTimeMs": 150.5,
      "opsPerSecond": 6644.5,
      "avgTimeMs": 0.15,
      "minTimeMs": 0.12,
      "maxTimeMs": 0.25
    }
  ],
  "comparison": {
    "baseline": "LowDB",
    "speedup": {
      "Single Insert": 2.5,
      "Random Read": 1.8
    }
  }
}
```

## Benchmark Details

### bench-json.ts
Compares against vanilla JSON storage (read entire file, modify, write entire file).
- Single insert/update/delete operations
- Batch inserts (100, 1000 docs)
- Random reads
- Query performance (indexed vs scan)

### bench-lowdb.ts
Compares against [LowDB](https://github.com/typicode/lowdb), a popular JSON database.
- Same operations as bench-json
- Tests persistence overhead

### bench-scale.ts
Tests SmolDB-specific features at scale:
- Insert performance at 1K, 10K, 50K documents
- Compaction efficiency
- Index persistence and reload time

## Understanding Results

### Key Metrics
- **ops/sec** - Operations per second (higher is better)
- **avg (ms)** - Average time per operation (lower is better)
- **speedup** - Ratio vs baseline (>1 means SmolDB is faster)

### Why SmolDB Wins at Scale
1. **O(1) reads** - Direct offset access vs JSON.parse entire file
2. **Indexed queries** - Secondary indexes vs full scan
3. **Incremental writes** - Append new data vs rewrite entire file
4. **Slab allocation** - Efficient space reuse after updates

### Where JSON/LowDB May Win
1. **Very small datasets** - Overhead of SmolDB structure
2. **Read-heavy, rarely-written** - JSON can be memory-mapped
3. **Simple key-value** - No need for indexes
