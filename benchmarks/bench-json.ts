#!/usr/bin/env bun
/**
 * Benchmark: SmolDB vs Vanilla JSON Storage
 *
 * Compares SmolDB against simple JSON file read/write operations.
 * Tests both "write-through" (persist every op) and "batched" modes.
 *
 * Run with: bun benchmarks/bench-json.ts
 */

import { rm, mkdir } from 'node:fs/promises';
import { SmolDB } from '../index';
import {
  benchmark,
  benchmarkBatch,
  createSuite,
  printResults,
  saveResults,
  generateDocument,
  measure,
  type BenchmarkSuite,
  type BenchmarkResult,
} from './utils';

const BENCH_PATH = '/tmp/smoldb-bench-json';
const SMALL_COUNT = 100;
const MEDIUM_COUNT = 1000;
const LARGE_COUNT = 10000;

/**
 * Vanilla JSON storage - write-through mode (persist every operation)
 */
class VanillaJsonDB {
  private data: Map<string, Record<string, unknown>> = new Map();
  private filePath: string;

  constructor(filePath: string) {
    this.filePath = filePath;
  }

  async load(): Promise<void> {
    const file = Bun.file(this.filePath);
    if (await file.exists()) {
      const content = await file.json();
      this.data = new Map(Object.entries(content));
    }
  }

  async save(): Promise<void> {
    const obj = Object.fromEntries(this.data);
    await Bun.write(this.filePath, JSON.stringify(obj));
  }

  // Write-through: persist on every insert
  async insert(id: string, doc: Record<string, unknown>): Promise<void> {
    this.data.set(id, doc);
    await this.save();
  }

  // Batched: only persist at end
  insertMemory(id: string, doc: Record<string, unknown>): void {
    this.data.set(id, doc);
  }

  get(id: string): Record<string, unknown> | undefined {
    return this.data.get(id);
  }

  async update(id: string, doc: Record<string, unknown>): Promise<void> {
    this.data.set(id, doc);
    await this.save();
  }

  async delete(id: string): Promise<void> {
    this.data.delete(id);
    await this.save();
  }

  find(predicate: (doc: Record<string, unknown>) => boolean): Record<string, unknown>[] {
    return Array.from(this.data.values()).filter(predicate);
  }

  clear(): void {
    this.data.clear();
  }

  size(): number {
    return this.data.size;
  }
}

async function runBenchmarks(): Promise<BenchmarkSuite> {
  const suite = createSuite('SmolDB vs Vanilla JSON');

  await rm(BENCH_PATH, { recursive: true, force: true });
  await mkdir(BENCH_PATH, { recursive: true });

  console.log('Running benchmarks...\n');

  // ============================================================
  // SECTION 1: Write-through mode (fair comparison - both persist)
  // ============================================================
  console.log('=== Write-Through Mode (persist every operation) ===\n');

  {
    const smoldb = new SmolDB(`${BENCH_PATH}/smoldb-wt`, { gcEnabled: false });
    await smoldb.init();
    const smolColl = smoldb.collection('test');
    const jsonDb = new VanillaJsonDB(`${BENCH_PATH}/vanilla-wt.json`);

    console.log('Benchmarking single inserts (write-through)...');

    let smolId = 0;
    suite.results.push(
      await benchmark(
        '[WT] SmolDB: Insert',
        async () => {
          await smolColl.insert(`s_${smolId++}`, generateDocument(smolId));
        },
        200,
        20
      )
    );

    let jsonId = 0;
    suite.results.push(
      await benchmark(
        '[WT] JSON: Insert',
        async () => {
          await jsonDb.insert(`j_${jsonId++}`, generateDocument(jsonId));
        },
        200,
        20
      )
    );

    // Updates (write-through)
    console.log('Benchmarking updates (write-through)...');

    let updateId = 0;
    suite.results.push(
      await benchmark(
        '[WT] SmolDB: Update',
        async () => {
          const id = `s_${(updateId++ % 200) + 1}`;
          await smolColl.update(id, generateDocument(updateId + 1000));
        },
        200,
        20
      )
    );

    updateId = 0;
    suite.results.push(
      await benchmark(
        '[WT] JSON: Update',
        async () => {
          const id = `j_${(updateId++ % 200) + 1}`;
          await jsonDb.update(id, generateDocument(updateId + 1000));
        },
        200,
        20
      )
    );

    await smoldb.close();
  }

  // ============================================================
  // SECTION 2: Batched writes (JSON advantage)
  // ============================================================
  console.log('\n=== Batched Mode (JSON writes once at end) ===\n');

  {
    const smoldb = new SmolDB(`${BENCH_PATH}/smoldb-batch`, { gcEnabled: false });
    await smoldb.init();
    const smolColl = smoldb.collection('test');
    const jsonDb = new VanillaJsonDB(`${BENCH_PATH}/vanilla-batch.json`);

    console.log(`Benchmarking batch insert (${MEDIUM_COUNT} docs)...`);

    suite.results.push(
      await benchmarkBatch(
        `[Batch] SmolDB: Insert ${MEDIUM_COUNT}`,
        async () => {
          await smolColl.reset();
          const items: Array<[string, Record<string, unknown>]> = [];
          for (let i = 0; i < MEDIUM_COUNT; i++) {
            items.push([`doc_${i}`, generateDocument(i)]);
          }
          await smolColl.insertMany(items);
        },
        MEDIUM_COUNT,
        3
      )
    );

    suite.results.push(
      await benchmarkBatch(
        `[Batch] JSON: Insert ${MEDIUM_COUNT}`,
        async () => {
          jsonDb.clear();
          for (let i = 0; i < MEDIUM_COUNT; i++) {
            jsonDb.insertMemory(`doc_${i}`, generateDocument(i));
          }
          await jsonDb.save(); // Single write at end
        },
        MEDIUM_COUNT,
        3
      )
    );

    await smoldb.close();
  }

  // ============================================================
  // SECTION 3: Read performance (SmolDB advantage at scale)
  // ============================================================
  console.log('\n=== Read Performance ===\n');

  {
    // Prepare large dataset
    const smoldb = new SmolDB(`${BENCH_PATH}/smoldb-read`, { gcEnabled: false, cacheSize: LARGE_COUNT });
    await smoldb.init();
    const smolColl = smoldb.collection('test');
    const jsonDb = new VanillaJsonDB(`${BENCH_PATH}/vanilla-read.json`);

    console.log(`Populating ${LARGE_COUNT} documents...`);
    await smolColl.batch(async (ops) => {
      for (let i = 0; i < LARGE_COUNT; i++) {
        await ops.insert(`doc_${i}`, generateDocument(i));
        jsonDb.insertMemory(`doc_${i}`, generateDocument(i));
      }
    });
    await jsonDb.save();
    await smolColl.persistIndex();

    console.log('Benchmarking random reads...');

    suite.results.push(
      await benchmark(
        `[Read] SmolDB: Random (${LARGE_COUNT} docs)`,
        async () => {
          const id = `doc_${Math.floor(Math.random() * LARGE_COUNT)}`;
          await smolColl.get(id);
        },
        1000,
        100
      )
    );

    suite.results.push(
      await benchmark(
        `[Read] JSON: Random (${LARGE_COUNT} docs)`,
        async () => {
          const id = `doc_${Math.floor(Math.random() * LARGE_COUNT)}`;
          jsonDb.get(id);
        },
        1000,
        100
      )
    );

    // Indexed query vs scan
    console.log('Benchmarking queries...');
    await smolColl.createIndex('role');

    suite.results.push(
      await benchmark(
        '[Query] SmolDB: Count (indexed)',
        async () => {
          await smolColl.count({ role: 'admin' });
        },
        100,
        10
      )
    );

    suite.results.push(
      await benchmark(
        '[Query] JSON: Count (scan)',
        async () => {
          void jsonDb.find((doc) => doc.role === 'admin').length;
        },
        100,
        10
      )
    );

    await smoldb.close();
  }

  // ============================================================
  // SECTION 4: Cold start (SmolDB advantage)
  // ============================================================
  console.log('\n=== Cold Start Performance ===\n');

  {
    // Prepare data
    const smoldb1 = new SmolDB(`${BENCH_PATH}/smoldb-cold`, { gcEnabled: false });
    await smoldb1.init();
    const smolColl1 = smoldb1.collection('test');
    const jsonDb1 = new VanillaJsonDB(`${BENCH_PATH}/vanilla-cold.json`);

    console.log(`Preparing ${LARGE_COUNT} documents...`);
    await smolColl1.batch(async (ops) => {
      for (let i = 0; i < LARGE_COUNT; i++) {
        await ops.insert(`doc_${i}`, generateDocument(i));
        jsonDb1.insertMemory(`doc_${i}`, generateDocument(i));
      }
    });
    await jsonDb1.save();
    await smolColl1.createIndex('role');
    await smoldb1.persistAllIndexes();
    await smoldb1.close();

    console.log('Benchmarking cold start + first read...');

    // SmolDB cold start
    const smolTimes: number[] = [];
    for (let run = 0; run < 5; run++) {
      const time = await measure(async () => {
        const db = new SmolDB(`${BENCH_PATH}/smoldb-cold`, { gcEnabled: false });
        await db.init();
        const coll = db.collection('test');
        await coll.get('doc_5000'); // Force load
        await db.close();
      });
      smolTimes.push(time);
    }

    suite.results.push({
      name: `[Cold] SmolDB: Load + Read (${LARGE_COUNT})`,
      operations: 1,
      totalTimeMs: smolTimes.reduce((a, b) => a + b, 0) / smolTimes.length,
      opsPerSecond: 1000 / (smolTimes.reduce((a, b) => a + b, 0) / smolTimes.length),
      avgTimeMs: smolTimes.reduce((a, b) => a + b, 0) / smolTimes.length,
      minTimeMs: Math.min(...smolTimes),
      maxTimeMs: Math.max(...smolTimes),
    });

    // JSON cold start (must parse entire file)
    const jsonTimes: number[] = [];
    for (let run = 0; run < 5; run++) {
      const time = await measure(async () => {
        const db = new VanillaJsonDB(`${BENCH_PATH}/vanilla-cold.json`);
        await db.load(); // Parse entire JSON
        db.get('doc_5000');
      });
      jsonTimes.push(time);
    }

    suite.results.push({
      name: `[Cold] JSON: Load + Read (${LARGE_COUNT})`,
      operations: 1,
      totalTimeMs: jsonTimes.reduce((a, b) => a + b, 0) / jsonTimes.length,
      opsPerSecond: 1000 / (jsonTimes.reduce((a, b) => a + b, 0) / jsonTimes.length),
      avgTimeMs: jsonTimes.reduce((a, b) => a + b, 0) / jsonTimes.length,
      minTimeMs: Math.min(...jsonTimes),
      maxTimeMs: Math.max(...jsonTimes),
    });
  }

  // ============================================================
  // Calculate comparison
  // ============================================================
  suite.comparison = {
    baseline: 'Vanilla JSON',
    speedup: {},
  };

  const pairs: Array<[string, string, string]> = [
    ['[WT] SmolDB: Insert', '[WT] JSON: Insert', 'Write-Through Insert'],
    ['[WT] SmolDB: Update', '[WT] JSON: Update', 'Write-Through Update'],
    ['[Query] SmolDB: Count (indexed)', '[Query] JSON: Count (scan)', 'Query Count (indexed vs scan)'],
    [`[Cold] SmolDB: Load + Read (${LARGE_COUNT})`, `[Cold] JSON: Load + Read (${LARGE_COUNT})`, 'Cold Start'],
  ];

  for (const [smolName, jsonName, label] of pairs) {
    const smolResult = suite.results.find((r) => r.name === smolName);
    const jsonResult = suite.results.find((r) => r.name === jsonName);
    if (smolResult && jsonResult) {
      suite.comparison.speedup[label] = smolResult.opsPerSecond / jsonResult.opsPerSecond;
    }
  }

  // Cleanup
  await rm(BENCH_PATH, { recursive: true, force: true });

  return suite;
}

// Main
async function main() {
  console.log('SmolDB vs Vanilla JSON Benchmark\n');

  const suite = await runBenchmarks();

  printResults(suite);
  await saveResults(suite, 'bench-json.json');
}

main().catch(console.error);
