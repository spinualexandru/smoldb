#!/usr/bin/env bun
/**
 * Benchmark: SmolDB vs LowDB
 *
 * Compares SmolDB against LowDB, a popular JSON-based database.
 * Tests write-through mode (both persist every operation) for fairness.
 *
 * Run with: bun benchmarks/bench-lowdb.ts
 */

import { rm, mkdir } from 'node:fs/promises';
import { SmolDB } from '../index';
import { Low } from 'lowdb';
import { JSONFile } from 'lowdb/node';
import {
  benchmark,
  benchmarkBatch,
  createSuite,
  printResults,
  saveResults,
  generateDocument,
  measure,
  type BenchmarkSuite,
} from './utils';

const BENCH_PATH = '/tmp/smoldb-bench-lowdb';
const SMALL_COUNT = 100;
const MEDIUM_COUNT = 1000;
const LARGE_COUNT = 10000;

interface LowDBData {
  documents: Record<string, Record<string, unknown>>;
}

async function runBenchmarks(): Promise<BenchmarkSuite> {
  const suite = createSuite('SmolDB vs LowDB');

  await rm(BENCH_PATH, { recursive: true, force: true });
  await mkdir(BENCH_PATH, { recursive: true });

  console.log('Running benchmarks...\n');

  // ============================================================
  // SECTION 1: Write-through mode (both persist every op)
  // ============================================================
  console.log('=== Write-Through Mode (persist every operation) ===\n');

  {
    const smoldb = new SmolDB(`${BENCH_PATH}/smoldb-wt`, { gcEnabled: false });
    await smoldb.init();
    const smolColl = smoldb.collection('test');

    const adapter = new JSONFile<LowDBData>(`${BENCH_PATH}/lowdb-wt.json`);
    const lowdb = new Low<LowDBData>(adapter, { documents: {} });
    await lowdb.read();

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

    let lowId = 0;
    suite.results.push(
      await benchmark(
        '[WT] LowDB: Insert',
        async () => {
          lowdb.data.documents[`l_${lowId}`] = generateDocument(lowId);
          lowId++;
          await lowdb.write();
        },
        200,
        20
      )
    );

    // Updates
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
        '[WT] LowDB: Update',
        async () => {
          const id = `l_${(updateId++ % 200) + 1}`;
          lowdb.data.documents[id] = generateDocument(updateId + 1000);
          await lowdb.write();
        },
        200,
        20
      )
    );

    // Deletes
    console.log('Benchmarking deletes (write-through)...');

    // Prepare delete data
    for (let i = 0; i < 100; i++) {
      await smolColl.insert(`del_${i}`, generateDocument(i));
      lowdb.data.documents[`del_${i}`] = generateDocument(i);
    }
    await lowdb.write();

    let delId = 0;
    suite.results.push(
      await benchmark(
        '[WT] SmolDB: Delete',
        async () => {
          if (delId < 100) {
            await smolColl.delete(`del_${delId++}`);
          }
        },
        100,
        0
      )
    );

    delId = 0;
    suite.results.push(
      await benchmark(
        '[WT] LowDB: Delete',
        async () => {
          if (delId < 100) {
            delete lowdb.data.documents[`del_${delId++}`];
            await lowdb.write();
          }
        },
        100,
        0
      )
    );

    await smoldb.close();
  }

  // ============================================================
  // SECTION 2: Batched inserts
  // ============================================================
  console.log('\n=== Batched Inserts ===\n');

  {
    const smoldb = new SmolDB(`${BENCH_PATH}/smoldb-batch`, { gcEnabled: false });
    await smoldb.init();
    const smolColl = smoldb.collection('test');

    const adapter = new JSONFile<LowDBData>(`${BENCH_PATH}/lowdb-batch.json`);
    const lowdb = new Low<LowDBData>(adapter, { documents: {} });

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
        `[Batch] LowDB: Insert ${MEDIUM_COUNT}`,
        async () => {
          lowdb.data.documents = {};
          for (let i = 0; i < MEDIUM_COUNT; i++) {
            lowdb.data.documents[`doc_${i}`] = generateDocument(i);
          }
          await lowdb.write(); // Single write
        },
        MEDIUM_COUNT,
        3
      )
    );

    await smoldb.close();
  }

  // ============================================================
  // SECTION 3: Read Performance at Scale
  // ============================================================
  console.log('\n=== Read Performance at Scale ===\n');

  {
    const smoldb = new SmolDB(`${BENCH_PATH}/smoldb-read`, { gcEnabled: false, cacheSize: LARGE_COUNT });
    await smoldb.init();
    const smolColl = smoldb.collection('test');

    const adapter = new JSONFile<LowDBData>(`${BENCH_PATH}/lowdb-read.json`);
    const lowdb = new Low<LowDBData>(adapter, { documents: {} });

    console.log(`Populating ${LARGE_COUNT} documents...`);
    await smolColl.batch(async (ops) => {
      for (let i = 0; i < LARGE_COUNT; i++) {
        await ops.insert(`doc_${i}`, generateDocument(i));
        lowdb.data.documents[`doc_${i}`] = generateDocument(i);
      }
    });
    await lowdb.write();
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
        `[Read] LowDB: Random (${LARGE_COUNT} docs)`,
        async () => {
          const id = `doc_${Math.floor(Math.random() * LARGE_COUNT)}`;
          void lowdb.data.documents[id];
        },
        1000,
        100
      )
    );

    // Query performance
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
        '[Query] LowDB: Count (filter)',
        async () => {
          void Object.values(lowdb.data.documents).filter((doc) => doc.role === 'admin').length;
        },
        100,
        10
      )
    );

    await smoldb.close();
  }

  // ============================================================
  // SECTION 4: Cold Start Performance
  // ============================================================
  console.log('\n=== Cold Start Performance ===\n');

  {
    // Prepare persisted data
    const smoldb1 = new SmolDB(`${BENCH_PATH}/smoldb-cold`, { gcEnabled: false });
    await smoldb1.init();
    const smolColl1 = smoldb1.collection('test');

    const adapter1 = new JSONFile<LowDBData>(`${BENCH_PATH}/lowdb-cold.json`);
    const lowdb1 = new Low<LowDBData>(adapter1, { documents: {} });

    console.log(`Preparing ${LARGE_COUNT} documents...`);
    await smolColl1.batch(async (ops) => {
      for (let i = 0; i < LARGE_COUNT; i++) {
        await ops.insert(`doc_${i}`, generateDocument(i));
        lowdb1.data.documents[`doc_${i}`] = generateDocument(i);
      }
    });
    await lowdb1.write();
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
        await coll.get('doc_5000');
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

    // LowDB cold start
    const lowTimes: number[] = [];
    for (let run = 0; run < 5; run++) {
      const time = await measure(async () => {
        const adapter = new JSONFile<LowDBData>(`${BENCH_PATH}/lowdb-cold.json`);
        const db = new Low<LowDBData>(adapter, { documents: {} });
        await db.read();
        void db.data.documents['doc_5000'];
      });
      lowTimes.push(time);
    }

    suite.results.push({
      name: `[Cold] LowDB: Load + Read (${LARGE_COUNT})`,
      operations: 1,
      totalTimeMs: lowTimes.reduce((a, b) => a + b, 0) / lowTimes.length,
      opsPerSecond: 1000 / (lowTimes.reduce((a, b) => a + b, 0) / lowTimes.length),
      avgTimeMs: lowTimes.reduce((a, b) => a + b, 0) / lowTimes.length,
      minTimeMs: Math.min(...lowTimes),
      maxTimeMs: Math.max(...lowTimes),
    });
  }

  // ============================================================
  // SECTION 5: Index Persistence
  // ============================================================
  console.log('\n=== Index Persistence ===\n');

  {
    const smoldb = new SmolDB(`${BENCH_PATH}/smoldb-persist`, { gcEnabled: false });
    await smoldb.init();
    const smolColl = smoldb.collection('test');

    const adapter = new JSONFile<LowDBData>(`${BENCH_PATH}/lowdb-persist.json`);
    const lowdb = new Low<LowDBData>(adapter, { documents: {} });

    // Populate
    for (let i = 0; i < MEDIUM_COUNT; i++) {
      await smolColl.insert(`doc_${i}`, generateDocument(i));
      lowdb.data.documents[`doc_${i}`] = generateDocument(i);
    }
    await smolColl.createIndex('role');
    await smolColl.createIndex('active');

    suite.results.push(
      await benchmark(
        '[Persist] SmolDB: Save Index',
        async () => {
          await smolColl.persistIndex();
        },
        50,
        5
      )
    );

    suite.results.push(
      await benchmark(
        '[Persist] LowDB: Save All',
        async () => {
          await lowdb.write();
        },
        50,
        5
      )
    );

    await smoldb.close();
  }

  // ============================================================
  // Calculate comparison
  // ============================================================
  suite.comparison = {
    baseline: 'LowDB',
    speedup: {},
  };

  const pairs: Array<[string, string, string]> = [
    ['[WT] SmolDB: Insert', '[WT] LowDB: Insert', 'Write-Through Insert'],
    ['[WT] SmolDB: Update', '[WT] LowDB: Update', 'Write-Through Update'],
    ['[WT] SmolDB: Delete', '[WT] LowDB: Delete', 'Write-Through Delete'],
    ['[Query] SmolDB: Count (indexed)', '[Query] LowDB: Count (filter)', 'Query Count'],
    [`[Cold] SmolDB: Load + Read (${LARGE_COUNT})`, `[Cold] LowDB: Load + Read (${LARGE_COUNT})`, 'Cold Start'],
    ['[Persist] SmolDB: Save Index', '[Persist] LowDB: Save All', 'Persistence'],
  ];

  for (const [smolName, lowName, label] of pairs) {
    const smolResult = suite.results.find((r) => r.name === smolName);
    const lowResult = suite.results.find((r) => r.name === lowName);
    if (smolResult && lowResult) {
      suite.comparison.speedup[label] = smolResult.opsPerSecond / lowResult.opsPerSecond;
    }
  }

  // Cleanup
  await rm(BENCH_PATH, { recursive: true, force: true });

  return suite;
}

// Main
async function main() {
  console.log('SmolDB vs LowDB Benchmark\n');

  const suite = await runBenchmarks();

  printResults(suite);
  await saveResults(suite, 'bench-lowdb.json');
}

main().catch(console.error);
