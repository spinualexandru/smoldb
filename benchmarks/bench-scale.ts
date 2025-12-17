#!/usr/bin/env bun
/**
 * Benchmark: SmolDB Scale Test
 *
 * Tests SmolDB performance at various scales (1K, 10K, 100K documents).
 * This benchmark focuses on SmolDB-specific features like compaction.
 *
 * Run with: bun benchmarks/bench-scale.ts
 */

import { rm, mkdir } from 'node:fs/promises';
import { SmolDB } from '../index';
import {
  benchmarkBatch,
  createSuite,
  printResults,
  saveResults,
  generateDocument,
  formatNumber,
  type BenchmarkSuite,
} from './utils';

const BENCH_PATH = '/tmp/smoldb-bench-scale';

async function runBenchmarks(): Promise<BenchmarkSuite> {
  const suite = createSuite('SmolDB Scale Test');

  await rm(BENCH_PATH, { recursive: true, force: true });
  await mkdir(BENCH_PATH, { recursive: true });

  const smoldb = new SmolDB(`${BENCH_PATH}/smoldb`, { gcEnabled: false });
  await smoldb.init();

  console.log('Running scale benchmarks...\n');

  // ============================================================
  // Scale Tests: Insert
  // ============================================================
  const scales = [1000, 10000, 50000];

  for (const count of scales) {
    const coll = smoldb.collection(`scale_${count}`);
    const batch = Array.from({ length: count }, (_, i) => ({
      id: `doc_${i}`,
      data: generateDocument(i),
    }));

    console.log(`Testing with ${formatNumber(count)} documents...`);

    // Insert benchmark
    suite.results.push(
      await benchmarkBatch(
        `Insert ${formatNumber(count)} docs`,
        async () => {
          await coll.clear();
          for (const { id, data } of batch) {
            await coll.insert(id, data);
          }
        },
        count,
        1
      )
    );

    // Re-populate for other tests
    await coll.clear();
    for (const { id, data } of batch) {
      await coll.insert(id, data);
    }

    // Random read benchmark
    suite.results.push(
      await benchmarkBatch(
        `Random Read (${formatNumber(count)} docs)`,
        async () => {
          for (let i = 0; i < 1000; i++) {
            const id = `doc_${Math.floor(Math.random() * count)}`;
            await coll.get(id);
          }
        },
        1000,
        3
      )
    );

    // Indexed query benchmark
    await coll.createIndex('role');
    suite.results.push(
      await benchmarkBatch(
        `Find (${formatNumber(count)} docs, indexed)`,
        async () => {
          for (let i = 0; i < 100; i++) {
            await coll.find({ role: 'admin' });
          }
        },
        100,
        3
      )
    );

    // Update benchmark
    suite.results.push(
      await benchmarkBatch(
        `Update (${formatNumber(count)} docs)`,
        async () => {
          for (let i = 0; i < 500; i++) {
            const id = `doc_${i % count}`;
            await coll.update(id, generateDocument(i + count));
          }
        },
        500,
        1
      )
    );

    // Get stats
    const stats = await coll.getStats();
    console.log(`  File size: ${formatNumber(stats.fileSize / 1024)} KB`);
    console.log(`  Live data: ${formatNumber(stats.liveDataSize / 1024)} KB`);
  }

  // ============================================================
  // Compaction Benchmark
  // ============================================================
  console.log('\nBenchmarking compaction...');

  const compactColl = smoldb.collection('compact_test');

  // Insert 10K documents
  for (let i = 0; i < 10000; i++) {
    await compactColl.insert(`doc_${i}`, generateDocument(i));
  }

  // Delete 70% to create fragmentation
  for (let i = 0; i < 7000; i++) {
    await compactColl.delete(`doc_${i}`);
  }

  const statsBefore = await compactColl.getStats();
  console.log(`Before compaction: ${formatNumber(statsBefore.fileSize / 1024)} KB`);

  suite.results.push(
    await benchmarkBatch(
      'Compact (3K live / 10K total)',
      async () => {
        await compactColl.compact();
      },
      1,
      1
    )
  );

  const statsAfter = await compactColl.getStats();
  console.log(`After compaction: ${formatNumber(statsAfter.fileSize / 1024)} KB`);
  console.log(`Space reclaimed: ${formatNumber((statsBefore.fileSize - statsAfter.fileSize) / 1024)} KB`);

  // ============================================================
  // Persistence Benchmark
  // ============================================================
  console.log('\nBenchmarking persistence/reload...');

  const persistColl = smoldb.collection('persist_test');
  for (let i = 0; i < 5000; i++) {
    await persistColl.insert(`doc_${i}`, generateDocument(i));
  }
  await persistColl.createIndex('role');
  await persistColl.createIndex('active');

  // Persist
  const persistStart = performance.now();
  await smoldb.persistAllIndexes();
  const persistTime = performance.now() - persistStart;

  suite.results.push({
    name: 'Persist Index (5K docs, 2 indexes)',
    operations: 1,
    totalTimeMs: persistTime,
    opsPerSecond: 1000 / persistTime,
    avgTimeMs: persistTime,
    minTimeMs: persistTime,
    maxTimeMs: persistTime,
  });

  await smoldb.close();

  // Reload
  const reloadStart = performance.now();
  const smoldb2 = new SmolDB(`${BENCH_PATH}/smoldb`, { gcEnabled: false });
  await smoldb2.init();
  const reloadedColl = smoldb2.collection('persist_test');
  await reloadedColl.get('doc_0'); // Force initialization
  const reloadTime = performance.now() - reloadStart;

  suite.results.push({
    name: 'Reload (5K docs, 2 indexes)',
    operations: 1,
    totalTimeMs: reloadTime,
    opsPerSecond: 1000 / reloadTime,
    avgTimeMs: reloadTime,
    minTimeMs: reloadTime,
    maxTimeMs: reloadTime,
  });

  // Verify data integrity
  const doc = await reloadedColl.get('doc_100');
  const admins = await reloadedColl.find({ role: 'admin' });
  console.log(`\nData integrity check:`);
  console.log(`  doc_100 exists: ${!!doc}`);
  console.log(`  Admins found: ${admins.length}`);

  await smoldb2.close();
  await rm(BENCH_PATH, { recursive: true, force: true });

  return suite;
}

// Main
async function main() {
  console.log('SmolDB Scale Benchmark\n');

  const suite = await runBenchmarks();

  printResults(suite);
  await saveResults(suite, 'bench-scale.json');
}

main().catch(console.error);
