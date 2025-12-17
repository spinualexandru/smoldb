/**
 * Benchmark Utilities
 */

export interface BenchmarkResult {
  name: string;
  operations: number;
  totalTimeMs: number;
  opsPerSecond: number;
  avgTimeMs: number;
  minTimeMs: number;
  maxTimeMs: number;
}

export interface BenchmarkSuite {
  name: string;
  timestamp: string;
  system: {
    platform: string;
    arch: string;
    runtime: string;
  };
  results: BenchmarkResult[];
  comparison?: {
    baseline: string;
    speedup: Record<string, number>;
  };
}

/**
 * Measure execution time of an async function
 */
export async function measure(fn: () => Promise<void>): Promise<number> {
  const start = performance.now();
  await fn();
  return performance.now() - start;
}

/**
 * Run a benchmark multiple times and collect stats
 */
export async function benchmark(
  name: string,
  fn: () => Promise<void>,
  iterations: number = 1000,
  warmupIterations: number = 100
): Promise<BenchmarkResult> {
  // Warmup
  for (let i = 0; i < warmupIterations; i++) {
    await fn();
  }

  // Collect timings
  const times: number[] = [];
  const start = performance.now();

  for (let i = 0; i < iterations; i++) {
    const time = await measure(fn);
    times.push(time);
  }

  const totalTimeMs = performance.now() - start;
  const avgTimeMs = times.reduce((a, b) => a + b, 0) / times.length;
  const minTimeMs = Math.min(...times);
  const maxTimeMs = Math.max(...times);
  const opsPerSecond = (iterations / totalTimeMs) * 1000;

  return {
    name,
    operations: iterations,
    totalTimeMs,
    opsPerSecond,
    avgTimeMs,
    minTimeMs,
    maxTimeMs,
  };
}

/**
 * Run a batch benchmark (single operation on many items)
 */
export async function benchmarkBatch(
  name: string,
  fn: () => Promise<void>,
  itemCount: number,
  runs: number = 5
): Promise<BenchmarkResult> {
  const times: number[] = [];

  for (let i = 0; i < runs; i++) {
    const time = await measure(fn);
    times.push(time);
  }

  const totalTimeMs = times.reduce((a, b) => a + b, 0);
  const avgTimeMs = totalTimeMs / runs;
  const minTimeMs = Math.min(...times);
  const maxTimeMs = Math.max(...times);
  const opsPerSecond = (itemCount / avgTimeMs) * 1000;

  return {
    name,
    operations: itemCount,
    totalTimeMs: avgTimeMs,
    opsPerSecond,
    avgTimeMs: avgTimeMs / itemCount,
    minTimeMs: minTimeMs / itemCount,
    maxTimeMs: maxTimeMs / itemCount,
  };
}

/**
 * Format a number with commas
 */
export function formatNumber(n: number): string {
  return n.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

/**
 * Print benchmark results to console
 */
export function printResults(suite: BenchmarkSuite): void {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Benchmark: ${suite.name}`);
  console.log(`${'='.repeat(60)}`);
  console.log(`Timestamp: ${suite.timestamp}`);
  console.log(`Runtime: ${suite.system.runtime}`);
  console.log(`Platform: ${suite.system.platform} (${suite.system.arch})`);
  console.log(`${'='.repeat(60)}\n`);

  // Table header
  console.log(
    `${'Operation'.padEnd(30)} | ${'Ops/sec'.padStart(12)} | ${'Avg (ms)'.padStart(10)} | ${'Min (ms)'.padStart(10)} | ${'Max (ms)'.padStart(10)}`
  );
  console.log('-'.repeat(82));

  for (const result of suite.results) {
    console.log(
      `${result.name.padEnd(30)} | ${formatNumber(result.opsPerSecond).padStart(12)} | ${formatNumber(result.avgTimeMs).padStart(10)} | ${formatNumber(result.minTimeMs).padStart(10)} | ${formatNumber(result.maxTimeMs).padStart(10)}`
    );
  }

  if (suite.comparison) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`Comparison (vs ${suite.comparison.baseline})`);
    console.log(`${'='.repeat(60)}`);

    for (const [name, speedup] of Object.entries(suite.comparison.speedup)) {
      const indicator = speedup > 1 ? '🟢' : speedup < 1 ? '🔴' : '🟡';
      const speedupStr = speedup > 1 ? `${formatNumber(speedup)}x faster` : `${formatNumber(1 / speedup)}x slower`;
      console.log(`${indicator} ${name}: ${speedupStr}`);
    }
  }

  console.log();
}

/**
 * Save benchmark results to JSON file
 */
export async function saveResults(suite: BenchmarkSuite, filename: string): Promise<void> {
  const outputPath = `./benchmarks/results/${filename}`;
  await Bun.write(outputPath, JSON.stringify(suite, null, 2));
  console.log(`Results saved to: ${outputPath}`);
}

/**
 * Generate sample document
 */
export function generateDocument(id: number): Record<string, unknown> {
  return {
    id: `doc_${id}`,
    name: `User ${id}`,
    email: `user${id}@example.com`,
    age: 20 + (id % 50),
    active: id % 2 === 0,
    role: ['admin', 'user', 'moderator'][id % 3],
    tags: [`tag${id % 10}`, `tag${(id + 1) % 10}`],
    metadata: {
      createdAt: Date.now(),
      updatedAt: Date.now(),
      version: 1,
    },
  };
}

/**
 * Create benchmark suite metadata
 */
export function createSuite(name: string): BenchmarkSuite {
  return {
    name,
    timestamp: new Date().toISOString(),
    system: {
      platform: process.platform,
      arch: process.arch,
      runtime: `Bun ${Bun.version}`,
    },
    results: [],
  };
}
