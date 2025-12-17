#!/usr/bin/env bun
/**
 * Benchmark Runner
 *
 * Run all benchmarks or specific ones:
 *   bun benchmarks/run.ts          # Run all
 *   bun benchmarks/run.ts json     # Run JSON comparison
 *   bun benchmarks/run.ts lowdb    # Run LowDB comparison
 *   bun benchmarks/run.ts scale    # Run scale tests
 *   bun benchmarks/run.ts --help   # Show help
 */

import { mkdir } from 'node:fs/promises';

const BENCHMARKS: Record<string, { file: string; description: string }> = {
  json: {
    file: './bench-json.ts',
    description: 'Compare SmolDB vs Vanilla JSON storage',
  },
  lowdb: {
    file: './bench-lowdb.ts',
    description: 'Compare SmolDB vs LowDB',
  },
  scale: {
    file: './bench-scale.ts',
    description: 'Test SmolDB at various scales (1K-50K docs)',
  },
};

function showHelp(): void {
  console.log('SmolDB Benchmark Runner\n');
  console.log('Usage:');
  console.log('  bun benchmarks/run.ts [benchmark...]  Run specified benchmarks');
  console.log('  bun benchmarks/run.ts                 Run all benchmarks');
  console.log('  bun benchmarks/run.ts --help          Show this help\n');
  console.log('Available benchmarks:');
  for (const [name, { description }] of Object.entries(BENCHMARKS)) {
    console.log(`  ${name.padEnd(10)} ${description}`);
  }
  console.log('\nResults are saved to: benchmarks/results/*.json');
}

async function runBenchmark(name: string): Promise<void> {
  const benchmark = BENCHMARKS[name];
  if (!benchmark) {
    console.error(`Unknown benchmark: ${name}`);
    console.error(`Available: ${Object.keys(BENCHMARKS).join(', ')}`);
    process.exit(1);
  }

  console.log(`\n${'#'.repeat(60)}`);
  console.log(`# Running: ${name}`);
  console.log(`# ${benchmark.description}`);
  console.log(`${'#'.repeat(60)}\n`);

  const proc = Bun.spawn(['bun', benchmark.file], {
    cwd: import.meta.dir,
    stdout: 'inherit',
    stderr: 'inherit',
  });

  const exitCode = await proc.exited;
  if (exitCode !== 0) {
    console.error(`Benchmark ${name} failed with exit code ${exitCode}`);
    process.exit(exitCode);
  }
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);

  if (args.includes('--help') || args.includes('-h')) {
    showHelp();
    return;
  }

  // Ensure results directory exists
  await mkdir('./benchmarks/results', { recursive: true });

  const toRun = args.length > 0 ? args : Object.keys(BENCHMARKS);

  console.log('SmolDB Benchmark Suite');
  console.log('='.repeat(60));
  console.log(`Benchmarks to run: ${toRun.join(', ')}`);
  console.log(`Results will be saved to: benchmarks/results/`);

  for (const name of toRun) {
    await runBenchmark(name);
  }

  console.log('\n' + '='.repeat(60));
  console.log('All benchmarks completed!');
  console.log('Results saved to benchmarks/results/');
}

main().catch(console.error);
