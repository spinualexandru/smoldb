/**
 * SmolDB Compaction Example
 *
 * Demonstrates how deleted space is reclaimed through compaction.
 *
 * Run with: bun examples/compaction.ts
 */

import { SmolDB } from '../index';

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

async function main() {
  const db = new SmolDB('./example-data', { gcEnabled: false });
  await db.init();

  console.log('=== SmolDB Compaction ===\n');

  const logs = db.collection('logs');

  // Insert many documents
  console.log('Inserting 100 log entries...');
  for (let i = 0; i < 100; i++) {
    await logs.insert(`log_${i}`, {
      timestamp: Date.now(),
      level: ['info', 'warn', 'error'][i % 3],
      message: `Log message ${i} with some padding data: ${'x'.repeat(200)}`,
      metadata: {
        source: 'app',
        requestId: `req_${i}`,
      },
    });
  }

  const statsAfterInsert = await logs.getStats();
  console.log('\nStats after insert:');
  console.log(`  Documents: ${statsAfterInsert.documentCount}`);
  console.log(`  File size: ${formatBytes(statsAfterInsert.fileSize)}`);
  console.log(`  Live data: ${formatBytes(statsAfterInsert.liveDataSize)}`);
  console.log(`  Free slots: ${statsAfterInsert.freeSlots}`);

  // Delete 70% of documents
  console.log('\nDeleting 70 documents...');
  for (let i = 0; i < 70; i++) {
    await logs.delete(`log_${i}`);
  }

  const statsAfterDelete = await logs.getStats();
  console.log('\nStats after delete:');
  console.log(`  Documents: ${statsAfterDelete.documentCount}`);
  console.log(`  File size: ${formatBytes(statsAfterDelete.fileSize)}`);
  console.log(`  Live data: ${formatBytes(statsAfterDelete.liveDataSize)}`);
  console.log(`  Free slots: ${statsAfterDelete.freeSlots}`);
  console.log(
    `  Wasted space: ${formatBytes(statsAfterDelete.fileSize - statsAfterDelete.liveDataSize)}`
  );

  // Run compaction
  console.log('\nRunning compaction...');
  const compactionResult = await logs.compact();
  console.log(`  Bytes freed: ${formatBytes(compactionResult.bytesFreed)}`);
  console.log(`  Documents compacted: ${compactionResult.documentsCompacted}`);

  const statsAfterCompact = await logs.getStats();
  console.log('\nStats after compaction:');
  console.log(`  Documents: ${statsAfterCompact.documentCount}`);
  console.log(`  File size: ${formatBytes(statsAfterCompact.fileSize)}`);
  console.log(`  Live data: ${formatBytes(statsAfterCompact.liveDataSize)}`);
  console.log(`  Free slots: ${statsAfterCompact.freeSlots}`);

  // Verify remaining documents are still accessible
  console.log('\nVerifying remaining documents...');
  let verified = 0;
  for (let i = 70; i < 100; i++) {
    const doc = await logs.get(`log_${i}`);
    if (doc && doc.message.includes(`Log message ${i}`)) {
      verified++;
    }
  }
  console.log(`  ${verified}/30 documents verified successfully`);

  // Show database-wide stats
  console.log('\nDatabase stats:');
  const dbStats = await db.getStats();
  console.log(dbStats);

  await db.close();
  await Bun.$`rm -rf ./example-data`;
}

main().catch(console.error);
