/**
 * SmolDB Basic Usage Example
 *
 * Run with: bun examples/basic-usage.ts
 */

import { SmolDB } from '../index';

async function main() {
  // Initialize database
  const db = new SmolDB('./example-data', { gcEnabled: false });
  await db.init();

  console.log('=== SmolDB Basic Usage ===\n');

  // Get a collection (creates it if doesn't exist)
  const users = db.collection('users');

  // Insert documents
  console.log('Inserting users...');
  await users.insert('user_1', {
    name: 'Alice',
    email: 'alice@example.com',
    role: 'admin',
    createdAt: Date.now(),
  });

  await users.insert('user_2', {
    name: 'Bob',
    email: 'bob@example.com',
    role: 'user',
    createdAt: Date.now(),
  });

  await users.insert('user_3', {
    name: 'Charlie',
    email: 'charlie@example.com',
    role: 'user',
    createdAt: Date.now(),
  });

  // Get a document by ID
  console.log('\nFetching user_1:');
  const alice = await users.get('user_1');
  console.log(alice);

  // Check if document exists
  console.log('\nChecking existence:');
  console.log('user_1 exists:', await users.has('user_1'));
  console.log('user_999 exists:', await users.has('user_999'));

  // Update a document
  console.log('\nUpdating user_2...');
  await users.update('user_2', {
    name: 'Bob Smith',
    email: 'bob.smith@example.com',
    role: 'moderator',
    updatedAt: Date.now(),
  });

  const bob = await users.get('user_2');
  console.log('Updated user_2:', bob);

  // Upsert (insert or update)
  console.log('\nUpserting user_4...');
  await users.upsert('user_4', { name: 'Diana', role: 'user' });
  await users.upsert('user_4', { name: 'Diana Prince', role: 'admin' }); // Updates existing
  console.log('user_4:', await users.get('user_4'));

  // Get all documents
  console.log('\nAll users:');
  const allUsers = await users.getAll();
  console.log(`Total: ${allUsers.length} users`);

  // Get all keys
  console.log('\nAll user IDs:', await users.keys());

  // Count documents
  console.log('\nDocument count:', await users.count());

  // Delete a document
  console.log('\nDeleting user_3...');
  await users.delete('user_3');
  console.log('user_3 exists after delete:', await users.has('user_3'));

  // Get collection stats
  console.log('\nCollection stats:');
  const stats = await users.getStats();
  console.log(stats);

  // Persist index and close
  await db.close();
  console.log('\nDatabase closed.');

  // Cleanup example data
  await Bun.$`rm -rf ./example-data`;
}

main().catch(console.error);
