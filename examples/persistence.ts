/**
 * SmolDB Persistence Example
 *
 * Demonstrates data persistence across database restarts.
 *
 * Run with: bun examples/persistence.ts
 */

import { SmolDB } from '../index';

async function main() {
  const DB_PATH = './example-data';

  console.log('=== SmolDB Persistence ===\n');

  // === First session: Create and populate ===
  console.log('--- Session 1: Creating data ---\n');
  {
    const db = new SmolDB(DB_PATH, { gcEnabled: false });
    await db.init();

    // Create multiple collections
    const users = db.collection('users');
    const posts = db.collection('posts');
    const comments = db.collection('comments');

    // Create indexes
    await users.createIndex('role');
    await posts.createIndex('authorId');
    await comments.createIndex('postId');

    // Insert users
    await users.insert('user_1', { name: 'Alice', role: 'admin' });
    await users.insert('user_2', { name: 'Bob', role: 'author' });
    await users.insert('user_3', { name: 'Charlie', role: 'reader' });

    // Insert posts
    await posts.insert('post_1', {
      title: 'Getting Started with SmolDB',
      authorId: 'user_1',
      content: 'SmolDB is a file-based NoSQL database...',
      createdAt: Date.now(),
    });
    await posts.insert('post_2', {
      title: 'Advanced Queries',
      authorId: 'user_2',
      content: 'Learn about secondary indexes...',
      createdAt: Date.now(),
    });

    // Insert comments
    await comments.insert('comment_1', {
      postId: 'post_1',
      userId: 'user_3',
      text: 'Great article!',
    });
    await comments.insert('comment_2', {
      postId: 'post_1',
      userId: 'user_2',
      text: 'Thanks for sharing!',
    });

    console.log('Created collections:', db.listCollections());
    console.log('Users:', await users.count());
    console.log('Posts:', await posts.count());
    console.log('Comments:', await comments.count());

    // Important: persist indexes before closing
    await db.persistAllIndexes();
    await db.close();
    console.log('\nSession 1 closed.\n');
  }

  // === Second session: Read and verify ===
  console.log('--- Session 2: Reading persisted data ---\n');
  {
    const db = new SmolDB(DB_PATH, { gcEnabled: false });
    await db.init();

    console.log('Collections found:', db.listCollections());

    const users = db.collection('users');
    const posts = db.collection('posts');
    const comments = db.collection('comments');

    // Verify data
    console.log('\nUsers:');
    for await (const { id, data } of users) {
      console.log(`  ${id}: ${data.name} (${data.role})`);
    }

    console.log('\nPosts:');
    const allPosts = await posts.getAll();
    allPosts.forEach((p) => console.log(`  - ${p.title} by ${p.authorId}`));

    // Verify indexes still work
    console.log('\nQuerying with indexes:');
    const authorPosts = await posts.find({ authorId: 'user_1' });
    console.log(`  Posts by user_1: ${authorPosts.length}`);

    const post1Comments = await comments.find({ postId: 'post_1' });
    console.log(`  Comments on post_1: ${post1Comments.length}`);

    // Add more data
    await users.insert('user_4', { name: 'Diana', role: 'author' });
    await posts.insert('post_3', {
      title: 'New Post After Restart',
      authorId: 'user_4',
      content: 'This was added in session 2',
      createdAt: Date.now(),
    });

    console.log('\nAfter adding more data:');
    console.log('  Total users:', await users.count());
    console.log('  Total posts:', await posts.count());

    await db.persistAllIndexes();
    await db.close();
    console.log('\nSession 2 closed.\n');
  }

  // === Third session: Final verification ===
  console.log('--- Session 3: Final verification ---\n');
  {
    const db = new SmolDB(DB_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    const posts = db.collection('posts');

    // Verify all data persisted
    const diana = await users.get('user_4');
    console.log('User added in session 2:', diana?.name);

    const newPost = await posts.get('post_3');
    console.log('Post added in session 2:', newPost?.title);

    // Database stats
    const stats = await db.getStats();
    console.log('\nFinal database stats:', stats);

    await db.close();
  }

  // Cleanup
  await Bun.$`rm -rf ${DB_PATH}`;
  console.log('\nExample data cleaned up.');
}

main().catch(console.error);
