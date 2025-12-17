/**
 * SmolDB Integration Tests
 */

import { test, expect, beforeEach, afterEach, describe } from 'bun:test';
import { rm, mkdir } from 'node:fs/promises';
import { SmolDB, DuplicateIdError, DocumentNotFoundError } from '../index';

const TEST_PATH = '/tmp/smoldb-test';

beforeEach(async () => {
  await rm(TEST_PATH, { recursive: true, force: true });
  await mkdir(TEST_PATH, { recursive: true });
});

afterEach(async () => {
  await rm(TEST_PATH, { recursive: true, force: true });
});

describe('SmolDB', () => {
  test('initializes database', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    expect(db.listCollections()).toEqual([]);

    await db.close();
  });

  test('creates and retrieves collection', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    expect(users.getName()).toBe('users');

    expect(db.listCollections()).toContain('users');

    await db.close();
  });

  test('persists collections across restarts', async () => {
    // Create and populate
    let db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });
    await users.persistIndex();
    await db.close();

    // Reopen and verify
    db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    expect(db.listCollections()).toContain('users');
    const doc = await db.collection('users').get('user_1');
    expect(doc).toEqual({ name: 'Alice' });

    await db.close();
  });

  test('drops collection', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });

    expect(db.listCollections()).toContain('users');

    await db.dropCollection('users');
    expect(db.listCollections()).not.toContain('users');

    await db.close();
  });

  test('gets database stats', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });
    await users.insert('user_2', { name: 'Bob' });

    const stats = await db.getStats();
    expect(stats.collections).toBe(1);
    expect(stats.totalDocuments).toBe(2);
    expect(stats.totalFileSize).toBeGreaterThan(0);

    await db.close();
  });
});

describe('Collection CRUD', () => {
  test('insert and get document', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice', role: 'admin' });

    const doc = await users.get('user_1');
    expect(doc).toEqual({ name: 'Alice', role: 'admin' });

    await db.close();
  });

  test('throws on duplicate insert', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });

    expect(users.insert('user_1', { name: 'Bob' })).rejects.toThrow(DuplicateIdError);

    await db.close();
  });

  test('returns null for non-existent document', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    const doc = await users.get('non_existent');

    expect(doc).toBeNull();

    await db.close();
  });

  test('update document', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });
    await users.update('user_1', { name: 'Alice Updated', role: 'admin' });

    const doc = await users.get('user_1');
    expect(doc).toEqual({ name: 'Alice Updated', role: 'admin' });

    await db.close();
  });

  test('update throws for non-existent document', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');

    expect(users.update('non_existent', { name: 'Bob' })).rejects.toThrow(DocumentNotFoundError);

    await db.close();
  });

  test('upsert creates or updates', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');

    // Create via upsert
    await users.upsert('user_1', { name: 'Alice' });
    expect(await users.get('user_1')).toEqual({ name: 'Alice' });

    // Update via upsert
    await users.upsert('user_1', { name: 'Alice Updated' });
    expect(await users.get('user_1')).toEqual({ name: 'Alice Updated' });

    await db.close();
  });

  test('delete document', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });

    expect(await users.has('user_1')).toBe(true);

    const deleted = await users.delete('user_1');
    expect(deleted).toBe(true);
    expect(await users.has('user_1')).toBe(false);
    expect(await users.get('user_1')).toBeNull();

    await db.close();
  });

  test('delete returns false for non-existent', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    const deleted = await users.delete('non_existent');

    expect(deleted).toBe(false);

    await db.close();
  });

  test('has checks existence', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });

    expect(await users.has('user_1')).toBe(true);
    expect(await users.has('user_2')).toBe(false);

    await db.close();
  });

  test('count returns document count', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    expect(await users.count()).toBe(0);

    await users.insert('user_1', { name: 'Alice' });
    await users.insert('user_2', { name: 'Bob' });

    expect(await users.count()).toBe(2);

    await db.close();
  });

  test('getAll returns all documents', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });
    await users.insert('user_2', { name: 'Bob' });

    const all = await users.getAll();
    expect(all).toHaveLength(2);
    expect(all).toContainEqual({ name: 'Alice' });
    expect(all).toContainEqual({ name: 'Bob' });

    await db.close();
  });

  test('keys returns all IDs', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });
    await users.insert('user_2', { name: 'Bob' });

    const keys = await users.keys();
    expect(keys).toHaveLength(2);
    expect(keys).toContain('user_1');
    expect(keys).toContain('user_2');

    await db.close();
  });

  test('clear removes all documents', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });
    await users.insert('user_2', { name: 'Bob' });

    await users.clear();
    expect(await users.count()).toBe(0);

    await db.close();
  });
});

describe('Secondary Indexes', () => {
  test('creates index and finds by field', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.createIndex('role');

    await users.insert('user_1', { name: 'Alice', role: 'admin' });
    await users.insert('user_2', { name: 'Bob', role: 'user' });
    await users.insert('user_3', { name: 'Charlie', role: 'admin' });

    const admins = await users.find({ role: 'admin' });
    expect(admins).toHaveLength(2);
    expect(admins.map((u) => u.name).sort()).toEqual(['Alice', 'Charlie']);

    await db.close();
  });

  test('finds with multiple filter fields', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.createIndex('role');
    await users.createIndex('active');

    await users.insert('user_1', { name: 'Alice', role: 'admin', active: true });
    await users.insert('user_2', { name: 'Bob', role: 'admin', active: false });
    await users.insert('user_3', { name: 'Charlie', role: 'user', active: true });

    const activeAdmins = await users.find({ role: 'admin', active: true });
    expect(activeAdmins).toHaveLength(1);
    expect(activeAdmins[0].name).toBe('Alice');

    await db.close();
  });

  test('findOne returns first match', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.createIndex('role');

    await users.insert('user_1', { name: 'Alice', role: 'admin' });
    await users.insert('user_2', { name: 'Bob', role: 'admin' });

    const admin = await users.findOne({ role: 'admin' });
    expect(admin).not.toBeNull();
    expect(admin?.role).toBe('admin');

    await db.close();
  });

  test('find returns empty array for no matches', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.createIndex('role');

    await users.insert('user_1', { name: 'Alice', role: 'admin' });

    const guests = await users.find({ role: 'guest' });
    expect(guests).toHaveLength(0);

    await db.close();
  });

  test('updates secondary index on document update', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.createIndex('role');

    await users.insert('user_1', { name: 'Alice', role: 'admin' });

    // Update role
    await users.update('user_1', { name: 'Alice', role: 'user' });

    const admins = await users.find({ role: 'admin' });
    expect(admins).toHaveLength(0);

    const regularUsers = await users.find({ role: 'user' });
    expect(regularUsers).toHaveLength(1);
    expect(regularUsers[0].name).toBe('Alice');

    await db.close();
  });

  test('lists indexed fields', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.createIndex('role');
    await users.createIndex('active');

    const indexes = await users.getIndexes();
    expect(indexes).toContain('role');
    expect(indexes).toContain('active');

    await db.close();
  });

  test('findIds returns matching IDs without reading docs when fully indexed', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.createIndex('role');

    await users.insert('user_1', { name: 'Alice', role: 'admin' });
    await users.insert('user_2', { name: 'Bob', role: 'user' });
    await users.insert('user_3', { name: 'Charlie', role: 'admin' });

    const ids = await users.findIds({ role: 'admin' });
    expect(ids.sort()).toEqual(['user_1', 'user_3']);

    await db.close();
  });

  test('count(filter) uses indexes when possible and falls back when not', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.createIndex('role');

    await users.insert('user_1', { name: 'Alice', role: 'admin', active: true });
    await users.insert('user_2', { name: 'Bob', role: 'admin', active: false });
    await users.insert('user_3', { name: 'Charlie', role: 'user', active: true });

    expect(await users.count({ role: 'admin' })).toBe(2);
    // 'active' is not indexed here, so this must validate by reading candidate docs.
    expect(await users.count({ role: 'admin', active: true })).toBe(1);

    await db.close();
  });
});

describe('Large Documents (Blob Store)', () => {
  test('stores large document in blob store', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false, blobThreshold: 1024 });
    await db.init();

    const docs = db.collection('docs');

    // Create document larger than threshold
    const largeContent = 'x'.repeat(2000);
    await docs.insert('doc_1', { content: largeContent });

    const doc = await docs.get('doc_1');
    expect(doc?.content).toBe(largeContent);

    await db.close();
  });

  test('updates large document', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false, blobThreshold: 1024 });
    await db.init();

    const docs = db.collection('docs');

    const largeContent1 = 'x'.repeat(2000);
    const largeContent2 = 'y'.repeat(3000);

    await docs.insert('doc_1', { content: largeContent1 });
    await docs.update('doc_1', { content: largeContent2 });

    const doc = await docs.get('doc_1');
    expect(doc?.content).toBe(largeContent2);

    await db.close();
  });
});

describe('Compaction', () => {
  test('compacts collection and reclaims space', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');

    // Insert documents
    for (let i = 0; i < 10; i++) {
      await users.insert(`user_${i}`, { name: `User ${i}`, data: 'x'.repeat(100) });
    }

    // Delete some documents
    for (let i = 0; i < 5; i++) {
      await users.delete(`user_${i}`);
    }

    const statsBefore = await users.getStats();

    // Compact
    const result = await users.compact();

    const statsAfter = await users.getStats();

    // Should have reclaimed space
    expect(result.bytesFreed).toBeGreaterThan(0);
    expect(statsAfter.fileSize).toBeLessThan(statsBefore.fileSize);

    // Remaining documents should still be accessible
    for (let i = 5; i < 10; i++) {
      const doc = await users.get(`user_${i}`);
      expect(doc?.name).toBe(`User ${i}`);
    }

    await db.close();
  });
});

describe('Iteration', () => {
  test('async iterator yields all documents', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.insert('user_1', { name: 'Alice' });
    await users.insert('user_2', { name: 'Bob' });

    const results: Array<{ id: string; data: Record<string, unknown> }> = [];
    for await (const entry of users) {
      results.push(entry);
    }

    expect(results).toHaveLength(2);
    expect(results.map((r) => r.id).sort()).toEqual(['user_1', 'user_2']);

    await db.close();
  });
});

describe('Nested Fields', () => {
  test('indexes and queries nested fields', async () => {
    const db = new SmolDB(TEST_PATH, { gcEnabled: false });
    await db.init();

    const users = db.collection('users');
    await users.createIndex('profile.country');

    await users.insert('user_1', { name: 'Alice', profile: { country: 'US', city: 'NYC' } });
    await users.insert('user_2', { name: 'Bob', profile: { country: 'UK', city: 'London' } });
    await users.insert('user_3', { name: 'Charlie', profile: { country: 'US', city: 'LA' } });

    const usUsers = await users.find({ 'profile.country': 'US' });
    expect(usUsers).toHaveLength(2);
    expect(usUsers.map((u) => u.name).sort()).toEqual(['Alice', 'Charlie']);

    await db.close();
  });
});
