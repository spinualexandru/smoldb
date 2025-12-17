/**
 * SmolDB HTTP API Example
 *
 * A simple REST API using SmolDB as the data store.
 *
 * Run with: bun examples/http-api.ts
 * Then test with:
 *   curl http://localhost:3000/users
 *   curl -X POST http://localhost:3000/users -d '{"name":"Alice","email":"alice@example.com"}'
 *   curl http://localhost:3000/users/1
 */

import { SmolDB, DuplicateIdError, DocumentNotFoundError } from '../index';

const db = new SmolDB('./api-data', { gcEnabled: false });

async function startServer() {
  await db.init();

  const users = db.collection('users');
  await users.createIndex('email');

  // Seed some data
  if ((await users.count()) === 0) {
    await users.insert('1', { name: 'Alice', email: 'alice@example.com', role: 'admin' });
    await users.insert('2', { name: 'Bob', email: 'bob@example.com', role: 'user' });
    console.log('Seeded initial data');
  }

  let nextId = (await users.count()) + 1;

  const server = Bun.serve({
    port: 3000,
    async fetch(req) {
      const url = new URL(req.url);
      const path = url.pathname;
      const method = req.method;

      // CORS headers
      const headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      };

      try {
        // GET /users - List all users
        if (path === '/users' && method === 'GET') {
          const allUsers = await users.getAll();
          return new Response(JSON.stringify(allUsers), { headers });
        }

        // POST /users - Create user
        if (path === '/users' && method === 'POST') {
          const body = await req.json();
          const id = String(nextId++);

          await users.insert(id, {
            ...body,
            createdAt: Date.now(),
          });

          const created = await users.get(id);
          return new Response(JSON.stringify({ id, ...created }), {
            status: 201,
            headers,
          });
        }

        // GET /users/:id - Get single user
        const userMatch = path.match(/^\/users\/(\w+)$/);
        if (userMatch && method === 'GET') {
          const id = userMatch[1];
          const user = await users.get(id);

          if (!user) {
            return new Response(JSON.stringify({ error: 'User not found' }), {
              status: 404,
              headers,
            });
          }

          return new Response(JSON.stringify({ id, ...user }), { headers });
        }

        // PUT /users/:id - Update user
        if (userMatch && method === 'PUT') {
          const id = userMatch[1];
          const body = await req.json();

          try {
            await users.update(id, {
              ...body,
              updatedAt: Date.now(),
            });
            const updated = await users.get(id);
            return new Response(JSON.stringify({ id, ...updated }), { headers });
          } catch (e) {
            if (e instanceof DocumentNotFoundError) {
              return new Response(JSON.stringify({ error: 'User not found' }), {
                status: 404,
                headers,
              });
            }
            throw e;
          }
        }

        // DELETE /users/:id - Delete user
        if (userMatch && method === 'DELETE') {
          const id = userMatch[1];
          const deleted = await users.delete(id);

          if (!deleted) {
            return new Response(JSON.stringify({ error: 'User not found' }), {
              status: 404,
              headers,
            });
          }

          return new Response(JSON.stringify({ success: true }), { headers });
        }

        // GET /users/search?email=... - Search by email
        if (path === '/users/search' && method === 'GET') {
          const email = url.searchParams.get('email');
          if (email) {
            const results = await users.find({ email });
            return new Response(JSON.stringify(results), { headers });
          }
        }

        // GET /stats - Database stats
        if (path === '/stats' && method === 'GET') {
          const stats = await users.getStats();
          return new Response(JSON.stringify(stats), { headers });
        }

        // POST /compact - Trigger compaction
        if (path === '/compact' && method === 'POST') {
          const result = await users.compact();
          return new Response(JSON.stringify(result), { headers });
        }

        return new Response(JSON.stringify({ error: 'Not found' }), {
          status: 404,
          headers,
        });
      } catch (error) {
        console.error(error);
        return new Response(
          JSON.stringify({ error: error instanceof Error ? error.message : 'Internal error' }),
          { status: 500, headers }
        );
      }
    },
  });

  console.log(`Server running at http://localhost:${server.port}`);
  console.log('\nTry these commands:');
  console.log('  curl http://localhost:3000/users');
  console.log('  curl http://localhost:3000/users/1');
  console.log('  curl -X POST http://localhost:3000/users -H "Content-Type: application/json" -d \'{"name":"Charlie","email":"charlie@example.com"}\'');
  console.log('  curl http://localhost:3000/stats');
  console.log('\nPress Ctrl+C to stop');

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\nShutting down...');
    await db.close();
    await Bun.$`rm -rf ./api-data`;
    process.exit(0);
  });
}

startServer().catch(console.error);
