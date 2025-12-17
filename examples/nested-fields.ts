/**
 * SmolDB Nested Fields Example
 *
 * Demonstrates indexing and querying nested object fields.
 *
 * Run with: bun examples/nested-fields.ts
 */

import { SmolDB } from '../index';

async function main() {
  const db = new SmolDB('./example-data', { gcEnabled: false });
  await db.init();

  console.log('=== SmolDB Nested Fields ===\n');

  const employees = db.collection('employees');

  // Create indexes on nested fields using dot notation
  await employees.createIndex('department.name');
  await employees.createIndex('address.country');
  await employees.createIndex('skills'); // Can index arrays too (matches exact array)

  // Insert employees with nested data
  const data = [
    {
      name: 'Alice Johnson',
      department: { name: 'Engineering', floor: 3, manager: 'Bob' },
      address: { country: 'US', city: 'San Francisco', zip: '94102' },
      skills: ['TypeScript', 'React', 'Node.js'],
    },
    {
      name: 'Bob Smith',
      department: { name: 'Engineering', floor: 3, manager: 'Carol' },
      address: { country: 'US', city: 'New York', zip: '10001' },
      skills: ['Python', 'Django', 'PostgreSQL'],
    },
    {
      name: 'Charlie Brown',
      department: { name: 'Marketing', floor: 2, manager: 'Diana' },
      address: { country: 'UK', city: 'London', zip: 'SW1A 1AA' },
      skills: ['SEO', 'Content', 'Analytics'],
    },
    {
      name: 'Diana Prince',
      department: { name: 'Sales', floor: 1, manager: 'Eve' },
      address: { country: 'UK', city: 'Manchester', zip: 'M1 1AA' },
      skills: ['Negotiation', 'CRM', 'Presentations'],
    },
    {
      name: 'Eve Wilson',
      department: { name: 'Engineering', floor: 3, manager: 'Carol' },
      address: { country: 'Germany', city: 'Berlin', zip: '10115' },
      skills: ['Rust', 'Go', 'Kubernetes'],
    },
  ];

  console.log('Inserting employees...\n');
  for (let i = 0; i < data.length; i++) {
    await employees.insert(`emp_${i + 1}`, data[i]);
  }

  // Query by nested field
  console.log('Engineering department:');
  const engineering = await employees.find({ 'department.name': 'Engineering' });
  engineering.forEach((e) => console.log(`  - ${e.name} (${e.address.city})`));

  console.log('\nEmployees in UK:');
  const ukEmployees = await employees.find({ 'address.country': 'UK' });
  ukEmployees.forEach((e) => console.log(`  - ${e.name} (${e.address.city})`));

  // Combined nested queries
  console.log('\nEngineering employees in US:');
  const usEngineers = await employees.find({
    'department.name': 'Engineering',
    'address.country': 'US',
  });
  usEngineers.forEach((e) => console.log(`  - ${e.name}`));

  // Iterate through all with async iterator
  console.log('\nAll employees (using async iterator):');
  for await (const { id, data } of employees) {
    console.log(`  ${id}: ${data.name} - ${data.department.name}`);
  }

  await db.close();
  await Bun.$`rm -rf ./example-data`;
}

main().catch(console.error);
