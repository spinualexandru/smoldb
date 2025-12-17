/**
 * SmolDB Secondary Indexes Example
 *
 * Demonstrates how to create indexes and query documents by field values.
 *
 * Run with: bun examples/secondary-indexes.ts
 */

import { SmolDB } from '../index';

async function main() {
  const db = new SmolDB('./example-data', { gcEnabled: false });
  await db.init();

  console.log('=== SmolDB Secondary Indexes ===\n');

  const products = db.collection('products');

  // Create indexes BEFORE inserting data for best performance
  // (Indexes can also be created after, but will scan existing docs)
  console.log('Creating indexes on category and inStock...');
  await products.createIndex('category');
  await products.createIndex('inStock');
  await products.createIndex('price'); // Can index numeric fields too

  // Insert sample products
  console.log('Inserting products...\n');
  const sampleProducts = [
    { name: 'Laptop', category: 'electronics', price: 999, inStock: true },
    { name: 'Mouse', category: 'electronics', price: 29, inStock: true },
    { name: 'Keyboard', category: 'electronics', price: 79, inStock: false },
    { name: 'Desk Chair', category: 'furniture', price: 299, inStock: true },
    { name: 'Standing Desk', category: 'furniture', price: 499, inStock: false },
    { name: 'Monitor', category: 'electronics', price: 349, inStock: true },
    { name: 'Bookshelf', category: 'furniture', price: 149, inStock: true },
    { name: 'Headphones', category: 'electronics', price: 199, inStock: true },
  ];

  for (let i = 0; i < sampleProducts.length; i++) {
    await products.insert(`product_${i + 1}`, sampleProducts[i]);
  }

  // Query by single field
  console.log('Electronics products:');
  const electronics = await products.find({ category: 'electronics' });
  electronics.forEach((p) => console.log(`  - ${p.name} ($${p.price})`));

  console.log('\nFurniture products:');
  const furniture = await products.find({ category: 'furniture' });
  furniture.forEach((p) => console.log(`  - ${p.name} ($${p.price})`));

  // Query by multiple fields (intersection)
  console.log('\nIn-stock electronics:');
  const inStockElectronics = await products.find({
    category: 'electronics',
    inStock: true,
  });
  inStockElectronics.forEach((p) => console.log(`  - ${p.name} ($${p.price})`));

  // Query by boolean field
  console.log('\nOut of stock items:');
  const outOfStock = await products.find({ inStock: false });
  outOfStock.forEach((p) => console.log(`  - ${p.name}`));

  // findOne - get first match
  console.log('\nFirst furniture item:');
  const firstFurniture = await products.findOne({ category: 'furniture' });
  console.log(firstFurniture);

  // List indexed fields
  console.log('\nIndexed fields:', await products.getIndexes());

  // Update a document - index is automatically updated
  console.log('\nUpdating Keyboard to be in stock...');
  await products.update('product_3', {
    name: 'Keyboard',
    category: 'electronics',
    price: 79,
    inStock: true, // Changed from false
  });

  console.log('In-stock electronics after update:');
  const updatedInStock = await products.find({
    category: 'electronics',
    inStock: true,
  });
  updatedInStock.forEach((p) => console.log(`  - ${p.name}`));

  await db.close();
  await Bun.$`rm -rf ./example-data`;
}

main().catch(console.error);
