// src/utils/avro-serializer.js
const avro = require('avsc');
const fs = require('fs');
const path = require('path');

// Load Avro schema
const schemaPath = path.join(__dirname, '../schemas/order.avsc');
const orderSchema = avro.Type.forSchema(JSON.parse(fs.readFileSync(schemaPath, 'utf8')));

/**
 * Serialize order object to Avro binary format
 * @param {Object} order - Order object with orderId, product, price
 * @returns {Buffer} Serialized Avro buffer
 */
function serializeOrder(order) {
  try {
    // Validate the order against schema
    if (!orderSchema.isValid(order)) {
      throw new Error('Invalid order schema');
    }
    
    // Serialize to binary
    const buffer = orderSchema.toBuffer(order);
    return buffer;
  } catch (error) {
    console.error('Serialization error:', error);
    throw error;
  }
}

/**
 * Deserialize Avro binary to order object
 * @param {Buffer} buffer - Avro serialized buffer
 * @returns {Object} Order object
 */
function deserializeOrder(buffer) {
  try {
    const order = orderSchema.fromBuffer(buffer);
    return order;
  } catch (error) {
    console.error('Deserialization error:', error);
    throw error;
  }
}

/**
 * Create a sample order for testing
 * @param {string} orderId 
 * @returns {Object} Sample order
 */
function createSampleOrder(orderId) {
  const products = ['Item1', 'Item2', 'Item3', 'Item4', 'Item5'];
  const randomProduct = products[Math.floor(Math.random() * products.length)];
  const randomPrice = parseFloat((Math.random() * 100 + 10).toFixed(2));
  
  return {
    orderId: orderId.toString(),
    product: randomProduct,
    price: randomPrice
  };
}

module.exports = {
  serializeOrder,
  deserializeOrder,
  createSampleOrder,
  orderSchema
};