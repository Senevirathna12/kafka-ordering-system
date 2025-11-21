// src/consumer.js
const { kafka, TOPICS } = require('./utils/kafka-config');
const { deserializeOrder, serializeOrder } = require('./utils/avro-serializer');

// State for aggregation
let totalPrice = 0;
let orderCount = 0;

// Retry tracking
const retryAttempts = new Map(); // orderId -> attempt count
const MAX_RETRIES = 3;

// Simulated failure rate (20% chance of temporary failure)
const FAILURE_RATE = 0.2;

async function consumeOrders() {
  const consumer = kafka.consumer({ groupId: 'order-processing-group' });
  const producer = kafka.producer(); // For DLQ and retry

  try {
    await consumer.connect();
    await producer.connect();
    console.log('Consumer connected successfully');
    console.log('Listening for orders...\n');

    // Subscribe to topics
    await consumer.subscribe({ topic: TOPICS.ORDERS, fromBeginning: false });
    await consumer.subscribe({ topic: TOPICS.ORDERS_RETRY, fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          // Deserialize Avro message
          const order = deserializeOrder(message.value);
          const orderId = order.orderId;

          console.log(`📦 Received order: ${orderId} from topic: ${topic}`);

          // Simulate processing with potential failures
          const shouldFail = Math.random() < FAILURE_RATE;
          
          if (shouldFail) {
            throw new Error(`Temporary processing failure for order ${orderId}`);
          }

          // Successful processing
          await processOrder(order);
          
          // Clear retry attempts on success
          if (retryAttempts.has(orderId)) {
            retryAttempts.delete(orderId);
          }

        } catch (error) {
          console.error(`❌ Error processing message:`, error.message);
          
          // Handle retry logic
          const orderId = message.key.toString();
          const currentAttempts = retryAttempts.get(orderId) || 0;
          
          if (currentAttempts < MAX_RETRIES) {
            // Retry
            await handleRetry(producer, message, orderId, currentAttempts);
          } else {
            // Send to DLQ
            await sendToDLQ(producer, message, orderId, error);
          }
        }
      }
    });

  } catch (error) {
    console.error('Consumer error:', error);
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(1);
  }

  // Graceful shutdown
  const shutdown = async () => {
    console.log('\n🛑 Shutting down consumer...');
    await consumer.disconnect();
    await producer.disconnect();
    console.log('Consumer disconnected');
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

/**
 * Process order and calculate running average
 */
async function processOrder(order) {
  // Update aggregation
  totalPrice += order.price;
  orderCount++;
  const runningAverage = totalPrice / orderCount;

  console.log(`✅ Processed: Order ${order.orderId}`);
  console.log(`   Product: ${order.product}`);
  console.log(`   Price: $${order.price.toFixed(2)}`);
  console.log(`   📊 Running Average: $${runningAverage.toFixed(2)}`);
  console.log(`   Total Orders: ${orderCount}\n`);
}

/**
 * Handle retry logic with exponential backoff
 */
async function handleRetry(producer, message, orderId, currentAttempts) {
  const nextAttempt = currentAttempts + 1;
  retryAttempts.set(orderId, nextAttempt);
  
  // Exponential backoff delay
  const delay = Math.pow(2, currentAttempts) * 1000; // 1s, 2s, 4s
  
  console.log(`🔄 Retry ${nextAttempt}/${MAX_RETRIES} for order ${orderId} (delay: ${delay}ms)`);
  
  // Send to retry topic after delay
  setTimeout(async () => {
    try {
      await producer.send({
        topic: TOPICS.ORDERS_RETRY,
        messages: [
          {
            key: message.key,
            value: message.value,
            headers: {
              ...message.headers,
              'retry-attempt': nextAttempt.toString(),
              'retry-timestamp': Date.now().toString()
            }
          }
        ]
      });
      console.log(`📤 Sent to retry queue: ${orderId}\n`);
    } catch (error) {
      console.error('Error sending to retry topic:', error);
    }
  }, delay);
}

/**
 * Send permanently failed messages to Dead Letter Queue
 */
async function sendToDLQ(producer, message, orderId, error) {
  console.log(`💀 Sending to DLQ: Order ${orderId} after ${MAX_RETRIES} retries`);
  
  try {
    await producer.send({
      topic: TOPICS.ORDERS_DLQ,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            'error-message': error.message,
            'dlq-timestamp': Date.now().toString(),
            'retry-attempts': MAX_RETRIES.toString()
          }
        }
      ]
    });
    
    console.log(`💀 Order ${orderId} moved to DLQ\n`);
    retryAttempts.delete(orderId);
    
  } catch (dlqError) {
    console.error('Error sending to DLQ:', dlqError);
  }
}

// Start the consumer
consumeOrders().catch(console.error);