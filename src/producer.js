// src/producer.js
const { kafka, TOPICS, initializeTopics } = require('./utils/kafka-config');
const { serializeOrder, createSampleOrder } = require('./utils/avro-serializer');

let orderCounter = 1001;

async function produceOrders() {
  const producer = kafka.producer();

  try {
    // Initialize topics first
    await initializeTopics();
    
    // Connect producer
    await producer.connect();
    console.log('Producer connected successfully');
    console.log('Starting to produce orders...\n');

    // Produce orders continuously
    const produceInterval = setInterval(async () => {
      try {
        // Create sample order
        const order = createSampleOrder(orderCounter);
        
        // Serialize with Avro
        const serializedOrder = serializeOrder(order);
        
        // Send to Kafka
        await producer.send({
          topic: TOPICS.ORDERS,
          messages: [
            {
              key: order.orderId,
              value: serializedOrder,
              headers: {
                'content-type': 'application/avro',
                'timestamp': Date.now().toString()
              }
            }
          ]
        });

        console.log(`✅ Order produced: ${order.orderId} | ${order.product} | $${order.price}`);
        orderCounter++;

      } catch (error) {
        console.error('Error producing message:', error);
      }
    }, 2000); // Produce every 2 seconds

    // Graceful shutdown
    const shutdown = async () => {
      console.log('\n🛑 Shutting down producer...');
      clearInterval(produceInterval);
      await producer.disconnect();
      console.log('Producer disconnected');
      process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

  } catch (error) {
    console.error('Producer error:', error);
    await producer.disconnect();
    process.exit(1);
  }
}

// Start the producer
produceOrders().catch(console.error);