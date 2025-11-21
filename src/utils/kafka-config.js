const { Kafka } = require('kafkajs');

// Kafka configuration
const kafka = new Kafka({
  clientId: 'order-processing-system',
  brokers: ['localhost:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

// Topic names
const TOPICS = {
  ORDERS: 'orders',
  ORDERS_RETRY: 'orders-retry',
  ORDERS_DLQ: 'orders-dlq'
};

// Create admin client for topic management
const admin = kafka.admin();

// Initialize topics
async function initializeTopics() {
  try {
    await admin.connect();
    console.log('Admin connected to Kafka');

    const existingTopics = await admin.listTopics();
    
    const topicsToCreate = [];
    
    Object.values(TOPICS).forEach(topic => {
      if (!existingTopics.includes(topic)) {
        topicsToCreate.push({
          topic,
          numPartitions: 1,
          replicationFactor: 1
        });
      }
    });

    if (topicsToCreate.length > 0) {
      await admin.createTopics({
        topics: topicsToCreate
      });
      console.log('Topics created:', topicsToCreate.map(t => t.topic).join(', '));
    } else {
      console.log('All topics already exist');
    }

    await admin.disconnect();
  } catch (error) {
    console.error('Error initializing topics:', error);
    throw error;
  }
}

module.exports = {
  kafka,
  TOPICS,
  initializeTopics
};