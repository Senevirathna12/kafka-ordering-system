# Kafka Ordering System (Node.js)

---

## Features

- Kafka producer and consumer using [kafkajs]
- Avro message serialization using [avsc]
- Retry logic with exponential backoff for temporary failures
- Dead Letter Queue for permanently failed messages
- Real-time aggregation of order prices
- Graceful shutdown handling (SIGINT & SIGTERM)

---

## Prerequisites

- Node.js v16+
- Docker & Docker Compose
- npm

## Setup & Run

1. **Start Kafka and Zookeeper**:

```bash
docker-compose up
```
2. **Install dependencies**:

```
npm install
```

3. **Run the producer**:
```
node producer.js
```

4. ***Run the consumer:***
```
node consumer.js
```

5. ***Observe logs showing processed orders, retries, and messages sent to the DLQ.***


Kafka Topics

 `orders`       -> Main topic for order messages                     
 `orders-retry` -> Retry queue for temporary failures                
 `orders-dlq`   -> Dead Letter Queue for permanently failed messages 


 ***How It Works***
1. Producer creates sample orders, serializes them using Avro, and sends them to orders topic.
2. Consumer processes each order:
    On success → updates running average of order prices.
    On temporary failure → sends to orders-retry with exponential backoff.
    On permanent failure → sends to orders-dlq.

3. Logs show live processing, retries, and permanently failed messages.







