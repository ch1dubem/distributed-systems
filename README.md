# Distributed Systems

## Description of the Project

This Java project builds a small Kafka-like message broker. Brokers register with ZooKeeper, elect a controller, store topic partitions in append-only log segments, and replicate writes to assigned followers. The client library discovers partition leaders and provides producer and polling consumer APIs.

## User Stories

- As an application, I can create a topic with a chosen number of partitions and replicas.
- As a producer, I can send UTF-8 strings or byte arrays and receive the stored offset.
- As a consumer, I can poll a topic partition and continue from its next offset.
- As a broker, I can recover topic logs after restart and serve data from replicas.
- As an operator, I can inspect broker and topic metadata through the client protocol.

## Setup

### Prerequisites

- Java 11 or later
- Apache Maven
- A running ZooKeeper service

Run the test suite from the project root:

```sh
mvn test
```

The test dependencies start embedded ZooKeeper servers, so a separate ZooKeeper process is not needed for tests.

### Starting a Broker

Start a ZooKeeper service, then create a broker in your Java application with its broker ID, advertised host and port, ZooKeeper connect string, and data directory:

```java
BrokerServer broker = new BrokerServer(
    1, "localhost", 9092, "localhost:2181", Path.of("data/1"));
broker.start();
// Keep the application running and call broker.close() during shutdown.
```

### Creating a Topic and Sending Messages

Create the topic through a connected broker, then use the producer and consumer APIs:

```java
try (BrokerClient admin = new BrokerClient("localhost", 9092)) {
    admin.createTopic("orders", 3, (short) 1);
}

try (SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost", 9092, "orders")) {
    producer.initialize();
    long offset = producer.send("order received");
}

try (SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("localhost", 9092, "orders", 0)) {
    consumer.initialize();
    List<byte[]> messages = consumer.poll();
}
```

The producer selects partitions in round-robin order. A consumer tracks its offset in memory for the lifetime of that consumer instance.

## Running the Tests

```sh
./scripts/test.sh
```

The suite covers request encoding, ZooKeeper registration and controller election, partition storage and recovery, broker networking and replication, and producer/consumer message flow.

## Technologies Used

- Java
- Apache ZooKeeper
- Apache Maven
- JUnit Jupiter
- Apache Curator test server

## Demo

The broker integration tests start two brokers and an embedded ZooKeeper service, create a replicated topic, send a message through the client API, and read it from the assigned follower. Other tests restart a broker and check that its topic data is still available.

## Future Work

- Reassign partition leaders and replicas when brokers join or leave.
- Retry or reconcile writes when a follower is unavailable.
- Replace the in-memory consumer offset with a durable offset store.
- Add persistent client connections and request batching.

## Resources

- [Build Your Own Kafka source project](https://github.com/buildthingsuseful/build-your-own-kafka)
- [Developing the client library](https://buildthingsuseful.medium.com/stage-6-build-your-own-kafka-developing-the-client-library-96ecc0218089)
- [Producer and consumer APIs](https://buildthingsuseful.medium.com/stage-7-build-your-own-kafka-building-higher-level-producer-and-consumer-apis-2fba12ecf062)
- [System checks](https://buildthingsuseful.medium.com/stage-8-build-your-own-kafka-test-the-system-39ca1c1dca5e)

## Thanks

Thanks to the Build Things That's Useful tutorial series for the project outline and to the Apache ZooKeeper and Curator projects for the coordination service and embedded test server.
