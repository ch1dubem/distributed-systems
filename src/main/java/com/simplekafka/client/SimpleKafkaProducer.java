package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/** String and byte-array producer with round-robin partition selection. */
public final class SimpleKafkaProducer implements AutoCloseable {
    private final BrokerClient client;
    private final String topic;
    private final AtomicInteger nextPartition = new AtomicInteger();

    public SimpleKafkaProducer(String bootstrapHost, int bootstrapPort, String topic) {
        if (topic == null || topic.trim().isEmpty()) throw new IllegalArgumentException("topic must not be blank");
        this.client = new BrokerClient(bootstrapHost, bootstrapPort);
        this.topic = topic;
    }

    public void initialize() throws IOException {
        client.initialize();
    }

    public long send(String message) throws IOException {
        if (message == null) throw new IllegalArgumentException("message must not be null");
        int partitionCount = client.getPartitionCount(topic);
        int partition = Math.floorMod(nextPartition.getAndIncrement(), partitionCount);
        return send(message.getBytes(StandardCharsets.UTF_8), partition);
    }

    public long send(String message, int partition) throws IOException {
        if (message == null) throw new IllegalArgumentException("message must not be null");
        return send(message.getBytes(StandardCharsets.UTF_8), partition);
    }

    public long send(byte[] message, int partition) throws IOException {
        return client.send(topic, partition, message);
    }

    @Override
    public void close() {
        client.close();
    }
}
