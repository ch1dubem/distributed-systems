package com.simplekafka.client;

import java.io.IOException;
import java.util.List;

/** Polling consumer that advances its offset after each successful fetch. */
public final class SimpleKafkaConsumer implements AutoCloseable {
    public static final int DEFAULT_MAX_BYTES = 1024 * 1024;

    private final BrokerClient client;
    private final String topic;
    private final int partition;
    private long currentOffset;

    public SimpleKafkaConsumer(String bootstrapHost, int bootstrapPort, String topic, int partition) {
        if (topic == null || topic.trim().isEmpty()) throw new IllegalArgumentException("topic must not be blank");
        if (partition < 0) throw new IllegalArgumentException("partition must be non-negative");
        this.client = new BrokerClient(bootstrapHost, bootstrapPort);
        this.topic = topic;
        this.partition = partition;
    }

    public void initialize() throws IOException {
        client.initialize();
        if (client.getPartitionCount(topic) <= partition) {
            throw new IOException("partition is not present in topic metadata: " + topic + "/" + partition);
        }
    }

    public List<byte[]> poll() throws IOException {
        return poll(DEFAULT_MAX_BYTES);
    }

    public List<byte[]> poll(int maxBytes) throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, maxBytes);
        currentOffset += messages.size();
        return messages;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public void close() {
        client.close();
    }
}
