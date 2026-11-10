package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Binary request framing shared by SimpleKafka clients and brokers. */
public final class Protocol {
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x1f;
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIFICATION = 0x23;

    private Protocol() {
    }

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = topicBytes(topic);
        if (partition < 0) throw new IllegalArgumentException("partition must be non-negative");
        if (message == null) throw new IllegalArgumentException("message must not be null");
        ByteBuffer request = ByteBuffer.allocate(1 + Short.BYTES + topicBytes.length + Integer.BYTES * 2 + message.length);
        request.put(PRODUCE).putShort((short) topicBytes.length).put(topicBytes)
                .putInt(partition).putInt(message.length).put(message).flip();
        return request;
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = topicBytes(topic);
        if (partition < 0 || offset < 0 || maxBytes < 1) {
            throw new IllegalArgumentException("partition and offset must be non-negative; maxBytes must be positive");
        }
        ByteBuffer request = ByteBuffer.allocate(1 + Short.BYTES + topicBytes.length + Integer.BYTES + Long.BYTES + Integer.BYTES);
        request.put(FETCH).putShort((short) topicBytes.length).put(topicBytes)
                .putInt(partition).putLong(offset).putInt(maxBytes).flip();
        return request;
    }

    public static ByteBuffer encodeMetadataRequest() {
        return ByteBuffer.wrap(new byte[] {METADATA});
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = topicBytes(topic);
        if (numPartitions < 1 || replicationFactor < 1) {
            throw new IllegalArgumentException("partitions and replication factor must be positive");
        }
        ByteBuffer request = ByteBuffer.allocate(1 + Short.BYTES + topicBytes.length + Integer.BYTES + Short.BYTES);
        request.put(CREATE_TOPIC).putShort((short) topicBytes.length).put(topicBytes)
                .putInt(numPartitions).putShort(replicationFactor).flip();
        return request;
    }

    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        byte[] topicBytes = topicBytes(topic);
        if (partition < 0 || offset < 0) {
            throw new IllegalArgumentException("partition and offset must be non-negative");
        }
        if (message == null) throw new IllegalArgumentException("message must not be null");
        ByteBuffer request = ByteBuffer.allocate(1 + Short.BYTES + topicBytes.length + Integer.BYTES
                + Long.BYTES + Integer.BYTES + message.length);
        request.put(REPLICATE).putShort((short) topicBytes.length).put(topicBytes)
                .putInt(partition).putLong(offset).putInt(message.length).put(message).flip();
        return request;
    }

    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = topicBytes(topic);
        ByteBuffer request = ByteBuffer.allocate(1 + Short.BYTES + topicBytes.length);
        request.put(TOPIC_NOTIFICATION).putShort((short) topicBytes.length).put(topicBytes).flip();
        return request;
    }

    private static byte[] topicBytes(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        byte[] bytes = topic.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("topic name is too long");
        }
        return bytes;
    }

}
