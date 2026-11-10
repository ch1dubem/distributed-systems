package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Small dependency-free checks; run with scripts/test.sh. */
public final class DayOneChecks {
    private static int checks;

    public static void main(String[] args) {
        brokerInfoChecks();
        protocolChecks();
        System.out.println("Passed " + checks + " checks");
    }

    private static void brokerInfoChecks() {
        BrokerInfo first = new BrokerInfo(3, "localhost", 9092);
        BrokerInfo sameId = new BrokerInfo(3, "broker.example", 9093);
        equal(first, sameId, "broker equality uses id");
        equal(first.hashCode(), sameId.hashCode(), "equal brokers share hash code");
        equal("localhost", first.getHost(), "host getter");
        equal(9092, first.getPort(), "port getter");
        fails(() -> new BrokerInfo(-1, "localhost", 9092), "negative broker id rejected");
        fails(() -> new BrokerInfo(1, " ", 9092), "blank host rejected");
        fails(() -> new BrokerInfo(1, "localhost", 65536), "out-of-range port rejected");
    }

    private static void protocolChecks() {
        byte[] message = "hello".getBytes(StandardCharsets.UTF_8);
        ByteBuffer payload = Protocol.encodeProduceRequest("events", 2, message).order(ByteOrder.BIG_ENDIAN);
        equal(Protocol.PRODUCE, payload.get(), "produce type");
        int topicLength = Short.toUnsignedInt(payload.getShort());
        byte[] topic = new byte[topicLength];
        payload.get(topic);
        equal("events", new String(topic, StandardCharsets.UTF_8), "produce topic");
        equal(2, payload.getInt(), "produce partition");
        int messageLength = payload.getInt();
        byte[] decodedMessage = new byte[messageLength];
        payload.get(decodedMessage);
        check(Arrays.equals(message, decodedMessage), "produce payload");

        ByteBuffer fetchPayload = Protocol.encodeFetchRequest("events", 1, 12L, 2048).order(ByteOrder.BIG_ENDIAN);
        equal(Protocol.FETCH, fetchPayload.get(), "fetch type");
        int fetchTopicLength = Short.toUnsignedInt(fetchPayload.getShort());
        fetchPayload.position(fetchPayload.position() + fetchTopicLength);
        equal(1, fetchPayload.getInt(), "fetch partition");
        equal(12L, fetchPayload.getLong(), "fetch offset");
        equal(2048, fetchPayload.getInt(), "fetch maximum bytes");

        ByteBuffer metadata = Protocol.encodeMetadataRequest();
        equal(Protocol.METADATA, metadata.get(), "metadata type");
        equal(0, metadata.remaining(), "metadata request has empty body");

        ByteBuffer createTopic = Protocol.encodeCreateTopicRequest("events", 4, (short) 2);
        equal(Protocol.CREATE_TOPIC, createTopic.get(), "create-topic type");
        int createTopicLength = Short.toUnsignedInt(createTopic.getShort());
        createTopic.position(createTopic.position() + createTopicLength);
        equal(4, createTopic.getInt(), "create-topic partition count");
        equal((short) 2, createTopic.getShort(), "create-topic replication factor");

        ByteBuffer replicate = Protocol.encodeReplicateRequest("events", 1, 12L, message);
        equal(Protocol.REPLICATE, replicate.get(), "replication type");
        int replicateTopicLength = Short.toUnsignedInt(replicate.getShort());
        replicate.position(replicate.position() + replicateTopicLength);
        equal(1, replicate.getInt(), "replication partition");
        equal(12L, replicate.getLong(), "replication offset");
        int replicatedMessageLength = replicate.getInt();
        byte[] replicatedMessage = new byte[replicatedMessageLength];
        replicate.get(replicatedMessage);
        check(Arrays.equals(message, replicatedMessage), "replication message");

        ByteBuffer notification = Protocol.encodeTopicNotification("events");
        equal(Protocol.TOPIC_NOTIFICATION, notification.get(), "topic-notification type");
        equal("events", readTopic(notification), "topic notification name");
        fails(() -> Protocol.encodeFetchRequest("events", -1, 0L, 4), "negative partition rejected");
        fails(() -> Protocol.encodeCreateTopicRequest("events", 0, (short) 1), "zero partitions rejected");
    }

    private static String readTopic(ByteBuffer buffer) {
        int length = Short.toUnsignedInt(buffer.getShort());
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void check(boolean condition, String name) {
        checks++;
        if (!condition) throw new AssertionError(name);
    }

    private static void equal(Object expected, Object actual, String name) {
        check(expected == null ? actual == null : expected.equals(actual), name + ": expected " + expected + ", got " + actual);
    }

    private static void fails(Runnable action, String name) {
        checks++;
        try {
            action.run();
        } catch (IllegalArgumentException expected) {
            return;
        }
        throw new AssertionError(name + ": expected IllegalArgumentException");
    }
}
