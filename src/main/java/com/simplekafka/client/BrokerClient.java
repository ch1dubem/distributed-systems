package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Client-side broker discovery and request handling. Each operation uses one TCP connection. */
public final class BrokerClient implements AutoCloseable {
    private static final int CONNECT_TIMEOUT_MS = 3_000;
    private static final int READ_TIMEOUT_MS = 5_000;
    private static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

    private final String bootstrapHost;
    private final int bootstrapPort;
    private final Map<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, Integer>> leaders = new ConcurrentHashMap<>();
    private volatile boolean initialized;
    private volatile boolean closed;

    public BrokerClient(String bootstrapHost, int bootstrapPort) {
        if (bootstrapHost == null || bootstrapHost.trim().isEmpty()) {
            throw new IllegalArgumentException("bootstrap host must not be blank");
        }
        if (bootstrapPort < 1 || bootstrapPort > 65535) {
            throw new IllegalArgumentException("bootstrap port must be between 1 and 65535");
        }
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
    }

    public synchronized void initialize() throws IOException {
        ensureOpen();
        refreshMetadata();
    }

    public synchronized void refreshMetadata() throws IOException {
        ensureOpen();
        try (Socket socket = connect(bootstrapHost, bootstrapPort)) {
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeByte(Protocol.METADATA);
            output.flush();
            DataInputStream input = new DataInputStream(socket.getInputStream());
            int type = input.readUnsignedByte();
            checkResponse(type, Protocol.METADATA_RESPONSE, input);

            int brokerCount = readCount(input, "broker");
            Map<Integer, BrokerInfo> newBrokers = new HashMap<>();
            for (int i = 0; i < brokerCount; i++) {
                int id = input.readInt();
                String host = readString(input);
                int port = input.readInt();
                newBrokers.put(id, new BrokerInfo(id, host, port));
            }
            int topicCount = readCount(input, "topic");
            Map<String, Map<Integer, Integer>> newLeaders = new HashMap<>();
            for (int i = 0; i < topicCount; i++) {
                String topic = readString(input);
                int partitionCount = readCount(input, "partition");
                Map<Integer, Integer> topicLeaders = new HashMap<>();
                for (int partition = 0; partition < partitionCount; partition++) {
                    int partitionId = input.readInt();
                    int leaderId = input.readInt();
                    if (partitionId < 0 || leaderId < 0) throw new IOException("invalid partition metadata");
                    topicLeaders.put(partitionId, leaderId);
                }
                newLeaders.put(topic, Collections.unmodifiableMap(topicLeaders));
            }
            brokers.clear();
            brokers.putAll(newBrokers);
            leaders.clear();
            leaders.putAll(newLeaders);
            initialized = true;
        }
    }

    public void createTopic(String topic, int partitions, short replicationFactor) throws IOException {
        ByteBuffer request;
        try {
            request = Protocol.encodeCreateTopicRequest(topic, partitions, replicationFactor);
        } catch (IllegalArgumentException error) {
            throw new IOException(error.getMessage(), error);
        }
        try (Socket socket = connect(bootstrapHost, bootstrapPort)) {
            writeRequest(socket, request);
            DataInputStream input = new DataInputStream(socket.getInputStream());
            int type = input.readUnsignedByte();
            checkResponse(type, Protocol.CREATE_TOPIC_RESPONSE, input);
            if (!input.readBoolean()) throw new IOException("broker did not create topic " + topic);
        }
        refreshMetadata();
    }

    public long send(String topic, int partition, byte[] message) throws IOException {
        ensureMetadata();
        BrokerInfo leader = leaderFor(topic, partition);
        ByteBuffer request;
        try {
            request = Protocol.encodeProduceRequest(topic, partition, message);
        } catch (IllegalArgumentException error) {
            throw new IOException(error.getMessage(), error);
        }
        try (Socket socket = connect(leader.getHost(), leader.getPort())) {
            writeRequest(socket, request);
            DataInputStream input = new DataInputStream(socket.getInputStream());
            int type = input.readUnsignedByte();
            checkResponse(type, Protocol.PRODUCE_RESPONSE, input);
            return input.readLong();
        }
    }

    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        ensureMetadata();
        BrokerInfo leader = leaderFor(topic, partition);
        if (maxBytes > MAX_RESPONSE_BYTES) {
            throw new IllegalArgumentException("offset must be non-negative and maxBytes must be between 1 and 16 MiB");
        }
        ByteBuffer request;
        try {
            request = Protocol.encodeFetchRequest(topic, partition, offset, maxBytes);
        } catch (IllegalArgumentException error) {
            throw new IOException(error.getMessage(), error);
        }
        try (Socket socket = connect(leader.getHost(), leader.getPort())) {
            writeRequest(socket, request);
            DataInputStream input = new DataInputStream(socket.getInputStream());
            int type = input.readUnsignedByte();
            checkResponse(type, Protocol.FETCH_RESPONSE, input);
            int count = readCount(input, "message");
            List<byte[]> messages = new ArrayList<>(count);
            long expectedOffset = offset;
            for (int i = 0; i < count; i++) {
                long actualOffset = input.readLong();
                if (actualOffset != expectedOffset++) throw new IOException("fetch response contains a non-sequential offset");
                int length = input.readInt();
                if (length < 0 || length > MAX_RESPONSE_BYTES) throw new IOException("invalid message length in fetch response");
                byte[] message = new byte[length];
                input.readFully(message);
                messages.add(message);
            }
            return messages;
        }
    }

    public int getPartitionCount(String topic) throws IOException {
        ensureMetadata();
        Map<Integer, Integer> topicLeaders = leaders.get(topic);
        if (topicLeaders == null) throw new IOException("unknown topic: " + topic);
        return topicLeaders.size();
    }

    @Override
    public void close() {
        closed = true;
        brokers.clear();
        leaders.clear();
    }

    private BrokerInfo leaderFor(String topic, int partition) throws IOException {
        Map<Integer, Integer> topicLeaders = leaders.get(topic);
        Integer leaderId = topicLeaders == null ? null : topicLeaders.get(partition);
        if (leaderId == null) throw new IOException("unknown topic partition: " + topic + "/" + partition);
        BrokerInfo leader = brokers.get(leaderId);
        if (leader == null) {
            refreshMetadata();
            leader = brokers.get(leaderId);
        }
        if (leader == null) throw new IOException("leader broker is not present in metadata: " + leaderId);
        return leader;
    }

    private void ensureMetadata() throws IOException {
        ensureOpen();
        if (!initialized) refreshMetadata();
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("client is closed");
    }

    private static Socket connect(String host, int port) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
        socket.setSoTimeout(READ_TIMEOUT_MS);
        return socket;
    }

    private static void writeRequest(Socket socket, ByteBuffer request) throws IOException {
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.write(request.array(), request.position(), request.remaining());
        output.flush();
    }

    private static void checkResponse(int actual, int expected, DataInputStream input) throws IOException {
        if (actual == Protocol.ERROR_RESPONSE) {
            int length = input.readUnsignedShort();
            byte[] bytes = new byte[length];
            input.readFully(bytes);
            throw new IOException(new String(bytes, StandardCharsets.UTF_8));
        }
        if (actual != expected) throw new IOException("unexpected response type: " + actual);
    }

    private static int readCount(DataInputStream input, String kind) throws IOException {
        int count = input.readInt();
        if (count < 0 || count > 100_000) throw new IOException("invalid " + kind + " count: " + count);
        return count;
    }

    private static String readString(DataInputStream input) throws IOException {
        int length = input.readUnsignedShort();
        if (length < 1 || length > Short.MAX_VALUE) throw new IOException("invalid string length");
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
