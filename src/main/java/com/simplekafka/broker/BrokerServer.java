package com.simplekafka.broker;

import org.apache.zookeeper.KeeperException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A small TCP broker that connects the request protocol to ZooKeeper and partition logs. */
public final class BrokerServer implements AutoCloseable {
    private static final int MAX_REQUEST_BYTES = 16 * 1024 * 1024;
    private static final Pattern BROKER_ID = Pattern.compile("\\\"id\\\":(\\d+)");
    private static final Pattern BROKER_HOST = Pattern.compile("\\\"host\\\":\\\"((?:\\\\.|[^\\\"])*)\\\"");
    private static final Pattern BROKER_PORT = Pattern.compile("\\\"port\\\":(\\d+)");
    private static final Pattern CONTROLLER_ID = Pattern.compile("\\\"broker\\\":(\\d+)");

    private final int brokerId;
    private final String host;
    private final int requestedPort;
    private final String zooKeeperConnectString;
    private final Path dataDirectory;
    private final Map<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final ExecutorService workers;
    private final ZookeeperClient zooKeeper;
    private volatile ServerSocket serverSocket;
    private volatile Thread acceptThread;
    private volatile boolean controller;
    private volatile int port;

    public BrokerServer(int brokerId, String host, int port, String zooKeeperConnectString,
                        Path dataDirectory) {
        this(brokerId, host, port, zooKeeperConnectString, dataDirectory, 10);
    }

    public BrokerServer(int brokerId, String host, int port, String zooKeeperConnectString,
                        Path dataDirectory, int workerCount) {
        if (brokerId < 0) throw new IllegalArgumentException("broker id must be non-negative");
        if (host == null || host.trim().isEmpty()) throw new IllegalArgumentException("host must not be blank");
        if (port < 0 || port > 65535) throw new IllegalArgumentException("port must be between 0 and 65535");
        if (zooKeeperConnectString == null || zooKeeperConnectString.trim().isEmpty()) {
            throw new IllegalArgumentException("ZooKeeper connect string must not be blank");
        }
        if (dataDirectory == null) throw new IllegalArgumentException("data directory must not be null");
        if (workerCount < 1) throw new IllegalArgumentException("worker count must be positive");
        this.brokerId = brokerId;
        this.host = host;
        this.requestedPort = port;
        this.zooKeeperConnectString = zooKeeperConnectString;
        this.dataDirectory = dataDirectory;
        this.workers = Executors.newFixedThreadPool(workerCount, runnable -> {
            Thread thread = new Thread(runnable, "broker-" + brokerId + "-request");
            thread.setDaemon(true);
            return thread;
        });
        this.zooKeeper = new ZookeeperClient(zooKeeperConnectString);
    }

    /** Binds the listener, registers this broker, loads topics, then begins accepting requests. */
    public synchronized void start() throws IOException, KeeperException, InterruptedException {
        if (running.get()) throw new IllegalStateException("broker is already running");
        ServerSocket listener = new ServerSocket();
        try {
            listener.setReuseAddress(true);
            listener.bind(new InetSocketAddress(host, requestedPort));
            this.serverSocket = listener;
            this.port = listener.getLocalPort();
            Files.createDirectories(dataDirectory);
            zooKeeper.connect();
            if (!zooKeeper.registerBroker(new BrokerInfo(brokerId, host, port))) {
                throw new IOException("broker id is already registered: " + brokerId);
            }
            for (String id : zooKeeper.listChildren("/brokers/ids")) loadBroker(id);
            watchBrokers();
            controller = zooKeeper.tryBecomeController(new BrokerInfo(brokerId, host, port));
            if (!controller) zooKeeper.watchNode("/controller", this::tryBecomeController);
            loadTopics(zooKeeper.listChildren("/topics"));
            zooKeeper.watchChildren("/topics", this::loadTopicsQuietly);
            running.set(true);
            acceptThread = new Thread(this::acceptConnections, "broker-" + brokerId + "-accept");
            acceptThread.setDaemon(true);
            acceptThread.start();
        } catch (IOException | KeeperException | InterruptedException | RuntimeException failure) {
            try {
                listener.close();
            } catch (IOException closeError) {
                failure.addSuppressed(closeError);
            }
            try {
                zooKeeper.close();
            } catch (InterruptedException closeError) {
                Thread.currentThread().interrupt();
                failure.addSuppressed(closeError);
            }
            throw failure;
        }
    }

    public int getPort() {
        return port;
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isController() {
        return controller;
    }

    @Override
    public synchronized void close() throws IOException {
        running.set(false);
        IOException failure = null;
        ServerSocket listener = serverSocket;
        if (listener != null) {
            try {
                listener.close();
            } catch (IOException error) {
                failure = error;
            }
        }
        workers.shutdownNow();
        for (Topic topic : topics.values()) {
            for (LocalPartition partition : topic.partitions.values()) {
                try {
                    partition.log.close();
                } catch (IOException error) {
                    if (failure == null) failure = error;
                    else failure.addSuppressed(error);
                }
            }
        }
        topics.clear();
        try {
            zooKeeper.close();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            IOException wrapped = new IOException("interrupted while closing ZooKeeper", error);
            if (failure == null) failure = wrapped;
            else failure.addSuppressed(wrapped);
        }
        if (failure != null) throw failure;
    }

    private void acceptConnections() {
        while (running.get()) {
            try {
                Socket socket = serverSocket.accept();
                workers.execute(() -> handleConnection(socket));
            } catch (IOException error) {
                if (running.get()) {
                    // A listener error ends this accept loop; callers can observe isRunning().
                    running.set(false);
                }
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (Socket client = socket) {
            client.setSoTimeout(10_000);
            DataInputStream input = new DataInputStream(client.getInputStream());
            DataOutputStream output = new DataOutputStream(client.getOutputStream());
            try {
                int type = input.read();
                if (type < 0) return;
                switch (type) {
                    case Protocol.PRODUCE:
                        handleProduce(input, output);
                        break;
                    case Protocol.FETCH:
                        handleFetch(input, output);
                        break;
                    case Protocol.METADATA:
                        handleMetadata(output);
                        break;
                    case Protocol.CREATE_TOPIC:
                        handleCreateTopic(input, output);
                        break;
                    case Protocol.REPLICATE:
                        handleReplicate(input, output);
                        break;
                    case Protocol.TOPIC_NOTIFICATION:
                        String notifiedTopic = readTopic(input);
                        loadTopic(notifiedTopic);
                        output.writeByte(Protocol.TOPIC_NOTIFICATION);
                        break;
                    default:
                        writeError(output, "unknown request type: " + type);
                }
            } catch (IllegalArgumentException | EOFException error) {
                writeError(output, error.getMessage());
            } catch (Exception error) {
                writeError(output, error.getMessage() == null ? "request failed" : error.getMessage());
            }
            output.flush();
        } catch (IOException ignored) {
            // The peer may close while the broker is reading or responding.
        }
    }

    private void handleProduce(DataInputStream input, DataOutputStream output) throws Exception {
        String topicName = readTopic(input);
        int partitionId = input.readInt();
        int messageLength = readBoundedLength(input);
        byte[] message = new byte[messageLength];
        input.readFully(message);
        LocalPartition partition = requirePartition(topicName, partitionId);
        PartitionState state = partition.state;
        if (state.leaderId != brokerId) {
            BrokerInfo leader = requireBroker(state.leaderId);
            long offset = forwardProduce(leader, topicName, partitionId, message);
            output.writeByte(Protocol.PRODUCE_RESPONSE);
            output.writeLong(offset);
            return;
        }
        long offset = partition.log.append(message);
        for (int replicaId : state.replicaIds) {
            if (replicaId != brokerId) replicate(requireBroker(replicaId), topicName, partitionId, offset, message);
        }
        output.writeByte(Protocol.PRODUCE_RESPONSE);
        output.writeLong(offset);
    }

    private void handleFetch(DataInputStream input, DataOutputStream output) throws Exception {
        String topicName = readTopic(input);
        int partitionId = input.readInt();
        long offset = input.readLong();
        int maxBytes = input.readInt();
        if (partitionId < 0 || offset < 0 || maxBytes < 1 || maxBytes > MAX_REQUEST_BYTES) {
            throw new IllegalArgumentException("invalid fetch bounds");
        }
        LocalPartition partition = requirePartition(topicName, partitionId);
        if (offset > partition.log.getNextOffset()) throw new IllegalArgumentException("offset is beyond the log end");
        List<byte[]> messages = partition.log.readMessages(offset, maxBytes);
        output.writeByte(Protocol.FETCH_RESPONSE);
        output.writeInt(messages.size());
        for (int i = 0; i < messages.size(); i++) {
            byte[] message = messages.get(i);
            output.writeLong(offset + i);
            output.writeInt(message.length);
            output.write(message);
        }
    }

    private void handleMetadata(DataOutputStream output) throws IOException {
        List<BrokerInfo> liveBrokers = new ArrayList<>(brokers.values());
        liveBrokers.sort(Comparator.comparingInt(BrokerInfo::getId));
        output.writeByte(Protocol.METADATA_RESPONSE);
        output.writeInt(liveBrokers.size());
        for (BrokerInfo broker : liveBrokers) {
            writeBroker(output, broker);
        }
        List<String> topicNames = new ArrayList<>(topics.keySet());
        Collections.sort(topicNames);
        output.writeInt(topicNames.size());
        for (String topicName : topicNames) {
            writeTopic(output, topicName);
            output.writeInt(topics.get(topicName).states.size());
        }
    }

    private void handleCreateTopic(DataInputStream input, DataOutputStream output) throws Exception {
        String topicName = readTopic(input);
        validateTopicName(topicName);
        int partitionCount = input.readInt();
        int replicationFactor = input.readUnsignedShort();
        if (partitionCount < 1 || replicationFactor < 1 || replicationFactor > brokers.size()) {
            throw new IllegalArgumentException("partition count and replication factor must be positive and replication must fit the live brokers");
        }
        if (!controller) throw new IllegalStateException("only the controller can create topics");
        String path = "/topics/" + topicName;
        if (zooKeeper.nodeExists(path)) throw new IllegalArgumentException("topic already exists: " + topicName);

        List<Integer> brokerIds = new ArrayList<>(brokers.keySet());
        Collections.sort(brokerIds);
        List<PartitionState> states = new ArrayList<>();
        for (int id = 0; id < partitionCount; id++) {
            List<Integer> replicas = new ArrayList<>();
            for (int replica = 0; replica < replicationFactor; replica++) {
                replicas.add(brokerIds.get((id + replica) % brokerIds.size()));
            }
            states.add(new PartitionState(id, replicas.get(0), replicas));
        }
        String metadata = TopicDefinition.encode(partitionCount, replicationFactor, states);
        zooKeeper.createPersistentNode(path, metadata);
        loadTopic(topicName);
        notifyTopic(topicName);
        output.writeByte(Protocol.CREATE_TOPIC_RESPONSE);
        output.writeBoolean(true);
    }

    private void handleReplicate(DataInputStream input, DataOutputStream output) throws Exception {
        String topicName = readTopic(input);
        int partitionId = input.readInt();
        long expectedOffset = input.readLong();
        int messageLength = readBoundedLength(input);
        byte[] message = new byte[messageLength];
        input.readFully(message);
        LocalPartition partition = requirePartition(topicName, partitionId);
        if (partition.state.leaderId == brokerId) throw new IllegalStateException("leader cannot accept a replica write");
        if (partition.log.getNextOffset() != expectedOffset) {
            throw new IllegalStateException("replica offset mismatch: expected " + expectedOffset
                    + " but next offset is " + partition.log.getNextOffset());
        }
        long actualOffset = partition.log.append(message);
        output.writeByte(Protocol.REPLICATE_ACK);
        output.writeLong(actualOffset);
    }

    private void watchBrokers() throws KeeperException, InterruptedException {
        zooKeeper.watchChildren("/brokers/ids", ids -> {
            brokers.keySet().removeIf(id -> !ids.contains(String.valueOf(id)));
            for (String id : ids) loadBrokerQuietly(id);
        });
    }

    private void loadBroker(String id) throws KeeperException, InterruptedException {
        String value = zooKeeper.readData("/brokers/ids/" + id);
        Matcher idMatch = BROKER_ID.matcher(value);
        Matcher hostMatch = BROKER_HOST.matcher(value);
        Matcher portMatch = BROKER_PORT.matcher(value);
        if (!idMatch.find() || !hostMatch.find() || !portMatch.find()) {
            throw new IllegalArgumentException("invalid broker metadata for id " + id);
        }
        String parsedHost = hostMatch.group(1).replace("\\\"", "\"").replace("\\\\", "\\");
        BrokerInfo broker = new BrokerInfo(Integer.parseInt(idMatch.group(1)), parsedHost,
                Integer.parseInt(portMatch.group(1)));
        brokers.put(broker.getId(), broker);
    }

    private void loadBrokerQuietly(String id) {
        try {
            loadBroker(id);
        } catch (KeeperException | InterruptedException | RuntimeException error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        }
    }

    private void tryBecomeController() {
        try {
            controller = zooKeeper.tryBecomeController(new BrokerInfo(brokerId, host, port));
        } catch (KeeperException | InterruptedException error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        }
    }

    private void loadTopicsQuietly(List<String> names) {
        try {
            loadTopics(names);
        } catch (IOException | KeeperException | InterruptedException | RuntimeException error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        }
    }

    private void loadTopics(List<String> names) throws IOException, KeeperException, InterruptedException {
        for (String name : names) loadTopic(name);
    }

    private synchronized void loadTopic(String name) throws IOException, KeeperException, InterruptedException {
        validateTopicName(name);
        TopicDefinition definition = TopicDefinition.decode(zooKeeper.readData("/topics/" + name));
        Topic existing = topics.get(name);
        if (existing != null && existing.states.equals(definition.states)) return;
        Topic replacement = new Topic(definition.states);
        Path topicDirectory = dataDirectory.resolve(name);
        for (PartitionState state : definition.states) {
            if (!state.replicaIds.contains(brokerId)) continue;
            Path partitionDirectory = topicDirectory.resolve(Integer.toString(state.partitionId));
            Partition partition = new Partition(state.partitionId, partitionDirectory);
            replacement.partitions.put(state.partitionId, new LocalPartition(state, partition));
        }
        Topic prior = topics.put(name, replacement);
        closeTopic(prior);
    }

    private void notifyTopic(String topicName) {
        for (BrokerInfo broker : brokers.values()) {
            if (broker.getId() == brokerId) continue;
            workers.execute(() -> {
                try (Socket socket = connect(broker)) {
                    DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                    ByteBuffer notification = Protocol.encodeTopicNotification(topicName);
                    output.write(notification.array(), notification.position(), notification.remaining());
                    output.flush();
                    socket.getInputStream().read();
                } catch (IOException ignored) {
                    // The ZooKeeper topic watch will load it when the remote broker is available.
                }
            });
        }
    }

    private long forwardProduce(BrokerInfo leader, String topic, int partition, byte[] message) throws IOException {
        try (Socket socket = connect(leader)) {
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, message);
            output.write(request.array(), request.position(), request.remaining());
            output.flush();
            DataInputStream input = new DataInputStream(socket.getInputStream());
            int response = input.readUnsignedByte();
            if (response == Protocol.ERROR_RESPONSE) throw readRemoteError(input);
            if (response != Protocol.PRODUCE_RESPONSE) throw new IOException("unexpected produce response: " + response);
            return input.readLong();
        }
    }

    private void replicate(BrokerInfo follower, String topic, int partition, long offset, byte[] message)
            throws IOException {
        try (Socket socket = connect(follower)) {
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            ByteBuffer request = Protocol.encodeReplicateRequest(topic, partition, offset, message);
            output.write(request.array(), request.position(), request.remaining());
            output.flush();
            DataInputStream input = new DataInputStream(socket.getInputStream());
            int response = input.readUnsignedByte();
            if (response == Protocol.ERROR_RESPONSE) throw readRemoteError(input);
            if (response != Protocol.REPLICATE_ACK || input.readLong() != offset) {
                throw new IOException("replica did not acknowledge offset " + offset);
            }
        }
    }

    private Socket connect(BrokerInfo broker) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(broker.getHost(), broker.getPort()), 3_000);
        socket.setSoTimeout(5_000);
        return socket;
    }

    private LocalPartition requirePartition(String topicName, int partitionId) {
        if (partitionId < 0) throw new IllegalArgumentException("partition id must be non-negative");
        Topic topic = topics.get(topicName);
        LocalPartition partition = topic == null ? null : topic.partitions.get(partitionId);
        if (partition == null) throw new IllegalArgumentException("unknown local partition " + topicName + "/" + partitionId);
        return partition;
    }

    private BrokerInfo requireBroker(int id) {
        BrokerInfo broker = brokers.get(id);
        if (broker == null) throw new IllegalStateException("broker is not available: " + id);
        return broker;
    }

    private static String readTopic(DataInputStream input) throws IOException {
        int length = input.readUnsignedShort();
        if (length < 1 || length > Short.MAX_VALUE) throw new IllegalArgumentException("invalid topic name length");
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int readBoundedLength(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > MAX_REQUEST_BYTES) throw new IllegalArgumentException("invalid message length");
        return length;
    }

    private static void writeTopic(DataOutputStream output, String topic) throws IOException {
        byte[] bytes = topic.getBytes(StandardCharsets.UTF_8);
        output.writeShort(bytes.length);
        output.write(bytes);
    }

    private static void writeBroker(DataOutputStream output, BrokerInfo broker) throws IOException {
        output.writeInt(broker.getId());
        byte[] bytes = broker.getHost().getBytes(StandardCharsets.UTF_8);
        output.writeShort(bytes.length);
        output.write(bytes);
        output.writeInt(broker.getPort());
    }

    private static void writeError(DataOutputStream output, String message) throws IOException {
        byte[] bytes = (message == null ? "request failed" : message).getBytes(StandardCharsets.UTF_8);
        int length = Math.min(bytes.length, Short.MAX_VALUE);
        output.writeByte(Protocol.ERROR_RESPONSE);
        output.writeShort(length);
        output.write(bytes, 0, length);
    }

    private static IOException readRemoteError(DataInputStream input) throws IOException {
        int length = input.readUnsignedShort();
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new IOException(new String(bytes, StandardCharsets.UTF_8));
    }

    private static void validateTopicName(String topic) {
        if (topic == null || topic.trim().isEmpty() || topic.equals(".") || topic.equals("..")
                || !topic.matches("[A-Za-z0-9._-]{1,249}")) {
            throw new IllegalArgumentException("topic name must use letters, numbers, dot, underscore, or hyphen");
        }
    }

    private static void closeTopic(Topic topic) throws IOException {
        if (topic == null) return;
        IOException failure = null;
        for (LocalPartition partition : topic.partitions.values()) {
            try {
                partition.log.close();
            } catch (IOException error) {
                if (failure == null) failure = error;
                else failure.addSuppressed(error);
            }
        }
        if (failure != null) throw failure;
    }

    private static final class Topic {
        private final List<PartitionState> states;
        private final Map<Integer, LocalPartition> partitions = new ConcurrentHashMap<>();

        private Topic(List<PartitionState> states) {
            this.states = states;
        }
    }

    private static final class LocalPartition {
        private final PartitionState state;
        private final Partition log;

        private LocalPartition(PartitionState state, Partition log) {
            this.state = state;
            this.log = log;
        }
    }

    private static final class PartitionState {
        private final int partitionId;
        private final int leaderId;
        private final List<Integer> replicaIds;

        private PartitionState(int partitionId, int leaderId, List<Integer> replicaIds) {
            this.partitionId = partitionId;
            this.leaderId = leaderId;
            this.replicaIds = Collections.unmodifiableList(new ArrayList<>(replicaIds));
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PartitionState)) return false;
            PartitionState state = (PartitionState) other;
            return partitionId == state.partitionId && leaderId == state.leaderId && replicaIds.equals(state.replicaIds);
        }

        @Override
        public int hashCode() {
            return 31 * (31 * partitionId + leaderId) + replicaIds.hashCode();
        }
    }

    private static final class TopicDefinition {
        private final List<PartitionState> states;

        private TopicDefinition(List<PartitionState> states) {
            this.states = states;
        }

        private static String encode(int partitionCount, int replicationFactor, List<PartitionState> states) {
            StringBuilder data = new StringBuilder("v1|").append(partitionCount).append('|').append(replicationFactor);
            for (PartitionState state : states) {
                data.append('\n').append(state.partitionId).append(':').append(state.leaderId).append(':');
                for (int i = 0; i < state.replicaIds.size(); i++) {
                    if (i > 0) data.append(',');
                    data.append(state.replicaIds.get(i));
                }
            }
            return data.toString();
        }

        private static TopicDefinition decode(String data) {
            String[] lines = data.split("\\n");
            String[] header = lines[0].split("\\|");
            if (header.length != 3 || !"v1".equals(header[0])) throw new IllegalArgumentException("invalid topic metadata");
            int partitions = Integer.parseInt(header[1]);
            int replicationFactor = Integer.parseInt(header[2]);
            if (partitions < 1 || replicationFactor < 1 || lines.length != partitions + 1) {
                throw new IllegalArgumentException("incomplete topic metadata");
            }
            List<PartitionState> states = new ArrayList<>();
            for (int line = 1; line < lines.length; line++) {
                String[] fields = lines[line].split(":", -1);
                if (fields.length != 3) throw new IllegalArgumentException("invalid partition metadata");
                int partitionId = Integer.parseInt(fields[0]);
                int leaderId = Integer.parseInt(fields[1]);
                String[] ids = fields[2].split(",");
                if (ids.length != replicationFactor) throw new IllegalArgumentException("invalid replica assignment");
                List<Integer> replicas = new ArrayList<>();
                for (String id : ids) replicas.add(Integer.parseInt(id));
                if (replicas.get(0) != leaderId) throw new IllegalArgumentException("leader must be the first replica");
                states.add(new PartitionState(partitionId, leaderId, replicas));
            }
            return new TopicDefinition(states);
        }
    }
}
