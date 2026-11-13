package com.simplekafka.broker;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerServerTest {
    @TempDir Path temporaryDirectory;

    @Test
    void startsRegistersAndServesCreateProduceFetchAndMetadataRequests() throws Exception {
        try (TestingServer zooKeeper = new TestingServer()) {
            int port = availablePort();
            try (BrokerServer broker = new BrokerServer(1, "127.0.0.1", port,
                    zooKeeper.getConnectString(), temporaryDirectory.resolve("broker-1"))) {
                broker.start();
                assertTrue(broker.isRunning());
                assertTrue(broker.isController());

                ByteBuffer create = Protocol.encodeCreateTopicRequest("orders", 2, (short) 1);
                try (DataInputStream response = send(broker.getPort(), create)) {
                    assertEquals(Protocol.CREATE_TOPIC_RESPONSE, response.readUnsignedByte());
                    assertTrue(response.readBoolean());
                }

                try (DataInputStream response = send(broker.getPort(),
                        Protocol.encodeProduceRequest("orders", 0, bytes("first")))) {
                    assertEquals(Protocol.PRODUCE_RESPONSE, response.readUnsignedByte());
                    assertEquals(0, response.readLong());
                }
                try (DataInputStream response = send(broker.getPort(),
                        Protocol.encodeProduceRequest("orders", 0, bytes("second")))) {
                    assertEquals(Protocol.PRODUCE_RESPONSE, response.readUnsignedByte());
                    assertEquals(1, response.readLong());
                }

                try (DataInputStream response = send(broker.getPort(), fetch("orders", 0, 0, 1024))) {
                    assertEquals(Protocol.FETCH_RESPONSE, response.readUnsignedByte());
                    assertEquals(2, response.readInt());
                    assertEquals(0, response.readLong());
                    assertBytes(bytes("first"), response);
                    assertEquals(1, response.readLong());
                    assertBytes(bytes("second"), response);
                }

                try (DataInputStream response = send(broker.getPort(), Protocol.encodeMetadataRequest())) {
                    assertEquals(Protocol.METADATA_RESPONSE, response.readUnsignedByte());
                    assertEquals(1, response.readInt());
                    assertEquals(1, response.readInt());
                    assertEquals("127.0.0.1", readString(response));
                    assertEquals(broker.getPort(), response.readInt());
                    assertEquals(1, response.readInt());
                    assertEquals("orders", readString(response));
                    assertEquals(2, response.readInt());
                    assertEquals(0, response.readInt());
                    assertEquals(1, response.readInt());
                    assertEquals(1, response.readInt());
                    assertEquals(1, response.readInt());
                }
            }
        }
    }

    @Test
    void forwardsProduceAndReplicatesToAssignedFollower() throws Exception {
        try (TestingServer zooKeeper = new TestingServer();
             BrokerServer leader = new BrokerServer(1, "127.0.0.1", availablePort(),
                     zooKeeper.getConnectString(), temporaryDirectory.resolve("leader"));
             BrokerServer follower = new BrokerServer(2, "127.0.0.1", availablePort(),
                     zooKeeper.getConnectString(), temporaryDirectory.resolve("follower"))) {
            leader.start();
            follower.start();
            awaitBrokerCount(leader, 2);
            try (DataInputStream response = send(leader.getPort(),
                    Protocol.encodeCreateTopicRequest("replicated", 1, (short) 2))) {
                assertEquals(Protocol.CREATE_TOPIC_RESPONSE, response.readUnsignedByte());
                assertTrue(response.readBoolean());
            }
            awaitTopicOnBroker(follower, "replicated");

            try (DataInputStream response = send(follower.getPort(),
                    Protocol.encodeProduceRequest("replicated", 0, bytes("on both brokers")))) {
                assertEquals(Protocol.PRODUCE_RESPONSE, response.readUnsignedByte());
                assertEquals(0, response.readLong());
            }
            try (DataInputStream response = send(follower.getPort(), fetch("replicated", 0, 0, 1024))) {
                assertEquals(Protocol.FETCH_RESPONSE, response.readUnsignedByte());
                assertEquals(1, response.readInt());
                assertEquals(0, response.readLong());
                assertBytes(bytes("on both brokers"), response);
            }
        }
    }

    @Test
    void reloadsTopicAndPartitionDataAfterBrokerRestart() throws Exception {
        try (TestingServer zooKeeper = new TestingServer()) {
            int port = availablePort();
            Path dataDirectory = temporaryDirectory.resolve("restart-data");
            try (BrokerServer first = new BrokerServer(4, "127.0.0.1", port,
                    zooKeeper.getConnectString(), dataDirectory)) {
                first.start();
                try (DataInputStream response = send(first.getPort(),
                        Protocol.encodeCreateTopicRequest("persisted", 1, (short) 1))) {
                    assertEquals(Protocol.CREATE_TOPIC_RESPONSE, response.readUnsignedByte());
                }
                try (DataInputStream response = send(first.getPort(),
                        Protocol.encodeProduceRequest("persisted", 0, bytes("saved")))) {
                    assertEquals(Protocol.PRODUCE_RESPONSE, response.readUnsignedByte());
                    assertEquals(0, response.readLong());
                }
            }

            try (BrokerServer restarted = new BrokerServer(4, "127.0.0.1", port,
                    zooKeeper.getConnectString(), dataDirectory)) {
                restarted.start();
                try (DataInputStream response = send(restarted.getPort(), fetch("persisted", 0, 0, 100))) {
                    assertEquals(Protocol.FETCH_RESPONSE, response.readUnsignedByte());
                    assertEquals(1, response.readInt());
                    assertEquals(0, response.readLong());
                    assertBytes(bytes("saved"), response);
                }
            }
        }
    }

    @Test
    void reportsUnknownTopicsAndInvalidTopicNamesAsProtocolErrors() throws Exception {
        try (TestingServer zooKeeper = new TestingServer();
             BrokerServer broker = new BrokerServer(3, "127.0.0.1", availablePort(),
                     zooKeeper.getConnectString(), temporaryDirectory.resolve("invalid"))) {
            broker.start();
            try (DataInputStream response = send(broker.getPort(),
                    Protocol.encodeProduceRequest("missing", 0, bytes("x")))) {
                assertEquals(Protocol.ERROR_RESPONSE, response.readUnsignedByte());
                assertTrue(readError(response).contains("unknown local partition"));
            }
            try (DataInputStream response = send(broker.getPort(),
                    Protocol.encodeCreateTopicRequest("../outside", 1, (short) 1))) {
                assertEquals(Protocol.ERROR_RESPONSE, response.readUnsignedByte());
                assertTrue(readError(response).contains("topic name"));
            }
        }
    }

    private static void awaitBrokerCount(BrokerServer broker, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
        while (System.nanoTime() < deadline) {
            try (DataInputStream response = send(broker.getPort(), Protocol.encodeMetadataRequest())) {
                response.readUnsignedByte();
                if (response.readInt() == expected) return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("broker registry did not reach " + expected + " members");
    }

    private static void awaitTopicOnBroker(BrokerServer broker, String topic) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
        while (System.nanoTime() < deadline) {
            try (DataInputStream response = send(broker.getPort(), fetch(topic, 0, 0, 1024))) {
                int type = response.readUnsignedByte();
                if (type == Protocol.FETCH_RESPONSE) return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("topic was not loaded by follower");
    }

    private static int availablePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static DataInputStream send(int port, ByteBuffer request) throws Exception {
        Socket socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(5_000);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.write(request.array(), request.position(), request.remaining());
        output.flush();
        return new SocketResponse(socket, new DataInputStream(socket.getInputStream()));
    }

    private static ByteBuffer fetch(String topic, int partition, long offset, int maxBytes) {
        byte[] name = bytes(topic);
        ByteBuffer request = ByteBuffer.allocate(1 + 2 + name.length + 4 + 8 + 4);
        request.put(Protocol.FETCH).putShort((short) name.length).put(name).putInt(partition)
                .putLong(offset).putInt(maxBytes).flip();
        return request;
    }

    private static void assertBytes(byte[] expected, DataInputStream input) throws Exception {
        int length = input.readInt();
        byte[] actual = new byte[length];
        input.readFully(actual);
        org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
    }

    private static String readString(DataInputStream input) throws Exception {
        int length = input.readUnsignedShort();
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String readError(DataInputStream input) throws Exception {
        return readString(input);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class SocketResponse extends DataInputStream {
        private final Socket socket;

        private SocketResponse(Socket socket, DataInputStream input) throws Exception {
            super(input);
            this.socket = socket;
        }

        @Override
        public void close() throws java.io.IOException {
            socket.close();
            super.close();
        }
    }
}
