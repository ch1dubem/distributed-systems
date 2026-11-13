package com.simplekafka.client;

import com.simplekafka.broker.BrokerServer;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientApiTest {
    @TempDir Path temporaryDirectory;

    @Test
    void producerAndConsumerUseMetadataAndAdvanceOffsets() throws Exception {
        try (TestingServer zooKeeper = new TestingServer();
             BrokerServer broker = new BrokerServer(8, "127.0.0.1", availablePort(),
                     zooKeeper.getConnectString(), temporaryDirectory.resolve("broker"))) {
            broker.start();
            try (BrokerClient admin = new BrokerClient("127.0.0.1", broker.getPort())) {
                admin.createTopic("events", 2, (short) 1);
                assertEquals(2, admin.getPartitionCount("events"));
            }

            try (SimpleKafkaProducer producer = new SimpleKafkaProducer("127.0.0.1", broker.getPort(), "events")) {
                producer.initialize();
                assertEquals(0, producer.send("alpha"));
                assertEquals(0, producer.send("beta"));
                assertEquals(1, producer.send("gamma"));
            }

            try (SimpleKafkaConsumer first = new SimpleKafkaConsumer("127.0.0.1", broker.getPort(), "events", 0);
                 SimpleKafkaConsumer second = new SimpleKafkaConsumer("127.0.0.1", broker.getPort(), "events", 1)) {
                first.initialize();
                second.initialize();
                List<byte[]> firstBatch = first.poll();
                assertEquals(2, firstBatch.size());
                assertEquals("alpha", new String(firstBatch.get(0), StandardCharsets.UTF_8));
                assertEquals("gamma", new String(firstBatch.get(1), StandardCharsets.UTF_8));
                assertEquals(2, first.getCurrentOffset());
                assertEquals(0, first.poll().size());

                List<byte[]> secondBatch = second.poll();
                assertEquals(1, secondBatch.size());
                assertArrayEquals(bytes("beta"), secondBatch.get(0));
                assertEquals(1, second.getCurrentOffset());
            }
        }
    }

    @Test
    void rejectsUnknownTopicsAndClosedClients() throws Exception {
        try (TestingServer zooKeeper = new TestingServer();
             BrokerServer broker = new BrokerServer(9, "127.0.0.1", availablePort(),
                     zooKeeper.getConnectString(), temporaryDirectory.resolve("errors"))) {
            broker.start();
            BrokerClient client = new BrokerClient("127.0.0.1", broker.getPort());
            client.initialize();
            assertThrows(java.io.IOException.class, () -> client.getPartitionCount("missing"));
            client.close();
            assertThrows(java.io.IOException.class, client::refreshMetadata);
        }
    }

    private static int availablePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
