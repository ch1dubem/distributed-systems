package com.simplekafka.broker;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ZookeeperClientTest {
    @Test
    void createsBasePathsAndUpdatesPersistentMetadata() throws Exception {
        try (TestingServer server = new TestingServer();
             ZookeeperClient client = connect(server)) {
            assertTrue(client.nodeExists("/brokers/ids"));
            assertTrue(client.nodeExists("/topics"));
            assertFalse(client.nodeExists("/controller"));

            client.createPersistentNode("/topics/orders", "partitions=3");
            client.createPersistentNode("/topics/orders", "partitions=4");
            assertEquals("partitions=4", client.readData("/topics/orders"));
        }
    }

    @Test
    void registersEphemeralBrokerAndNotifiesChildWatchers() throws Exception {
        try (TestingServer server = new TestingServer();
             ZookeeperClient brokerClient = connect(server);
             ZookeeperClient observer = connect(server)) {
            CountDownLatch registered = new CountDownLatch(1);
            CountDownLatch removed = new CountDownLatch(1);
            AtomicReference<List<String>> latest = new AtomicReference<>();
            observer.watchChildren("/brokers/ids", children -> {
                latest.set(children);
                if (children.contains("7")) registered.countDown();
                if (!children.contains("7")) removed.countDown();
            });

            assertTrue(brokerClient.registerBroker(new BrokerInfo(7, "localhost", 9097)));
            assertTrue(registered.await(3, TimeUnit.SECONDS));
            assertEquals("{\"id\":7,\"host\":\"localhost\",\"port\":9097}",
                    brokerClient.readData("/brokers/ids/7"));

            brokerClient.close();
            assertTrue(removed.await(3, TimeUnit.SECONDS));
            assertFalse(latest.get().contains("7"));
        }
    }

    @Test
    void electsOnlyOneControllerAndAllowsReplacementAfterSessionCloses() throws Exception {
        try (TestingServer server = new TestingServer();
             ZookeeperClient first = connect(server);
             ZookeeperClient second = connect(server)) {
            BrokerInfo firstBroker = new BrokerInfo(1, "localhost", 9091);
            BrokerInfo secondBroker = new BrokerInfo(2, "localhost", 9092);
            CountDownLatch deleted = new CountDownLatch(1);

            assertTrue(first.tryBecomeController(firstBroker));
            assertFalse(second.tryBecomeController(secondBroker));
            second.watchNode("/controller", deleted::countDown);
            first.close();
            assertTrue(deleted.await(3, TimeUnit.SECONDS));
            assertTrue(second.tryBecomeController(secondBroker));
            assertEquals("{\"broker\":2}", second.readData("/controller"));
        }
    }

    private static ZookeeperClient connect(TestingServer server) throws Exception {
        ZookeeperClient client = new ZookeeperClient(server.getConnectString(), 3_000);
        client.connect();
        return client;
    }
}
