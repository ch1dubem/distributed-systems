package com.simplekafka.broker;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Coordinates broker registration, metadata nodes, and controller election. */
public final class ZookeeperClient implements AutoCloseable {
    private static final int DEFAULT_SESSION_TIMEOUT_MS = 10_000;
    private final String connectString;
    private final int sessionTimeoutMs;
    private final CountDownLatch connected = new CountDownLatch(1);
    private volatile ZooKeeper zooKeeper;

    public ZookeeperClient(String connectString) {
        this(connectString, DEFAULT_SESSION_TIMEOUT_MS);
    }

    public ZookeeperClient(String connectString, int sessionTimeoutMs) {
        if (connectString == null || connectString.trim().isEmpty()) {
            throw new IllegalArgumentException("connect string must not be blank");
        }
        if (sessionTimeoutMs < 1_000) {
            throw new IllegalArgumentException("session timeout must be at least 1000 ms");
        }
        this.connectString = connectString;
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public synchronized void connect() throws IOException, InterruptedException, KeeperException {
        if (zooKeeper != null && zooKeeper.getState().isConnected()) {
            return;
        }
        zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        if (!connected.await(sessionTimeoutMs * 2L, TimeUnit.MILLISECONDS)) {
            zooKeeper.close();
            zooKeeper = null;
            throw new IOException("timed out connecting to ZooKeeper at " + connectString);
        }
        ensurePersistentPath("/brokers");
        ensurePersistentPath("/brokers/ids");
        ensurePersistentPath("/topics");
    }

    public boolean registerBroker(BrokerInfo broker) throws KeeperException, InterruptedException {
        if (broker == null) throw new IllegalArgumentException("broker must not be null");
        String data = "{\"id\":" + broker.getId() + ",\"host\":\"" + jsonEscape(broker.getHost())
                + "\",\"port\":" + broker.getPort() + "}";
        return createEphemeralNode("/brokers/ids/" + broker.getId(), data);
    }

    public boolean tryBecomeController(BrokerInfo broker) throws KeeperException, InterruptedException {
        if (broker == null) throw new IllegalArgumentException("broker must not be null");
        String data = "{\"broker\":" + broker.getId() + "}";
        return createEphemeralNode("/controller", data);
    }

    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        validatePathAndData(path, data);
        ensureParentPath(path);
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        try {
            client().create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException alreadyExists) {
            client().setData(path, bytes, -1);
        }
    }

    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        validatePathAndData(path, data);
        ensureParentPath(path);
        try {
            client().create(path, data.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true;
        } catch (KeeperException.NodeExistsException alreadyExists) {
            return false;
        }
    }

    public String readData(String path) throws KeeperException, InterruptedException {
        if (path == null || path.isEmpty()) throw new IllegalArgumentException("path must not be blank");
        return new String(client().getData(path, false, null), StandardCharsets.UTF_8);
    }

    public boolean nodeExists(String path) throws KeeperException, InterruptedException {
        if (path == null || path.isEmpty()) throw new IllegalArgumentException("path must not be blank");
        return client().exists(path, false) != null;
    }

    /** Installs a one-shot child watch again after every child change. */
    public void watchChildren(String path, Consumer<List<String>> callback)
            throws KeeperException, InterruptedException {
        if (path == null || path.isEmpty() || callback == null) {
            throw new IllegalArgumentException("path and callback are required");
        }
        List<String> children = client().getChildren(path, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    watchChildren(path, callback);
                } catch (KeeperException | InterruptedException error) {
                    if (error instanceof InterruptedException) Thread.currentThread().interrupt();
                    throw new IllegalStateException("could not re-arm child watch for " + path, error);
                }
            }
        });
        callback.accept(Collections.unmodifiableList(new ArrayList<>(children)));
    }

    /** Re-arms an existence watch so callers also observe a later recreation. */
    public void watchNode(String path, Runnable callback) throws KeeperException, InterruptedException {
        if (path == null || path.isEmpty() || callback == null) {
            throw new IllegalArgumentException("path and callback are required");
        }
        client().exists(path, event -> {
            callback.run();
            try {
                watchNode(path, callback);
            } catch (KeeperException | InterruptedException error) {
                if (error instanceof InterruptedException) Thread.currentThread().interrupt();
                throw new IllegalStateException("could not re-arm node watch for " + path, error);
            }
        });
    }

    @Override
    public synchronized void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
            zooKeeper = null;
        }
    }

    private ZooKeeper client() {
        ZooKeeper current = zooKeeper;
        if (current == null || !current.getState().isConnected()) {
            throw new IllegalStateException("ZooKeeper client is not connected");
        }
        return current;
    }

    private void ensurePersistentPath(String path) throws KeeperException, InterruptedException {
        try {
            client().create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ignored) {
            // The path was created by another connection.
        }
    }

    private void ensureParentPath(String path) throws KeeperException, InterruptedException {
        int separator = path.lastIndexOf('/');
        String parent = separator <= 0 ? null : path.substring(0, separator);
        if (parent == null || parent.isEmpty()) return;
        int next = parent.indexOf('/', 1);
        while (next >= 0) {
            ensurePersistentPath(parent.substring(0, next));
            next = parent.indexOf('/', next + 1);
        }
        ensurePersistentPath(parent);
    }

    private static void validatePathAndData(String path, String data) {
        if (path == null || path.length() < 2 || path.charAt(0) != '/' || path.endsWith("/")) {
            throw new IllegalArgumentException("path must be an absolute ZooKeeper path");
        }
        if (data == null) throw new IllegalArgumentException("node data must not be null");
    }

    private static String jsonEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
