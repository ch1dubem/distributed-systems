package com.simplekafka.broker;

import java.util.Objects;

/** Identifies a broker endpoint in the cluster. */
public final class BrokerInfo {
    private final int id;
    private final String host;
    private final int port;

    public BrokerInfo(int id, String host, int port) {
        if (id < 0) {
            throw new IllegalArgumentException("broker id must be non-negative");
        }
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public int getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof BrokerInfo)) return false;
        return id == ((BrokerInfo) other).id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "BrokerInfo{id=" + id + ", host='" + host + "', port=" + port + "}";
    }
}
