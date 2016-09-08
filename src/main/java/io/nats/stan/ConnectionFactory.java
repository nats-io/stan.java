/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import io.nats.client.Connection;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@code ConnectionFactory} object encapsulates a set of connection configuration options. A
 * client uses it to create a connection to the STAN streaming data system.
 * 
 */
public class ConnectionFactory {
    private Duration ackTimeout = Duration.ofMillis(SubscriptionImpl.DEFAULT_ACK_WAIT);
    private Duration connectTimeout = Duration.ofSeconds(ConnectionImpl.DEFAULT_CONNECT_WAIT);
    private String discoverPrefix = ConnectionImpl.DEFAULT_DISCOVER_PREFIX;
    private int maxPubAcksInFlight = ConnectionImpl.DEFAULT_MAX_PUB_ACKS_IN_FLIGHT;
    private String natsUrl = ConnectionImpl.DEFAULT_NATS_URL;
    private Connection natsConn;
    private String clientId;
    private String clusterId;

    public ConnectionFactory() {}

    public ConnectionFactory(String clusterId, String clientId) {
        setClusterId(clusterId);
        setClientId(clientId);
    }

    public io.nats.stan.Connection createConnection() throws IOException, TimeoutException {
        ConnectionImpl conn = new ConnectionImpl(clusterId, clientId, options());
        conn.connect();
        return conn;
    }

    Options options() {
        Options opts =
                new Options.Builder().setConnectTimeout(connectTimeout).setAckTimeout(ackTimeout)
                        .setDiscoverPrefix(discoverPrefix).setMaxPubAcksInFlight(maxPubAcksInFlight)
                        .setNatsConn(natsConn).setNatsUrl(natsUrl).create();
        return opts;
    }

    /**
     * Returns the ACK timeout.
     * 
     * @return the ackTimeout
     */
    public Duration getAckTimeout() {
        return ackTimeout;
    }

    /**
     * Sets the ACK timeout duration.
     * 
     * @param ackTimeout the ackTimeout to set
     */
    public void setAckTimeout(Duration ackTimeout) {
        this.ackTimeout = ackTimeout;
    }

    /**
     * Sets the ACK timeout in the specified time unit.
     * 
     * @param ackTimeout the ackTimeout to set
     * @param unit the time unit to set
     */
    public void setAckTimeout(long ackTimeout, TimeUnit unit) {
        this.ackTimeout = Duration.ofMillis(unit.toMillis(ackTimeout));
    }

    /**
     * Returns the connect timeout interval in milliseconds.
     * 
     * @return the connectTimeout
     */
    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Sets the connect timeout duration.
     * 
     * @param connectTimeout the connectTimeout to set
     */
    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Sets the connect timeout in the specified time unit.
     * 
     * @param connectTimeout the connectTimeout to set
     * @param unit the time unit to set
     */
    public void setConnectTimeout(long connectTimeout, TimeUnit unit) {
        this.connectTimeout = Duration.ofMillis(unit.toMillis(connectTimeout));
    }


    /**
     * Returns the currently configured discover prefix string.
     * 
     * @return the discoverPrefix
     */
    public String getDiscoverPrefix() {
        return discoverPrefix;
    }

    /**
     * Sets the discover prefix string that is used to establish a STAN session.
     * 
     * @param discoverPrefix the discoverPrefix to set
     */
    public void setDiscoverPrefix(String discoverPrefix) {
        if (discoverPrefix == null) {
            throw new NullPointerException("stan: discoverPrefix must be non-null");
        }
        this.discoverPrefix = discoverPrefix;
    }

    /**
     * Returns the maximum number of publish ACKs that may be in flight at any point in time.
     * 
     * @return the maxPubAcksInFlight
     */
    public int getMaxPubAcksInFlight() {
        return maxPubAcksInFlight;
    }

    /**
     * Sets the maximum number of publish ACKs that may be in flight at any point in time.
     * 
     * @param maxPubAcksInFlight the maxPubAcksInFlight to set
     */
    public void setMaxPubAcksInFlight(int maxPubAcksInFlight) {
        if (maxPubAcksInFlight < 0) {
            throw new IllegalArgumentException("stan: max publish acks in flight must be >= 0");
        }
        this.maxPubAcksInFlight = maxPubAcksInFlight;
    }

    /**
     * Returns the NATS connection URL.
     * 
     * @return the NATS connection URL
     */
    public String getNatsUrl() {
        return natsUrl;
    }

    /**
     * Sets the NATS URL.
     * 
     * @param natsUrl the natsUrl to set
     */
    public void setNatsUrl(String natsUrl) {
        if (natsUrl == null) {
            throw new NullPointerException("stan: NATS URL must be non-null");
        }
        this.natsUrl = natsUrl;
    }

    /**
     * Returns the NATS Connection, if set.
     * 
     * @return the NATS Connection
     */
    public io.nats.client.Connection getNatsConnection() {
        return this.natsConn;
    }

    /**
     * Sets the NATS Connection.
     * 
     * @param natsConn the NATS connection to set
     */
    public void setNatsConnection(io.nats.client.Connection natsConn) {
        this.natsConn = natsConn;
    }


    /**
     * Returns the client ID of the current STAN session.
     * 
     * @return the client ID of the current STAN session
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Sets the client ID for the current STAN session.
     * 
     * @param clientId the clientId to set
     */
    public void setClientId(String clientId) {
        if (clientId == null) {
            throw new NullPointerException("stan: client ID must be non-null");
        }
        this.clientId = clientId;
    }

    /**
     * Returns the cluster ID of the current STAN session.
     * 
     * @return the clusterId
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Sets the cluster ID of the current STAN session.
     * 
     * @param clusterId the clusterId to set
     */
    public void setClusterId(String clusterId) {
        if (clusterId == null) {
            throw new NullPointerException("stan: cluster ID must be non-null");
        }

        this.clusterId = clusterId;
    }
}
