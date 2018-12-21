// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.streaming;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.ErrorListener;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * A {@code StreamingConnectionFactory} object encapsulates a set of connection configuration
 * options. A client uses it to create a connection to the NATS streaming data system.
 */
public class StreamingConnectionFactory {
    private Duration ackTimeout = SubscriptionOptions.DEFAULT_ACK_WAIT;
    private Duration connectTimeout = Duration.ofSeconds(NatsStreaming
            .DEFAULT_CONNECT_WAIT);
    private String discoverPrefix = NatsStreaming.DEFAULT_DISCOVER_PREFIX;
    private int maxPubAcksInFlight = NatsStreaming.DEFAULT_MAX_PUB_ACKS_IN_FLIGHT;
    private String natsUrl = NatsStreaming.DEFAULT_NATS_URL;
    private Connection natsConn;
    private String clientId;
    private String clusterId;
    private ConnectionListener connListener;
    private ErrorListener errListener;

    public StreamingConnectionFactory() {
    }

    public StreamingConnectionFactory(String clusterId, String clientId) {
        setClusterId(clusterId);
        setClientId(clientId);
    }

    /**
     * Creates an active connection to a NATS Streaming server.
     *
     * @return the StreamingConnection.
     * @throws IOException          if a StreamingConnection cannot be established for some reason.
     * @throws InterruptedException if the calling thread is interrupted before the connection can
     *                              be established
     */
    public StreamingConnection createConnection() throws IOException, InterruptedException {
        StreamingConnectionImpl conn = new StreamingConnectionImpl(clusterId, clientId, options());
        conn.connect();
        return conn;
    }

    Options options() {
        return new Options.Builder().connectWait(connectTimeout).pubAckWait(ackTimeout)
                .discoverPrefix(discoverPrefix).maxPubAcksInFlight(maxPubAcksInFlight)
                .natsConn(natsConn).natsUrl(natsUrl).connectionListener(connListener).errorListener(errListener).build();
    }

    /**
     * Returns the ACK timeout.
     *
     * @return the pubAckWait
     */
    public Duration getAckTimeout() {
        return ackTimeout;
    }

    /**
     * Sets the ACK timeout duration.
     *
     * @param ackTimeout the pubAckWait to set
     */
    public void setAckTimeout(Duration ackTimeout) {
        this.ackTimeout = ackTimeout;
    }

    /**
     * Sets the ACK timeout in the specified time unit.
     *
     * @param ackTimeout the pubAckWait to set
     * @param unit       the time unit to set
     */
    public void setAckTimeout(long ackTimeout, TimeUnit unit) {
        this.ackTimeout = Duration.ofMillis(unit.toMillis(ackTimeout));
    }

    /**
     * Returns the connect timeout interval in milliseconds.
     *
     * @return the connectWait
     */
    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Sets the connect timeout duration.
     *
     * @param connectTimeout the connectWait to set
     */
    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Sets the connect timeout in the specified time unit.
     *
     * @param connectTimeout the connectWait to set
     * @param unit           the time unit to set
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
     * Sets the discover prefix string that is used to establish a nats streaming session.
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
     * Returns the NATS StreamingConnection, if set.
     *
     * @return the NATS StreamingConnection
     */
    public io.nats.client.Connection getNatsConnection() {
        return this.natsConn;
    }

    /**
     * Sets the NATS StreamingConnection.
     *
     * @param natsConn the NATS connection to set
     */
    public void setNatsConnection(io.nats.client.Connection natsConn) {
        this.natsConn = natsConn;
    }


    /**
     * Returns the client ID of the current NATS streaming session.
     *
     * @return the client ID of the current NATS streaming session
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Sets the client ID for the current NATS streaming session.
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
     * Returns the cluster ID of the current NATS streaming session.
     *
     * @return the clusterId
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Sets the cluster ID of the current NATS streaming session.
     *
     * @param clusterId the clusterId to set
     */
    public void setClusterId(String clusterId) {
        if (clusterId == null) {
            throw new NullPointerException("stan: cluster ID must be non-null");
        }

        this.clusterId = clusterId;
    }

    /**
     * @return the connection listener configured for this factory
     */
    public ConnectionListener getConnectionListener() {
        return this.connListener;
    }

    /**
     * Set a connection listener for the underlying nats connection.
     * @param l The new connection listener
     */
    public void setConnectionListener(ConnectionListener l) {
        this.connListener = l;
    }

    /**
     * @return the error listener associated with this factory
     */
    public ErrorListener getErrorListener() {
        return this.errListener;
    }


    /**
     * Set a error listener for the underlying nats connection.
     * @param l The new error listener
     */
    public void setErrorListener(ErrorListener l) {
        this.errListener = l;
    }
}
