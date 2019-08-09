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

import io.nats.client.ConnectionListener;
import io.nats.client.ErrorListener;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * A {@code StreamingConnectionFactory} object encapsulates a set of connection configuration
 * options. A client uses it to create a connection to the NATS streaming data system.
 * 
 * This class provides some options that are mapped to the underlying NATS connection, but you can
 * also create a streaming connection from an existing NATS core connection. Using an existing connection
 * allows complete control over the core NATS options.
 * 
 * As of version 2.2.0 the way this class should be used has changed and accessors on this class are
 * deprecated. Instead create an Options.Builder, set the attributes there and assign that options builder
 * to a connection factory with setOptions or use the new constructor. New properties will only be added
 * to the options class and not replicated here. The existing accessors work, but should be moved away from.
 */
public class StreamingConnectionFactory {
    private Options.Builder options;

    /**
     * Create a new, un-configured, connection factory.
     * 
     * The cluster and client id must be set before use.
     */
    public StreamingConnectionFactory() {
        options = new Options.Builder();
    }

    /**
     * Create a connection factory with the specified cluster and client ids.
     * 
     * @param clusterId the cluster id to connect to
     * @param clientId the id for this client, with respect to the cluster
     */
    public StreamingConnectionFactory(String clusterId, String clientId) {
        this();
        this.options.clusterId(clusterId).clientId(clientId);
    }

    /**
     * Create a connection factory with the specified cluster and client ids.
     * 
     * @param options the options to create this factory with
     */
    public StreamingConnectionFactory(Options options) {
        this.options = new Options.Builder(options);
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
        StreamingConnectionImpl conn = new StreamingConnectionImpl(options());
        conn.connect();
        return conn;
    }

    /**
     * Copies the options for use in future connections.
     * 
     * @param o the options to copy
     */
    public void setOptions(Options o) {
        this.options = new Options.Builder(o);
    }

    Options options() {
        return this.options.build();
    }

    /**
     * Returns the ACK timeout.
     *
     * @deprecated use options directly
     * @return the pubAckWait
     */
    public Duration getAckTimeout() {
        return this.options.getAckTimeout();
    }

    /**
     * Sets the ACK timeout duration.
     *
     * @param ackTimeout the pubAckWait to set
     * @deprecated use options directly
     */
    public void setAckTimeout(Duration ackTimeout) {
        this.options.pubAckWait(ackTimeout);
    }

    /**
     * Sets the ACK timeout in the specified time unit.
     *
     * @param ackTimeout the pubAckWait to set
     * @param unit       the time unit to set
     * @deprecated use options directly
     */
    public void setAckTimeout(long ackTimeout, TimeUnit unit) {
        this.options.pubAckWait(Duration.ofMillis(unit.toMillis(ackTimeout)));
    }

    /**
     * Returns the connect timeout interval in milliseconds.
     *
     * @return the connectWait
     * @deprecated use options directly
     */
    public Duration getConnectTimeout() {
        return this.options.getConnectTimeout();
    }

    /**
     * Sets the connect timeout duration.
     *
     * @param connectTimeout the connectWait to set
     * @deprecated use options directly
     */
    public void setConnectTimeout(Duration connectTimeout) {
        this.options.connectWait(connectTimeout);
    }

    /**
     * Sets the connect timeout in the specified time unit.
     *
     * @param connectTimeout the connectWait to set
     * @param unit           the time unit to set
     * @deprecated use options directly
     */
    public void setConnectTimeout(long connectTimeout, TimeUnit unit) {
        this.options.connectWait(Duration.ofMillis(unit.toMillis(connectTimeout)));
    }


    /**
     * Returns the currently configured discover prefix string.
     *
     * @return the discoverPrefix
     * @deprecated use options directly
     */
    public String getDiscoverPrefix() {
        return this.options.getDiscoverPrefix();
    }

    /**
     * Sets the discover prefix string that is used to establish a nats streaming session.
     *
     * @param discoverPrefix the discoverPrefix to set
     * @deprecated use options directly
     */
    public void setDiscoverPrefix(String discoverPrefix) {
        this.options.discoverPrefix(discoverPrefix);
    }

    /**
     * Returns the maximum number of publish ACKs that may be in flight at any point in time.
     *
     * @return the maxPubAcksInFlight
     * @deprecated use options directly
     */
    public int getMaxPubAcksInFlight() {
        return this.options.getMaxPubAcksInFlight();
    }

    /**
     * Sets the maximum number of publish ACKs that may be in flight at any point in time.
     *
     * @param maxPubAcksInFlight the maxPubAcksInFlight to set
     * @deprecated use options directly
     */
    public void setMaxPubAcksInFlight(int maxPubAcksInFlight) {
        this.options.maxPubAcksInFlight(maxPubAcksInFlight);
    }

    /**
     * Returns the NATS connection URL.
     *
     * @return the NATS connection URL
     * @deprecated use options directly
     */
    public String getNatsUrl() {
        return this.options.getNatsUrl();
    }

    /**
     * Sets the NATS URL.
     *
     * @param natsUrl the natsUrl to set
     * @deprecated use options directly
     */
    public void setNatsUrl(String natsUrl) {
        this.options.natsUrl(natsUrl);
    }

    /**
     * Returns the NATS StreamingConnection, if set.
     *
     * @return the NATS StreamingConnection
     * @deprecated use options directly
     */
    public io.nats.client.Connection getNatsConnection() {
        return this.options.getNatsConn();
    }

    /**
     * Sets the NATS StreamingConnection.
     *
     * @param natsConn the NATS connection to set
     * @deprecated use options directly
     */
    public void setNatsConnection(io.nats.client.Connection natsConn) {
        this.options.natsConn(natsConn);
    }


    /**
     * Returns the client ID of the current NATS streaming session.
     *
     * @return the client ID of the current NATS streaming session
     * @deprecated use options directly
     */
    public String getClientId() {
        return this.options.getClientId();
    }

    /**
     * Sets the client ID for the current NATS streaming session.
     *
     * @param clientId the clientId to set
     * @deprecated use options directly
     */
    public void setClientId(String clientId) {
        this.options.clientId(clientId);
    }

    /**
     * Returns the cluster ID of the current NATS streaming session.
     *
     * @return the clusterId
     * @deprecated use options directly
     */
    public String getClusterId() {
        return this.options.getClusterID();
    }

    /**
     * Sets the cluster ID of the current NATS streaming session.
     *
     * @param clusterId the clusterId to set
     * @deprecated use options directly
     */
    public void setClusterId(String clusterId) {
        this.options.clusterId(clusterId);
    }

    /**
     * @return the connection listener configured for this factory
     * @deprecated use options directly
     */
    public ConnectionListener getConnectionListener() {
        return this.options.getConnectionListener();
    }

    /**
     * Set a connection listener for the underlying nats connection.
     * @param l The new connection listener
     * @deprecated use options directly
     */
    public void setConnectionListener(ConnectionListener l) {
        this.options.connectionListener(l);
    }

    /**
     * @return the error listener associated with this factory
     * @deprecated use options directly
     */
    public ErrorListener getErrorListener() {
        return this.options.getErrorListener();
    }


    /**
     * Set a error listener for the underlying nats connection.
     * @param l The new error listener
     * @deprecated use options directly
     */
    public void setErrorListener(ErrorListener l) {
        this.options.errorListener(l);
    }
}
