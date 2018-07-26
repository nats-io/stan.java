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

import java.io.IOException;

public final class NatsStreaming {
    static final String DEFAULT_NATS_URL = io.nats.client.Options.DEFAULT_URL;
    static final int DEFAULT_CONNECT_WAIT = 2; // Seconds
    static final String DEFAULT_DISCOVER_PREFIX = "_STAN.discover";
    static final String DEFAULT_ACK_PREFIX = "_STAN.acks";
    static final int DEFAULT_MAX_PUB_ACKS_IN_FLIGHT = 16384;
    static final String PFX = "stan: ";
    static final String ERR_CONNECTION_REQ_TIMEOUT = PFX + "connect request timeout";
    static final String ERR_CLOSE_REQ_TIMEOUT = PFX + "close request timeout";
    static final String ERR_SUB_REQ_TIMEOUT = PFX + "subscribe request timeout";
    static final String ERR_UNSUB_REQ_TIMEOUT = PFX + "unsubscribe request timeout";
    static final String ERR_CONNECTION_CLOSED = PFX + "connection closed";
    static final String ERR_TIMEOUT = PFX + "publish ack timeout";
    static final String ERR_BAD_ACK = PFX + "malformed ack";
    static final String ERR_BAD_SUBSCRIPTION = PFX + "invalid subscription";
    static final String ERR_BAD_CONNECTION = PFX + "invalid connection";
    static final String ERR_MANUAL_ACK = PFX + "cannot manually ack in auto-ack mode";
    static final String ERR_NULL_MSG = PFX + "null message";
    static final String ERR_NO_SERVER_SUPPORT = PFX + "not supported by server";
    // Server errors
    static final String SERVER_ERR_INVALID_SUBJECT = PFX + "invalid subject";
    static final String SERVER_ERR_INVALID_SEQUENCE = PFX + "invalid start sequence";
    static final String SERVER_ERR_INVALID_TIME = PFX + "invalid start time";
    static final String SERVER_ERR_INVALID_SUB = PFX + "invalid subscription";
    static final String SERVER_ERR_INVALID_CLIENT = PFX + "clientID already registered";
    static final String SERVER_ERR_INVALID_ACK_WAIT = PFX + "invalid ack wait time, should be >= "
            + "1s";
    static final String SERVER_ERR_INVALID_CONN_REQ = PFX + "invalid connection request";
    static final String SERVER_ERR_INVALID_PUB_REQ = PFX + "invalid publish request";
    static final String SERVER_ERR_INVALID_SUB_REQ = PFX + "invalid subscription request";
    static final String SERVER_ERR_INVALID_UNSUB_REQ = PFX + "invalid unsubscribe request";
    static final String SERVER_ERR_INVALID_CLOSE_REQ = PFX + "invalid close request";
    static final String SERVER_ERR_DUP_DURABLE = PFX + "duplicate durable registration";
    static final String SERVER_ERR_INVALID_DURABLE_NAME =
            PFX + "durable name of a durable queue subscriber can't contain the character ':'";
    static final String SERVER_ERR_UNKNOWN_CLIENT = PFX + "unknown clientID";

    private NatsStreaming() {
    }

    /**
     * Creates a NATS Streaming connection using the supplied cluster ID and client ID.
     *
     * @param clusterId the NATS Streaming cluster ID
     * @param clientId  the NATS Streaming client ID
     * @return the {@link StreamingConnection}
     * @throws IOException          if a problem occurs
     * @throws InterruptedException if the calling thread is interrupted before the connection
     *                              attempt succeeds
     */
    public static StreamingConnection connect(String clusterId, String clientId)
            throws IOException, InterruptedException {
        return connect(clusterId, clientId, defaultOptions());
    }

    /**
     * Creates a NATS Streaming connection using the supplied cluster ID, client ID, and
     * {@link Options}.
     *
     * @param clusterId the NATS Streaming cluster ID
     * @param clientId  the NATS Streaming client ID
     * @param opts      the configuration {@link Options}
     * @return the {@link StreamingConnection}
     * @throws IOException          if a problem occurs
     * @throws InterruptedException if the calling thread is interrupted before the connection
     *                              attempt succeeds
     */
    public static StreamingConnection connect(
            String clusterId, String clientId, Options opts)
            throws IOException, InterruptedException {
        try {
            StreamingConnectionImpl conn = new StreamingConnectionImpl(clusterId, clientId, opts);
            conn.connect();
            return conn;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * The default Options settings.
     *
     * @return the default {@link Options}
     */
    public static Options defaultOptions() {
        return new Options.Builder().build();
    }
}
