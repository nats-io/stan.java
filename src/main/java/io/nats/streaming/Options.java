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

import java.io.Serializable;
import java.time.Duration;

public class Options {
    private final String natsUrl;
    private final io.nats.client.Connection natsConn;
    Duration connectTimeout;
    private final Duration ackTimeout;
    private final String discoverPrefix;
    private final int maxPubAcksInFlight;

    private Options(Builder builder) {
        this.natsUrl = builder.natsUrl;
        this.natsConn = builder.natsConn;
        this.connectTimeout = builder.connectTimeout;
        this.ackTimeout = builder.ackTimeout;
        this.discoverPrefix = builder.discoverPrefix;
        this.maxPubAcksInFlight = builder.maxPubAcksInFlight;
    }

    /**
     * @return the nats url to use to connect to gnatsd.
     */
    String getNatsUrl() {
        return natsUrl;
    }

    /**
     * @return the underlying nats connection
     */
    io.nats.client.Connection getNatsConn() {
        return natsConn;
    }

    /**
     * @return the connect timeout
     */
    Duration getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * @return the ack timeout
     */
    Duration getAckTimeout() {
        return ackTimeout;
    }

    /**
     * @return the prefix used in discovery for the cluster
     */
    String getDiscoverPrefix() {
        return discoverPrefix;
    }

    /**
     * @return the maximum publish acks allowed at one time, subscription acks are set up on the subscription options
     */
    int getMaxPubAcksInFlight() {
        return maxPubAcksInFlight;
    }

    public static final class Builder implements Serializable {
		private static final long serialVersionUID = 4774214916207501660L;
		
		private String natsUrl = NatsStreaming.DEFAULT_NATS_URL;
        private transient io.nats.client.Connection natsConn; // A Connection is not Serializable
        private Duration connectTimeout = Duration.ofSeconds(NatsStreaming.DEFAULT_CONNECT_WAIT);
        private Duration ackTimeout = SubscriptionOptions.DEFAULT_ACK_WAIT;
        private String discoverPrefix = NatsStreaming.DEFAULT_DISCOVER_PREFIX;
        private int maxPubAcksInFlight = NatsStreaming.DEFAULT_MAX_PUB_ACKS_IN_FLIGHT;

        public Builder() {}

        /**
         * Constructs a {@link Builder} instance based on the supplied {@link Options} instance.
         *
         * @param template the {@link Options} object to use as a template
         */
        public Builder(Options template) {
            this.natsUrl = template.natsUrl;
            this.natsConn = template.natsConn;
            this.connectTimeout = template.connectTimeout;
            this.ackTimeout = template.ackTimeout;
            this.discoverPrefix = template.discoverPrefix;
            this.maxPubAcksInFlight = template.maxPubAcksInFlight;
        }

        /**
         * Set the ack timeout
         * @param ackTimeout the timeout
         * @return the builder for chaining
         */
        public Builder pubAckWait(Duration ackTimeout) {
            this.ackTimeout = ackTimeout;
            return this;
        }

        /**
         * Set the connect timeout
         * @param connectTimeout the timeout
         * @return the builder for chaining
         */
        public Builder connectWait(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Set the discovery prefix
         * @param discoverPrefix the prefix, defaults to {@link NatsStreaming#DEFAULT_DISCOVER_PREFIX DEFAULT_DISCOVER_PREFIX}.
         * @return the builder for chaining
         */
        public Builder discoverPrefix(String discoverPrefix) {
            this.discoverPrefix = discoverPrefix;
            return this;
        }

        /**
         * Set the max pub acks the server/client will allow
         * @param maxPubAcksInFlight the max acks
         * @return the builder for chaining
         */
        public Builder maxPubAcksInFlight(int maxPubAcksInFlight) {
            this.maxPubAcksInFlight = maxPubAcksInFlight;
            return this;
        }

        /**
         * Manually set the gnatsd connection
         * @param natsConn a valid nats connection (from the latest version of the library)
         * @return the builder for chaining
         */
        public Builder natsConn(io.nats.client.Connection natsConn) {
            this.natsConn = natsConn;
            return this;
        }

        /**
         * Set the url to connect to for gnatsd
         * @param natsUrl a valid nats url, see the client library for more information
         * @return the builder for chaining
         */
        public Builder natsUrl(String natsUrl) {
            this.natsUrl = natsUrl;
            return this;
        }

        /**
         * Construct the options object.
         * @return the options object
         */
        public Options build() {
            return new Options(this);
        }
    }
}
