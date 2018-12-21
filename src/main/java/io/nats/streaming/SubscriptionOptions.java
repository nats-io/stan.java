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

import io.nats.streaming.protobuf.StartPosition;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * A SubscriptionOptions object defines the configurable parameters of a STAN Subscription object.
 */
public class SubscriptionOptions {
    /**
     * Default ack wait time for a subscriptions.
     */
    public static final Duration DEFAULT_ACK_WAIT = Duration.ofSeconds(30);

    /**
     * Default max messages in flight for a subscriptions.
     */
    public static final int DEFAULT_MAX_IN_FLIGHT = 1024;

    /**
     * Default time a connection will wait to create a subscription.
     */
    public static final Duration DEFAULT_SUBSCRIPTION_TIMEOUT = Duration.ofSeconds(2);

    // DurableName, if set will survive client restarts.
    private final String durableName;
    // Controls the number of messages the cluster will have inflight without an ACK.
    private final int maxInFlight;
    // Controls the time the cluster will wait for an ACK for a given message.
    private final Duration ackWait;
    // StartPosition enum from proto.
    StartPosition startAt;
    // Optional start sequence number.
    final long startSequence;
    // Optional start time in nanoseconds since the UNIX epoch.
    private final Instant startTime;
    // Option to do Manual Acks
    private final boolean manualAcks;
    private final Duration subscriptionTimeout;
    private final String dispatcher;

    // Date startTimeAsDate;

    private SubscriptionOptions(Builder builder) {
        this.durableName = builder.durableName;
        this.maxInFlight = builder.maxInFlight;
        this.ackWait = builder.ackWait;
        this.startAt = builder.startAt;
        this.startSequence = builder.startSequence;
        this.startTime = builder.startTime;
        this.manualAcks = builder.manualAcks;
        this.subscriptionTimeout = builder.subscriptionTimeout;
        this.dispatcher = builder.dispatcher;
    }

    /**
     * @return the time to wait when creating a subscription if there is a network problem
     */
    public Duration getSubscriptionTimeout() {
        return this.subscriptionTimeout;
    }

    /**
     * Returns the name of the durable subscriber.
     *
     * @return the name of the durable subscriber
     */
    public String getDurableName() {
        return durableName;
    }

    /**
     * Returns the maximum number of messages the cluster will send without an ACK.
     *
     * @return the maximum number of messages the cluster will send without an ACK
     */
    public int getMaxInFlight() {
        return maxInFlight;
    }

    /**
     * Returns the timeout for waiting for an ACK from the cluster's point of view for delivered
     * messages.
     *
     * @return the timeout for waiting for an ACK from the cluster's point of view for delivered
     *     messages
     */
    public Duration getAckWait() {
        return ackWait;
    }

    /**
     * Returns the desired start position for the message stream.
     *
     * @return the desired start position for the message stream
     */
    public StartPosition getStartAt() {
        return startAt;
    }

    /**
     * Returns the desired start sequence position.
     *
     * @return the desired start sequence position
     */
    public long getStartSequence() {
        return startSequence;
    }

    /**
     * Returns the desired start time position.
     *
     * @return the desired start time position
     */
    public Instant getStartTime() {
        return startTime;
    }

    /**
     * Returns the desired start time position in the requested units.
     *
     * @param unit the unit of time
     * @return the desired start time position
     */
    public long getStartTime(TimeUnit unit) {
        // FIXME use BigInteger representation
        long totalNanos = TimeUnit.SECONDS.toNanos(startTime.getEpochSecond());
        totalNanos += startTime.getNano();
        return unit.convert(totalNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Returns whether or not messages for this subscription must be acknowledged individually by
     * calling {@link Message#ack()}.
     *
     * @return whether or not manual acks are required for this subscription.
     */
    public boolean isManualAcks() {
        return manualAcks;
    }

    /**
     * Returns name of the dispatcher to use for this subscription. Can be null.
     * If no dispatcher is provided in the options, a single dispatcher/thread is shared
     * with other similarly configured subscriptions. By sharing dispatchers suscriptions
     * can reduce the threads used in their application.
     * 
     * @return the dispatcher name for this subscription.
     */
    public String getDispatcherName() {
        return dispatcher;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "SubscriptionOptions{" +
          "durableName='" + durableName + '\'' +
          ", maxInFlight=" + maxInFlight +
          ", ackWait=" + ackWait +
          ", startAt=" + startAt +
          ", startSequence=" + startSequence +
          ", startTime=" + startTime +
          ", manualAcks=" + manualAcks +
          '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubscriptionOptions that = (SubscriptionOptions) o;

        if (maxInFlight != that.maxInFlight) return false;
        if (startSequence != that.startSequence) return false;
        if (manualAcks != that.manualAcks) return false;
        if (durableName != null ? !durableName.equals(that.durableName) : that.durableName != null) return false;
        if (ackWait != null ? !ackWait.equals(that.ackWait) : that.ackWait != null) return false;
        if (startAt != that.startAt) return false;
        if (dispatcher != that.dispatcher) return false;
        return startTime != null ? startTime.equals(that.startTime) : that.startTime == null;
    }

    @Override
    public int hashCode() {
        int result = durableName != null ? durableName.hashCode() : 0;
        result = 31 * result + maxInFlight;
        result = 31 * result + (ackWait != null ? ackWait.hashCode() : 0);
        result = 31 * result + (startAt != null ? startAt.hashCode() : 0);
        result = 31 * result + (int) (startSequence ^ (startSequence >>> 32));
        result = 31 * result + (startTime != null ? startTime.getNano() : 0);
        result = 31 * result + (dispatcher != null ? dispatcher.hashCode() : 0);
        result = 31 * result + (manualAcks ? 1 : 0);
        return result;
    }

    /**
     * A Builder implementation for creating an immutable {@code SubscriptionOptions} object.
     */
    public static final class Builder implements Serializable {
        private static final long serialVersionUID = 1476017376308805473L;

		String durableName;
        int maxInFlight = SubscriptionOptions.DEFAULT_MAX_IN_FLIGHT;
        Duration ackWait = SubscriptionOptions.DEFAULT_ACK_WAIT;
        StartPosition startAt = StartPosition.NewOnly;
        long startSequence;
        Instant startTime;
        boolean manualAcks;
        Date startTimeAsDate;
        String dispatcher;
        Duration subscriptionTimeout = SubscriptionOptions.DEFAULT_SUBSCRIPTION_TIMEOUT;

        /**
         * Sets the durable subscriber name for the subscription.
         *
         * @param durableName the name of the durable subscriber
         * @return this
         */
        public Builder durableName(String durableName) {
            this.durableName = durableName;
            return this;
        }

        /**
         * Sets the maximum number of in-flight (unacknowledged) messages for the subscription.
         *
         * @param max the maximum number of in-flight messages
         * @return this
         */
        public Builder maxInFlight(int max) {
            this.maxInFlight = max;
            return this;
        }

        /**
         * Sets the amount of time the subscription will wait during creation on a network failure.
         *
         * @param timeout the amount of time the subscription will wait to be created
         * @return this
         */
        public Builder subscriptionTimeout(Duration timeout) {
            this.subscriptionTimeout = timeout;
            return this;
        }

        /**
         * Sets the amount of time the subscription will wait for ACKs from the cluster.
         *
         * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
         * @return this
         */
        public Builder ackWait(Duration ackWait) {
            this.ackWait = ackWait;
            return this;
        }

        /**
         * Sets the amount of time the subscription will wait for ACKs from the cluster.
         *
         * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
         * @param unit    the time unit
         * @return this
         */
        public Builder ackWait(long ackWait, TimeUnit unit) {
            this.ackWait = Duration.ofMillis(unit.toMillis(ackWait));
            return this;
        }

        /**
         * Sets whether or not messages must be acknowledge individually by calling
         * {@link Message#ack()}.
         *
         * @return this
         */
        public Builder manualAcks() {
            this.manualAcks = true;
            return this;
        }

        /**
         * Specifies the sequence number from which to start receiving messages.
         *
         * @param seq the sequence number from which to start receiving messages
         * @return this
         */
        public Builder startAtSequence(long seq) {
            this.startAt = StartPosition.SequenceStart;
            this.startSequence = seq;
            return this;
        }

        /**
         * Specifies the desired start time position using {@code java.time.Instant}.
         *
         * @param start the desired start time position expressed as a {@code java.time.Instant}
         * @return this
         */
        public Builder startAtTime(Instant start) {
            this.startAt = StartPosition.TimeDeltaStart;
            this.startTime = start;
            return this;
        }

        /**
         * Specifies the desired delta start time position in the desired unit.
         *
         * @param ago  the historical time delta (from now) from which to start receiving messages
         * @param unit the time unit
         * @return this
         */
        public Builder startAtTimeDelta(long ago, TimeUnit unit) {
            this.startAt = StartPosition.TimeDeltaStart;
            // this.startTime =
            // TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - unit.toMillis(ago));
            this.startTime = Instant.now().minusNanos(unit.toNanos(ago));
            return this;
        }

        /**
         * Specifies the desired delta start time as a {@link java.time.Duration}.
         *
         * @param ago the historical time delta (from now) from which to start receiving messages
         * @return this
         */
        public Builder startAtTimeDelta(Duration ago) {
            this.startAt = StartPosition.TimeDeltaStart;
            // this.startTime =
            // TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - unit.toMillis(ago));
            this.startTime = Instant.now().minusNanos(ago.toNanos());
            return this;
        }

        /**
         * Specifies that message delivery should start with the last (most recent) message stored
         * for this subject.
         *
         * @return this
         */
        public Builder startWithLastReceived() {
            this.startAt = StartPosition.LastReceived;
            return this;
        }

        /**
         * Specifies that message delivery should begin at the oldest available message for this
         * subject.
         *
         * @return this
         */
        public Builder deliverAllAvailable() {
            this.startAt = StartPosition.First;
            return this;
        }

        /**
         * Specify a dispatcher for this subscription. Dispatchers are essentially a message queue
         * and thread to handle callbacks. By sharing dispatchers an application can reduce thread resources.
         * By splitting subscriptions between dispatchers it is possible to have multiple messages handled
         * at the same time.
         * 
         * A unique dispatcher will be created automatically for each name. Reusing the name reuses the dispatcher.
         * 
         * @param dispatcherName the shared name to use for the dispatcher
         * @return this
         */
        public Builder dispatcher(String dispatcherName) {
            this.dispatcher = dispatcherName;
            return this;
        }

        /**
         * Creates a {@link SubscriptionOptions} instance based on the current configuration.
         *
         * @return the created {@link SubscriptionOptions} instance
         */
        public SubscriptionOptions build() {
            return new SubscriptionOptions(this);
        }
    }
}

