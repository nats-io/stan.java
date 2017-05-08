/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import java.io.IOException;

/**
 * A client uses a {@code Subscription} object to receive messages that have been published to a
 * subject.
 *
 * <p>Each {@code Subscription} object is unique, even if the subscription is to the same subject.
 * This means that if {@code StreamingConnection.subscribe("foo", cb)} is called twice in a row,
 * each of the
 * resulting {@code Subscription} objects will be unique, and any message delivered on subject "foo"
 * will be delivered individually to both {@code Subscription} objects.
 */
public interface Subscription extends AutoCloseable {
    /**
     * Retrieves the subject of interest from the {@code Subscription} object.
     *
     * @return the subject of interest
     */
    String getSubject();

    /**
     * Returns the optional queue group name. If present, all subscriptions with the same name will
     * form a distributed queue, and each message will only be processed by one member of the group.
     *
     * @return the name of the queue group this Subscription belongs to.
     */
    String getQueue();

    /**
     * Removes interest in the {@code Subscription}.
     *
     * <p>For durables, it means that the durable interest is also removed from the server.
     * Restarting a durable with the same name will not resume the subscription, it will be
     * considered a new one.
     *
     * <p>This call is equivalent to {@code close(true)}
     *
     * @throws IOException if an error occurs while notifying the server
     */
    void unsubscribe() throws IOException;

    /**
     * Removes this subscriber from the server without removing durable interest (if it exists).
     *
     * <p>This call is equivalent to {@code close(false)}
     *
     * @throws IOException if the close request times out or the client is connected to a server
     *                     for which this features is not available.
     * @see java.lang.AutoCloseable#close()
     */
    void close() throws IOException;

    /**
     * Removes this subscriber from the server.
     *
     * @param unsubscribe if {@code true}, durable interest is also removed.
     * @throws IOException if the close request times out or the client is connected to a server
     *                     for which this features is not available.
     */
    void close(boolean unsubscribe) throws IOException;

    /**
     * Returns the {@code SubscriptionOptions} object for this {@code Subscription} object.
     *
     * @return this {@code Subscription}'s code SubscriptionOptions} object.
     * @see io.nats.streaming.SubscriptionOptions
     */
    SubscriptionOptions getOptions();
}

