/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * A {@code Subscriber} object is a client's active subscriber to the STAN streaming data system.
 */
public interface Subscriber {
    /**
     * Creates a {@code Subscription} with interest in a given subject, assigns the callback, and
     * immediately starts receiving messages.
     * 
     * @param subject the subject of interest
     * @param cb a {@code MessageHandler} callback used to process messages received by the
     *        {@code Subscription}
     * @return the {@code Subscription} object
     * @throws IOException if an I/O exception is encountered
     * @throws TimeoutException if the subscription request times out
     * @see MessageHandler
     * @see Subscription
     */
    Subscription subscribe(String subject, MessageHandler cb) throws IOException, TimeoutException;

    /**
     * Creates a {@code Subscription} with interest in a given subject using the given
     * {@code SubscriptionOptions}, assigns the callback, and immediately starts receiving messages.
     * 
     * @param subject the subject of interest
     * @param cb a {@code MessageHandler} callback used to process messages received by the
     *        {@code Subscription}
     * 
     * @param opts the {@code SubscriptionOptions} to configure this {@code Subscription}
     * @return the {@code Subscription} object
     * @throws IOException if an I/O exception is encountered
     * @throws TimeoutException if the subscription request times out
     * @see MessageHandler
     * @see Subscription
     * @see SubscriptionOptions
     */
    Subscription subscribe(String subject, MessageHandler cb, SubscriptionOptions opts)
            throws IOException, TimeoutException;

    /**
     * Creates a {@code Subscription} in the queue group specified by {@code queue} with interest in
     * a given subject, assigns the message callback, and immediately starts receiving messages.
     * 
     * @param subject the subject of interest
     * @param queue optional queue group
     * @param cb a {@code MessageHandler} callback used to process messages received by the
     *        {@code Subscription}
     * @return the {@code Subscription} object
     * @throws IOException if an I/O exception is encountered
     * @throws TimeoutException if the subscription request times out
     */
    Subscription subscribe(String subject, String queue, MessageHandler cb)
            throws IOException, TimeoutException;

    /**
     * Creates a {@code Subscription} in the queue group specified by {@code queue} with interest in
     * a given subject, assigns the message callback, and immediately starts receiving messages.
     * 
     * @param subject the subject of interest
     * @param queue optional queue group
     * @param cb a {@code MessageHandler} callback used to process messages received by the
     *        {@code Subscription}
     * @param opts the {@code SubscriptionOptions} to configure for this {@code Subscription}
     * @return the {@code Subscription} object
     * @throws IOException if an I/O exception is encountered
     * @throws TimeoutException if the subscription request times out
     */
    Subscription subscribe(String subject, String queue, MessageHandler cb,
            SubscriptionOptions opts) throws IOException, TimeoutException;
}
