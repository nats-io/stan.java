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
import java.util.concurrent.TimeoutException;

/**
 * A {@code StreamingConnection} object is a client's active connection to the nats streaming
 * data system.
 */
public interface StreamingConnection extends AutoCloseable {
    /**
     * Publishes the payload specified by {@code data} to the subject specified by {@code subject},
     * and blocks until an ACK or error is returned.
     * 
     * If the underlying NATs connection is disconnected, due to network problems, messages will not flow until
     * that connection is re-established. The underlying connection will batch published messages until the reconnect
     * buffer is full. See the JavaDoc for Options in the NATs client for more information on the reconnect buffer.
     * 
     * However, keep in mind that ACK timeouts can come in to play and result in an exception despite the underlying connections
     * caching attempt.
     *
     * @param subject the subject to which the message is to be published
     * @param data    the message payload
     * @throws IOException           if the publish operation is not successful
     * @throws InterruptedException  if the calling thread is interrupted before the call completes
     * @throws IllegalStateException if the connection is closed
     * @throws TimeoutException if there is a timeout trying to ack
     */
    void publish(String subject, byte[] data) throws IOException, InterruptedException, TimeoutException;

    /**
     * Publishes the payload specified by {@code data} to the subject specified by {@code subject}
     * and asynchronously processes the ACK or error state via the supplied {@link AckHandler}
     *
     * If the underlying NATs connection is disconnected, due to network problems, messages will not flow until
     * that connection is re-established. The underlying connection will batch published messages until the reconnect
     * buffer is full. See the JavaDoc for Options in the NATs client for more information on the reconnect buffer.
     * 
     * However, keep in mind that ACK timeouts can come in to play and result in an exception despite the underlying connections
     * caching attempt.
     * 
     * @param subject the subject to which the message is to be published
     * @param data    the message payload, if an ack handler is used, this data is saved to pass to it and should not be reused
     * @param ah      the {@link AckHandler} to invoke when an ack is received, passing the
     *                message GUID
     *                and any error information.
     * @return the message GUID
     * @throws IOException           if an I/O exception is encountered
     * @throws InterruptedException  if the calling thread is interrupted before the call completes
     * @throws IllegalStateException if the connection is closed
     * @throws TimeoutException if there is a timeout trying to ack
     * @see AckHandler
     */
    String publish(String subject, byte[] data, AckHandler ah)
            throws IOException, InterruptedException, TimeoutException;

    /**
     * Creates a {@link Subscription} with interest in a given subject, assigns the callback, and
     * immediately starts receiving messages.
     * 
     * If the underlying NATs connection is disconnected, due to network problems, messages will not flow until
     * that connection is re-established. That includes telling the server about the subscription. In that
     * situation it is possible for a timeout to occur.
     *
     * @param subject the subject of interest
     * @param cb      a {@code MessageHandler} callback used to process messages received by the
     *                {@code Subscription}
     * @return the {@code Subscription} object, or null if the subscription request timed out
     * @throws IOException          if an I/O exception is encountered
     * @throws InterruptedException if the calling thread is interrupted before the call completes
     * @throws TimeoutException if the server request cannot complete within subscription timeout
     * @see MessageHandler
     * @see Subscription
     */
    Subscription subscribe(String subject, MessageHandler cb)
            throws IOException, InterruptedException, TimeoutException;

    /**
     * Creates a {@link Subscription} with interest in a given subject using the given
     * {@link SubscriptionOptions}, assigns the callback, and immediately starts receiving messages.
     * 
     * If the underlying NATs connection is disconnected, due to network problems, messages will not flow until
     * that connection is re-established. That includes telling the server about the subscription. In that
     * situation it is possible for a timeout to occur.
     *
     * @param subject the subject of interest
     * @param cb      a {@link MessageHandler} callback used to process messages received by the
     *                {@link Subscription}
     * @param opts    the {@link SubscriptionOptions} to configure this {@link Subscription}
     * @return the {@link Subscription} object, or null if the subscription request timed out
     * @throws IOException          if an I/O exception is encountered
     * @throws InterruptedException if the calling thread is interrupted before the call completes
     * @throws TimeoutException if the server request cannot complete within subscription timeout
     * @see MessageHandler
     * @see Subscription
     * @see SubscriptionOptions
     */
    Subscription subscribe(String subject, MessageHandler cb, SubscriptionOptions opts)
            throws IOException, InterruptedException, TimeoutException;

    /**
     * Creates a {@code Subscription} in the queue group specified by {@code queue} with interest in
     * a given subject, assigns the message callback, and immediately starts receiving messages.
     * 
     * If the underlying NATs connection is disconnected, due to network problems, messages will not flow until
     * that connection is re-established. That includes telling the server about the subscription. In that
     * situation it is possible for a timeout to occur.
     *
     * @param subject the subject of interest
     * @param queue   optional queue group
     * @param cb      a {@link MessageHandler} callback used to process messages received by the
     *                {@link Subscription}
     * @return the {@link Subscription} object, or null if the subscription request timed out
     * @throws IOException          if an I/O exception is encountered
     * @throws InterruptedException if the calling thread is interrupted before the call completes
     * @throws TimeoutException if the server request cannot complete within subscription timeout
     */
    Subscription subscribe(String subject, String queue, MessageHandler cb)
            throws IOException, InterruptedException, TimeoutException;

    /**
     * Creates a {@code Subscription} in the queue group specified by {@code queue} with interest in
     * a given subject, assigns the message callback, and immediately starts receiving messages.
     * 
     * If the underlying NATs connection is disconnected, due to network problems, messages will not flow until
     * that connection is re-established. That includes telling the server about the subscription. In that
     * situation it is possible for a timeout to occur.
     *
     * @param subject the subject of interest
     * @param queue   optional queue group
     * @param cb      a {@link MessageHandler} callback used to process messages received by the
     *                {@link Subscription}
     * @param opts    the {@link SubscriptionOptions} to configure for this {@link Subscription}
     * @return the {@code Subscription} object, or null if the subscription request timed out
     * @throws IOException          if an I/O exception is encountered
     * @throws InterruptedException if the calling thread is interrupted before the call completes
     * @throws TimeoutException if the server request cannot complete within subscription timeout
     */
    Subscription subscribe(String subject, String queue, MessageHandler cb,
                           SubscriptionOptions opts) throws IOException, InterruptedException, TimeoutException;
                           
    /**
     * Returns the underlying NATS connection. Use with caution, especially if you didn't create the
     * connection.
     *
     * @return the underlying NATS connection.
     * @see io.nats.client.Connection
     */
    io.nats.client.Connection getNatsConnection();

    /**
     * Closes the connection.
     *
     * @throws IOException      if an error occurs
     * @throws TimeoutException if the close request is not responded to within the timeout period.
     */
    void close() throws IOException, TimeoutException, InterruptedException;
}
