/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import io.nats.client.NUID;

import java.io.IOException;

/**
 * A {@code Publisher} object is a client's active publisher to the STAN streaming data system.
 */
public interface Publisher {
    /**
     * Publishes the payload specified by {@code data} to the subject specified by {@code subject}.
     * 
     * @param subject the subject to which the message is to be published
     * @param data the message payload
     * @throws IOException if the publish operation is not successful
     * @throws IllegalStateException if the connection is closed
     */
    void publish(String subject, byte[] data) throws IOException;

    /**
     * Publishes the payload specified by {@code data} to the subject specified by {@code subject}
     * and asynchronously processes the ACK or error state via the supplied {@code AckHandler}
     * 
     * @param subject the subject to which the message is to be published
     * @param data the message payload
     * @param ah the {@code AckHandler} to invoke when an ack is received, passing the message GUID
     *        and any error information.
     * @return the message GUID
     * @throws IOException if an I/O exception is encountered
     * @throws IllegalStateException if the connection is closed
     * @see AckHandler
     */
    String publish(String subject, byte[] data, AckHandler ah) throws IOException;

    /**
     * Publishes the payload specified by {@code data} to the subject specified by {@code subject},
     * and blocks until an ACK or error is returned.
     * 
     * @param subject the subject to publish the message to
     * @param reply the subject to which subscribers should send responses
     * @param data the message payload
     * @throws IOException if an I/O exception is encountered
     * @throws IllegalStateException if the connection is closed
     * @see AckHandler
     */
    void publish(String subject, String reply, byte[] data) throws IOException;

    /**
     * Publishes the payload specified by {@code data} to the subject specified by {@code subject}
     * and asynchronously processes the ACK or error state.
     * 
     * @param subject the subject to publish the message to
     * @param reply the subject to which subscribers should send responses
     * @param data the message payload
     * @param ah an {@code AckHandler} to process the ACK or error state
     * @return the {@code NUID} for the published message
     * @throws IOException if an I/O exception is encountered
     * @throws IllegalStateException if the connection is closed
     * @see AckHandler
     * @see NUID
     */
    String publish(String subject, String reply, byte[] data, AckHandler ah) throws IOException;
}
