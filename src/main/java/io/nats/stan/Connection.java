/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * A {@code Connection} object is a client's active connection to the STAN streaming data system.
 */
public interface Connection extends Publisher, Subscriber, AutoCloseable {
    /**
     * Closes the connection.
     * 
     * @throws IOException if an error occurs
     * @throws TimeoutException if the close request is not responded to within the timeout period.
     */
    void close() throws IOException, TimeoutException;
}
