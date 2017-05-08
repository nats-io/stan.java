/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

/**
 * A callback interface for handling NATS Streaming message acknowledgements.
 */
public interface AckHandler {
    /**
     * This method is called when a message has been acknowledged by the STAN server, or if an error
     * has occurred during the publish operations. Processes the message acknowledgement (
     * {@code NUID} ), along with any error that was encountered
     * 
     * @param nuid the message NUID
     * @param ex any exception that was encountered
     */
    void onAck(String nuid, Exception ex);
}
