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

/**
 * A callback interface for handling NATS Streaming message acknowledgements.
 */
public interface AckHandler {
    /**
     * This method is called when a message has been acknowledged by the STAN
     * server, or if an error has occurred during the publish operations. Processes
     * the message acknowledgement ( {@code NUID} ), along with any error that was
     * encountered.
     * 
     * onAck with a message is now called, with the default behavior of calling this version.
     * Not deprecated yet to avoid multiple warnings, will deprecate in the future.
     * 
     * @param nuid the message NUID
     * @param ex   any exception that was encountered
     */
    void onAck(String nuid, Exception ex);

    /**
     * New version of onAck that includes the message that failed so that applications can more
     * easily resend it.
     * 
     * Added in 2.2.0 as a default method to avoid breaking existing applications.
     * 
     * @param nuid the message NUID
     * @param subject the subject the failed message was sent to
     * @param data the data passed into publish, this is the same data passed to publish
     *              if the byte array is reused in application code, it will be corrupted here
     *              in the case where no ack handler is passed to publish the data will be null
     *              but since there is no ack handler it won't matter except to allow garbage collection.
     * @param ex   any exception that was encountered
     */
    default void onAck(String nuid, String subject, byte[] data, Exception ex) {
        this.onAck(nuid, ex);
    }
}
