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
 * A callback interface for handling NATS Streaming connection lost notifications.
 */
public interface ConnectionLostHandler {
    /**
     * The connection will ping the server regularly to make sure the connection is up.
     * This method is called if the connection goes away for some reason.
     * 
     * Keep in mind that the NATS connection listener may also receive a message if
     * the disconnect is at that level.
     * 
     * @param conn The streaming connection
     * @param ex   any exception that was encountered
     */
    void connectionLost(StreamingConnection conn, Exception ex);
}
