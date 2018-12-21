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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.Duration;
import org.junit.Test;

import io.nats.client.ConnectionListener;
import io.nats.client.ErrorListener;

public class StreamingConnectionFactoryTest {
    private static final String clusterName = "test-cluster";
    private static final String clientName = "me";

    /**
     * Test method for {@link StreamingConnectionFactory#StreamingConnectionFactory()}. Tests that no
     * exception is thrown
     */
    @Test
    public void testConnectionFactory() {
        new StreamingConnectionFactory();
    }

    /**
     * Test method for
     * {@link StreamingConnectionFactory#StreamingConnectionFactory(java.lang.String, java.lang.String)}.
     * Tests that no exception is thrown and that cluster name and clientID are properly set.
     */
    @Test
    public void testConnectionFactoryStringString() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(clusterName, clientName);
        assertEquals(clusterName, cf.getClusterId());
        assertEquals(clientName, cf.getClientId());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#options()}.
     */
    @Test
    public void testOptions() throws Exception {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(clusterName, clientName);
        cf.setAckTimeout(Duration.ofMillis(100));
        cf.setConnectTimeout(Duration.ofMillis(500));
        cf.setDiscoverPrefix("_FOO");
        cf.setMaxPubAcksInFlight(1000);

        ErrorListener err = new TestHandler();
        ConnectionListener conn = new TestHandler();

        cf.setErrorListener(err);
        cf.setConnectionListener(conn);

        cf.setNatsUrl("nats://foobar:1234");

        Options opts = cf.options();
        assertEquals(100, opts.getAckTimeout().toMillis());
        assertEquals(cf.getConnectTimeout(), opts.getConnectTimeout());
        assertEquals(cf.getConnectTimeout().toMillis(), opts.getConnectTimeout().toMillis());
        assertEquals(cf.getDiscoverPrefix(), opts.getDiscoverPrefix());
        assertEquals(cf.getMaxPubAcksInFlight(), opts.getMaxPubAcksInFlight());
        assertEquals(cf.getNatsUrl(), opts.getNatsUrl());
        assertEquals(cf.getNatsConnection(), opts.getNatsConn());
        assertEquals(cf.getConnectionListener(), opts.getConnectionListener());
        assertEquals(cf.getErrorListener(), opts.getErrorListener());
    }

    @Test(expected = NullPointerException.class)
    public void testSetDiscoverPrefixNull() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(clusterName, clientName);
        cf.setDiscoverPrefix(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInvalidMaxPubAcksInFlight() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(clusterName, clientName);
        cf.setMaxPubAcksInFlight(-1); // should throw IllegalArgumentException
    }

    @Test
    public void testDefaultNatsUrl() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(clusterName, clientName);
        assertNotNull(cf.getNatsUrl());
        assertEquals(NatsStreaming.DEFAULT_NATS_URL, cf.getNatsUrl());
    }

    @Test(expected = NullPointerException.class)
    public void testSetNatsUrlNull() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(clusterName, clientName);
        cf.setNatsUrl(null); // Should throw
    }

    @Test
    public void testGetClientAndClusterId() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(clusterName, clientName);
        assertEquals(clientName, cf.getClientId());
        assertEquals(cf.getClusterId(), clusterName);
    }

    @Test(expected = NullPointerException.class)
    public void testSetClientIdNull() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(clusterName, clientName);
        cf.setClientId(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetClusterIdNull() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(clusterName, clientName);
        cf.setClusterId(null);
    }
}
