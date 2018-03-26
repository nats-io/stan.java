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

import static io.nats.streaming.UnitTestUtilities.setupMockNatsConnection;
import static io.nats.streaming.UnitTestUtilities.testClientName;
import static io.nats.streaming.UnitTestUtilities.testClusterName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.client.Connection;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class StreamingConnectionFactoryTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

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
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        assertEquals(testClusterName, cf.getClusterId());
        assertEquals(testClientName, cf.getClientId());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#createConnection()}.
     */
    @Test
    public void testCreateConnection() throws Exception {
        try (io.nats.client.Connection nc = setupMockNatsConnection()) {
            StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
            cf.setNatsConnection(nc);
            try (StreamingConnection sc = cf.createConnection()) {
                assertTrue(sc instanceof StreamingConnectionImpl);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    /**
     * Test method for {@link StreamingConnectionFactory#options()}.
     */
    @Test
    public void testOptions() throws Exception {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setAckTimeout(Duration.ofMillis(100));
        cf.setConnectTimeout(Duration.ofMillis(500));
        cf.setDiscoverPrefix("_FOO");
        cf.setMaxPubAcksInFlight(1000);
        Connection nc;

        nc = setupMockNatsConnection();

        cf.setNatsConnection(nc);
        cf.setNatsUrl("nats://foobar:1234");

        Options opts = cf.options();
        assertEquals(100, opts.getAckTimeout().toMillis());
        assertEquals(cf.getConnectTimeout(), opts.getConnectTimeout());
        assertEquals(cf.getConnectTimeout().toMillis(), opts.getConnectTimeout().toMillis());
        assertEquals(cf.getDiscoverPrefix(), opts.getDiscoverPrefix());
        assertEquals(cf.getMaxPubAcksInFlight(), opts.getMaxPubAcksInFlight());
        assertEquals(cf.getNatsUrl(), opts.getNatsUrl());
        assertEquals(cf.getNatsConnection(), opts.getNatsConn());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#getAckTimeout()}.
     */
    @Test
    public void testGetAckTimeout() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setAckTimeout(Duration.ofMillis(100));
        assertEquals(100, cf.getAckTimeout().toMillis());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#setAckTimeout(java.time.Duration)}.
     */
    @Test
    public void testSetAckTimeoutDuration() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setAckTimeout(Duration.ofMillis(100));
    }

    /**
     * Test method for
     * {@link StreamingConnectionFactory#setAckTimeout(long, java.util.concurrent.TimeUnit)}.
     */
    @Test
    public void testSetAckTimeoutLongTimeUnit() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setAckTimeout(100, TimeUnit.MILLISECONDS);
        assertEquals(100, cf.getAckTimeout().toMillis());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#getConnectTimeout()}.
     */
    @Test
    public void testGetConnectTimeout() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setConnectTimeout(Duration.ofMillis(250));
        assertEquals(250, cf.getConnectTimeout().toMillis());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#setConnectTimeout(java.time.Duration)}.
     */
    @Test
    public void testSetConnectTimeoutDuration() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setConnectTimeout(Duration.ofMillis(250));
        assertEquals(250, cf.getConnectTimeout().toMillis());
    }

    /**
     * Test method for
     * {@link StreamingConnectionFactory#setConnectTimeout(long, java.util.concurrent.TimeUnit)}
     * .
     */
    @Test
    public void testSetConnectTimeoutLongTimeUnit() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setConnectTimeout(250, TimeUnit.MILLISECONDS);
        assertEquals(250, cf.getConnectTimeout().toMillis());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#getDiscoverPrefix()}.
     */
    @Test
    public void testGetDiscoverPrefix() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        assertNotNull(cf.getDiscoverPrefix());
        assertEquals(cf.getDiscoverPrefix(), NatsStreaming.DEFAULT_DISCOVER_PREFIX);
    }

    /**
     * Test method for {@link StreamingConnectionFactory#setDiscoverPrefix(java.lang.String)}.
     */
    @Test(expected = NullPointerException.class)
    public void testSetDiscoverPrefix() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setDiscoverPrefix("_FOO");
        assertEquals(cf.getDiscoverPrefix(), "_FOO");
        cf.setDiscoverPrefix(null);
    }

    /**
     * Test method for {@link StreamingConnectionFactory#getMaxPubAcksInFlight()}.
     */
    @Test
    public void testGetMaxPubAcksInFlight() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setMaxPubAcksInFlight(1000);
        assertEquals(1000, cf.getMaxPubAcksInFlight());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#setMaxPubAcksInFlight(int)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetMaxPubAcksInFlight() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setMaxPubAcksInFlight(1000); // should work
        cf.setMaxPubAcksInFlight(-1); // should throw IllegalArgumentException
    }

    /**
     * Test method for {@link StreamingConnectionFactory#getNatsUrl()}.
     */
    @Test
    public void testGetNatsUrl() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        assertNotNull(cf.getNatsUrl());
        assertEquals(NatsStreaming.DEFAULT_NATS_URL, cf.getNatsUrl());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#setNatsUrl(java.lang.String)}.
     */
    @Test
    public void testSetNatsUrl() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setNatsUrl("nats://foobar:1234"); // Should work
    }

    @Test(expected = NullPointerException.class)
    public void testSetNatsUrlNull() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setNatsUrl(null); // Should throw
    }

    /**
     * Test method for
     * {@link StreamingConnectionFactory#setNatsConnection(io.nats.client.Connection)}.
     */
    @Test
    public void testSetNatsConnection() throws Exception {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setNatsConnection(null);
        cf.setNatsConnection(setupMockNatsConnection());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#getClientId()}.
     */
    @Test
    public void testGetClientId() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        assertEquals(testClientName, cf.getClientId());
    }

    /**
     * Test method for {@link StreamingConnectionFactory#setClientId(java.lang.String)}.
     */
    @Test(expected = NullPointerException.class)
    public void testSetClientId() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setClientId("foo");
        cf.setClientId(null);
    }

    /**
     * Test method for {@link StreamingConnectionFactory#getClusterId()}.
     */
    @Test
    public void testGetClusterId() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        assertEquals(cf.getClusterId(), testClusterName);
    }

    /**
     * Test method for {@link StreamingConnectionFactory#setClusterId(java.lang.String)}.
     */
    @Test(expected = NullPointerException.class)
    public void testSetClusterId() {
        StreamingConnectionFactory cf = new StreamingConnectionFactory(testClusterName, testClientName);
        cf.setClusterId("foo");
        cf.setClusterId(null);
    }

}
