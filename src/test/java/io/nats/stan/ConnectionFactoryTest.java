/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import static io.nats.stan.UnitTestUtilities.setupMockNatsConnection;
import static io.nats.stan.UnitTestUtilities.testClientName;
import static io.nats.stan.UnitTestUtilities.testClusterName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Category(UnitTest.class)
@RunWith(MockitoJUnitRunner.class)
public class ConnectionFactoryTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#ConnectionFactory()}. Tests that no
     * exception is thrown
     */
    @Test
    public void testConnectionFactory() {
        new ConnectionFactory();
    }

    /**
     * Test method for
     * {@link io.nats.stan.ConnectionFactory#ConnectionFactory(java.lang.String, java.lang.String)}.
     * Tests that no exception is thrown and that cluster name and clientID are properly set.
     */
    @Test
    public void testConnectionFactoryStringString() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        assertEquals(testClusterName, cf.getClusterId());
        assertEquals(testClientName, cf.getClientId());
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#createConnection()}.
     */
    @Test
    public void testCreateConnection() {
        try (io.nats.client.Connection nc = setupMockNatsConnection()) {
            ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
            cf.setNatsConnection(nc);
            try (Connection sc = cf.createConnection()) {
                assertTrue(sc instanceof ConnectionImpl);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#options()}.
     */
    @Test
    public void testOptions() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setAckTimeout(Duration.ofMillis(100));
        cf.setConnectTimeout(Duration.ofMillis(500));
        cf.setDiscoverPrefix("_FOO");
        cf.setMaxPubAcksInFlight(1000);
        io.nats.client.Connection nc = null;
        try {
            nc = setupMockNatsConnection();
        } catch (IOException | TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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
     * Test method for {@link io.nats.stan.ConnectionFactory#getAckTimeout()}.
     */
    @Test
    public void testGetAckTimeout() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setAckTimeout(Duration.ofMillis(100));
        assertEquals(100, cf.getAckTimeout().toMillis());
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#setAckTimeout(java.time.Duration)}.
     */
    @Test
    public void testSetAckTimeoutDuration() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setAckTimeout(Duration.ofMillis(100));
    }

    /**
     * Test method for
     * {@link io.nats.stan.ConnectionFactory#setAckTimeout(long, java.util.concurrent.TimeUnit)}.
     */
    @Test
    public void testSetAckTimeoutLongTimeUnit() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setAckTimeout(100, TimeUnit.MILLISECONDS);
        assertEquals(100, cf.getAckTimeout().toMillis());
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#getConnectTimeout()}.
     */
    @Test
    public void testGetConnectTimeout() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setConnectTimeout(Duration.ofMillis(250));
        assertEquals(250, cf.getConnectTimeout().toMillis());
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#setConnectTimeout(java.time.Duration)}.
     */
    @Test
    public void testSetConnectTimeoutDuration() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setConnectTimeout(Duration.ofMillis(250));
        assertEquals(250, cf.getConnectTimeout().toMillis());
    }

    /**
     * Test method for
     * {@link io.nats.stan.ConnectionFactory#setConnectTimeout(long, java.util.concurrent.TimeUnit)}
     * .
     */
    @Test
    public void testSetConnectTimeoutLongTimeUnit() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setConnectTimeout(250, TimeUnit.MILLISECONDS);
        assertEquals(250, cf.getConnectTimeout().toMillis());
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#getDiscoverPrefix()}.
     */
    @Test
    public void testGetDiscoverPrefix() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        assertNotNull(cf.getDiscoverPrefix());
        assertEquals(cf.getDiscoverPrefix(), ConnectionImpl.DEFAULT_DISCOVER_PREFIX);
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#setDiscoverPrefix(java.lang.String)}.
     */
    @Test(expected = NullPointerException.class)
    public void testSetDiscoverPrefix() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setDiscoverPrefix("_FOO");
        assertEquals(cf.getDiscoverPrefix(), "_FOO");
        cf.setDiscoverPrefix(null);
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#getMaxPubAcksInFlight()}.
     */
    @Test
    public void testGetMaxPubAcksInFlight() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setMaxPubAcksInFlight(1000);
        assertEquals(1000, cf.getMaxPubAcksInFlight());
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#setMaxPubAcksInFlight(int)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetMaxPubAcksInFlight() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setMaxPubAcksInFlight(1000); // should work
        cf.setMaxPubAcksInFlight(-1); // should throw IllegalArgumentException
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#getnatsUrl()}.
     */
    @Test
    public void testGetNatsUrl() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        assertNotNull(cf.getNatsUrl());
        assertEquals(ConnectionImpl.DEFAULT_NATS_URL, cf.getNatsUrl());
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#setNatsUrl(java.lang.String)}.
     */
    @Test
    public void testSetNatsUrl() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setNatsUrl("nats://foobar:1234"); // Should work
    }

    @Test(expected = NullPointerException.class)
    public void testSetNatsUrlNull() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setNatsUrl(null); // Should throw
    }

    /**
     * Test method for
     * {@link io.nats.stan.ConnectionFactory#setNatsConnection(io.nats.client.Connection)}.
     */
    @Test
    public void testSetNatsConnection() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setNatsConnection(null);
        try {
            cf.setNatsConnection(setupMockNatsConnection());
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#getClientId()}.
     */
    @Test
    public void testGetClientId() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        assertEquals(testClientName, cf.getClientId());
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#setClientId(java.lang.String)}.
     */
    @Test(expected = NullPointerException.class)
    public void testSetClientId() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setClientId("foo");
        cf.setClientId(null);
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#getClusterId()}.
     */
    @Test
    public void testGetClusterId() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        assertEquals(cf.getClusterId(), testClusterName);
    }

    /**
     * Test method for {@link io.nats.stan.ConnectionFactory#setClusterId(java.lang.String)}.
     */
    @Test(expected = NullPointerException.class)
    public void testSetClusterId() {
        ConnectionFactory cf = new ConnectionFactory(testClusterName, testClientName);
        cf.setClusterId("foo");
        cf.setClusterId(null);
    }

}
