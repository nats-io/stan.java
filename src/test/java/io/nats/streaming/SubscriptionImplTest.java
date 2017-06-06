/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import static io.nats.streaming.NatsStreaming.ERR_UNSUB_REQ_TIMEOUT;
import static io.nats.streaming.UnitTestUtilities.newMockedConnection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.streaming.protobuf.SubscriptionResponse;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

@Category(UnitTest.class)
public class SubscriptionImplTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionImpl#SubscriptionImpl()}.
     */
    @Test
    public void testSubscriptionImpl() throws Exception {
        try (Subscription sub = new SubscriptionImpl()) {
            assertNotNull(sub);
        }
    }

    @Test
    public void testSubscriptionImplStringStringMessageHandlerConnectionImplSubscriptionOptions()
            throws Exception {
        StreamingConnectionImpl conn = null;
        try {
            conn = (StreamingConnectionImpl) newMockedConnection();
        } catch (IOException e) {
            /* NOOP */
        }
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (SubscriptionImpl sub = new SubscriptionImpl("foo", "bar", null, conn, opts)) {
            sub.setAckInbox("_INBOX.foo");
        }
    }

    // /**
    // * Test method for {@link io.nats.streaming.SubscriptionImpl#rLock()}.
    // */
    // @Test
    // public void testRLock() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.SubscriptionImpl#rUnlock()}.
    // */
    // @Test
    // public void testRUnlock() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.SubscriptionImpl#wLock()}.
    // */
    // @Test
    // public void testWLock() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.SubscriptionImpl#wUnlock()}.
    // */
    // @Test
    // public void testWUnlock() {
    // fail("Not yet implemented"); // TODO
    // }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionImpl} get*() methods.
     */
    @Test
    public void testGetters() throws Exception {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (SubscriptionImpl sub = new SubscriptionImpl()) {
            sub.opts = opts;
            sub.rLock();
            sub.getAckInbox();
            sub.getConnection();
            sub.getMessageHandler();
            sub.rUnlock();
        }
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionImpl#getOptions()}.
     */
    @Test
    public void testGetOptions() throws Exception {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (SubscriptionImpl sub = new SubscriptionImpl()) {
            sub.opts = opts;
            assertEquals(opts, sub.getOptions());
        }
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionImpl#unsubscribe()}.
     *
     * @throws TimeoutException if unsubscribe request times out
     * @throws IOException      if unsubscribe throws an IOException
     */
    @Test
    public void testUnsubscribe() throws Exception {
        StreamingConnectionImpl conn = (StreamingConnectionImpl) newMockedConnection();
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (Subscription ignored = conn.subscribe("foo", null, opts)) {
            SubscriptionResponse sr = SubscriptionResponse.newBuilder().build();
            io.nats.client.Message raw = mock(io.nats.client.Message.class);
            when(raw.getData()).thenReturn(sr.toByteArray());
            when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                    any(TimeUnit.class))).thenReturn(raw);
        }
    }


    /**
     * Test method for {@link io.nats.streaming.SubscriptionImpl#unsubscribe()}. This should throw a
     * IOException.
     *
     * @throws IOException if unsubscribe request times out (which it should)
     */
    @Test
    public void testUnsubscribeTimeout() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_UNSUB_REQ_TIMEOUT);

        StreamingConnectionImpl conn = null;
        try {
            conn = (StreamingConnectionImpl) newMockedConnection();
        } catch (IOException e) {
            /* NOOP */
        }
        assertNotNull(conn);
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        SubscriptionImpl sub = new SubscriptionImpl("foo", "bar", null, conn, opts);
        sub.setAckInbox("_ACK.streaming.foo");
        SubscriptionResponse usr = SubscriptionResponse.newBuilder().build();
        doReturn(null).when(conn.nc).request(eq(conn.unsubRequests),
                any(byte[].class), any(long.class));
        sub.unsubscribe();
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionImpl#unsubscribe()}. This should
     * throw an
     * IOException.
     *
     * @throws IOException if unsubscribe request times out (which it should)
     */
    @Test(expected = IOException.class)
    public void testUnsubscribeThrowsIoEx() throws Exception {
        StreamingConnectionImpl conn = null;
        try {
            conn = (StreamingConnectionImpl) newMockedConnection();
        } catch (IOException e) {
            /* NOOP */
        }
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        try (SubscriptionImpl sub = new SubscriptionImpl("foo", "bar", null, conn, opts)) {
            sub.setAckInbox("_ACK.streaming.foo");
            SubscriptionResponse usr =
                    SubscriptionResponse.newBuilder().setError("An error occurred").build();
            io.nats.client.Message raw = new io.nats.client.Message();
            raw.setData(usr.toByteArray());
            assertNotNull(conn);
            when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class)))
                    .thenReturn(raw);
            sub.unsubscribe(); // should throw IOException
            verify(conn.nc).request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                    any(TimeUnit.class));
        }
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionImpl#unsubscribe()}. This should
     * throw an
     * IllegalStateException.
     *
     * @throws TimeoutException      - if a timeout occurs
     * @throws IOException           - if an I/O exeception occurs
     * @throws IllegalStateException if the NATS connection is null (which it should be)
     */
    @Test
    public void testUnsubscribeBadSubscription() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(NatsStreaming.ERR_BAD_SUBSCRIPTION);

        try (StreamingConnectionImpl conn = (StreamingConnectionImpl) newMockedConnection()) {
            SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
            try (Subscription sub = conn.subscribe("foo", null, opts)) {
                SubscriptionResponse sr = SubscriptionResponse.newBuilder().build();
                io.nats.client.Message raw = mock(io.nats.client.Message.class);
                when(raw.getData()).thenReturn(sr.toByteArray());
                when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                        any(TimeUnit.class))).thenReturn(raw);
                sub.unsubscribe();
                // Should throw IllegalStateException
                sub.unsubscribe();
            }
        }
    }

    @Test
    public void testUnsubscribeNatsConnClosed() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(NatsStreaming.ERR_CONNECTION_CLOSED);

        try (StreamingConnectionImpl conn = (StreamingConnectionImpl) newMockedConnection()) {
            SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
            try (Subscription sub = conn.subscribe("foo", null, opts)) {
                SubscriptionResponse sr = SubscriptionResponse.newBuilder().build();
                io.nats.client.Message raw = mock(io.nats.client.Message.class);
                when(raw.getData()).thenReturn(sr.toByteArray());
                when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                        any(TimeUnit.class))).thenReturn(raw);
                conn.nc = null;
                // Should throw IllegalStateException
                sub.unsubscribe();
            }
        }
    }

    @Test
    public void testCloseSuccess() throws Exception {
        try (StreamingConnectionImpl conn = (StreamingConnectionImpl) newMockedConnection()) {
            SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
            SubscriptionImpl sub =
                    Mockito.spy(new SubscriptionImpl("foo", "bar", null, conn, opts));
            sub.inboxSub = mock(io.nats.client.Subscription.class);
            // w
            sub.sc = conn;
            sub.setAckInbox("FOO");
            assertNotNull(sub.sc);
            sub.close();
        }
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionImpl#close()}.
     */
    @Test
    public void testCloseException() throws Exception {
        StreamingConnectionImpl conn = null;
        try {
            conn = (StreamingConnectionImpl) newMockedConnection();
        } catch (IOException e) {
            /* NOOP */
        }
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        SubscriptionImpl sub = new SubscriptionImpl("foo", "bar", null, conn, opts);
        sub.setAckInbox("_ACK.streaming.foo");
        SubscriptionResponse usr =
                SubscriptionResponse.newBuilder().setError("An error occurred").build();
        io.nats.client.Message raw = new io.nats.client.Message();
        raw.setData(usr.toByteArray());
        assertNotNull(conn);
        when(conn.nc.request(eq(conn.unsubRequests), any(byte[].class), any(long.class),
                any(TimeUnit.class))).thenReturn(raw);
        sub.close();
        assertNull(sub.sc);
    }

//    @Test
//    public void testCloseUnsubscribeException() throws Exception {
//        try (StreamingConnectionImpl conn = (StreamingConnectionImpl) newMockedConnection()) {
//            SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
//            SubscriptionImpl sub =
//                    Mockito.spy(new SubscriptionImpl("foo", "bar", null, conn, opts));
//            doThrow(new IOException("FOO")).when(sub).close(true);
//
//            sub.close();
//            verifier.verifyLogMsgEquals(Level.WARN,
//                    "stan: exception during unsubscribe for subject foo");
//        }
//    }

}
