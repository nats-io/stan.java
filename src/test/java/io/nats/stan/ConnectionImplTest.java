/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import static io.nats.stan.UnitTestUtilities.newMockedConnection;
import static io.nats.stan.UnitTestUtilities.setupMockNatsConnection;
import static io.nats.stan.UnitTestUtilities.sleep;
import static io.nats.stan.UnitTestUtilities.testClientName;
import static io.nats.stan.UnitTestUtilities.testClusterName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.client.Channel;
import io.nats.client.NUID;
import io.nats.stan.ConnectionImpl.AckClosure;
import io.nats.stan.protobuf.CloseResponse;
import io.nats.stan.protobuf.ConnectResponse;
import io.nats.stan.protobuf.MsgProto;
import io.nats.stan.protobuf.PubAck;
import io.nats.stan.protobuf.StartPosition;
import io.nats.stan.protobuf.SubscriptionRequest;
import io.nats.stan.protobuf.SubscriptionResponse;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Category(UnitTest.class)
@RunWith(MockitoJUnitRunner.class)
public class ConnectionImplTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = (Logger) LoggerFactory.getLogger(ConnectionImplTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @Mock
    private Channel<PubAck> pac;

    @Mock
    Map<String, AckClosure> pubAckMap;

    @Mock
    Map<String, Subscription> subMap;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        verifier.setup();
    }

    // Always have this teardown otherwise we can stuff up our expectations. Besides, it's
    // good coding practise
    @After
    public void tearDown() throws Exception {
        verifier.teardown();
    }

    @SuppressWarnings("resource")
    @Test
    public void testConnectionImpl() {
        new ConnectionImpl();
    }

    @Test
    public void testConnectionImplStringString() {
        ConnectionImpl conn = new ConnectionImpl(testClusterName, testClientName);
        assertNotNull(conn);
    }

    @Test
    public void testConnectionImplOptions() {
        Options opts = new Options.Builder().setAckTimeout(Duration.ofSeconds(555)).create();
        ConnectionImpl conn = new ConnectionImpl(testClusterName, testClientName, opts);
        assertNotNull(conn);
        assertEquals(555, conn.opts.getAckTimeout().getSeconds());
    }

    @Test
    public void testConnectSuccess() {
        try (io.nats.client.Connection nconn = setupMockNatsConnection()) {
            assertNotNull(nconn);
            assertFalse(nconn.isClosed());
            Options opts = new Options.Builder().setNatsConn(nconn).create();
            assertNotNull(opts.getNatsConn());
            try (ConnectionImpl conn = new ConnectionImpl(testClusterName, testClientName, opts)) {
                assertNotNull(conn);
                assertNotNull(conn.nc);
                conn.connect();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Tests that connect() will correctly create a NATS connection if needed.
     */
    @Test
    public void testConnectSuccessInternalNats() {
        try {
            io.nats.client.ConnectionFactory ncf = mock(io.nats.client.ConnectionFactory.class);
            io.nats.client.Connection nc = setupMockNatsConnection();
            ConnectionImpl conn = Mockito.spy(new ConnectionImpl(testClusterName, testClientName));
            assertNotNull(conn);
            when(conn.createNatsConnectionFactory()).thenReturn(ncf);
            when(ncf.createConnection()).thenReturn(nc);
            conn.connect();
            assertNotNull(conn.nc);
            assertEquals(nc, conn.nc);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Tests for IOException when ConnectResponse contains a non-empty error.
     */
    @Test
    public void testConnectResponseError() {
        try (io.nats.client.Connection nconn = setupMockNatsConnection()) {
            // Test for request IOException
            String discoverSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_DISCOVER_PREFIX, testClusterName);
            ConnectResponse cr =
                    ConnectResponse.newBuilder().setError("stan: this is a fake error").build();
            io.nats.client.Message raw = new io.nats.client.Message("foo", "bar", cr.toByteArray());
            when(nconn.request(eq(discoverSubject), any(byte[].class), any(long.class)))
                    .thenReturn(raw);

            Options opts = new Options.Builder().setNatsConn(nconn).create();
            boolean exThrown = false;
            try (ConnectionImpl conn = new ConnectionImpl(testClusterName, testClientName, opts)) {
                conn.connect();
            } catch (Exception e) {
                assertTrue("Wrong exception type: " + e.getClass().getName(),
                        e instanceof IOException);
                assertEquals("stan: this is a fake error", e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown IOException", exThrown);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testConnectIoError() {
        try (io.nats.client.Connection nconn = setupMockNatsConnection()) {
            // Test for request IOException
            String discoverSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_DISCOVER_PREFIX, testClusterName);
            doThrow(new IOException("fake I/O exception")).when(nconn).request(eq(discoverSubject),
                    any(byte[].class), any(long.class));

            Options opts = new Options.Builder().setNatsConn(nconn).create();
            boolean exThrown = false;
            try (ConnectionImpl conn = new ConnectionImpl(testClusterName, testClientName, opts)) {
                conn.connect();
            } catch (Exception e) {
                assertTrue("Wrong exception type: " + e.getClass().getName(),
                        e instanceof IOException);
                assertEquals("fake I/O exception", e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown IOException", exThrown);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testConnectTimeout() {
        try (io.nats.client.Connection nconn = setupMockNatsConnection()) {
            // Test for request TimeoutException
            String discoverSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_DISCOVER_PREFIX, testClusterName);
            doThrow(new TimeoutException()).when(nconn).request(eq(discoverSubject),
                    any(byte[].class), any(long.class));

            Options opts = new Options.Builder().setNatsConn(nconn).create();
            boolean exThrown = false;
            try (ConnectionImpl conn = new ConnectionImpl(testClusterName, testClientName, opts)) {
                conn.connect();
            } catch (Exception e) {
                assertTrue("Wrong exception type: " + e.getClass().getName(),
                        e instanceof TimeoutException);
                assertEquals(ConnectionImpl.ERR_CONNECTION_REQ_TIMEOUT, e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown TimeoutException", exThrown);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetterSetters() {
        try (ConnectionImpl conn = newMockedConnection()) {
            // Channel<PubAck> mockChan = mock(Channel.class);
            conn.setPubAckChan(pac);
            assertEquals(conn.getPubAckChan(), pac);

            @SuppressWarnings("unchecked")
            Map<String, AckClosure> mockMap = (Map<String, AckClosure>) mock(Map.class);
            conn.setPubAckMap(mockMap);
            assertEquals(mockMap, conn.getPubAckMap());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseSuccess() {
        try (ConnectionImpl conn = newMockedConnection()) {
            assertNotNull(conn);
            conn.close();
            assertNull(conn.nc);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseAckUnsubscribeFailure() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            assertNotNull(conn);
            io.nats.client.Subscription mockSub = mock(io.nats.client.Subscription.class);
            when(conn.getAckSubscription()).thenReturn(mockSub);
            doThrow(new IOException("fake I/O exception")).when(mockSub).unsubscribe();
            conn.close();
            verifier.verifyLogMsgEquals(Level.WARN,
                    "stan: error unsubscribing from acks during connection close");
            assertNull(conn.nc);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseAckSubscriptionNull() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            when(conn.getAckSubscription()).thenReturn(null);
            conn.close();
            assertNull(conn.nc);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseRequestTimeout() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            doThrow(new TimeoutException()).when(conn.nc).request(eq(conn.closeRequests),
                    any(byte[].class), any(long.class));
            boolean exThrown = false;
            try {
                conn.close();
            } catch (Exception e) {
                assertTrue(e instanceof TimeoutException);
                assertEquals(ConnectionImpl.ERR_CLOSE_REQ_TIMEOUT, e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown exception", exThrown);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseRequestIoError() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            doThrow(new IOException("TEST")).when(conn.nc).request(eq(conn.closeRequests),
                    any(byte[].class), any(long.class));
            boolean exThrown = false;
            try {
                conn.close();
            } catch (Exception e) {
                assertTrue(e instanceof IOException);
                assertEquals("TEST", e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown exception", exThrown);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseResponseErrorThrows() {
        final String errorText = "TEST";
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            // Close response has non-empty error
            CloseResponse closeResponseProto =
                    CloseResponse.newBuilder().setError(errorText).build();
            io.nats.client.Message closeResponse =
                    new io.nats.client.Message("foo", "bar", closeResponseProto.toByteArray());
            when(conn.nc.request(eq(conn.closeRequests), any(byte[].class), any(long.class)))
                    .thenReturn(closeResponse);

            // Should throw IOException when cr.getError isn't empty
            boolean exThrown = false;
            try {
                conn.close();
            } catch (Exception e) {
                assertTrue(e instanceof IOException);
                assertEquals(errorText, e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown exception", exThrown);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseResponseNullPayload() throws IOException, TimeoutException {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            io.nats.client.Message closeResponse = new io.nats.client.Message("foo", "bar", null);
            when(conn.nc.request(eq(conn.closeRequests), any(byte[].class), any(long.class)))
                    .thenReturn(closeResponse);

            // Should not throw any exception on empty payload
            conn.close();
            // verifier.verifyLogMsgEquals(Level.WARN, "stan: CloseResponse was null");
        }
    }

    @Test
    public void testPublishNullNatsConnThrowsEx() throws IOException, TimeoutException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("stan: connection closed");

        try (ConnectionImpl conn = newMockedConnection()) {
            conn.nc = null;
            io.nats.client.Message msg = new io.nats.client.Message();
            msg.setReplyTo("bar");
            conn.publish("testPublishStringByteArray", "Hello World".getBytes());
        }
    }

    @Test
    public void testPublishStringByteArray() {
        String subj = "testPublishStringByteArray";
        byte[] payload = "Hello World".getBytes();
        try (ConnectionImpl conn = newMockedConnection()) {
            conn.publish(subj, payload);
            verify(conn.nc).publish(matches(conn.pubPrefix + "." + subj), eq(conn.ackSubject),
                    any(byte[].class));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPublishStringByteArrayAckHandler() throws IOException, TimeoutException {
        AckHandler mockHandler = mock(AckHandler.class);

        try (Connection conn = newMockedConnection()) {
            // publish null msg
            conn.publish("foo", (byte[]) null, mockHandler);
        }
    }

    @Test
    public void testPublishStringStringByteArray() throws IOException, TimeoutException {
        final byte[] payload = "Hello World".getBytes();
        try (Connection conn = newMockedConnection()) {
            // publish null msg
            conn.publish("foo", null, payload);
        }
    }

    @Test
    public void testPublishStringStringByteArrayThrowsEx() throws IOException, TimeoutException {
        thrown.expect(IOException.class);

        final String exMessage = "ack exception";
        thrown.expect(IOException.class);
        thrown.expectMessage(exMessage);
        final String subject = "testAckException";
        final String reply = null;
        final byte[] payload = "Hello World".getBytes();

        // final AckHandler[] ah = new AckHandler[1];
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            doAnswer(new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    AckHandler ah = (AckHandler) args[3];
                    ah.onAck("foobar", new IOException(exMessage));
                    return null;
                }
            }).when(conn).publish(eq(subject), eq(reply), eq(payload), any(AckHandler.class));

            // publish null msg
            conn.publish(subject, reply, payload);
        }
    }

    @Test
    public void testPublishStringStringByteArrayAckHandler() {
        try (Connection conn = newMockedConnection()) {
            // publish null msg
            String guid = conn.publish("foo", null, null, null);
            assertNotNull(guid);
            assertEquals(22, guid.length());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // @Test
    // public void testPublishStringStringByteArrayAckHandlerAckChannelAddFailed() {
    // try (final ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
    // // Simulate PubAckChan add failure
    //
    // when(pac.add(any(PubAck.class))).thenReturn(false);
    //
    // conn.setPubAckChan(pac);
    //
    // conn.publish("foo", "bar", null, null);
    //
    // // Now verify our logging interactions
    // verifier.verifyLogMsgEquals(Level.ERROR,
    // "Failed to add ack token to buffered channel, count=0");
    // } catch (Exception e) {
    // e.printStackTrace();
    // fail(e.getMessage());
    // }
    // }

    @Test
    public void testPublishStringStringByteArrayAckHandlerAckChannelPutInterrupted() {
        try (final ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            // Simulate PubAckChan interruption
            conn.setPubAckChan(pac);
            doThrow(new InterruptedException()).when(pac).put(eq(PubAck.getDefaultInstance()));

            conn.publish("foo", "bar", null, null);

            // Now verify our logging interactions
            verifier.verifyLogMsgEquals(Level.WARN,
                    "stan: interrupted while writing to publish ack channel");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPublishStringStringByteArrayAckHandlerNatsPublishFailure()
            throws IOException, TimeoutException {
        thrown.expect(IOException.class);
        thrown.expectMessage("Test exception");

        try (ConnectionImpl conn = newMockedConnection()) {
            // test for interrupted channel add
            io.nats.client.Connection nc = mock(io.nats.client.Connection.class);
            doThrow(new IOException("Test exception")).when(nc).publish(any(String.class),
                    any(String.class), any(byte[].class));
            io.nats.client.Connection origNatsConn = conn.nc;

            // Set connection to mock in order to trigger exception
            conn.nc = nc;
            conn.publish("foo", "bar", null, null);

            // Reset connection to allow proper close()
            conn.nc = origNatsConn;
        }
    }

    @Test
    public void testPublishStringStringByteArrayAckHandlerIllegalAckTimeoutValue()
            throws IOException, TimeoutException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Negative delay.");
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            String guid = NUID.nextGlobal();
            AckClosure ac = mock(AckClosure.class);
            ac.ah = mock(AckHandler.class);
            conn.getPubAckMap().put(guid, ac);

            // Now process with Timer schedule exception
            conn.opts = new Options.Builder().setAckTimeout(Duration.ofMillis(-1)).create();
            assertEquals(-1, conn.opts.getAckTimeout().toMillis());

            conn.publish("foo", "bar", null, null);

        }
    }


    @Test
    public void test_SubscribeNullNatsConnection() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            io.nats.client.Connection oldNc = conn.nc;
            boolean exThrown = false;
            try {
                conn.nc = null;
                conn.subscribe("foo", null);
            } catch (Exception e) {
                assertTrue(e instanceof IllegalStateException);
                assertEquals(ConnectionImpl.ERR_CONNECTION_CLOSED, e.getMessage());
                exThrown = true;
            }
            conn.nc = oldNc;
            assertTrue("Should have thrown exception", exThrown);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test(expected = TimeoutException.class)
    public void testSubscriptionRequestTimeout() throws TimeoutException {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            doThrow(new TimeoutException()).when(conn.nc).request(eq(conn.subRequests),
                    any(byte[].class), any(long.class), any(TimeUnit.class));
            conn.subscribe("foo", null);
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testBasicSubscribeSuccess() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            conn.subscribe("foobar", null);

            conn.subscribe("foobar2", "bar", new MessageHandler() {
                public void onMessage(Message msg) {}
            });
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testBasicSubscribeDurableSuccess() {
        try (ConnectionImpl conn = newMockedConnection()) {
            conn.subscribe("foo", null,
                    new SubscriptionOptions.Builder().setDurableName("my-durable").build());
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testBasicSubscribeProtoUnmarshalError() throws TimeoutException, IOException {
        thrown.expect(IOException.class);
        thrown.expectMessage("stan: bad request");
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            SubscriptionResponse subRespProto =
                    SubscriptionResponse.newBuilder().setError("stan: bad request").build();
            io.nats.client.Message rawSubResponse =
                    new io.nats.client.Message("foo", "bar", subRespProto.toByteArray());
            when(conn.nc.request(eq(conn.subRequests), any(byte[].class), any(long.class),
                    any(TimeUnit.class))).thenReturn(rawSubResponse);
            conn.subscribe("foo", "bar", null, null);
        }
    }

    @Test
    public void testBasicSubscribeProtoResponseError() throws TimeoutException, IOException {
        thrown.expect(InvalidProtocolBufferException.class);
        // thrown.expectMessage("stan: bad request");
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            byte[] payload = "Junk".getBytes();
            io.nats.client.Message rawSubResponse =
                    new io.nats.client.Message("foo", "bar", payload);
            when(conn.nc.request(eq(conn.subRequests), any(byte[].class), any(long.class),
                    any(TimeUnit.class))).thenReturn(rawSubResponse);
            conn.subscribe("foo", "bar", null, null);
        }
    }

    @Test
    public void testSubscriptionStartAt() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            io.nats.client.Connection nc = conn.nc;
            SubscriptionOptions opts = null;

            SubscriptionResponse response =
                    SubscriptionResponse.newBuilder().setAckInbox("_ACKS.FOO").build();
            io.nats.client.Message raw =
                    new io.nats.client.Message("foo", "bar", response.toByteArray());

            when(nc.request(eq(conn.subRequests), any(byte[].class), any(long.class),
                    any(TimeUnit.class))).thenReturn(raw);

            // Test for StartPosition.First
            opts = new SubscriptionOptions.Builder().deliverAllAvailable().build();

            conn.subscribe("foo", null, opts);

            ArgumentCaptor<byte[]> argument = ArgumentCaptor.forClass(byte[].class);
            verify(nc, times(1)).request(eq(conn.subRequests), argument.capture(), any(long.class),
                    any(TimeUnit.class));
            assertNotNull(argument.getValue());
            SubscriptionRequest sr = SubscriptionRequest.parseFrom(argument.getValue());
            assertEquals(StartPosition.First, sr.getStartPosition());

            // Test for StartPosition.LastReceived
            opts = new SubscriptionOptions.Builder().startWithLastReceived().build();

            conn.subscribe("foo", null, opts);

            argument = ArgumentCaptor.forClass(byte[].class);
            verify(nc, times(2)).request(eq(conn.subRequests), argument.capture(), any(long.class),
                    any(TimeUnit.class));
            assertNotNull(argument.getValue());
            sr = SubscriptionRequest.parseFrom(argument.getValue());
            assertEquals(StartPosition.LastReceived, sr.getStartPosition());

            // Test for StartPosition.NewOnly
            opts = new SubscriptionOptions.Builder().build();

            conn.subscribe("foo", null, opts);

            argument = ArgumentCaptor.forClass(byte[].class);
            verify(nc, times(3)).request(eq(conn.subRequests), argument.capture(), any(long.class),
                    any(TimeUnit.class));
            assertNotNull(argument.getValue());
            sr = SubscriptionRequest.parseFrom(argument.getValue());
            assertEquals(StartPosition.NewOnly, sr.getStartPosition());

            // Test for StartPosition.SequenceStart
            opts = new SubscriptionOptions.Builder().startAtSequence(12345).build();

            conn.subscribe("foo", null, opts);

            argument = ArgumentCaptor.forClass(byte[].class);
            verify(nc, times(4)).request(eq(conn.subRequests), argument.capture(), any(long.class),
                    any(TimeUnit.class));
            assertNotNull(argument.getValue());
            sr = SubscriptionRequest.parseFrom(argument.getValue());
            assertEquals(StartPosition.SequenceStart, sr.getStartPosition());
            assertEquals(12345, sr.getStartSequence());

            Duration delta = Duration.ofSeconds(30);
            // Test for StartPosition.TimeDeltaStart
            opts = new SubscriptionOptions.Builder().startAtTimeDelta(delta).build();

            conn.subscribe("foo", null, opts);

            argument = ArgumentCaptor.forClass(byte[].class);
            verify(nc, times(5)).request(eq(conn.subRequests), argument.capture(), any(long.class),
                    any(TimeUnit.class));
            assertNotNull(argument.getValue());
            sr = SubscriptionRequest.parseFrom(argument.getValue());
            assertEquals(StartPosition.TimeDeltaStart, sr.getStartPosition());
            // since StartAtTimeDelta recalculates
            assertTrue(sr.getStartTimeDelta() - delta.toNanos() <= Duration.ofMillis(10).toNanos());

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }


    // @Test
    // public void testSubscribeStringMessageHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSubscribeStringMessageHandlerSubscriptionOptions() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSubscribeStringStringMessageHandler() {
    // fail("Not yet implemented"); // TODO
    // }

    @Test
    public void testSubscribeStringStringMessageHandlerSubscriptionOptionsBadStartPosition()
            throws IOException, TimeoutException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Can't get the number of an unknown enum value.");
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            SubscriptionOptions subOpts = new SubscriptionOptions.Builder().build();
            subOpts.startAt = StartPosition.UNRECOGNIZED;
            try {
                conn.subscribe("foo", "bar", null, subOpts);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }
    }

    // @Test
    // public void test_subscribe() {
    // fail("Not yet implemented"); // TODO
    // }

    @Test
    public void testProcessAckSuccess() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            String guid = NUID.nextGlobal();
            AckClosure ac = mock(AckClosure.class);
            ac.ah = mock(AckHandler.class);
            conn.pubAckMap.put(guid, ac);
            PubAck pa = PubAck.newBuilder().setGuid(guid).build();
            io.nats.client.Message raw = new io.nats.client.Message("foo", "bar", pa.toByteArray());
            conn.processAck(raw);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessAckTimeoutMethod() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            String guid = NUID.nextGlobal();
            AckClosure ac = mock(AckClosure.class);
            ac.ah = mock(AckHandler.class);
            conn.getPubAckMap().put(guid, ac);

            // Test with non-null AckHandler
            conn.processAckTimeout(guid, ac.ah);

            // ackClosure should have been removed
            assertNull(conn.getPubAckMap().get(guid));

            // AckHandler should have been invoked
            verify(ac.ah, times(1)).onAck(eq(guid), any(TimeoutException.class));

            // Now test with null AckHandler
            conn.processAckTimeout(guid, null);

            // No new invocation of AckHandler
            verify(ac.ah, times(1)).onAck(eq(guid), any(TimeoutException.class));

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessAckUnmarshalError() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            String guid = NUID.nextGlobal();
            AckHandler ah = mock(AckHandler.class);
            AckClosure ac = conn.createAckClosure(guid, ah);
            conn.pubAckMap.put(guid, ac);
            io.nats.client.Message raw =
                    new io.nats.client.Message("foo", "bar", "junk".getBytes());
            conn.processAck(raw);
            verifier.verifyLogMsgEquals(Level.ERROR, "stan: error unmarshaling PubAck");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessAckNonEmptyErrorField() {
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            String guid = NUID.nextGlobal();
            AckHandler ah = mock(AckHandler.class);
            AckClosure ac = conn.createAckClosure(guid, ah);
            conn.pubAckMap.put(guid, ac);
            PubAck pa = PubAck.newBuilder().setGuid(guid).setError("ERROR").build();
            io.nats.client.Message raw = new io.nats.client.Message("foo", "bar", pa.toByteArray());
            conn.processAck(raw);
            verifier.verifyLogMsgEquals(Level.ERROR, "stan: protobuf PubAck error: ERROR");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessHeartBeatCallbackSuccess() {
        try (ConnectionImpl conn = newMockedConnection()) {
            io.nats.client.Message msg = new io.nats.client.Message();
            assertNotNull(conn.hbSubscription);
            assertNotNull(conn.hbSubscription.getSubject());
            msg.setSubject(conn.hbSubscription.getSubject());
            msg.setReplyTo("foo");
            assertNotNull(conn.hbCallback);
            conn.hbCallback.onMessage(msg);
            verify(conn.nc).publish(eq("foo"), eq(null));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessHeartBeatPublishLogsError() {
        final String error = "TEST";
        final String errorMsg = "stan: error publishing heartbeat response: " + error;
        try (ConnectionImpl conn = newMockedConnection()) {
            doThrow(new IOException(error)).when(conn.nc).publish(any(String.class), eq(null));
            io.nats.client.Message msg = new io.nats.client.Message();
            msg.setReplyTo("foo");
            conn.processHeartBeat(msg);
            verifier.verifyLogMsgEquals(Level.WARN, errorMsg);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRemoveAckSuccess() {
        try (ConnectionImpl conn = newMockedConnection()) {
            when(pac.get()).thenReturn(PubAck.getDefaultInstance());
            String guid = NUID.nextGlobal();
            TimerTask ackTask = mock(TimerTask.class);
            AckClosure ac = mock(AckClosure.class);
            ac.ackTask = ackTask;
            ac.guid = guid;
            conn.pubAckMap.put(guid, ac);

            // Should return our AckClosure
            assertEquals(ac, conn.removeAck(guid));

            // Should no longer be in the pubAckMap
            assertNull(conn.pubAckMap.get(guid));

            // aclTask should be null
            assertNull(ac.ackTask);

            // ackTask should have been canceled
            verify(ackTask, times(1)).cancel();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRemoveAckMapThrowsEx() throws IOException, TimeoutException {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Test");

        try (ConnectionImpl conn = newMockedConnection()) {
            when(pac.get()).thenReturn(PubAck.getDefaultInstance());
            String guid = NUID.nextGlobal();
            TimerTask ackTask = mock(TimerTask.class);
            AckClosure ac = mock(AckClosure.class);
            ac.ackTask = ackTask;
            ac.guid = guid;
            conn.setPubAckMap(pubAckMap);
            doThrow(new UnsupportedOperationException("Test")).when(pubAckMap).get(guid);
            // conn.pubAckMap.put(guid, ac);

            conn.removeAck(guid);
        }
    }

    @Test
    public void testRemoveAckNullTimerTask() {
        try (ConnectionImpl conn = newMockedConnection()) {
            when(pac.get()).thenReturn(PubAck.getDefaultInstance());
            String guid = NUID.nextGlobal();
            AckClosure ac = mock(AckClosure.class);
            ac.guid = guid;
            conn.pubAckMap.put(guid, ac);

            assertEquals(ac, conn.removeAck(guid));
            assertNull(conn.pubAckMap.get(guid));
            assertNull(ac.ackTask);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRemoveAckNullAckClosure() {
        try (ConnectionImpl conn = newMockedConnection()) {
            when(pac.get()).thenReturn(PubAck.getDefaultInstance());
            when(pac.getCount()).thenReturn(0);
            String guid = NUID.nextGlobal();

            assertNull(conn.pubAckMap.get(guid));

            // Should return null
            assertNull(conn.removeAck(guid));

            // Should not have called pac.get()
            verify(pac, never()).get();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testOnMessage() {
        // This tests the NATS message handler installed on the STAN connection
        io.nats.client.Message msg = new io.nats.client.Message();
        try (ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            io.nats.client.MessageHandler cb = (io.nats.client.MessageHandler) conn;
            MsgProto msgp = MsgProto.newBuilder().setSubject("foo").setReply("bar").build();
            msg.setSubject("foo");
            msg.setData(msgp.toByteArray());
            cb.onMessage(msg);
            verify(conn, times(1)).processMsg(eq(msg));

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessMsgBasicSuccess() {
        String subject = "foo";
        try (final ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            MessageHandler mockCb = mock(MessageHandler.class);
            SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
            String ackSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_ACK_PREFIX, NUID.nextGlobal());
            when(mockSub.getAckInbox()).thenReturn(ackSubject);
            when(mockSub.getMessageHandler()).thenReturn(mockCb);
            when(mockSub.getConnection()).thenReturn(conn);

            when(mockSub.getOptions()).thenReturn(mock(SubscriptionOptions.class));
            conn.subMap.put(subject, mockSub);

            MsgProto msgp = MsgProto.newBuilder().setSubject("foo").setReply("bar")
                    .setSequence(123456789).setData(ByteString.copyFrom("".getBytes())).build();
            io.nats.client.Message raw =
                    new io.nats.client.Message("foo", ackSubject, msgp.toByteArray());

            conn.processMsg(raw);
            verify(mockCb, times(1)).onMessage(any(Message.class));
            verify(conn.nc, times(1)).publish(eq(ackSubject), any(byte[].class));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessMsgRuntimeExceptions() {
        String subject = "foo";
        try (final ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            conn.setSubMap(subMap);
            assertEquals(subMap, conn.getSubMap());
            MessageHandler mockCb = mock(MessageHandler.class);
            SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
            String ackSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_ACK_PREFIX, NUID.nextGlobal());
            when(mockSub.getAckInbox()).thenReturn(ackSubject);
            when(mockSub.getMessageHandler()).thenReturn(mockCb);
            when(mockSub.getConnection()).thenReturn(conn);

            when(mockSub.getOptions()).thenReturn(mock(SubscriptionOptions.class));
            // conn.subMap.put(subject, mockSub);

            MsgProto msgp = MsgProto.newBuilder().setSubject("foo").setReply("bar")
                    .setSequence(123456789).setData(ByteString.copyFrom("".getBytes())).build();
            io.nats.client.Message raw =
                    new io.nats.client.Message("foo", ackSubject, msgp.toByteArray());

            when(subMap.get(subject)).thenReturn(mockSub);
            when(mockSub.getOptions()).thenReturn(null);
            try {
                conn.processMsg(raw);
            } catch (Exception e) {
                /* NOOP */
            }
            verify(mockCb, never()).onMessage(any(Message.class));
            verify(conn.nc, never()).publish(eq(ackSubject), any(byte[].class));

            doThrow(new NullPointerException()).when(subMap).get(eq(subject));
            try {
                conn.processMsg(raw);
            } catch (Exception e) {
                /* NOOP */
            }
            verify(mockCb, never()).onMessage(any(Message.class));
            verify(conn.nc, never()).publish(eq(ackSubject), any(byte[].class));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessMsgManualAck() {
        String subject = "foo";
        try (final ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
            String ackSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_ACK_PREFIX, NUID.nextGlobal());
            mockSub.ackInbox = ackSubject;
            mockSub.sc = conn;
            when(mockSub.getOptions())
                    .thenReturn(new SubscriptionOptions.Builder().setManualAcks(true).build());
            conn.subMap.put(subject, mockSub);

            MsgProto msgp = MsgProto.newBuilder().setSubject("foo").setReply("bar")
                    .setSequence(123456789).setData(ByteString.copyFrom("".getBytes())).build();
            io.nats.client.Message raw =
                    new io.nats.client.Message("foo", ackSubject, msgp.toByteArray());

            conn.processMsg(raw);
            // Should not have published ack
            verify(conn.nc, never()).publish(eq(conn.ackSubject), any(byte[].class));

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessMsgNullNatsConnectionReturnsEarly() {
        String subject = "foo";
        try (final ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
            String ackSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_ACK_PREFIX, NUID.nextGlobal());
            mockSub.ackInbox = ackSubject;
            mockSub.sc = conn;
            when(mockSub.getOptions()).thenReturn(mock(SubscriptionOptions.class));
            conn.subMap.put(subject, mockSub);

            MsgProto msgp = MsgProto.newBuilder().setSubject("foo").setReply("bar")
                    .setSequence(123456789).setData(ByteString.copyFrom("".getBytes())).build();
            io.nats.client.Message raw =
                    new io.nats.client.Message("foo", ackSubject, msgp.toByteArray());
            conn.nc = null;

            Message stanMsgMock = new Message(msgp);

            try {
                when(conn.createStanMessage(eq(msgp))).thenReturn(stanMsgMock);
            } catch (NullPointerException e) {
                if (!e.getMessage().equals("stan: MsgProto cannot be null")) {
                    throw e;
                }
            }

            conn.processMsg(raw);
            assertNull(stanMsgMock.getSubscription());
            // verifier.verifyLogMsgEquals(Level.ERROR, "Exception while publishing auto-ack:");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessMsgSubConnIsNull() {
        String subject = "foo";
        try (final ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
            String ackSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_ACK_PREFIX, NUID.nextGlobal());
            when(mockSub.getAckInbox()).thenReturn(ackSubject);
            // Sub's connection needs to be null for this test.
            when(mockSub.getConnection()).thenReturn(null);
            MessageHandler mockCb = mock(MessageHandler.class);
            when(mockSub.getMessageHandler()).thenReturn(mockCb);
            when(mockSub.getOptions()).thenReturn(mock(SubscriptionOptions.class));
            conn.subMap.put(subject, mockSub);

            MsgProto msgp = MsgProto.newBuilder().setSubject("foo").setReply("bar")
                    .setSequence(123456789).setData(ByteString.copyFrom("".getBytes())).build();
            io.nats.client.Message raw =
                    new io.nats.client.Message("foo", ackSubject, msgp.toByteArray());

            conn.processMsg(raw);
            verify(mockSub, times(1)).getConnection();
            // If the sub's conn is null, we should never be trying to call getNatsConnection
            verify(conn, never()).getNatsConnection();
            // Callback should never be called in this case
            verify(mockCb, never()).onMessage(any(Message.class));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessMsgSubNatsConnIsNull() {
        String subject = "foo";
        try (final ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
            String ackSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_ACK_PREFIX, NUID.nextGlobal());
            mockSub.ackInbox = ackSubject;
            MessageHandler mockCb = mock(MessageHandler.class);
            mockSub.cb = mockCb;
            // Sub's connection needs to be null for this test.
            // mockSub.sc = conn;
            when(mockSub.getOptions()).thenReturn(mock(SubscriptionOptions.class));
            conn.subMap.put(subject, mockSub);

            MsgProto msgp = MsgProto.newBuilder().setSubject("foo").setReply("bar")
                    .setSequence(123456789).setData(ByteString.copyFrom("".getBytes())).build();
            io.nats.client.Message raw =
                    new io.nats.client.Message("foo", ackSubject, msgp.toByteArray());
            conn.nc = null;

            conn.processMsg(raw);
            // the sub's callback should not be called
            verify(mockCb, never()).onMessage(any(Message.class));
            // verifier.verifyLogMsgEquals(Level.ERROR, "Exception while publishing auto-ack:");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessMsgSubCbIsNull() {
        String subject = "foo";
        try (final ConnectionImpl conn = Mockito.spy(newMockedConnection())) {
            SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
            String ackSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_ACK_PREFIX, NUID.nextGlobal());
            mockSub.ackInbox = ackSubject;
            final MessageHandler mockCb = mock(MessageHandler.class);
            when(mockSub.getOptions()).thenReturn(mock(SubscriptionOptions.class));
            conn.subMap.put(subject, mockSub);

            MsgProto msgp = MsgProto.newBuilder().setSubject("foo").setReply("bar")
                    .setSequence(123456789).setData(ByteString.copyFrom("".getBytes())).build();
            io.nats.client.Message raw =
                    new io.nats.client.Message("foo", ackSubject, msgp.toByteArray());

            conn.processMsg(raw);
            // the sub's callback should not be called
            verify(mockCb, never()).onMessage(any(Message.class));
            // verifier.verifyLogMsgEquals(Level.ERROR, "Exception while publishing auto-ack:");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }


    @Test
    public void testProcessMsgPublishAutoAckFailure() {
        String subject = "foo";
        try (ConnectionImpl conn = newMockedConnection()) {
            SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
            String ackSubject =
                    String.format("%s.%s", ConnectionImpl.DEFAULT_ACK_PREFIX, NUID.nextGlobal());
            mockSub.ackInbox = ackSubject;
            mockSub.sc = conn;
            MessageHandler mockCb = mock(MessageHandler.class);
            mockSub.cb = mockCb;
            when(mockSub.getOptions()).thenReturn(mock(SubscriptionOptions.class));
            when(mockSub.getAckInbox()).thenReturn(ackSubject);

            when(mockSub.getConnection()).thenReturn(conn);
            conn.subMap.put(subject, mockSub);

            MsgProto msgp = MsgProto.newBuilder().setSubject("foo").setReply("bar")
                    .setSequence(123456789).setData(ByteString.copyFrom("".getBytes())).build();
            io.nats.client.Message raw =
                    new io.nats.client.Message("foo", ackSubject, msgp.toByteArray());
            doThrow(new IOException("TEST")).when(conn.nc).publish(eq(ackSubject),
                    any(byte[].class));

            assertNotNull(mockSub);
            assertNotNull(conn.nc);
            assertNotNull(mockSub.getConnection().nc);

            conn.processMsg(raw);
            verify(mockCb, never()).onMessage(any(Message.class));
            verify(conn.nc, times(1)).publish(eq(ackSubject), any(byte[].class));
            verifier.verifyLogMsgEquals(Level.ERROR, "Exception while publishing auto-ack: TEST");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessMsgUnmarshalError() {
        try (ConnectionImpl conn = newMockedConnection()) {
            io.nats.client.Message msg = new io.nats.client.Message();
            msg.setSubject("foo");
            msg.setData("junk".getBytes());
            conn.processMsg(msg);
            verifier.verifyLogMsgEquals(Level.ERROR, "stan: error unmarshaling msg");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // @Test
    // public void testNewInbox() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testLock() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testUnlock() {
    // fail("Not yet implemented"); // TODO
    // }

    @Test
    public void testSetupMockNatsConnection() {
        try (io.nats.client.Connection nc = setupMockNatsConnection()) {
            Options opts = new Options.Builder().setNatsConn(nc).create();
            try (ConnectionImpl conn = new ConnectionImpl(testClusterName, testClientName, opts)) {
                conn.connect();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCreateAckTimerTask() throws IOException, TimeoutException {
        final String guid = NUID.nextGlobal();
        AckHandler ah = new AckHandler() {
            public void onAck(String nuid, Exception ex) {}
        };
        // fail("Need to finish this test");
        try (ConnectionImpl conn = newMockedConnection()) {
            AckClosure ac = conn.createAckClosure(guid, ah);
            Map<String, AckClosure> pubAckMap = conn.getPubAckMap();
            pubAckMap.put(guid, ac);
            assertEquals(ac, pubAckMap.get(guid));
            TimerTask t = conn.createAckTimerTask(guid, ah);
            conn.ackTimer.schedule(t, 10);
            sleep(100);
            assertNull(pubAckMap.get(guid));
        }
    }

    // @Test
    // public void testAckTimerTimeout() {
    // Timer timer = new Timer();
    //
    // }
}
