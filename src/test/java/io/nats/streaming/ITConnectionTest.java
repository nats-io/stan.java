/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import static io.nats.streaming.NatsStreaming.ERR_CLOSE_REQ_TIMEOUT;
import static io.nats.streaming.NatsStreaming.ERR_SUB_REQ_TIMEOUT;
import static io.nats.streaming.NatsStreaming.ERR_UNSUB_REQ_TIMEOUT;
import static io.nats.streaming.UnitTestUtilities.runServer;
import static io.nats.streaming.UnitTestUtilities.sleep;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Stopwatch;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Nats.ConnState;
import io.nats.streaming.protobuf.StartPosition;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(IntegrationTest.class)
public class ITConnectionTest {

    private final ExecutorService service = Executors.newCachedThreadPool();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    private static final String clusterName = "test-cluster";
    private static final String clientName = "me";

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

    private StreamingConnection newDefaultConnection() throws IOException, InterruptedException {
        StreamingConnection conn = NatsStreaming.connect(clusterName, clientName);
        assertNotNull(conn);
        return conn;
    }

    @Test
    public void testNoNats() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(Nats.ERR_NO_SERVERS);

        sleep(500, TimeUnit.MILLISECONDS);
        boolean exThrown = false;
        try (StreamingConnection c =
                     NatsStreaming.connect("someNonExistentClusterID", "myTestClient")) {
            fail("Should not have connected.");
        }
    }

    @Test
    public void testUnreachable() throws Exception {
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            boolean exThrown = false;

            // Non-existent or unreachable
            final long connectTime = 25;
            Stopwatch st = Stopwatch.createStarted();
            try (StreamingConnection c =
                         NatsStreaming.connect("someNonExistentServerID", "myTestClient")) {
                fail("Should not have connected.");
            } catch (IOException e) {
                // e.printStackTrace();
                assertEquals(NatsStreaming.ERR_CONNECTION_REQ_TIMEOUT, e.getMessage());
            }
            st.stop();
            long delta = st.elapsed(TimeUnit.MILLISECONDS);
            String msg = String.format("Expected to wait at least %dms, but only waited %dms",
                    connectTime, delta);
            assertFalse(msg, delta < connectTime);
        }
    }

    @Test
    public void testConnClosedOnConnectFailure() throws Exception {
        try (NatsStreamingServer srv = runServer(clusterName)) {
            // Non-Existent or Unreachable
            int connectTime = 25;
            Options opts = new Options.Builder()
                    .connectWait(Duration.ofMillis(connectTime))
                    .build();
            boolean exThrown = false;
            try (StreamingConnection sc =
                         NatsStreaming.connect("myTestClient", "someNonExistentServerId", opts)) {
                fail("Shouldn't have connected");
            } catch (IOException e) {
                assertEquals(NatsStreaming.ERR_CONNECTION_REQ_TIMEOUT, e.getMessage());
                exThrown = true;
            } finally {
                assertTrue(exThrown);
            }

            // Check that the underlying NATS connection has been closed.
            // We will first stop the server. If we have left the NATS connection
            // opened, it should be trying to reconnect.
            srv.shutdown();

            // Wait a bit
            sleep(500, TimeUnit.MILLISECONDS);

            // Inspect threads to find reconnect
            // Thread reconnectThread = getThreadByName("reconnect");
            // assertNull("NATS StreamingConnection suspected to not have been closed.",
            // reconnectThread);
            StackTraceElement[] stack = getStackTraceByName("reconnect");
            if (stack != null) {
                for (StackTraceElement el : stack) {
                    System.err.println(el);
                    assertFalse("NATS StreamingConnection suspected to not have been closed.",
                            el.toString().contains("doReconnect"));
                }
            }
        }
    }

    @Test
    public void testNatsConnNotClosedOnClose() throws Exception {
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            // Create a NATS connection
            try (io.nats.client.Connection nc = Nats.connect()) {
                // Pass this NATS connection to NATS Streaming
                StreamingConnection sc = NatsStreaming.connect(clusterName, clientName,
                        new Options.Builder().natsConn(nc).build());
                assertNotNull(sc);
                // Now close the NATS Streaming connection
                sc.close();

                // Verify that NATS connection is not closed
                assertFalse("NATS connection should NOT have been closed in Connect",
                        nc.isClosed());
            } // nc
        } // srv
    }

    private static StackTraceElement[] getStackTraceByName(String threadName) {
        Thread key = getThreadByName(threadName);
        return Thread.getAllStackTraces().get(key);
    }

    private static Thread getThreadByName(String threadName) {
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread thd : threadSet) {
            if (thd.getName().equals(threadName)) {
                return thd;
            }
        }
        return null;
    }

    @Test
    public void testBasicConnect() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                sleep(100, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Test
    public void testBasicPublish() throws Exception {
        // Run a STAN server
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                sc.publish("foo", "Hello World!".getBytes());
            }
        }
    }

    @Test
    public void testBasicPublishAsync() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final String[] cbguid = new String[1];
        // final Lock glock = new ReentrantLock();
        // Run a STAN server
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                AckHandler acb = (lguid, ex) -> {
                    cbguid[0] = lguid;
                    latch.countDown();
                };
                String pubguid = sc.publish("foo", "Hello World!".getBytes(), acb);
                assertFalse("Expected non-empty guid to be returned", pubguid.isEmpty());

                assertTrue("Did not receive our ack callback", latch.await(5, TimeUnit.SECONDS));
                assertEquals("Expected a matching guid in ack callback", pubguid, cbguid[0]);
            }
        }
    }

    @Test
    public void testTimeoutPublishAsync() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(NatsStreaming.ERR_CLOSE_REQ_TIMEOUT);

        final CountDownLatch latch = new CountDownLatch(1);
        final String[] guid = new String[1];
        // Run a STAN server
        try (NatsStreamingServer s = runServer(clusterName)) {
            Options opts = new Options.Builder().pubAckWait(Duration.ofMillis(50)).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, opts)) {
                assertNotNull(sc);
                AckHandler acb = (lguid, ex) -> {
                    assertEquals(guid[0], lguid);
                    assertNotNull(ex);
                    assertTrue(ex instanceof TimeoutException);
                    assertEquals("Expected a matching guid in ack callback", ex.getMessage(),
                            NatsStreaming.ERR_TIMEOUT);
                    latch.countDown();
                };
                // Kill the NATS Streaming server so we timeout
                s.shutdown();

                guid[0] = sc.publish("foo", "Hello World!".getBytes(), acb);
                assertNotNull(guid[0]);
                assertFalse("Expected non-empty guid to be returned.", guid[0].isEmpty());

                assertTrue("Did not receive our ack callback with a timeout err",
                        latch.await(5, TimeUnit.SECONDS));
            }
        }

    }

    @Test
    public void testBasicSubscription() throws Exception {
        // Run a STAN server
        try (NatsStreamingServer srv = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                SubscriptionOptions sopts = new SubscriptionOptions.Builder().build();
                try (Subscription sub = sc.subscribe("foo", msg -> {
                }, sopts)) {
                    assertNotNull(sub);
                } catch (Exception e) {
                    fail("Unexpected error on Subscribe, got: " + e.getMessage());
                }
            }
        }

    }

    @Test
    public void testBasicQueueSubscription()
            throws Exception {
        // Run a STAN server
        try (NatsStreamingServer srv = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final AtomicInteger count = new AtomicInteger();
                final CountDownLatch latch = new CountDownLatch(1);
                MessageHandler cb = msg -> {
                    if (msg.getSequence() == 1) {
                        if (count.incrementAndGet() == 2) {
                            latch.countDown();
                        }
                    }
                };

                try (Subscription sub = sc.subscribe("foo", "bar", cb)) {
                    // Test that durable and non durable queue subscribers with
                    // same name can coexist and they both receive the same message.
                    SubscriptionOptions sopts = new SubscriptionOptions.Builder()
                            .durableName("durable-queue-sub").build();
                    try (Subscription ignored = sc.subscribe("foo", "bar", cb, sopts)) {

                        // Publish a message
                        sc.publish("foo", "msg".getBytes());

                        // Wait for both copies of the message to be received.
                        assertTrue("Did not get our message", latch.await(5, TimeUnit.SECONDS));

                    } catch (Exception e) {
                        fail("Unexpected error on queue subscribe with durable name");
                    }

                    // Check that one cannot use ':' for the queue durable name.
                    sopts = new SubscriptionOptions.Builder().durableName("my:dur").build();
                    boolean exThrown = false;
                    try (Subscription sub3 = sc.subscribe("foo", "bar", cb, sopts)) {
                        fail("Subscription should not have succeeded");
                    } catch (IOException e) {
                        assertEquals(NatsStreaming.SERVER_ERR_INVALID_DURABLE_NAME,
                                e.getMessage());
                        exThrown = true;
                    } finally {
                        assertTrue("Expected to get an error regarding durable name", exThrown);
                    }

                }

            }
        }
    }

    @Test
    public void testDurableQueueSubscriber() throws Exception {
        final long total = 5;
        final long firstBatch = total;
        final long secondBatch = 2 * total;
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                for (int i = 0; i < total; i++) {
                    sc.publish("foo", "msg".getBytes());
                }
                final CountDownLatch latch = new CountDownLatch(1);
                MessageHandler cb = msg -> {
                    if (!msg.isRedelivered() && (msg.getSequence() == firstBatch
                            || msg.getSequence() == secondBatch)) {
                        latch.countDown();
                    }
                };
                sc.subscribe("foo", "bar", cb, new SubscriptionOptions.Builder()
                        .deliverAllAvailable().durableName("durable-queue-sub").build());

                assertTrue("Did not get our message", latch.await(5, TimeUnit.SECONDS));
                // Give a chance to ACKs to make it to the server.
                // This step is not necessary. Worst could happen is that messages
                // are redelivered. This is why we check on !msg.getRedelivered() in the
                // callback to validate the counts.
                sleep(500, TimeUnit.MILLISECONDS);

                // StreamingConnection closes here
            }

            // Create new connection
            try (StreamingConnection sc = newDefaultConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                MessageHandler cb = msg -> {
                    if (!msg.isRedelivered() && (msg.getSequence() == firstBatch
                            || msg.getSequence() == secondBatch)) {
                        latch.countDown();
                    }
                };
                for (int i = 0; i < total; i++) {
                    sc.publish("foo", "msg".getBytes());
                }
                // Create durable queue sub, it should receive from where it left off,
                // and ignore the start position
                try (Subscription sub = sc.subscribe("foo", "bar", cb,
                        new SubscriptionOptions.Builder().startAtSequence(10 * total)
                                .durableName("durable-queue-sub").build())) {

                    assertTrue("Did not get our message.", latch.await(5, TimeUnit.SECONDS));
                }

            }

        }
    }

    @Test
    public void testBasicPubSub() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int toSend = 500;
                final byte[] hw = "Hello World".getBytes();
                final ArrayList<Long> msgList = new ArrayList<>();

                try (Subscription sub = sc.subscribe("foo", msg -> {
                    assertEquals("foo", msg.getSubject());
                    assertArrayEquals(hw, msg.getData());
                    // Make sure Seq and Timestamp are set
                    assertNotEquals(0, msg.getSequence());
                    assertNotEquals(0, msg.getTimestamp());
                    assertFalse("Detected duplicate for sequence no: " + msg.getSequence(),
                            msgList.contains(msg.getSequence()));
                    msgList.add(msg.getSequence());

                    if (received.incrementAndGet() >= toSend) {
                        latch.countDown();
                    }
                })) {
                    for (int i = 0; i < toSend; i++) {
                        sc.publish("foo", hw);
                    }

                    assertTrue("Did not receive our messages", latch.await(1, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    public void testBasicPubQueueSub() throws Exception {
        try (NatsStreamingServer srv = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int toSend = 100;
                final byte[] hw = "Hello World".getBytes();

                try (Subscription sub = sc.subscribe("foo", "bar", msg -> {
                    assertEquals("Wrong subject.", "foo", msg.getSubject());
                    assertArrayEquals("Wrong payload. ", hw, msg.getData());
                    // Make sure Seq and Timestamp are set
                    assertNotEquals("Expected sequence to be set", 0, msg.getSequence());
                    assertNotEquals("Expected timestamp to be set", 0, msg.getTimestamp());
                    if (received.incrementAndGet() >= toSend) {
                        latch.countDown();
                    }
                })) {
                    for (int i = 0; i < toSend; i++) {
                        sc.publish("foo", hw);
                    }
                    assertTrue("Did not receive all our messages",
                            latch.await(1, TimeUnit.SECONDS));
                }
            }
        }
    }

    // TODO where did this test come from?
    @Test
    public void testBasicPubSubFlowControl()
            throws Exception {
        try (NatsStreamingServer srv = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int toSend = 500;
                final byte[] hw = "Hello World".getBytes();

                SubscriptionOptions opts =
                        new SubscriptionOptions.Builder().maxInFlight(25).build();
                try (Subscription ignored = sc.subscribe("foo", msg -> {
                    if (received.incrementAndGet() >= toSend) {
                        latch.countDown();
                    }
                }, opts)) {
                    for (int i = 0; i < toSend; i++) {
                        try {
                            sc.publish("foo", hw);
                        } catch (IOException e) {
                            e.printStackTrace();
                            fail("Received error on publish: " + e.getMessage());
                        }
                    }
                    assertTrue("Did not receive all our messages",
                            latch.await(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    public void testSubscriptionStartPositionLast() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                int toSend = 10;
                final AtomicInteger received = new AtomicInteger(0);
                final List<Message> savedMsgs = new ArrayList<>();

                // Publish ten messages
                for (int i = 0; i < toSend; i++) {
                    byte[] data = String.format("%d", i).getBytes();
                    sc.publish("foo", data);
                }

                // Now subscribe and set start position to last received.
                final CountDownLatch latch = new CountDownLatch(1);
                MessageHandler mcb = msg -> {
                    received.incrementAndGet();
                    assertEquals("Wrong message sequence received", toSend, msg.getSequence());
                    savedMsgs.add(msg);
                    latch.countDown();
                };

                // Now subscribe and set start position to last received.
                SubscriptionOptions opts =
                        new SubscriptionOptions.Builder().startWithLastReceived().build();
                try (SubscriptionImpl sub = (SubscriptionImpl) sc.subscribe("foo", mcb, opts)) {
                    // Check for sub setup
                    assertEquals(
                            String.format("Incorrect StartAt state: %s", sub.opts.getStartAt()),
                            sub.opts.getStartAt(), StartPosition.LastReceived);

                    // Make sure we got our message
                    assertTrue("Did not receive our message", latch.await(5, TimeUnit.SECONDS));
                    if (received.get() != 1) {
                        System.err.printf("Should have received 1 message with sequence {}, "
                                + "but got these {} messages:\n", toSend, savedMsgs.size());
                        for (Message savedMsg : savedMsgs) {
                            System.err.println(savedMsg);
                        }
                        fail("Wrong number of messages");
                    }
                    assertEquals("Wrong message sequence received,", toSend,
                            savedMsgs.get(0).getSequence());

                    assertEquals(1, savedMsgs.size());
                }
            }
        }
    }

    @Test
    public void testSubscriptionStartAtSequence() throws Exception {
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                // Publish ten messages
                for (int i = 1; i <= 10; i++) {
                    byte[] data = String.format("%d", i).getBytes();
                    sc.publish("foo", data);
                }

                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int shouldReceive = 5;

                // Capture the messages that are delivered.
                final List<Message> savedMsgs = new ArrayList<>();

                MessageHandler mcb = msg -> {
                    savedMsgs.add(msg);
                    if (received.incrementAndGet() >= shouldReceive) {
                        latch.countDown();
                    }
                };
                // Now subscribe and set start position to #6, so should receive 6-10.
                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().startAtSequence(6).build())) {

                    // Check for sub setup
                    assertEquals(StartPosition.SequenceStart,((SubscriptionImpl)sub).opts.startAt);
                    assertEquals(6, ((SubscriptionImpl)sub).opts.startSequence);

                    assertTrue("Did not receive our messages", latch.await(5, TimeUnit.SECONDS));

                    // Check we received them in order
                    long seq = 6;
                    for (Message msg : savedMsgs) {
                        // Check sequence
                        assertEquals(seq, msg.getSequence());
                        // Check payload
                        long dseq = Long.valueOf(new String(msg.getData()));
                        assertEquals("Wrong payload.", seq, dseq);
                        seq++;
                    }
                }
            }
        }
    }

    private static Instant getInstantFromNanos(long timestamp) {
        long seconds = TimeUnit.NANOSECONDS.toSeconds(timestamp);
        long nanos = timestamp - TimeUnit.SECONDS.toNanos(seconds);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    @Test
    public void testSubscriptionStartAtTime() throws Exception {
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                // Publish first five
                for (int i = 1; i <= 5; i++) {
                    byte[] data = String.format("%d", i).getBytes();
                    sc.publish("foo", data);
                }
                // Buffer each side so slow tests still work.
                sleep(250, TimeUnit.MILLISECONDS);
                // Date startTime = new Date(System.currentTimeMillis());
                final Instant startTime = Instant.now();
                sleep(250, TimeUnit.MILLISECONDS);

                // Publish last 5
                for (int i = 6; i <= 10; i++) {
                    byte[] data = String.format("%d", i).getBytes();
                    sc.publish("foo", data);
                }

                final CountDownLatch[] latch = new CountDownLatch[1];
                latch[0] = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int shouldReceive = 5;

                // Capture the messages that are delivered.
                final List<Message> savedMsgs = new ArrayList<>();

                MessageHandler mcb = msg -> {
                    savedMsgs.add(msg);
                    if (received.incrementAndGet() >= shouldReceive) {
                        latch[0].countDown();
                    }
                };
                // Now subscribe and set start time to startTime, so we should
                // receive messages >= startTime
                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().startAtTime(startTime).build())) {

                    // Check for sub setup
                    assertEquals("Incorrect StartAt state.", StartPosition.TimeDeltaStart,
                            sub.getOptions().getStartAt());
                    assertEquals("Incorrect start time.", startTime,
                            sub.getOptions().getStartTime());

                    assertTrue("Did not receive our messages",
                            latch[0].await(5, TimeUnit.SECONDS));

                    // Check we received them in order
                    Iterator<Message> it = savedMsgs.iterator();
                    long seq = 6;
                    while (it.hasNext()) {
                        Message msg = it.next();
                        // Check that time is always greater than startTime
                        Instant timestamp = getInstantFromNanos(msg.getTimestamp());
                        assertFalse("Expected all messages to have timestamp > startTime.",
                                timestamp.isBefore(startTime));

                        // Check sequence
                        assertEquals("Wrong sequence.", seq, msg.getSequence());

                        // Check payload
                        long dseq = Long.valueOf(new String(msg.getData()));
                        assertEquals("Wrong payload.", seq, dseq);
                        seq++;
                    }

                    // Now test Ago helper
                    long delta = ChronoUnit.NANOS.between(startTime, Instant.now());

                    latch[0] = new CountDownLatch(1);
                    try (Subscription sub2 =
                                 sc.subscribe("foo", mcb, new SubscriptionOptions.Builder()
                                         .startAtTimeDelta(Duration.ofNanos(delta)).build())) {
                        assertTrue("Did not receive our messages.",
                                latch[0].await(5, TimeUnit.SECONDS));
                    }
                }
            }
        }
    }

    @Test
    public void testSubscriptionStartAtWithEmptyStore() throws Exception {
        // Run a NATS Streaming server
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {

                MessageHandler mcb = msg -> {
                };

//                try (Subscription sub = sc.subscribe("foo", mcb,
//                        new SubscriptionOptions.Builder().startAtTime(Instant.now()).build())) {
//                    // Should work fine
//                }
//
//                try (Subscription sub = sc.subscribe("foo", mcb,
//                        new SubscriptionOptions.Builder().startAtSequence(0).build())) {
//                    // Should work fine
//                }

                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().startWithLastReceived().build())) {
                    assertNotNull("Should have subscribed successfully", sub);
                } catch (IOException | InterruptedException e) {
                    fail(String.format("Expected no error on Subscribe, got: '%s'",
                            e.getMessage()));
                }

                try (Subscription sub = sc.subscribe("foo", mcb)) {
                    assertNotNull("Should have subscribed successfully", sub);
                } catch (Exception e) {
                    fail(String.format("Expected no error on Subscribe, got: '%s'",
                            e.getMessage()));
                }
            }
        }
    }

    @Test
    public void testSubscriptionStartAtFirst() throws Exception {
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                // Publish ten messages
                for (int i = 1; i <= 10; i++) {
                    byte[] data = String.format("%d", i).getBytes();
                    sc.publish("foo", data);
                }

                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final int shouldReceive = 10;

                // Capture the messages that are delivered.
                final List<Message> savedMsgs = new ArrayList<>();
                MessageHandler mcb = msg -> {
                    savedMsgs.add(msg);
                    if (received.incrementAndGet() >= shouldReceive) {
                        latch.countDown();
                    }
                };

                // Should receive all messages.
                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().deliverAllAvailable().build())) {
                    // Check for sub setup
                    assertEquals(StartPosition.First, sub.getOptions().getStartAt());
                    assertTrue("Did not receive our messages", latch.await(5, TimeUnit.SECONDS));
                    assertEquals("Got wrong number of msgs", shouldReceive, received.get());
                    assertEquals("Wrong number of msgs in map", shouldReceive, savedMsgs.size());
                    // Check we received them in order
                    Iterator<Message> it = savedMsgs.iterator();
                    long seq = 1;
                    while (it.hasNext()) {
                        Message msg = it.next();
                        // Check sequence
                        assertEquals(seq, msg.getSequence());

                        // Check payload
                        long dseq = Long.valueOf(new String(msg.getData()));
                        assertEquals(seq, dseq);
                        seq++;
                    }
                }
            }
        }
    }

    @Test
    public void testUnsubscribe() throws Exception {
        // Run a NATS Streaming server
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                boolean exThrown = false;

                // test null
                try (SubscriptionImpl nsub = new SubscriptionImpl()) {
                    try {
                        nsub.unsubscribe();
                    } catch (Exception e) {
                        assertEquals(NatsStreaming.ERR_BAD_SUBSCRIPTION, e.getMessage());
                        exThrown = true;
                    } finally {
                        assertTrue("Should have thrown exception", exThrown);
                    }
                }

                // Create a valid one
                sc.subscribe("foo", null);

                // Now subscribe, but we will unsubscribe before sending any
                // messages.
                Subscription sub = null;
                try {
                    sub = sc.subscribe("foo", msg -> {
                        fail("Did not expect to receive any messages");
                    });
                } catch (Exception e) {
                    fail("Expected no error on subscribe, got " + e.getMessage());
                }

                // Create another valid one
                sc.subscribe("foo", null);

                // Unsubscribe middle one.
                try {
                    sub.unsubscribe();
                } catch (Exception e) {
                    fail("Expected no errors from unsubscribe: got " + e.getMessage());
                }

                // Do it again, should not dump, but should get error.
                exThrown = false;
                try {
                    sub.unsubscribe();
                } catch (Exception e) {
                    assertEquals("Wrong error.", NatsStreaming.ERR_BAD_SUBSCRIPTION,
                            e.getMessage());
                    exThrown = true;
                }
                assertTrue("Should have thrown exception", exThrown);

                // Publish ten messages
                for (int i = 1; i <= 10; i++) {
                    sc.publish("foo", String.format("%d", i).getBytes());
                }

            }
        }
    }

    @Test
    public void testUnsubscribeWhileConnClosing() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            Options opts = new Options.Builder()
                    .pubAckWait(Duration.ofMillis(50))
                    .build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, opts)) {
                assertNotNull(sc);
                Subscription sub = sc.subscribe("foo", null);
                final CountDownLatch wg = new CountDownLatch(1);

                service.execute(() -> {
                    sleep(ThreadLocalRandom.current().nextInt(0, 50));
                    try {
                        sc.close();
                    } catch (Exception e) {
                        System.err.println("CLOSE ERROR");
                        e.printStackTrace();
                    }
                    wg.countDown();
                });

                // Unsubscribe
                sub.unsubscribe();

                wg.await();
            }
        }
    }

    @Test
    public void testSubscribeShrink() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (final StreamingConnection sc = newDefaultConnection()) {
                int nsubs = 1000;
                List<Subscription> subs = new CopyOnWriteArrayList<>();
                for (int i = 0; i < nsubs; i++) {
                    // Create a valid one
                    Subscription sub = null;
                    try {
                        sub = sc.subscribe("foo", null);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                    subs.add(sub);
                }

                assertEquals(nsubs, subs.size());

                // Now unsubscribe them all
                for (Subscription sub : subs) {
                    try {
                        sub.unsubscribe();
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                }
            }
        }
    }

    @Test
    public void testDupClientId() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            boolean exThrown = false;
            try (final StreamingConnection sc = newDefaultConnection()) {
                try (final StreamingConnection sc2 = newDefaultConnection()) {
                    fail("Subscription should not have succeeded");
                } catch (IOException | TimeoutException e) {
                    assertEquals(NatsStreaming.SERVER_ERR_INVALID_CLIENT, e.getMessage());
                    exThrown = true;
                }
            }
        }
    }

    @Test
    public void testClose() {
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            StreamingConnection sc = null;
            Subscription sub = null;

            try {
                sc = newDefaultConnection();
            } catch (Exception e) {
                if (!e.getMessage().equals(NatsStreaming.ERR_CONNECTION_CLOSED)) {
                    e.printStackTrace();
                    fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
                }
            }

            try {
                sub = sc.subscribe("foo", msg -> {
                    fail("Did not expect to receive any messages");
                });
            } catch (Exception e) {
                if (!e.getMessage().equals(NatsStreaming.ERR_CONNECTION_CLOSED)) {
                    e.printStackTrace();
                    fail("Expected to subscribe successfully, got err [" + e.getMessage() + "]");
                }
            }

            try {
                sc.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail("Did not expect error on close(), got: " + e.getMessage());
            }

            try {
                for (int i = 0; i < 10; i++) {
                    sc.publish("foo", "ok".getBytes());
                }
            } catch (Exception e) {
                // NOOP
            }

            boolean exThrown = false;
            try {
                sc.publish("foo", "Hello World!".getBytes());
            } catch (Exception e) {
                assertEquals(NatsStreaming.ERR_CONNECTION_CLOSED, e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown exception", exThrown);

            exThrown = false;
            try {
                sub.unsubscribe();
            } catch (Exception e) {
                // e.printStackTrace();
                assertEquals(NatsStreaming.ERR_CONNECTION_CLOSED, e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown exception", exThrown);

        }
    }

    @Test
    public void testDoubleClose() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            StreamingConnection sc = newDefaultConnection();
            sc.close();
            sc.close();
        }
    }

    @Test
    public void testManualAck() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {

                final int toSend = 100;
                byte[] hw = "Hello World".getBytes();

                for (int i = 0; i < toSend; i++) {
                    sc.publish("foo", hw, null);
                }
                sc.publish("foo", hw);

                final CountDownLatch fch = new CountDownLatch(1);

                // Test that we can't Ack if not in manual mode.
                try (Subscription sub = sc.subscribe("foo", msg -> {
                    boolean exThrown = false;
                    try {
                        msg.ack();
                    } catch (Exception e) {
                        assertEquals(StreamingConnectionImpl.ERR_MANUAL_ACK, e.getMessage());
                        exThrown = true;
                    }
                    assertTrue("Expected manual ack exception", exThrown);
                    fch.countDown();
                }, new SubscriptionOptions.Builder().deliverAllAvailable().build())) {

                    assertTrue("Did not receive our first message", fch.await(5, TimeUnit.SECONDS));

                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Expected successful subscribe, but got: " + e.getMessage());
                }

                final CountDownLatch ch = new CountDownLatch(1);
                final CountDownLatch sch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);

                // Capture the messages that are delivered.
                final List<Message> msgs = new CopyOnWriteArrayList<>();

                // Test we only receive MaxInflight if we do not ack
                try (Subscription sub = sc.subscribe("foo", msg -> {
                    msgs.add(msg);
                    int nr = received.incrementAndGet();
                    if (nr == 10) {
                        ch.countDown();
                    } else if (nr > 10) {
                        try {
                            msg.ack();
                        } catch (IOException e) {
                            // NOOP
                            // e.printStackTrace();
                        }
                        if (nr >= (toSend + 1)) { // sync Publish +1
                            sch.countDown();
                        }
                    }
                }, new SubscriptionOptions.Builder().deliverAllAvailable().maxInFlight(10)
                        .manualAcks().build())) {
                    assertTrue("Did not receive at least 10 messages",
                            ch.await(5, TimeUnit.SECONDS));

                    // Wait a bit longer for other messages which would be an
                    // error.
                    sleep(50, TimeUnit.MILLISECONDS);

                    assertEquals(
                            "Only expected to get 10 messages to match MaxInflight without Acks, "
                                    + "got " + received.get(),
                            10, received.get());

                    // Now make sure we get the rest of them. So ack the ones we
                    // have so far.
                    for (Message msg : msgs) {
                        try {
                            msg.ack();
                        } catch (Exception e) {
                            e.printStackTrace();
                            fail("Unexpected exception on Ack: " + e.getMessage());
                        }
                    }

                    assertTrue("Did not receive all our messages", sch.await(5, TimeUnit.SECONDS));
                    assertEquals("Did not receive correct number of messages", toSend + 1,
                            received.get());
                }
            }
        }
    }

    @Test
    public void testRedelivery() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {

                final int toSend = 100;
                byte[] hw = "Hello World".getBytes();

                for (int i = 0; i < toSend; i++) {
                    sc.publish("foo", hw, null);
                }

                // Make sure we get an error on bad ackWait
                boolean exThrown = false;
                try {
                    sc.subscribe("foo", null, new SubscriptionOptions.Builder()
                            .ackWait(20, TimeUnit.MILLISECONDS).build());
                } catch (Exception e) {
                    assertEquals(NatsStreaming.SERVER_ERR_INVALID_ACK_WAIT, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Expected an error for AckWait < 1 second", exThrown);

                final CountDownLatch ch = new CountDownLatch(1);
                final CountDownLatch sch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);

                Duration ackRedeliverTime = Duration.ofSeconds(1); // 1 second

                // Test we only receive MaxInflight if we do not ack
                try (Subscription sub = sc.subscribe("foo", msg -> {
                    int nr = received.incrementAndGet();
                    if (nr == toSend) {
                        ch.countDown();
                    } else if (nr == (2 * toSend)) {
                        sch.countDown();
                    }
                }, new SubscriptionOptions.Builder().deliverAllAvailable()
                        .maxInFlight(toSend + 1).ackWait(ackRedeliverTime).manualAcks()
                        .build())) {
                    assertTrue("Did not receive first delivery of all messages",
                            ch.await(5, TimeUnit.SECONDS));
                    assertEquals("Did not receive correct number of messages", toSend,
                            received.get());
                    assertTrue("Did not receive re-delivery of all messages",
                            sch.await(5, TimeUnit.SECONDS));
                    assertEquals("Did not receive correct number of messages", toSend * 2,
                            received.get());
                }
            }
        }
    }

    private void checkTime(String label, Instant time1, Instant time2, Duration expected,
                           Duration tolerance) {
        Duration duration = Duration.between(time1, time2);
        Duration lowerBoundary = expected.minus(tolerance);
        Duration upperBoundary = expected.plus(tolerance);
        if ((duration.compareTo(lowerBoundary) < 0) || (duration.compareTo(upperBoundary) > 0)) {
            fail(String.format("%s not in range: %s (expected %s +/- %s)", label, duration,
                    expected, tolerance));
        }
    }

    private void checkRedelivery(int count, boolean queueSub) throws Exception {
        final int toSend = count;
        final byte[] hw = "Hello World".getBytes();
        final CountDownLatch latch = new CountDownLatch(1);

        // Run a NATS Streaming server
        try (NatsStreamingServer srv = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final AtomicInteger acked = new AtomicInteger();
                final AtomicBoolean secondRedelivery = new AtomicBoolean(false);
                final AtomicInteger firstDeliveryCount = new AtomicInteger(0);
                final AtomicInteger firstRedeliveryCount = new AtomicInteger(0);
                final AtomicLong startDelivery = new AtomicLong(0);
                final AtomicLong startFirstRedelivery = new AtomicLong(0);
                final AtomicLong startSecondRedelivery = new AtomicLong(0);

                Duration ackRedeliverTime = Duration.ofSeconds(1);

                MessageHandler recvCb = msg -> {
                    if (msg.isRedelivered()) {
                        if (secondRedelivery.get()) {
                            if (startSecondRedelivery.get() == 0L) {
                                startSecondRedelivery.set(Instant.now().toEpochMilli());
                            }
                            int acks = acked.incrementAndGet();
                            if (acks <= toSend) {
                                try {
                                    msg.ack();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    fail(e.getMessage());
                                }
                                if (acks == toSend) {
                                    latch.countDown();
                                }
                            }
                        } else {
                            if (startFirstRedelivery.get() == 0L) {
                                startFirstRedelivery.set(Instant.now().toEpochMilli());
                            }
                            if (firstRedeliveryCount.incrementAndGet() == toSend) {
                                secondRedelivery.set(true);
                            }
                        }
                    } else {
                        if (startDelivery.get() == 0L) {
                            startDelivery.set(Instant.now().toEpochMilli());
                        }
                        firstDeliveryCount.incrementAndGet();
                    }
                };

                SubscriptionOptions sopts = new SubscriptionOptions.Builder()
                        .ackWait(ackRedeliverTime).manualAcks().build();
                String queue = null;
                if (queueSub) {
                    queue = "bar";
                }
                try (Subscription sub = sc.subscribe("foo", queue, recvCb, sopts)) {
                    for (int i = 0; i < toSend; i++) {
                        sc.publish("foo", hw);
                    }

                    // If this succeeds, it means that we got all messages first delivered,
                    // and then at least 2 * toSend messages received as redelivered.
                    assertTrue("Did not ack all expected messages",
                            latch.await(5, TimeUnit.SECONDS));

                    // Wait a period and bit more to make sure that no more message are
                    // redelivered (acked will then be > toSend)
                    TimeUnit.MILLISECONDS.sleep(ackRedeliverTime.toMillis() + 100);

                    // Verify first redelivery happens when expected
                    checkTime("First redelivery", Instant.ofEpochMilli(startDelivery.get()),
                            Instant.ofEpochMilli(startFirstRedelivery.get()), ackRedeliverTime,
                            ackRedeliverTime.dividedBy(2));

                    // Verify second redelivery happens when expected
                    checkTime("Second redelivery", Instant.ofEpochMilli(startFirstRedelivery.get()),
                            Instant.ofEpochMilli(startSecondRedelivery.get()), ackRedeliverTime,
                            ackRedeliverTime.dividedBy(2));

                    // Check counts
                    assertEquals("Did not receive all messages during delivery.", toSend,
                            firstDeliveryCount.get());

                    assertEquals("Did not receive all messages during first redelivery.", toSend,
                            firstRedeliveryCount.get());

                    assertEquals("Did not get expected acks.", acked.get(), toSend);
                }
            }
        }
    }

    @Test
    public void testLowRedeliveryToSubMoreThanOnce() throws Exception {
        checkRedelivery(10, false);
    }

    @Test
    public void testHighRedeliveryToSubMoreThanOnce() throws Exception {
        checkRedelivery(100, false);
    }

    @Test
    public void testLowRedeliveryToQueueSubMoreThanOnce() throws Exception {
        checkRedelivery(10, true);
    }

    @Test
    public void testHighRedeliveryToQueueSubMoreThanOnce() throws Exception {
        checkRedelivery(100, true);
    }

    @Test
    public void testDurableSubscriber() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            final StreamingConnection sc = newDefaultConnection();

            final int toSend = 100;
            byte[] hw = "Hello World".getBytes();

            // Capture the messages that are delivered.
            final List<Message> msgs = new CopyOnWriteArrayList<>();
            Lock msgsGuard = new ReentrantLock();

            for (int i = 0; i < toSend; i++) {
                sc.publish("foo", hw);
            }

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicInteger received = new AtomicInteger(0);

            try {
                sc.subscribe("foo", msg -> {
                    int nr = received.incrementAndGet();
                    if (nr == 10) {
                        // Reduce risk of test failure by allowing server to
                        // process acks before processing Close() requesting
                        sleep(500, TimeUnit.MILLISECONDS);
                        try {
                            sc.close();
                        } catch (Exception e) {
                            e.printStackTrace(); // NOOP
                        }
                        latch.countDown();
                    } else {
                        msgsGuard.lock();
                        msgs.add(msg);
                        msgsGuard.unlock();
                    }
                }, new SubscriptionOptions.Builder().deliverAllAvailable()
                        .durableName("durable-foo").build());

                assertTrue("Did not receive first delivery of all messages",
                        latch.await(5, TimeUnit.SECONDS));

                assertEquals(
                        String.format("Expected to get only 10 messages, got %d", received.get()),
                        10, received.get());

                // reset in case we get more messages in the above callback
                final CountDownLatch latch2 = new CountDownLatch(1);

                // This is auto-ack, so undo received for check.
                // Close will prevent ack from going out, so #10 will be
                // redelivered
                received.decrementAndGet();

                // sc is closed here from above...

                // Recreate the connection
                Options opts = new Options.Builder().pubAckWait(Duration.ofMillis(50)).build();
                final StreamingConnection sc2 =
                        NatsStreaming.connect(clusterName, clientName, opts);
                // Create the same durable subscription.
                try {
                    sc2.subscribe("foo", msg -> {
                        msgsGuard.lock();
                        msgs.add(msg);
                        msgsGuard.unlock();
                        received.incrementAndGet();
                        if (received.get() == toSend) {
                            latch2.countDown();
                        }
                    }, new SubscriptionOptions.Builder().deliverAllAvailable()
                            .durableName("durable-foo").build());
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Should have subscribed successfully, but got: " + e.getMessage());
                }

                // Check that durables cannot be subscribed to again by same
                // client.
                boolean exThrown = false;
                try {
                    sc2.subscribe("foo", null, new SubscriptionOptions.Builder()
                            .durableName("durable-foo").build());
                } catch (Exception e) {
                    assertEquals(NatsStreaming.SERVER_ERR_DUP_DURABLE, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Expected duplicate durable exception", exThrown);

                // Check that durables with same name, but subscribed to
                // different subject are ok.
                try {
                    sc2.subscribe("bar", null, new SubscriptionOptions.Builder()
                            .durableName("durable-foo").build());
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }

                assertTrue(String.format(
                        "Did not receive delivery of all messages, got %d, expected %d",
                        received.get(), toSend), latch2.await(5, TimeUnit.SECONDS));
                assertEquals("Didn't receive all messages", toSend, received.get());
                assertEquals("Didn't save all messages", toSend, msgs.size());
                // Check we received them in order
                int idx = 0;
                for (Message msg : msgs) {
                    assertEquals("Wrong sequence number", ++idx, msg.getSequence());
                }
                sc2.close();

            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
            } catch (IllegalStateException e) {
                // NOOP, connection already closed during close
            } finally {
                sc.close();
            }
        } // runServer()
    }

    @Test
    public void testPubMultiQueueSub() throws InterruptedException {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final AtomicInteger s1Received = new AtomicInteger(0);
                final AtomicInteger s2Received = new AtomicInteger(0);
                final int toSend = 1000;
                final Subscription[] subs = new Subscription[2];

                final Map<Long, Object> msgMap = new ConcurrentHashMap<>();
                MessageHandler mcb = msg -> {
                    // Remember the message sequence.
                    assertFalse("Detected duplicate for sequence: " + msg.getSequence(),
                            msgMap.containsKey(msg.getSequence()));
                    msgMap.put(msg.getSequence(), new Object());
                    // Track received for each receiver
                    if (msg.getSubscription().equals(subs[0])) {
                        s1Received.incrementAndGet();
                    } else if (msg.getSubscription().equals(subs[1])) {
                        s2Received.incrementAndGet();
                    } else {
                        fail("Received message on unknown subscription");
                    }
                    // Track total
                    if (received.incrementAndGet() == toSend) {
                        latch.countDown();
                    }
                };

                try (Subscription s1 = sc.subscribe("foo", "bar", mcb)) {
                    try (Subscription s2 = sc.subscribe("foo", "bar", mcb)) {
                        subs[0] = s1;
                        subs[1] = s2;
                        // Publish out the messages.
                        for (int i = 0; i < toSend; i++) {
                            byte[] data = String.format("%d", i).getBytes();
                            sc.publish("foo", data);
                        }

                        assertTrue("Did not receive all our messages",
                                latch.await(5, TimeUnit.SECONDS));
                        assertEquals("Did not receive correct number of messages", toSend,
                                received.get());
                        double var = ((float) toSend * 0.25);
                        int expected = toSend / 2;
                        int d1 = (int) Math.abs((double) (expected - s1Received.get()));
                        int d2 = (int) Math.abs((double) (expected - s2Received.get()));
                        if (d1 > var || d2 > var) {
                            fail(String.format("Too much variance in totals: %d, %d > %f", d1, d2,
                                    var));
                        }
                    }
                }

            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail("Expected to connect correctly, got err [" + e.getMessage() + "]");
            }
        }
    }

    /*
     * This test tends to crash gnatsd when tracing is enabled (-DV, which is enabled by passing
     * `true` as the second argument of runServer) and toSend is 500 or greater.
     * 
     */
    @Test
    public void testPubMultiQueueSubWithSlowSubscriberAndFlapping()
            throws InterruptedException, IOException, TimeoutException {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final Subscription[] subs = new Subscription[2];
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final AtomicInteger s1Received = new AtomicInteger(0);
                final AtomicInteger s2Received = new AtomicInteger(0);
                final int toSend = 500;
                final Map<Long, Object> msgMap = new ConcurrentHashMap<>();
                final Object msgMapLock = new Object();
                MessageHandler mcb = msg -> {
                    // Remember the message sequence.
                    synchronized (msgMapLock) {
                        assertFalse("Detected duplicate for sequence: " + msg.getSequence(),
                                msgMap.containsKey(msg.getSequence()));
                        msgMap.put(msg.getSequence(), new Object());
                    }
                    // Track received for each receiver
                    if (msg.getSubscription().equals(subs[0])) {
                        s1Received.incrementAndGet();
                    } else if (msg.getSubscription().equals(subs[1])) {
                        // Slow down this subscriber
                        sleep(50, TimeUnit.MILLISECONDS);
                        s2Received.incrementAndGet();
                    } else {
                        fail("Received message on unknown subscription");
                    }
                    // Track total
                    int nr = received.incrementAndGet();
                    if (nr == toSend) {
                        latch.countDown();
                    }
                };

                try (Subscription s1 = sc.subscribe("foo", "bar", mcb)) {
                    try (Subscription s2 = sc.subscribe("foo", "bar", mcb)) {
                        subs[0] = s1;
                        subs[1] = s2;
                        // Publish out the messages.
                        for (int i = 0; i < toSend; i++) {
                            byte[] data = String.format("%d", i).getBytes();
                            sc.publish("foo", data);
                            sleep(1, TimeUnit.MICROSECONDS);
                        }

                        assertTrue("Did not receive all our messages",
                                latch.await(10, TimeUnit.SECONDS));
                        assertEquals("Did not receive correct number of messages", toSend,
                                received.get());

                        // Since we slowed down sub2, sub1 should get the
                        // majority of messages.
                        int minCountForS1 = (toSend / 2) + 2;
                        assertTrue(
                                String.format("Expected s1 to get at least %d msgs, was %d\n",
                                        minCountForS1, s1Received.get()),
                                s1Received.get() > minCountForS1);

                        if (s1Received.get() != (toSend - s2Received.get())) {
                            fail(String.format("Expected %d for sub1, got %d",
                                    (toSend - s2Received.get()), s1Received.get()));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testPubMultiQueueSubWithSlowSubscriber()
            throws Exception {
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final Subscription[] subs = new Subscription[2];
                final CountDownLatch latch = new CountDownLatch(1);
                final CountDownLatch s2BlockedLatch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final AtomicInteger s1Received = new AtomicInteger(0);
                final AtomicInteger s2Received = new AtomicInteger(0);
                final int toSend = 100;
                final Map<Long, Object> msgMap = new ConcurrentHashMap<>();
                final Object msgMapLock = new Object();
                MessageHandler mcb = msg -> {
                    // Remember the message sequence.
                    synchronized (msgMapLock) {
                        assertFalse("Detected duplicate for sequence: " + msg.getSequence(),
                                msgMap.containsKey(msg.getSequence()));
                        msgMap.put(msg.getSequence(), new Object());
                    }
                    // Track received for each receiver
                    if (msg.getSubscription().equals(subs[0])) {
                        s1Received.incrementAndGet();
                    } else if (msg.getSubscription().equals(subs[1])) {
                        // Block this subscriber
                        try {
                            s2BlockedLatch.await();
                        } catch (InterruptedException e) {
                            System.err.println("Interrupted:" + e);
                        }
                        s2Received.incrementAndGet();
                    } else {
                        fail("Received message on unknown subscription");
                    }
                    // Track total
                    int nr = received.incrementAndGet();
                    if (nr == toSend) {
                        latch.countDown();
                    }
                };

                try (Subscription s1 = sc.subscribe("foo", "bar", mcb)) {
                    try (Subscription s2 = sc.subscribe("foo", "bar", mcb)) {
                        subs[0] = s1;
                        subs[1] = s2;
                        // Publish out the messages.
                        for (int i = 0; i < toSend; i++) {
                            byte[] data = String.format("%d", i).getBytes();
                            sc.publish("foo", data);
                            // sleep(1, TimeUnit.MICROSECONDS);
                        }
                        s2BlockedLatch.countDown();

                        assertTrue("Did not receive all our messages",
                                latch.await(10, TimeUnit.SECONDS));
                        assertEquals("Did not receive correct number of messages", toSend,
                                received.get());

                        // Since we slowed down sub2, sub1 should get the
                        // majority of messages.
                        int s1r = s1Received.get();
                        int s2r = s2Received.get();

                        assertFalse(String.format(
                                "Expected sub2 to receive no more than half, but got %d msgs\n",
                                s2r), s2r > toSend / 2);
                        assertTrue(String.format("Expected %d msgs for sub1, got %d",
                                (toSend - s2r), s1r), s1r == toSend - s2r);

                    }
                }
            }
        }
    }

    @Test
    public void testPubMultiQueueSubWithRedelivery() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);
                final AtomicInteger s1Received = new AtomicInteger(0);
                final int toSend = 500;
                final Subscription[] subs = new Subscription[2];

                MessageHandler mcb = msg -> {
                    // Track received for each receiver
                    if (msg.getSubscription().equals(subs[0])) {
                        try {
                            msg.ack();
                        } catch (Exception e) {
                            // NOOP
                            e.printStackTrace();
                        }
                        s1Received.incrementAndGet();

                        // Track total only for sub1
                        if (received.incrementAndGet() == toSend) {
                            latch.countDown();
                        }
                    } else if (msg.getSubscription().equals(subs[1])) {
                        // We will not ack this subscriber
                    } else {
                        fail("Received message on unknown subscription");
                    }
                };

                try (Subscription s1 = sc.subscribe("foo", "bar", mcb,
                        new SubscriptionOptions.Builder().manualAcks().build())) {
                    try (Subscription s2 =
                                 sc.subscribe("foo", "bar", mcb, new SubscriptionOptions.Builder()
                                         .manualAcks().ackWait(1, TimeUnit.SECONDS)
                                         .build())) {
                        subs[0] = s1;
                        subs[1] = s2;
                        // Publish out the messages.
                        for (int i = 0; i < toSend; i++) {
                            byte[] data = String.format("%d", i).getBytes();
                            sc.publish("foo", data);
                        }

                        assertTrue("Did not receive all our messages",
                                latch.await(30, TimeUnit.SECONDS));
                        assertEquals("Did not receive correct number of messages:", toSend,
                                received.get());

                        // Since we never ack'd sub2, we should receive all our messages on sub1
                        assertEquals("Sub1 received wrong number of messages", toSend,
                                s1Received.get());
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Subscription s2 failed: " + e.getMessage());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Subscription s1 failed: " + e.getMessage());
                }
            }
        }
    }

    @Test
    public void testPubMultiQueueSubWithDelayRedelivery() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger ackCount = new AtomicInteger(0);
                final int toSend = 500;
                final Subscription[] subs = new Subscription[2];

                MessageHandler mcb = msg -> {
                    // Track received for each receiver
                    if (msg.getSubscription().equals(subs[0])) {
                        try {
                            msg.ack();
                        } catch (Exception e) {
                            e.printStackTrace();
                            fail(e.getMessage());
                        }
                        int nr = ackCount.incrementAndGet();

                        if (nr == toSend) {
                            latch.countDown();
                        }

                        if (nr > 0 && nr % (toSend / 2) == 0) {
                            // This depends on the internal algorithm where the
                            // best resend subscriber is the one with the least number
                            // of outstanding acks.
                            //
                            // Sleep to allow the acks to back up, so s2 will look
                            // like a better subscriber to send messages to.
                            sleep(200, TimeUnit.MILLISECONDS);
                        }
                    } else if (msg.getSubscription().equals(subs[1])) {
                        // We will not ack this subscriber
                    } else {
                        fail("Received message on unknown subscription");
                    }
                };

                try (Subscription s1 = sc.subscribe("foo", "bar", mcb,
                        new SubscriptionOptions.Builder().manualAcks().build())) {
                    try (Subscription s2 =
                                 sc.subscribe("foo", "bar", mcb, new SubscriptionOptions.Builder()
                                         .manualAcks().ackWait(1, TimeUnit.SECONDS)
                                         .build())) {
                        subs[0] = s1;
                        subs[1] = s2;
                        // Publish out the messages.
                        for (int i = 0; i < toSend; i++) {
                            byte[] data = String.format("%d", i).getBytes();
                            sc.publish("foo", data);
                        }

                        assertTrue("Did not ack expected count of messages",
                                latch.await(30, TimeUnit.SECONDS));
                        assertEquals("Did not ack correct number of messages", toSend,
                                ackCount.get());
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Subscription s2 failed: " + e.getMessage());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Subscription s1 failed: " + e.getMessage());
                }
            }
        }
    }

    @Test
    public void testRedeliveredFlag() throws Exception {
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                final int toSend = 100;
                byte[] hw = "Hello World".getBytes();

                for (int i = 0; i < toSend; i++) {
                    try {
                        sc.publish("foo", hw);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Error publishing message: " + e.getMessage());
                    }
                }

                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger(0);

                // Capture the messages that are delivered.
                final Map<Long, Message> msgs = new ConcurrentHashMap<>();
                MessageHandler mcb = msg -> {
                    // Remember the message.
                    msgs.put(msg.getSequence(), msg);

                    // Only Ack odd numbers
                    if ((msg.getSequence() % 2) != 0) {
                        try {
                            msg.ack();
                        } catch (Exception e) {
                            e.printStackTrace();
                            fail("Unexpected error on Ack: " + e.getMessage());
                        }
                    }
                    if (received.incrementAndGet() == toSend) {
                        latch.countDown();
                    }
                };

                // Now subscribe and set start position to #6, so should
                // received 6-10.
                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().deliverAllAvailable()
                                .ackWait(1, TimeUnit.SECONDS).manualAcks().build())) {
                    assertTrue("Did not receive at least 10 messages",
                            latch.await(5, TimeUnit.SECONDS));

                    sleep(1500, TimeUnit.MILLISECONDS); // Wait for redelivery
                    for (Message msg : msgs.values()) {
                        if ((msg.getSequence() % 2 == 0) && !msg.isRedelivered()) {
                            fail("Expected a redelivered flag to be set on msg: "
                                    + msg.getSequence());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    fail("Subscription error: " + e.getMessage());
                }
            }
        }
    }

    // testNoDuplicatesOnSubscriberStart tests that a subscriber does not
    // receive duplicate when requesting a replay while messages are being
    // published on its subject.
    @Test
    public void testNoDuplicatesOnSubscriberStart() throws Exception {
        // Run a NATS Streaming server
        try (NatsStreamingServer s = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                int batch = 100;
                final CountDownLatch latch = new CountDownLatch(1);
                final CountDownLatch pubLatch = new CountDownLatch(1);
                final AtomicInteger received = new AtomicInteger();
                final AtomicInteger sent = new AtomicInteger();

                MessageHandler mcb = msg -> {
                    // signal when we've reached the expected messages count
                    if (received.incrementAndGet() == sent.get()) {
                        latch.countDown();
                    }
                };

                service.execute(() -> {
                    // publish until the receiver starts, then one additional batch.
                    // This primes NATS Streaming with messages, and gives us a point to stop
                    // when the subscriber has started processing messages.
                    while (received.get() == 0) {
                        for (int i = 0; i < batch; i++) {
                            sent.incrementAndGet();
                            try {
                                sc.publish("foo", "hello".getBytes(), null);
                                // signal that we've published a batch.
                                pubLatch.countDown();
                            } catch (IOException e) {
                                e.printStackTrace();
                            } catch (TimeoutException e) {
                                System.err.println("publish timed out");
                                Thread.currentThread().interrupt();
                            } catch (InterruptedException e) {
                                System.err.println("publish interrupted");
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                });

                // wait until the publisher has published at least one batch
                assertTrue("Didn't publish any batches", pubLatch.await(5, TimeUnit.SECONDS));

                // start the subscriber
                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().deliverAllAvailable().build())) {

                    // Wait for our expected count.
                    assertTrue("Did not receive our messages", latch.await(5, TimeUnit.SECONDS));

                    // Wait to see if the subscriber receives any duplicate messages.
                    sleep(250);

                    // Make sure we've received the exact count of sent messages.
                    assertEquals("Didn't get expected #messages.", sent.get(), received.get());

                }
            }
        }
    }

    @Test(timeout = 3000)
    public void testRaceOnClose() throws Exception {
        try (NatsStreamingServer srv = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                // Seems that this sleep makes it happen all the time.
                sleep(1250);
            }
        }
    }

    @Test(timeout = 5000)
    public void testRaceAckOnClose() throws Exception {
        try (NatsStreamingServer srv = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                int toSend = 100;

                // Send our messages
                for (int i = 0; i < toSend; i++) {
                    sc.publish("foo", "msg".getBytes());
                }

                MessageHandler cb = msg -> {
                    try {
                        msg.ack();
                    } catch (IOException e) {
                        /* NOOP */
                    }
                };

                SubscriptionOptions sopts = new SubscriptionOptions.Builder().manualAcks()
                        .deliverAllAvailable().build();
                sc.subscribe("foo", cb, sopts);
                // Close while acking may happen
                sleep(10);
                sc.close();
            }
        }
    }

    @Test
    public void testNatsConn() throws Exception {
        try (NatsStreamingServer srv = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                // Make sure we can get the STAN-created Conn.
                io.nats.client.Connection nc = sc.getNatsConnection();
                assertNotNull(nc);

                assertEquals("Should have status set to CONNECTED.", ConnState.CONNECTED,
                        nc.getState());

                nc.close();
                assertEquals("Should have status set to CLOSED.", ConnState.CLOSED, nc.getState());

                try {
                    sc.close();
                } catch (IllegalStateException e) {
                    assertEquals(Nats.ERR_CONNECTION_CLOSED, e.getMessage());
                }

                assertNull("Wrapped conn should be null after close", sc.getNatsConnection());
            } // outer sc

            // Bail if we have a custom connection but not connected
            Connection cnc = Nats.connect();
            cnc.close();
            Options opts = new Options.Builder().natsConn(cnc).build();
            boolean exThrown = false;
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, opts)) {
                fail("Expected to get an invalid connection error");
            } catch (Exception e) {
                assertEquals(NatsStreaming.ERR_BAD_CONNECTION, e.getMessage());
                exThrown = true;
            } finally {
                assertTrue("Expected to get an invalid connection error", exThrown);
            }

            // Allow custom conn only if already connected
            try (Connection nc = Nats.connect()) {
                opts = new Options.Builder().natsConn(nc).build();
                try (StreamingConnection sc =
                             NatsStreaming.connect(clusterName, clientName, opts)) {
                    nc.close();
                    assertEquals("Should have status set to CLOSED", ConnState.CLOSED,
                            nc.getState());
                } catch (IllegalStateException e) {
                    assertEquals(Nats.ERR_CONNECTION_CLOSED, e.getMessage());
                }
            }

            // Make sure we can get the Conn we provide.
            try (Connection nc = Nats.connect()) {
                opts = new Options.Builder().natsConn(nc).build();
                try (StreamingConnection sc =
                             NatsStreaming.connect(clusterName, clientName, opts)) {
                    assertNotNull(sc.getNatsConnection());
                    assertEquals("Unexpected wrapped conn", nc, sc.getNatsConnection());
                }
            }
        }
    }

    @Test
    public void testMaxPubAcksInFlight() throws Exception {
        try (NatsStreamingServer srv = runServer(clusterName)) {
            try (Connection nc = Nats.connect()) {
                Options opts = new Options.Builder()
                        .maxPubAcksInFlight(1)
                        .pubAckWait(Duration.ofSeconds(1))
                        .natsConn(nc)
                        .build();

                StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, opts);
                // Don't defer the close of connection since the server is stopped,
                // the close would delay the test.

                // Cause the ACK to not come by shutdown the server now
                srv.shutdown();

                byte[] msg = "hello".getBytes();

                // Send more than one message, if MaxPubAcksInflight() works, one
                // of the publish call should block for up to PubAckWait.
                Instant start = Instant.now();
                for (int i = 0; i < 2; i++) {
                    sc.publish("foo", msg, null);
                }
                Instant end = Instant.now();
                // So if the loop ended before the PubAckWait timeout, then it's a failure.
                if (Duration.between(start, end).compareTo(Duration.ofSeconds(1)) < 0) {
                    fail("Should have blocked after 1 message sent");
                }
            }
        }
    }

    @Test
    public void testNatsUrlOption() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(Nats.ERR_NO_SERVERS);
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            Options opts = new Options.Builder()
                    .natsUrl("nats://localhost:5555")
                    .build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, opts)) {
                fail("Expected connect to fail");
            }
        }
    }

    @Test
    public void testNatsConnectionName() throws Exception {
        try (NatsStreamingServer ignored = runServer(clusterName)) {
            Options opts = new Options.Builder().build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, opts)) {
                Connection nc = sc.getNatsConnection();
                assertEquals(clientName, nc.getName());
            }
        }
    }

    @Test
    public void testTimeoutOnRequests() throws Exception {
        try (NatsStreamingServer srv = runServer(clusterName)) {
            try (StreamingConnection sc = newDefaultConnection()) {
                Subscription sub1 = sc.subscribe("foo", msg -> {});
                Subscription sub2 = sc.subscribe("foo", msg -> {});

                // For this test, change the reqTimeout to a very low value
                ((StreamingConnectionImpl)sc).lock();
                try {
                    ((StreamingConnectionImpl)sc).opts.connectTimeout = Duration.ofMillis(10);
                } finally {
                    ((StreamingConnectionImpl)sc).unlock();
                }

                // Shutdown server
                srv.shutdown();

                // Subscribe
                try (Subscription ignored = sc.subscribe("foo", msg -> {})) {
                    fail("Should not have subscribed successfully");
                } catch (Exception e) {
                    assertTrue("Expected IOException", e instanceof IOException);
                    assertEquals("Wrong error message", ERR_SUB_REQ_TIMEOUT, e.getMessage());
                }

                // If connecting to an old server...
                if (((StreamingConnectionImpl) sc).subCloseRequests.isEmpty()) {
                    // Trick the API into thinking that it can send, and make sure the call
                    // times out
                    ((StreamingConnectionImpl) sc).lock();
                    try {
                        ((StreamingConnectionImpl) sc).subCloseRequests = "sub.close.subject";
                    } finally {
                        ((StreamingConnectionImpl) sc).unlock();
                    }
                }

                // Subscription Close
                try {
                    sub1.close(false);
                    fail("Should have thrown an exception");
                } catch (Exception e) {
                    assertTrue("Expected IOException", e instanceof IOException);
                    assertEquals("Wrong exception message", ERR_CLOSE_REQ_TIMEOUT, e.getMessage());
                }

                // Unsubscribe
                try {
                    sub2.unsubscribe();
                    fail("Should have thrown an exception");
                } catch (Exception e) {
                    assertTrue("Expected IOException", e instanceof IOException);
                    assertEquals("Wrong exception message", ERR_UNSUB_REQ_TIMEOUT, e.getMessage());
                }

                // Connection close
                try {
                    sc.close();
                    fail("Should have thrown an exception");
                } catch (Exception e) {
                    assertTrue("Expected IOException", e instanceof IOException);
                    assertEquals("Wrong exception message", ERR_CLOSE_REQ_TIMEOUT, e.getMessage());
                }
            }
        }
    }
}
