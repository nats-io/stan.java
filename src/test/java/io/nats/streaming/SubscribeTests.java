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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import io.nats.streaming.protobuf.StartPosition;

public class SubscribeTests {
    private static final String clusterName = "test-cluster";
    private static final String clientName = "me";

    @Test
    public void testBasicSubscription() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
    public void testBasicQueueSubscription() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
                try{Thread.sleep(500);}catch(Exception exp){}

                // StreamingConnection closes here
            }

            // Create new connection
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
    public void testSubscriptionStartPositionLast() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
                // Publish first five
                for (int i = 1; i <= 5; i++) {
                    byte[] data = String.format("%d", i).getBytes();
                    sc.publish("foo", data);
                }
                // Buffer each side so slow tests still work.
                try{Thread.sleep(250);}catch(Exception exp){}
                // Date startTime = new Date(System.currentTimeMillis());
                final Instant startTime = Instant.now();
                try{Thread.sleep(250);}catch(Exception exp){}

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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {

                MessageHandler mcb = msg -> {
                };

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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options opts = new Options.Builder()
                    .natsUrl(srv.getURI())
                    .pubAckWait(Duration.ofMillis(50))
                    .build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, opts)) {
                assertNotNull(sc);
                Subscription sub = sc.subscribe("foo", null);
                final CountDownLatch wg = new CountDownLatch(1);

                Thread t = new Thread(() -> {
                    try{Thread.sleep(ThreadLocalRandom.current().nextInt(0, 50));}catch(Exception exp){}
                    try {
                        sc.close();
                    } catch (Exception e) {
                        System.err.println("CLOSE ERROR");
                        e.printStackTrace();
                    }
                    wg.countDown();
                });
                t.start();

                // Unsubscribe
                sub.unsubscribe();

                wg.await();
            }
        }
    }

    @Test
    public void testSubscribeShrink() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
                try (final StreamingConnection sc2 = NatsStreaming.connect(clusterName, clientName, options)) {
                    fail("Subscription should not have succeeded");
                } catch (IOException | TimeoutException e) {
                    assertEquals(NatsStreaming.SERVER_ERR_INVALID_CLIENT, e.getMessage());
                }
            }
        }
    }


    @Test
    public void testDurableSubscriber() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            final StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options);

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
                        try{Thread.sleep(500);}catch(Exception exp){}
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
                Options opts = new Options.Builder().natsUrl(srv.getURI()).pubAckWait(Duration.ofMillis(50)).build();
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

    // testNoDuplicatesOnSubscriberStart tests that a subscriber does not
    // receive duplicate when requesting a replay while messages are being
    // published on its subject.
    @Test
    public void testNoDuplicatesOnSubscriberStart() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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

                Thread t = new Thread(() -> {
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
                            } catch (IOException | TimeoutException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                System.err.println("publish interrupted");
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                });
                t.start();

                // wait until the publisher has published at least one batch
                assertTrue("Didn't publish any batches", pubLatch.await(5, TimeUnit.SECONDS));

                // start the subscriber
                try (Subscription sub = sc.subscribe("foo", mcb,
                        new SubscriptionOptions.Builder().deliverAllAvailable().build())) {

                    // Wait for our expected count.
                    assertTrue("Did not receive our messages", latch.await(10, TimeUnit.SECONDS));

                    // Wait to see if the subscriber receives any duplicate messages.
                    try{Thread.sleep(1000);}catch(Exception exp){}

                    // Make sure we've received the exact count of sent messages.
                    assertEquals("Didn't get expected number of messages.", sent.get(), received.get());

                }
            }
        }
    }
}