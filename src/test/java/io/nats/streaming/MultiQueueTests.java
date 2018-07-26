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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class MultiQueueTests {
    private static final String clusterName = "test-cluster";
    private static final String clientName = "me";
    
    @Test
    public void testPubMultiQueueSub() throws InterruptedException {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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

    @Test
    public void testPubMultiQueueSubWithSlowSubscriber() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
                            try{Thread.sleep(200);}catch(Exception exp){}
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
}