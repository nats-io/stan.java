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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class RedeliveryTests {
    private static final String clusterName = "test-cluster";
    private static final String clientName = "me";


    @Test
    public void testRedelivery() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {

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

        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
    public void testRedeliveredFlag() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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

                    try{Thread.sleep(1500);}catch(Exception exp){}
                    
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

}