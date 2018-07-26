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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class PubSubTests {
    private static final String clusterName = "test-cluster";
    private static final String clientName = "me";

    @Test
    public void testBasicPubSub() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
    public void testBasicPubSubFlowControl() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
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
    public void testManualAck() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {

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
                    try{Thread.sleep(50);}catch(Exception exp){}

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
}