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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Nats;

public class SharedNatsConnectionTests {
    private static final String clusterName = "test-cluster";

    @Test
    public void testSharedOnConnect() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            io.nats.client.Options options = new io.nats.client.Options.Builder().server(srv.getURI()).maxReconnects(0).build();
            try (Connection nc = Nats.connect(options)){
                Options streamingOptions = new Options.Builder().natsConn(nc).build();

                StreamingConnection one = NatsStreaming.connect(clusterName, "one", streamingOptions);
                StreamingConnection two = NatsStreaming.connect(clusterName, "two", streamingOptions);

                try {
                    assertNotNull(one);
                    assertNotNull(two);
                } finally {
                    one.close();
                    two.close();
                }
            }
        }
    }

    @Test
    public void testReusedConnect() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options streamingOptions = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection one = NatsStreaming.connect(clusterName, "one", streamingOptions)){
                Options streamingOptionsTwo = new Options.Builder().natsConn(one.getNatsConnection()).build();
                StreamingConnection two = NatsStreaming.connect(clusterName, "two", streamingOptionsTwo);

                try {
                    assertNotNull(one);
                    assertNotNull(two);
                } finally {
                    two.close();
                    one.close(); // Have to close the nats connection owner last
                }
            }
        }
    }

    @Test
    public void testSubsDontCross() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            io.nats.client.Options options = new io.nats.client.Options.Builder().server(srv.getURI()).maxReconnects(0).build();
            try (Connection nc = Nats.connect(options)){
                Options streamingOptions = new Options.Builder().natsConn(nc).build();
                try (StreamingConnection one = NatsStreaming.connect(clusterName, "one", streamingOptions);
                            StreamingConnection two = NatsStreaming.connect(clusterName, "two", streamingOptions)) {

                    final int toSend = 100;
                    final byte[] payload = "Hello World".getBytes(StandardCharsets.UTF_8);
                    final CountDownLatch latch = new CountDownLatch(2);
                    final AtomicInteger receivedOne = new AtomicInteger(0);
                    final AtomicInteger receivedTwo = new AtomicInteger(0);
                    final ArrayList<Long> msgListOne = new ArrayList<>();
                    final ArrayList<Long> msgListTwo = new ArrayList<>();

                    Subscription subOne = one.subscribe("foo", msg -> {
                        assertEquals("foo", msg.getSubject());
                        assertArrayEquals(payload, msg.getData());
                        // Make sure Seq and Timestamp are set
                        assertNotEquals(0, msg.getSequence());
                        assertNotEquals(0, msg.getTimestamp());
                        assertFalse("Detected duplicate for sequence no: " + msg.getSequence(),
                                msgListOne.contains(msg.getSequence()));
                        msgListOne.add(msg.getSequence());

                        if (receivedOne.incrementAndGet() >= 2*toSend) {
                            latch.countDown();
                        }
                    });

                    Subscription subTwo = two.subscribe("foo", msg -> {
                        assertEquals("foo", msg.getSubject());
                        assertArrayEquals(payload, msg.getData());
                        // Make sure Seq and Timestamp are set
                        assertNotEquals(0, msg.getSequence());
                        assertNotEquals(0, msg.getTimestamp());
                        assertFalse("Detected duplicate for sequence no: " + msg.getSequence(),
                        msgListTwo.contains(msg.getSequence()));
                        msgListTwo.add(msg.getSequence());

                        if (receivedTwo.incrementAndGet() >= 2*toSend) {
                            latch.countDown();
                        }
                    });
                    
                    for (int i = 0; i < toSend; i++) {
                        one.publish("foo", payload);
                        two.publish("foo", payload);
                    }

                    assertTrue(latch.await(10, TimeUnit.SECONDS));

                    Thread.sleep(500); //Give extra messages time to go through, there shouldn't be any

                    assertEquals(msgListOne.size(), 2*toSend);
                    assertEquals(msgListTwo.size(), 2*toSend);
                    assertEquals(receivedOne.get(), 2*toSend);
                    assertEquals(receivedTwo.get(), 2*toSend);

                    subOne.close();
                    subTwo.close();
                }
            }
        }
    }
}