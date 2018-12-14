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
import static org.junit.Assert.assertNotNull;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import io.nats.client.ConnectionListener.Events;

public class OptionsTests {
    private static final String clusterName = "test-cluster";
    private static final String clientName = "me";

    @Test
    public void testBuilderFromTempalte() {
        Options opts = new Options.Builder().
                        maxPubAcksInFlight(10).
                        natsUrl("nats://superserver:4222").
                        pubAckWait(Duration.ofMillis(1000)).
                        build();
        Options opts2 = new Options.Builder(opts).build();

        assertEquals(opts.getMaxPubAcksInFlight(), opts2.getMaxPubAcksInFlight());
        assertEquals(opts.getNatsUrl(), opts2.getNatsUrl());
        assertEquals(opts.getAckTimeout(), opts2.getAckTimeout());
    }

    @Test
    public void testErrorListener() throws Exception {
        TestHandler handler = new TestHandler();
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).errorListener(handler).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
                final CountDownLatch latch = new CountDownLatch(1);
                assertNotNull(sc);

                SubscriptionOptions sopts = new SubscriptionOptions.Builder().build();
                Subscription sub = sc.subscribe("foo", msg -> {
                    latch.countDown();
                    throw new RuntimeException(); // trigger the error handler
                }, sopts);
                assertNotNull(sub);

                sc.publish("foo", "Hello World!".getBytes());

                // Wait for the latch, then wait a bit more for the exception to flow to the handler
                latch.await(1, TimeUnit.SECONDS);
                try {
                    Thread.sleep(500);
                } catch (Exception ex) {
                    // ignore
                }
                assertEquals(handler.getExceptionCount(), 1);
            }
        }
    }

    @Test
    public void testErrorListenerViaFactory() throws Exception {
        TestHandler handler = new TestHandler();
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            StreamingConnectionFactory factory = new StreamingConnectionFactory(clusterName, clientName);
            factory.setNatsUrl(srv.getURI());
            factory.setErrorListener(handler);
            try (StreamingConnection sc = factory.createConnection()) {
                final CountDownLatch latch = new CountDownLatch(1);
                assertNotNull(sc);

                SubscriptionOptions sopts = new SubscriptionOptions.Builder().build();
                Subscription sub = sc.subscribe("foo", msg -> {
                    latch.countDown();
                    throw new RuntimeException(); // trigger the error handler
                }, sopts);
                assertNotNull(sub);

                sc.publish("foo", "Hello World!".getBytes());

                // Wait for the latch, then wait a bit more for the exception to flow to the handler
                latch.await(1, TimeUnit.SECONDS);
                try {
                    Thread.sleep(500);
                } catch (Exception ex) {
                    // ignore
                }
                assertEquals(handler.getExceptionCount(), 1);
            }
        }
    }

    @Test
    public void testConnectionListener() throws Exception {
        TestHandler handler = new TestHandler();
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).connectionListener(handler).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
                assertNotNull(sc);
                assertEquals(handler.getEventCount(Events.CONNECTED), 1);
                assertNotNull(handler.getConnection());
            }
        }

        assertEquals(handler.getEventCount(Events.CLOSED), 1);
    }

    @Test
    public void testConnectionListenerViaFactory() throws Exception {
        TestHandler handler = new TestHandler();
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            StreamingConnectionFactory factory = new StreamingConnectionFactory(clusterName, clientName);
            factory.setNatsUrl(srv.getURI());
            factory.setConnectionListener(handler);
            try (StreamingConnection sc = factory.createConnection()) {
                assertNotNull(sc);
                assertEquals(handler.getEventCount(Events.CONNECTED), 1);
                assertNotNull(handler.getConnection());
            }
        }

        assertEquals(handler.getEventCount(Events.CLOSED), 1);
    }
}