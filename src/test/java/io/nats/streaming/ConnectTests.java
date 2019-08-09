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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Connection.Status;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import org.junit.Test;

//TODO: Remove as many sleeps as possible, they can result in flaky tests
public class ConnectTests {
    private static final String clusterName = "test-cluster";
    private static final String clientName = "me";

    @Test(expected=IOException.class)
    public void testNoNats() throws Exception {
        try (StreamingConnection c =
                     NatsStreaming.connect("someNonExistentClusterID", "myTestClient")) {
            fail("Should not have connected.");
        }
    }

    @Test
    public void testUnreachable() throws Exception {
        try (NatsStreamingTestServer ignored = new NatsStreamingTestServer(clusterName, false)) {
            // Non-existent or unreachable
            final long connectTime = NatsStreaming.DEFAULT_CONNECT_WAIT * 1_000L; //into Millis
            final long start = System.nanoTime();
            try (StreamingConnection c =
                         NatsStreaming.connect("someNonExistentServerID", "myTestClient")) {
                fail("Should not have connected.");
            } catch (IOException e) {
                // e.printStackTrace();
            }
            long delta = (System.nanoTime() - start) * 1_000_000;//into millis
            String msg = String.format("Expected to wait at least %dms, but only waited %dms",
                    connectTime, delta);
            assertTrue(msg, delta > connectTime);
        }
    }

    @Test
    public void testConnClosedOnConnectFailure() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            // Non-Existent or Unreachable
            int connectTime = 25;
            Options opts = new Options.Builder()
                    .natsUrl(srv.getURI())
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
            try{Thread.sleep(500);}catch(Exception exp){}

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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            try (io.nats.client.Connection nc = Nats.connect(srv.getURI())) {
                // Pass this NATS connection to NATS Streaming
                StreamingConnection sc = NatsStreaming.connect(clusterName, clientName,
                        new Options.Builder().natsConn(nc).build());
                assertNotNull(sc);
                // Now close the NATS Streaming connection
                sc.close();

                // Verify that NATS connection is not closed
                assertFalse("NATS connection should NOT have been closed in Connect",
                        nc.getStatus() == Status.CLOSED);
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
                assertNotNull(sc);
            }
        }
    }

    @Test
    public void testClose() {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            StreamingConnection sc = null;
            Subscription sub = null;
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();

            try {
                sc = NatsStreaming.connect(clusterName, clientName, options);
            } catch (Exception e) {
                fail("Should connect");
            }

            try {
                sub = sc.subscribe("foo", msg -> {
                    fail("Did not expect to receive any messages");
                });
                
            } catch (Exception e) {
                fail("Subscribe should not have thrown");
            }

            try {
                sc.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail("Did not expect error on close(), got: " + e.getMessage());
            }

            try {
                sc.publish("foo", "Hello World!".getBytes());
                fail("Publish should fail when closed");
            } catch (Exception e) {
                //
            }

            try {
                sub.unsubscribe();
                fail("Unsubscribe should fail when closed");
            } catch (Exception e) {
                //
            }

        }
    }

    @Test
    public void testDoubleClose() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options);
            sc.close();
            sc.close();
        }
    }

    @Test(timeout = 3000)
    public void testRaceOnClose() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
                // Seems that this sleep makes it happen all the time.
                try{Thread.sleep(1250);}catch(Exception exp){}
            }
        }
    }

    @Test(timeout = 5000)
    public void testRaceAckOnClose() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
                int toSend = 100;

                // Send our messages
                for (int i = 0; i < toSend; i++) {
                    sc.publish("foo", "msg".getBytes());
                }
                sc.getNatsConnection().flush(Duration.ofSeconds(1));

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
                try{Thread.sleep(10);}catch(Exception exp){}
                sc.close();
            }
        }
    }

    @Test
    public void testManualNatsConn() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options options = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, options)) {
                // Make sure we can get the STAN-created Conn.
                io.nats.client.Connection nc = sc.getNatsConnection();
                assertNotNull(nc);

                assertTrue("Should have status set to CONNECTED.", nc.getStatus() == Status.CONNECTED);

                nc.close();
                assertTrue("Should have status set to CLOSED.", nc.getStatus() == Status.CLOSED);

                try {
                    sc.close();
                    fail("can't close when nats connection is closed");
                } catch (IllegalStateException e) {
                    //ok
                }

                assertNull("Wrapped conn should be null after close", sc.getNatsConnection());
            } // outer sc

            // Bail if we have a custom connection but not connected
            Connection cnc = Nats.connect(srv.getURI());
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
            try (Connection nc = Nats.connect(srv.getURI())) {
                opts = new Options.Builder().natsConn(nc).build();
                try (StreamingConnection sc =
                             NatsStreaming.connect(clusterName, clientName, opts)) {
                    nc.close();
                    assertTrue("Should have status set to CLOSED", nc.getStatus() == Status.CLOSED);
                } catch (IllegalStateException e) {
                     //Close should fail
                }
            }

            // Make sure we can get the Conn we provide.
            try (Connection nc = Nats.connect(srv.getURI())) {
                opts = new Options.Builder().natsConn(nc).build();
                try (StreamingConnection sc =
                             NatsStreaming.connect(clusterName, clientName, opts)) {
                    assertNotNull(sc.getNatsConnection());
                    assertEquals("Unexpected wrapped conn", nc, sc.getNatsConnection());
                }
            }
        }
    }

    @Test(expected=IOException.class)
    public void testNatsUrlOption() throws Exception {
        try (NatsStreamingTestServer ignored = new NatsStreamingTestServer(clusterName, false)) {
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
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options opts = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, opts)) {
                Connection nc = sc.getNatsConnection();
                assertEquals(clientName, nc.getOptions().getConnectionName());
            }
        }
    }

    @Test
    public void testTimeoutOnRequests() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Options opts = new Options.Builder().natsUrl(srv.getURI()).build();
            try (StreamingConnection sc = NatsStreaming.connect(clusterName, clientName, opts)) {
                Subscription sub1 = sc.subscribe("foo", msg -> {});
                Subscription sub2 = sc.subscribe("foo", msg -> {});

                // For this test, change the reqTimeout to a very low value
                ((StreamingConnectionImpl)sc).lock();
                try {
                    ((StreamingConnectionImpl)sc).opts = new Options.Builder(opts).connectWait(Duration.ofMillis(1)).build();
                } finally {
                    ((StreamingConnectionImpl)sc).unlock();
                }

                // Shutdown server
                srv.shutdown();

                Connection nc = sc.getNatsConnection();

                nc.flush(Duration.ofSeconds(1));

                int tries = 30;

                // Wait for disconnect, could be a bit flaky if this is really slow
                while (tries > 0 && nc.getStatus() == Connection.Status.CONNECTED) {
                    tries--;
                    Thread.sleep(100);
                }

                assertFalse(nc.getStatus() == Connection.Status.CONNECTED);

                // Subscribe
                try (Subscription ignored = sc.subscribe("foo", msg -> {})) {
                    fail("Should not have subscribed successfully");
                } catch (Exception e) {
                    assertTrue("Expected IOException", e instanceof IOException);
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
                }

                // Unsubscribe
                try {
                    sub2.unsubscribe();
                    fail("Should have thrown an exception");
                } catch (Exception e) {
                    assertTrue("Expected IOException", e instanceof IOException);
                }

                // Connection close
                try {
                    sc.close();
                    fail("Should have thrown an exception");
                } catch (Exception e) {
                    //
                }
            }
        }
    }
}
