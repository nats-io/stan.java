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


import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.nats.client.AsyncSubscription;
import io.nats.client.NUID;
import io.nats.streaming.protobuf.CloseResponse;
import io.nats.streaming.protobuf.ConnectResponse;
import io.nats.streaming.protobuf.PubAck;
import io.nats.streaming.protobuf.PubMsg;
import io.nats.streaming.protobuf.SubscriptionRequest;
import io.nats.streaming.protobuf.SubscriptionResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

class UnitTestUtilities {

    // final Object mu = new Object();
    private static NatsStreamingServer defaultServer = null;

    static final String testClusterName = "test-cluster";
    static final String testClientName = "me";

    static StreamingConnection newDefaultConnection() throws IOException, InterruptedException {
        io.nats.client.Connection nc = new io.nats.client
                .Options.Builder()
                .noReconnect()
                .build()
                .connect();

        Options opts = new Options.Builder().natsConn(nc).build();
        return NatsStreaming.connect(testClusterName, testClientName, opts);
    }

    static StreamingConnection newMockedConnection() throws IOException, InterruptedException {
        io.nats.client.Connection nc = setupMockNatsConnection();
        Options opts = new Options.Builder().natsConn(nc).build();
        StreamingConnectionImpl conn = new StreamingConnectionImpl(testClusterName,
                testClientName, opts);
        return conn.connect();
    }

    static StreamingConnection newMockedConnection(boolean owned) throws IOException,
            InterruptedException {
        StreamingConnectionImpl conn;
        io.nats.client.Connection nc = setupMockNatsConnection();
        if (owned) {
            Options opts = new Options.Builder().build();
            conn = Mockito.spy(new StreamingConnectionImpl(testClusterName, testClientName, opts));
            conn.nc = nc;
            conn.ncOwned = true;
            conn.connect();
        } else {
            conn = (StreamingConnectionImpl) newMockedConnection();
        }
        return conn;
    }


    static io.nats.client.Connection setupMockNatsConnection()
            throws IOException, InterruptedException {
        final String subRequests = String.format("_STAN.sub.%s", NUID.nextGlobal());
        final String pubPrefix = String.format("_STAN.pub.%s", NUID.nextGlobal());
        final String unsubRequests = String.format("_STAN.unsub.%s", NUID.nextGlobal());
        final String closeRequests = String.format("_STAN.close.%s", NUID.nextGlobal());
        final String hbInbox = String.format("_INBOX.%s", io.nats.client.NUID.nextGlobal());

        io.nats.client.Connection nc = mock(io.nats.client.Connection.class);

        doReturn(true).when(nc).isConnected();

        when(nc.newInbox()).thenReturn(hbInbox);

        AsyncSubscription hbSubscription = mock(AsyncSubscription.class);
        when(hbSubscription.getSubject()).thenReturn(hbInbox);
        final io.nats.client.MessageHandler[] hbCallback = new io.nats.client.MessageHandler[1];
        doAnswer(new Answer<AsyncSubscription>() {
            @Override
            public AsyncSubscription answer(InvocationOnMock invocation) throws Throwable {
                // when(br.readLine()).thenReturn("PONG");
                Object[] args = invocation.getArguments();
                hbCallback[0] = (io.nats.client.MessageHandler) args[1];
                return hbSubscription;
            }
        }).when(nc).subscribe(eq(hbInbox), any(io.nats.client.MessageHandler.class));

        doReturn(hbCallback[0]).when(hbSubscription).getMessageHandler();

        String discoverSubject =
                String.format("%s.%s", NatsStreaming.DEFAULT_DISCOVER_PREFIX, testClusterName);
        ConnectResponse crProto =
                ConnectResponse.newBuilder().setPubPrefix(pubPrefix).setSubRequests(subRequests)
                        .setUnsubRequests(unsubRequests).setCloseRequests(closeRequests).build();
        io.nats.client.Message cr = new io.nats.client.Message("foo", "bar", crProto.toByteArray());
        try {
            when(nc.request(eq(discoverSubject), any(byte[].class), any(long.class)))
                    .thenReturn(cr);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail(e.getMessage());
        }

        AsyncSubscription ackSubscription = mock(AsyncSubscription.class);
        final io.nats.client.MessageHandler[] ackMsgHandler = new io.nats.client.MessageHandler[1];
        // Capture the ackSubject and ackHandler
        doAnswer(new Answer<AsyncSubscription>() {
            @Override
            public AsyncSubscription answer(InvocationOnMock invocation) throws Throwable {
                // when(br.readLine()).thenReturn("PONG");
                Object[] args = invocation.getArguments();
                // System.err.println("ackSubject has been set to " + ackSubject[0]);
                ackMsgHandler[0] = (io.nats.client.MessageHandler) args[1];
                doReturn(ackMsgHandler[0]).when(ackSubscription).getMessageHandler();
                return ackSubscription;
            }
        }).when(nc).subscribe(matches("^" + NatsStreaming.DEFAULT_ACK_PREFIX + "\\..*$"),
                any(io.nats.client.MessageHandler.class));

        when(nc.isClosed()).thenReturn(false);

        // Handle SubscriptionRequests
        doAnswer(new Answer<io.nats.client.Message>() {
            @Override
            public io.nats.client.Message answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                SubscriptionRequest req = SubscriptionRequest.parseFrom((byte[]) args[1]);
                String subInbox = req.getInbox();
                String ackInbox = String.format("_INBOX.%s", NUID.nextGlobal());
                SubscriptionResponse sr =
                        SubscriptionResponse.newBuilder().setAckInbox(ackInbox).build();
                io.nats.client.Message rawResponse = new io.nats.client.Message();
                rawResponse.setSubject(subInbox);
                rawResponse.setData(sr.toByteArray());
                return rawResponse;
            }
        }).when(nc).request(matches(subRequests), any(byte[].class), any(long.class),
                any(TimeUnit.class));

        SubscriptionResponse unsubResponseProto = SubscriptionResponse.newBuilder().build();
        io.nats.client.Message unsubResponse =
                new io.nats.client.Message("foo", "bar", unsubResponseProto.toByteArray());
        try {
            when(nc.request(eq(unsubRequests), any(byte[].class), any(long.class)))
                    .thenReturn(unsubResponse);
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        CloseResponse closeResponseProto = CloseResponse.newBuilder().build();
        io.nats.client.Message closeResponse =
                new io.nats.client.Message("foo", "bar", closeResponseProto.toByteArray());
        try {
            when(nc.request(eq(closeRequests), any(byte[].class), any(long.class)))
                    .thenReturn(closeResponse);
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        /**
         * Anytime a STAN message is published synchronously, call the ackSubscription's handler
         * with a valid ACK so that publish will return.
         */
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                // String pubSubject = (String) args[0];
                String localAckSubject = (String) args[1];
                byte[] payload = (byte[]) args[2];
                PubMsg pubMsg = PubMsg.parseFrom(payload);
                // System.err.printf("mock received PubMsg:\n%s", pubMsg);
                String nuid = pubMsg.getGuid();
                PubAck pubAck = PubAck.newBuilder().setGuid(nuid).build();
                io.nats.client.Message raw = new io.nats.client.Message();
                raw.setSubject(localAckSubject);
                raw.setData(pubAck.toByteArray());
                ackMsgHandler[0].onMessage(raw);
                return null;
            }
        }).when(nc).publish(any(String.class),
                matches("^" + NatsStreaming.DEFAULT_ACK_PREFIX + "\\..*$"), any(byte[].class),
                any(boolean.class));
        // }).when(nc).publish(any(String.class), eq(ackSubject[0]), any(byte[].class));

        return nc;
    }


    private static synchronized void startDefaultServer() {
        startDefaultServer(false);
    }

    private static synchronized void startDefaultServer(boolean debug) {
        if (defaultServer == null) {
            defaultServer = new NatsStreamingServer(debug);
            try {
    			waitForStreamingServerUp();
    		} catch (TimeoutException | InterruptedException e) {
    			e.printStackTrace();
    		}
        }
    }

    private static synchronized void stopDefaultServer() {
        if (defaultServer != null) {
            defaultServer.shutdown();
            defaultServer = null;
        }
    }

    static synchronized void bounceDefaultServer(int delayMillis) {
        stopDefaultServer();
        try {
			waitForStreamingServerDown();
            Thread.sleep(delayMillis);
		} catch (TimeoutException | InterruptedException e) {
			e.printStackTrace();
		}
        startDefaultServer();
    }

    void startAuthServer() throws IOException {
        Process authServerProcess = Runtime.getRuntime().exec("gnatsd -config auth.conf");
    }

    NatsStreamingServer createServerOnPort(int port) {
        return createServerOnPort(port, false);
    }

    private NatsStreamingServer createServerOnPort(int port, boolean debug) {
        NatsStreamingServer nsrv = new NatsStreamingServer(port, debug);
        try {
			waitForStreamingServerUp();
		} catch (TimeoutException | InterruptedException e) {
			e.printStackTrace();
		}
        return nsrv;
    }

    NatsStreamingServer createServerWithConfig(String configFile) {
        return createServerWithConfig(configFile, false);
    }

    private NatsStreamingServer createServerWithConfig(String configFile, boolean debug) {
        NatsStreamingServer nsrv = new NatsStreamingServer(configFile, debug);
        try {
			waitForStreamingServerUp();
		} catch (TimeoutException | InterruptedException e) {
			e.printStackTrace();
		}
        return nsrv;
    }

	protected static void waitForStreamingServerUp() throws TimeoutException, InterruptedException {
		for (int i = 1; i <= 20; i++) {
			Thread.sleep(100 * i);
			if (isNatsStreamingServerRunning())
				return ;
		}
		throw new TimeoutException();
	}

	protected static void waitForStreamingServerDown() throws TimeoutException, InterruptedException {
		for (int i = 1; i <= 20; i++) {
			Thread.sleep(100 * i);
			if (! isNatsStreamingServerRunning())
				return ;
		}
		throw new TimeoutException();
	}
    
    protected static boolean isNatsStreamingServerRunning() {
        try {
    		final StreamingConnection connection = NatsStreaming.connect(testClusterName, testClientName + "_tmp");
    		connection.close();
            return true;
        } catch (Exception e) {
            return false;
        }
	}

    void getConnz() {
        URL url = null;
        try {
            url = new URL("http://localhost:8222/connz");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
            for (String line; (line = reader.readLine()) != null; ) {
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void sleep(int timeout) {
        sleep(timeout, TimeUnit.MILLISECONDS);
    }

    static void sleep(int duration, TimeUnit unit) {
        try {
            unit.sleep(duration);
        } catch (InterruptedException e) {
            /* NOOP */
        }
    }

    static boolean await(CountDownLatch latch) {
        return await(latch, 5, TimeUnit.SECONDS);
    }

    private static boolean await(CountDownLatch latch, long timeout, TimeUnit unit) {
        boolean val = false;
        try {
            val = latch.await(timeout, unit);
        } catch (InterruptedException e) {
            /* NOOP */
        }
        return val;
    }

    static NatsStreamingServer runServer(String clusterId) {
        return runServer(clusterId, false);
    }

    private static NatsStreamingServer runServer(String clusterId, boolean debug) {
        NatsStreamingServer srv = new NatsStreamingServer(clusterId, debug);
        try {
			waitForStreamingServerUp();
		} catch (TimeoutException | InterruptedException e) {
			e.printStackTrace();
		}
        return srv;
    }
    
	protected static Object serializeDeserialize(Object object)
			throws IOException, ClassNotFoundException {
		byte[] bytes = null;
		ByteArrayOutputStream bos = null;
		ObjectOutputStream oos = null;
		bos = new ByteArrayOutputStream();
		oos = new ObjectOutputStream(bos);
		oos.writeObject(object);
		oos.flush();
		bytes = bos.toByteArray();
		oos.close();
		bos.close();

		Object obj = null;
		ByteArrayInputStream bis = null;
		ObjectInputStream ois = null;
		bis = new ByteArrayInputStream(bytes);
		ois = new ObjectInputStream(bis);
		obj = ois.readObject();
		bis.close();
		ois.close();
		return obj;
	}
	
}
