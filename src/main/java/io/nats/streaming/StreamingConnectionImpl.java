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

import static io.nats.streaming.NatsStreaming.ERR_CONNECTION_REQ_TIMEOUT;
import static io.nats.streaming.NatsStreaming.ERR_SUB_REQ_TIMEOUT;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.ErrorListener;
import io.nats.client.Message;
import io.nats.client.NUID;
import io.nats.client.Nats;
import io.nats.streaming.protobuf.Ack;
import io.nats.streaming.protobuf.CloseRequest;
import io.nats.streaming.protobuf.CloseResponse;
import io.nats.streaming.protobuf.ConnectRequest;
import io.nats.streaming.protobuf.ConnectResponse;
import io.nats.streaming.protobuf.MsgProto;
import io.nats.streaming.protobuf.Ping;
import io.nats.streaming.protobuf.PingResponse;
import io.nats.streaming.protobuf.PubAck;
import io.nats.streaming.protobuf.PubMsg;
import io.nats.streaming.protobuf.SubscriptionRequest;
import io.nats.streaming.protobuf.SubscriptionResponse;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class StreamingConnectionImpl implements StreamingConnection, io.nats.client.MessageHandler {

    static final String ERR_MANUAL_ACK = NatsStreaming.PFX + "cannot manually ack in auto-ack mode";
    static final String INBOX_PREFIX = "_INBOX.";

    private final ReadWriteLock mu = new ReentrantReadWriteLock();

    private String clientId;
    private String clusterId;
    private String connectionId;

    String pubPrefix; // Publish prefix set by streaming, append our subject.
    String subRequests; // Subject to send subscription requests.
    String unsubRequests; // Subject to send unsubscribe requests.
    String subCloseRequests; // Subject to send subscription close requests.
    String closeRequests; // Subject to send close requests.
    
    String ackSubject; // publish acks
    String hbSubject;

    String pingInbox;
    Duration pingInterval;
    int pingMaxOut;
    byte[] pingBytes;
    String pingRequests;
    int pingsOut;
    Timer pingTimer;

    Map<String, Subscription> subMap;
    Map<String, AckClosure> pubAckMap;
    private BlockingQueue<PubAck> pubAckChan;
    Options opts;
    io.nats.client.Connection nc;
    io.nats.client.Dispatcher ackDispatcher;
    io.nats.client.Dispatcher messageDispatcher;
    Map<String, io.nats.client.Dispatcher> customDispatchers;
    io.nats.client.Dispatcher dispatcher; // used for internal messaging, heartbeats and pings

    io.nats.client.Subscription pingSub;
    io.nats.client.Subscription hbSub;

    io.nats.client.NUID nuid;

    final Timer ackTimer = new Timer("jnats-streaming ack timeout thread", true);

    boolean ncOwned = false;

    StreamingConnectionImpl(String clusterId, String clientId, Options opts) {
        this.clusterId = clusterId;
        this.clientId = clientId;
        this.nuid = new NUID();
        this.opts = opts;
        this.connectionId = this.nuid.next();

        if (opts == null) { 
            opts = new Options.Builder().build();
        }
        
        // Check if the user has provided a connection as an option
        if (this.opts.getNatsConn() != null) {
            setNatsConnection(this.opts.getNatsConn());
        }
    }

    StreamingConnectionImpl(Options opts) {
        this(opts.getClusterId(), opts.getClientId(), opts);
    }

    void timeTrace(boolean trace, String format, Object... args) {
        if (trace) {
            String timeStr = DateTimeFormatter.ISO_TIME.format(LocalDateTime.now());
            System.out.printf("[%s] connect trace: ", timeStr);
            System.out.printf(format, args);
            System.out.println();

        }
    }

    // Connect will form a connection to the STAN subsystem.
    StreamingConnectionImpl connect() throws IOException, InterruptedException {
        boolean exThrown = false;
        boolean trace = opts.isTraceConnection();

        timeTrace(trace, "starting connection to streaming cluster %s as %s", this.clusterId, this.clientId);

        io.nats.client.Connection nc = getNatsConnection();
        // Create a NATS connection if it doesn't exist
        if (nc == null) {
            nc = createNatsConnection();
            setNatsConnection(nc);
            ncOwned = true;
        } else if (nc.getStatus() != Connection.Status.CONNECTED) {
            // Bail if the custom NATS connection is disconnected
            throw new IOException(NatsStreaming.ERR_BAD_CONNECTION);
        } else {
            timeTrace(trace, "skipped NATS connection, using existing one");
        }

        try {
            timeTrace(trace, "creating inboxes");
            this.hbSubject = this.newInbox();
            this.pingInbox = this.newInbox();
            this.ackSubject = String.format("%s.%s", NatsStreaming.DEFAULT_ACK_PREFIX, this.nuid.next());

            timeTrace(trace, "creating ack dispatcher");
            this.ackDispatcher = nc.createDispatcher(msg -> {
                this.processAck(msg);
            });
            this.ackDispatcher.subscribe(this.ackSubject);

            timeTrace(trace, "creating hb/ping dispatcher and subscribing");
            this.dispatcher = nc.createDispatcher(msg -> {});
            this.hbSub = this.dispatcher.subscribe(this.hbSubject, msg -> {
                this.processHeartBeat(msg);
            });

            this.pingSub = this.dispatcher.subscribe(this.pingInbox, msg -> {
                this.processPing(msg);
            });

            timeTrace(trace, "setting pending limits on dispatchers");
            this.dispatcher.setPendingLimits(-1, -1);
            this.ackDispatcher.setPendingLimits(-1, -1);

            this.customDispatchers = new HashMap<>();

            // Send Request to discover the cluster
            String discoverSubject = String.format("%s.%s", opts.getDiscoverPrefix(), clusterId);

            // For tests we set to negative for millis
            // otherwise convert to seconds
            long pingInterval = opts.getPingInterval().toMillis();
            if (pingInterval < 1000) {
                pingInterval = -pingInterval;
            } else {
                pingInterval = pingInterval/1000;
            }

            timeTrace(trace, "sending connection request");
            ConnectRequest req = ConnectRequest.newBuilder()
                    .setClientID(clientId)
                    .setConnID(ByteString.copyFromUtf8(this.connectionId))
                    .setHeartbeatInbox(this.hbSubject)
                    .setProtocol(NatsStreaming.PROTOCOL_ONE)
                    .setPingInterval((int)pingInterval)
                    .setPingMaxOut(opts.getMaxPingsOut()).build();

            byte[] bytes = req.toByteArray();
            Message reply = nc.request(discoverSubject, bytes, opts.getConnectTimeout());

            if (reply == null) {
                throw new IOException(ERR_CONNECTION_REQ_TIMEOUT);
            }
            
            timeTrace(trace, "received connection request");
            ConnectResponse cr = ConnectResponse.parseFrom(reply.getData());
            if (!cr.getError().isEmpty()) {
                // This is already a properly formatted streaming error message
                // (StreamingConnectionImpl.SERVER_ERR_INVALID_CLIENT)
                throw new IOException(cr.getError());
            }

            // Capture cluster configuration endpoints to publish and
            // subscribe/unsubscribe.
            pubPrefix = cr.getPubPrefix();
            subRequests = cr.getSubRequests();
            unsubRequests = cr.getUnsubRequests();
            subCloseRequests = cr.getSubCloseRequests();
            closeRequests = cr.getCloseRequests();

            boolean unsubPings = true;

            if (cr.getProtocol() >= NatsStreaming.PROTOCOL_ONE) {
                timeTrace(trace, "setting up server ping");

                // Note that in the future server may override client ping
                // interval value sent in ConnectRequest, so use the
                // value in ConnectResponse to decide if we send PINGs
                // and at what interval.
                // In tests, the interval could be negative to indicate
                // milliseconds.
                if (cr.getPingInterval() != 0) {
                    unsubPings = false;

                    // These will be immutable.
                    this.pingRequests = cr.getPingRequests();

                    // In test, it is possible that we get a negative value
                    // to represent milliseconds.
                    if (cr.getPingInterval() < 0) {
                        this.pingInterval = Duration.ofMillis(-cr.getPingInterval());
                    } else {
                        // PingInterval is otherwise assumed to be in seconds.
                        this.pingInterval = Duration.ofSeconds(cr.getPingInterval());
                    }

                    this.pingMaxOut = cr.getPingMaxOut();
                    this.pingBytes = Ping.newBuilder().setConnID(ByteString.copyFromUtf8(this.connectionId)).build().toByteArray();
                    
                    // Set the timer now that we are set. Use lock to create
                    // synchronization point.
                    this.pingTimer = new Timer("jnats streaming ping timer", true);
                    this.pingTimer.schedule(new TimerTask() {
                        public void run() {
                            try {
                                pingServer();
                            } catch (Exception e) {
                                // catch exception to prevent the timer to be closed, but cancel this task
                                cancel();
                                // TODO:  Ignore, but re-evaluate this
                            }
                        }
                    }, this.pingInterval.toMillis(), this.pingInterval.toMillis());
                }
            }
            
            if (unsubPings) {
                timeTrace(trace, "removing ping subscriber, not supported by server");
                this.dispatcher.unsubscribe(pingSub);
                this.pingSub = null;
            }

            // Setup the ACK subscription
            pubAckMap = new HashMap<>();

            // Create Subscription map
            subMap = new HashMap<>();

            pubAckChan = new LinkedBlockingQueue<>(opts.getMaxPubAcksInFlight());
        } catch (IOException e) {
            exThrown = true;
            throw e;
        } finally {
            if (exThrown) {
                try {
                    close();
                } catch (Exception e) {
                    /* NOOP -- can't do anything if close fails */
                }
            } else {
                timeTrace(trace, "connection complete");
            }
        }
        return this;
    }

    io.nats.client.Connection createNatsConnection() throws IOException, InterruptedException {
        io.nats.client.Connection nc = null;
        if (getNatsConnection() == null) {
            if (opts.getNatsUrl() != null) {
                io.nats.client.Options.Builder natsOpts = new io.nats.client.Options.Builder().
                                                    connectionName(clientId).
                                                    errorListener(opts.getErrorListener()).
                                                    connectionListener(opts.getConnectionListener()).
                                                    server(opts.getNatsUrl());
                if (opts.isTraceConnection()) {
                    natsOpts.traceConnection();
                }
                
                nc = Nats.connect(natsOpts.build());
            } else {
                nc = Nats.connect();
            }
            ncOwned = true;
        }
        return nc;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        this.close(false);
    }

    // If silent is true we don't try to notify the server
    void close(boolean silent) throws IOException, InterruptedException {
        io.nats.client.Connection nc;
        this.lock();
        try {
            // Capture for NATS calls below
            if (getNatsConnection() == null) {
                // We are already closed
                return;
            }

            // Capture for NATS calls below.
            nc = getNatsConnection();

            // if ncOwned, we close it at the end
            try {
                // Signals we are closed.
                setNatsConnection(null);

                if (pingTimer != null) {
                    pingTimer.cancel();
                }

                for (AckClosure ac : this.pubAckMap.values()) {
                    ac.ackTask.cancel();

                    if (!ac.ch.isEmpty()) {
                        ac.ch.take();
                    }
                }
                ackTimer.cancel();

                if (this.messageDispatcher != null && this.messageDispatcher.isActive()) {
                    nc.closeDispatcher(this.messageDispatcher);
                }

                for (Dispatcher d : this.customDispatchers.values()) {
                    if (d.isActive()) {
                        nc.closeDispatcher(d);
                    }
                }

                if (this.ackDispatcher != null && this.ackDispatcher.isActive()) {
                    nc.closeDispatcher(this.ackDispatcher);
                }

                if (this.dispatcher != null && this.dispatcher.isActive()) {
                    nc.closeDispatcher(this.dispatcher);
                }

                if (!silent) {
                    CloseRequest req = CloseRequest.newBuilder().setClientID(clientId).build();
                    byte[] bytes = req.toByteArray();
                    Message reply = nc.request(closeRequests, bytes, opts.getConnectTimeout());

                    if (reply == null) {
                        throw new IOException(NatsStreaming.ERR_CLOSE_REQ_TIMEOUT);
                    }
                    if (reply.getData() != null) {
                        CloseResponse cr = CloseResponse.parseFrom(reply.getData());

                        if (!cr.getError().isEmpty()) {
                            throw new IOException(cr.getError());
                        }
                    }
                }
            } finally {
                if (ncOwned) {
                    try {
                        nc.close();
                    } catch (Exception ignore) {
                        // ignore
                    }
                }
            } // first finally
        } finally {
            this.unlock();
        }
    }

    private SubscriptionImpl createSubscription(String subject, String qgroup,
                                                io.nats.streaming.MessageHandler cb,
                                                StreamingConnectionImpl conn,
                                                SubscriptionOptions opts) {
        return new SubscriptionImpl(subject, qgroup, cb, conn, opts);
    }

    void processHeartBeat(Message msg) {
        // No payload assumed, just reply.
        io.nats.client.Connection nc;
        this.rLock();
        nc = this.nc;
        this.rUnlock();
        if (nc != null) {
            nc.publish(msg.getReplyTo(), null);
        }
    }

    BlockingQueue<String> createErrorChannel() {
        return new LinkedBlockingQueue<>();
    }

    // Publish will publish to the cluster and wait for an ACK.
    // Prior to 2.2.0 this method did not block properly.
    @Override
    public void publish(String subject, byte[] data) throws IOException, InterruptedException, TimeoutException {
        final BlockingQueue<String> ch = createErrorChannel();
        Duration ackTimeout = opts.getAckTimeout();

        publish(subject, data, null, ch);

        // Should get a timeout from the timer at ackTimeout, but fail no matter what
        // don't leave a dangling "take" call that can lock the thread
        String err = ch.poll(2 * ackTimeout.toMillis(), TimeUnit.MILLISECONDS);
        if (err == null) {
            throw new TimeoutException(NatsStreaming.ERR_TIMEOUT);
        } else if (!err.isEmpty()) {
            throw new IOException(err);
        }
    }

    /*
     * Publish with an ack handler will publish to the cluster on pubPrefix+subject and asynchronously process the
     * ACK or error state. It will return the GUID for the message being sent.
     */
    @Override
    public String publish(String subject, byte[] data, AckHandler ah) throws IOException,
            InterruptedException, TimeoutException {
        return publish(subject, data, ah, null);
    }

    private String publish(String subject, byte[] data, AckHandler ah, BlockingQueue<String> ch)
            throws IOException, InterruptedException, TimeoutException {
        String subj;
        String ackSubject;
        Duration ackTimeout = opts.getAckTimeout();
        BlockingQueue<PubAck> pac;
        final AckClosure a= new AckClosure(ah, subject, (ah != null) ? data : null, ch);
        final PubMsg pe;
        String guid;
        byte[] bytes;

        this.lock();
        try {
            if (getNatsConnection() == null) {
                throw new IllegalStateException(NatsStreaming.ERR_CONNECTION_CLOSED);
            }

            subj = pubPrefix + "." + subject;
            guid = NUID.nextGlobal();
            PubMsg.Builder pb =
                    PubMsg.newBuilder().setClientID(clientId).setGuid(guid).setSubject(subject);
            if (data != null) {
                pb = pb.setData(ByteString.copyFrom(data));
            }
            pe = pb.build();
            bytes = pe.toByteArray();

            // Map ack to guid
            pubAckMap.put(guid, a);
            // snapshot
            ackSubject = this.ackSubject;
            pac = pubAckChan;
        } finally {
            this.unlock();
        }

        // Use the buffered channel to control the number of outstanding acks.
        try {
            pac.put(PubAck.getDefaultInstance());
        } catch (InterruptedException e) {
            // TODO:  Reevaluate this.
            // Eat this because you can't really do anything with it
        }

        nc.publish(subj, ackSubject, bytes);

        // Setup the timer for expiration.
        this.lock();
        try {
            a.ackTask = createAckTimerTask(guid);
            ackTimer.schedule(a.ackTask, ackTimeout.toMillis());
        } finally {
            this.unlock();
        }
        return guid;
    }

    Dispatcher getDispatcherByName(String name) {
        Dispatcher d = null;
        this.lock();
        try {
            if (name == null || name.isEmpty()) {
                if (this.messageDispatcher == null) {
                    this.messageDispatcher = nc.createDispatcher(msg -> {
                        this.processMsg(msg);
                    });
                    this.messageDispatcher.setPendingLimits(-1, -1);
                }

                return this.messageDispatcher;
            }

            d = customDispatchers.get(name);

            if (d == null) {
                d = this.getNatsConnection().createDispatcher(msg -> {
                    this.processMsg(msg);
                });
                d.setPendingLimits(-1, -1);
                customDispatchers.put(name, d);
            }
        } finally {
            this.unlock();
        }
        return d;
    }

    @Override
    public Subscription subscribe(String subject, io.nats.streaming.MessageHandler cb)
            throws IOException, InterruptedException, TimeoutException {
        return subscribe(subject, cb, null);
    }

    @Override
    public Subscription subscribe(String subject, io.nats.streaming.MessageHandler cb,
                                  SubscriptionOptions opts) throws IOException,
            InterruptedException, TimeoutException {
        return subscribe(subject, null, cb, opts);
    }

    @Override
    public Subscription subscribe(String subject, String queue, io.nats.streaming.MessageHandler cb)
            throws IOException, InterruptedException, TimeoutException {
        return subscribe(subject, queue, cb, null);
    }

    @Override
    public Subscription subscribe(String subject, String queue, io.nats.streaming.MessageHandler cb,
                                  SubscriptionOptions opts) throws IOException,
            InterruptedException, TimeoutException {
        SubscriptionImpl sub;
        io.nats.client.Connection nc;

        if (opts == null) {
            opts = new SubscriptionOptions.Builder().build();
        }
        
        this.lock();
        try {
            if (getNatsConnection() == null) {
                throw new IllegalStateException(NatsStreaming.ERR_CONNECTION_CLOSED);
            }
            sub = createSubscription(subject, queue, cb, this, opts);

            // Register subscription.
            subMap.put(sub.inbox, sub);
            nc = getNatsConnection();
        } finally {
            this.unlock();
        }

        // Hold lock throughout.
        sub.wLock();
        try {
            Dispatcher d = this.getDispatcherByName(opts.getDispatcherName());

            // Listen for actual messages
            d.subscribe(sub.inbox);

            // Create a subscription request
            // FIXME(dlc) add others.
            SubscriptionRequest sr = createSubscriptionRequest(sub);

            Message reply = nc.request(subRequests, sr.toByteArray(), opts.getSubscriptionTimeout());
            
            if (reply == null) {
                d.unsubscribe(sub.inbox);
                throw new IOException(ERR_SUB_REQ_TIMEOUT);
            }

            SubscriptionResponse response;
            try {
                response = SubscriptionResponse.parseFrom(reply.getData());
            } catch (InvalidProtocolBufferException e) {
                d.unsubscribe(sub.inbox);
                throw e;
            }
            if (!response.getError().isEmpty()) {
                d.unsubscribe(sub.inbox);
                throw new IOException(response.getError());
            }
            sub.setAckInbox(response.getAckInbox());
        } finally {
            sub.wUnlock();
        }
        return sub;
    }

    SubscriptionRequest createSubscriptionRequest(SubscriptionImpl sub) {
        SubscriptionOptions subOpts = sub.getOptions();
        SubscriptionRequest.Builder srb = SubscriptionRequest.newBuilder();
        String clientId = sub.getConnection().getClientId();
        String queue = sub.getQueue();
        String subject = sub.getSubject();

        srb.setClientID(clientId).setSubject(subject).setQGroup(queue == null ? "" : queue)
                .setInbox(sub.getInbox()).setMaxInFlight(subOpts.getMaxInFlight())
                .setAckWaitInSecs((int) subOpts.getAckWait().getSeconds());

        switch (subOpts.getStartAt()) {
            case First:
                break;
            case LastReceived:
                break;
            case NewOnly:
                break;
            case SequenceStart:
                srb.setStartSequence(subOpts.getStartSequence());
                break;
            case TimeDeltaStart:
                long delta = ChronoUnit.NANOS.between(subOpts.getStartTime(), Instant.now());
                srb.setStartTimeDelta(delta);
                break;
            case UNRECOGNIZED:
            default:
                break;
        }
        srb.setStartPosition(subOpts.getStartAt());

        if (subOpts.getDurableName() != null) {
            srb.setDurableName(subOpts.getDurableName());
        }

        return srb.build();
    }

    // Process an ack from the STAN cluster
    void processAck(Message msg) {
        PubAck pa;
        Exception ex = null;
        try {
            pa = PubAck.parseFrom(msg.getData());
        } catch (InvalidProtocolBufferException e) {
            // If we are speaking to a server we don't understand, let the
            // user know.
            System.err.println("Protocol error: " +  e.getStackTrace());
            return;
        }

        // Remove
        AckClosure ackClosure = removeAck(pa.getGuid());
        if (ackClosure != null) {
            // Capture error if it exists.
            String ackError = pa.getError();

            if (ackClosure.ah != null) {
                if (!ackError.isEmpty()) {
                    ex = new IOException(ackError);
                }
                // Perform the ackHandler callback
                ackClosure.ah.onAck(pa.getGuid(), ackClosure.subject, ackClosure.data, ex);

                // clean up to allow GC of data
                ackClosure.subject = null;
                ackClosure.data = null;
            } else if (ackClosure.ch != null) {
                try {
                    ackClosure.ch.put(ackError);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    TimerTask createAckTimerTask(String guid) {
        return new TimerTask() {
            public void run() {
                try {
                    processAckTimeout(guid);
                } catch (Exception e) {
                    // catch exception to prevent the timer to be closed, but cancel this task
                    cancel();
                }
            }
        };
    }

    void processAckTimeout(String guid) {
        AckClosure ackClosure = removeAck(guid);
        if (ackClosure == null) {
            return;
        }
        if (ackClosure.ah != null) {
            ackClosure.ah.onAck(guid, ackClosure.subject, ackClosure.data, new TimeoutException(NatsStreaming.ERR_TIMEOUT));
        } else if (ackClosure.ch != null) {
            try {
                ackClosure.ch.put(NatsStreaming.ERR_TIMEOUT);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    AckClosure removeAck(String guid) {
        AckClosure ackClosure;
        BlockingQueue<PubAck> pac;
        TimerTask timerTask = null;
        this.lock();
        try {
            ackClosure = pubAckMap.get(guid);
            if (ackClosure != null) {
                timerTask = ackClosure.ackTask;
                pubAckMap.remove(guid);
            }
            pac = pubAckChan;
        } finally {
            this.unlock();
        }

        // Cancel timer if needed
        if (timerTask != null) {
            timerTask.cancel();
        }

        // Remove from channel to unblock async publish
        if (ackClosure != null && pac.size() > 0) {
            try {
                // remove from queue to unblock publish
                pac.take();
            } catch (InterruptedException e) {
                // TODO:  Ignore, but re-evaluate this
            }
        }

        return ackClosure;
    }

    /**
     * Closes a connection and invoke the connection error callback if one
     * was registered when the connection was created.
     */
    void closeDueToPing(String error) {
        boolean isClosed = false;
        ConnectionLostHandler lost = null;
        Exception ex = null;

        lock();
        try {
            isClosed = (getNatsConnection() == null);
            lost = opts.getConnectionLostHandler();
        } finally {
            unlock();
        }

        // Check if connection has been closed.
        if (isClosed) {
            return;
        }

        try {
            close(true);
        } catch (Exception exp) {
            ex = exp;
        }

        if (lost != null) {
            if (ex == null) {
                ex = new Exception(error);
            } else {
                ex = new Exception(error, ex);
            }
            lost.connectionLost(this, ex);
        }
    }

    /** 
     * Sends a PING (containing the connection's ID) to the server at intervals
     * specified by PingInterval option when connection is created.
     * Everytime a PING is sent, the number of outstanding PINGs is increased.
     * If the total number is > than the PingMaxOut option, then the connection
     * is closed, and connection error callback invoked if one was specified.
     */
    void pingServer() {
        this.lock();
        try {
            this.pingsOut++;
            if (this.pingsOut > this.pingMaxOut) {
                this.unlock();
                this.closeDueToPing(NatsStreaming.SERVER_ERR_MAX_PINGS);
                return;
            }
            Connection conn = this.nc;
            this.unlock();

            // Send the PING now. If the NATS connection is reported closed,
            // we are done.
            try {
                conn.publish(this.pingRequests, this.pingInbox, this.pingBytes);
            } catch (Exception exp) {
                if (conn.getStatus() == io.nats.client.Connection.Status.CLOSED) {
                    this.closeDueToPing(exp.getMessage());
                }
            }
        } catch (Exception exp) {
            this.unlock();
            throw exp;
        }
    }

    /**
     * Receives PING responses from the server.
     * If the response contains an error message, the connection is closed
     * and the connection error callback is invoked (if one is specified).
     * If no error, the number of ping out is reset to 0. There is no
     * decrement by one since for a given PING, the client may received
     * many responses when servers are running in channel partitioning mode.
     * Regardless, any positive response from the server ought to signal
     * that the connection is ok.
     */
    void processPing(Message msg) {
        // No data means OK (we don't have to call Unmarshal)
        if (msg.getData() != null && msg.getData().length > 0) {
            try {
                PingResponse pingResp = PingResponse.parseFrom(msg.getData());
                String error = pingResp.getError();
                if (error != null && !error.isEmpty()) {
                    closeDueToPing(error);
                    return;
                }
            } catch (Exception e) {
                return; // exception here stops us from reseting pings out
            }
        }
        // Do not attempt to decrement, simply reset to 0.
        this.lock();
        this.pingsOut = 0;
        this.unlock();
    }

    @Override
    public void onMessage(io.nats.client.Message msg) {
        // For handling inbound NATS messages
        processMsg(msg);
    }

    io.nats.streaming.Message createStanMessage(MsgProto msgp) {
        return new io.nats.streaming.Message(msgp);
    }

    void processMsg(io.nats.client.Message raw) {
        io.nats.streaming.Message stanMsg = null;
        boolean isClosed;
        SubscriptionImpl sub;
        io.nats.client.Connection nc;

        try {
            MsgProto msgp = MsgProto.parseFrom(raw.getData());
            stanMsg = createStanMessage(msgp);
        } catch (InvalidProtocolBufferException e) {
            // TODO:  Ignore, but re-evaluate this
        }

        // Lookup the subscription
        lock();
        try {
            nc = getNatsConnection();
            isClosed = (nc == null);
            sub = (SubscriptionImpl) subMap.get(raw.getSubject());
        } finally {
            unlock();
        }

        // Check if sub is no longer valid or connection has been closed.
        if (sub == null || isClosed) {
            return;
        }

        // Store in msg for backlink
        stanMsg.setSubscription(sub);

        io.nats.streaming.MessageHandler cb;
        String ackSubject;
        boolean isManualAck;
        StreamingConnectionImpl subsc;

        sub.rLock();
        try {
            cb = sub.getMessageHandler();
            ackSubject = sub.getAckInbox();
            isManualAck = sub.getOptions().isManualAcks();
            subsc = sub.getConnection(); // Can be nil if sub has been unsubscribed
        } finally {
            sub.rUnlock();
        }

        // Perform the callback
        if (cb != null && subsc != null) {
            try {
                cb.onMessage(stanMsg);
            } catch (Exception e) {
                ErrorListener handler = nc.getOptions().getErrorListener();
                if (handler != null) {
                    try {
                        handler.exceptionOccurred(this.nc, e);
                    } catch (Exception ex) {
                        // Now we just have to eat it
                    }
                }
            }
        }

        // Process auto-ack
        if (!isManualAck) {
            Ack ack = Ack.newBuilder().setSubject(stanMsg.getSubject())
                    .setSequence(stanMsg.getSequence()).build();
            nc.publish(ackSubject, ack.toByteArray());
        }
    }

    public String getClientId() {
        return this.clientId;
    }

    @Override
    public io.nats.client.Connection getNatsConnection() {
        return this.nc;
    }

    private void setNatsConnection(io.nats.client.Connection nc) {
        this.nc = nc;
    }

    public String newInbox() {
        StringBuilder builder = new StringBuilder();
        builder.append(INBOX_PREFIX);
        builder.append(this.nuid.next());
        return builder.toString();
    }

    void lock() {
        mu.writeLock().lock();
    }

    void unlock() {
        mu.writeLock().unlock();
    }

    private void rLock() {
        mu.readLock().lock();
    }

    private void rUnlock() {
        mu.readLock().unlock();
    }

    class AckClosure {
        TimerTask ackTask;
        AckHandler ah;
        String subject;
        byte[] data;
        BlockingQueue<String> ch;

        AckClosure(final AckHandler ah, final String subject, final byte[] data, final BlockingQueue<String> ch) {
            this.ah = ah;
            this.ch = ch;
            this.subject = subject;
            this.data = data;
        }
    }
}
