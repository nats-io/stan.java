/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import static io.nats.streaming.NatsStreaming.ERR_CLOSE_REQ_TIMEOUT;
import static io.nats.streaming.NatsStreaming.ERR_NO_SERVER_SUPPORT;
import static io.nats.streaming.NatsStreaming.ERR_UNSUB_REQ_TIMEOUT;
import static io.nats.streaming.NatsStreaming.PFX;

import io.nats.client.Connection;
import io.nats.streaming.protobuf.SubscriptionResponse;
import io.nats.streaming.protobuf.UnsubscribeRequest;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class SubscriptionImpl implements Subscription {

    static final long DEFAULT_ACK_WAIT = 30 * 1000;
    static final int DEFAULT_MAX_IN_FLIGHT = 1024;

    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    StreamingConnectionImpl sc;
    private String subject;
    private String qgroup;
    String inbox;
    String ackInbox;
    io.nats.client.Subscription inboxSub;
    SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
    MessageHandler cb;

    SubscriptionImpl() {
    }

    SubscriptionImpl(String subject, String qgroup, MessageHandler cb,
                     StreamingConnectionImpl sc,
                     SubscriptionOptions opts) {
        this.subject = subject;
        this.qgroup = qgroup;
        this.cb = cb;
        this.sc = sc;
        if (opts != null) {
            this.opts = opts;
        }
        this.inbox = sc.newInbox();
    }

    void rLock() {
        rwlock.readLock().lock();
    }

    void rUnlock() {
        rwlock.readLock().unlock();
    }

    void wLock() {
        rwlock.writeLock().lock();
    }

    void wUnlock() {
        rwlock.writeLock().unlock();
    }

    String getAckInbox() {
        return this.ackInbox;
    }

    StreamingConnectionImpl getConnection() {
        return this.sc;
    }

    String getInbox() {
        return this.inbox;
    }

    MessageHandler getMessageHandler() {
        return this.cb;
    }

    @Override
    public String getQueue() {
        return this.qgroup;
    }

    @Override
    public String getSubject() {
        return this.subject;
    }

    @Override
    public SubscriptionOptions getOptions() {
        return this.opts;
    }

    @Override
    public void close() throws IOException {
        if (this.sc == null) {
            // already closed
            return;
        }
        close(true);
    }

    @Override
    public void close(boolean unsubscribe) throws IOException {
        StreamingConnectionImpl sc;
        String reqSubject;
        Connection nc;
        wLock();
        try {
            sc = this.sc;
            if (sc == null) {
                throw new IllegalStateException(NatsStreaming.ERR_BAD_SUBSCRIPTION);
            }
            this.sc = null;
            if (inboxSub != null) {
                try {
                    inboxSub.unsubscribe();
                } catch (Exception e) {
                    // Silently ignore this, we can't do anything about it
                }
                inboxSub = null;
            }
        } finally {
            wUnlock();
        }

        if (sc == null) {
            throw new IllegalStateException(NatsStreaming.ERR_BAD_SUBSCRIPTION);
        }

        sc.lock();
        try {
            if (sc.nc == null) {
                throw new IllegalStateException(NatsStreaming.ERR_CONNECTION_CLOSED);
            }

            sc.subMap.remove(this.inbox);
            reqSubject = sc.unsubRequests;
            if (!unsubscribe) {
                reqSubject = sc.subCloseRequests;
                if (reqSubject.isEmpty()) {
                    throw new IllegalStateException(ERR_NO_SERVER_SUPPORT);
                }
            }
            // Snapshot connection to avoid data race, since the connection may be
            // closing while we try to send the request
            nc = sc.getNatsConnection();
        } finally {
            sc.unlock();
        }

        byte[] bytes;

        UnsubscribeRequest usr = UnsubscribeRequest.newBuilder()
                .setClientID(sc.getClientId()).setSubject(subject).setInbox(ackInbox).build();
        bytes = usr.toByteArray();

        io.nats.client.Message reply;

        try {
            reply = nc.request(reqSubject, bytes, sc.opts.connectTimeout.toMillis());
            if (reply == null) {
                if (unsubscribe) {
                    throw new IOException(ERR_UNSUB_REQ_TIMEOUT);
                }
                throw new IOException(ERR_CLOSE_REQ_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }

        SubscriptionResponse response = SubscriptionResponse.parseFrom(reply.getData());
        if (!response.getError().isEmpty()) {
            throw new IOException(PFX + response.getError());
        }
    }

    @Override
    public void unsubscribe() throws IOException {
        close(true);
    }

    void setAckInbox(String ackInbox) {
        this.ackInbox = ackInbox;
    }
}
