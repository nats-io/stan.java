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

import static io.nats.streaming.NatsStreaming.ERR_CLOSE_REQ_TIMEOUT;
import static io.nats.streaming.NatsStreaming.ERR_NO_SERVER_SUPPORT;
import static io.nats.streaming.NatsStreaming.ERR_UNSUB_REQ_TIMEOUT;
import static io.nats.streaming.NatsStreaming.PFX;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.streaming.protobuf.SubscriptionResponse;
import io.nats.streaming.protobuf.UnsubscribeRequest;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class SubscriptionImpl implements Subscription {

    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    StreamingConnectionImpl sc;
    private String subject;
    private String qgroup;
    String inbox;
    String ackInbox;
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
        close(false);
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

            Dispatcher d = sc.getDispatcherByName(this.getOptions().getDispatcherName());
            d.unsubscribe(this.inbox);

            this.sc = null;

        } finally {
            wUnlock();
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
            Future<io.nats.client.Message> incoming = nc.request(reqSubject, bytes);
            reply = incoming.get(sc.opts.connectTimeout.toMillis(), TimeUnit.MILLISECONDS);
            if (reply == null) {
                if (unsubscribe) {
                    throw new IOException(ERR_UNSUB_REQ_TIMEOUT);
                }
                throw new IOException(ERR_CLOSE_REQ_TIMEOUT);
            }
        } catch (TimeoutException|ExecutionException|InterruptedException e) {
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
