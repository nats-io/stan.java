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

/**
 * 
 */

package io.nats.streaming;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.nats.streaming.protobuf.MsgProto;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(UnitTest.class)
public class MessageTest {

    static final String clusterName = "my_test_cluster";
    static final String clientName = "me";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    /**
     * Test method for {@link io.nats.streaming.Message#Message()}.
     */
    @Test
    public void testMessage() {
        new Message();
    }

    private long getTimeNanos() {
        Instant inst = Instant.now();
        long timestamp = inst.getEpochSecond();
        timestamp *= 1000000000L; // convert to nanoseconds
        // the nanoseconds returned by inst.getNano() are the
        // nanoseconds past the second so they need to be added to the
        // epoch second
        timestamp += inst.getNano();
        return timestamp;
    }

    /**
     * Test method for {@link io.nats.streaming.Message#Message(io.nats.streaming.protobuf.MsgProto)}.
     */
    @Test
    public void testMessageMsgProto() {
        final String subject = "foo";
        final String reply = "bar";
        final byte[] data = "Hello World".getBytes();
        final long sequence = 1234567890;
        final boolean redelivered = true;
        final int crc32 = 9898989;

        long timestamp = getTimeNanos();

        MsgProto msgp = MsgProto.newBuilder().setSubject(subject).setReply(reply)
                .setData(ByteString.copyFrom(data)).setTimestamp(timestamp).setSequence(sequence)
                .setRedelivered(redelivered).setCRC32(crc32).build();
        Message msg = new Message(msgp);
        assertEquals(subject, msg.getSubject());
        assertEquals(reply, msg.getReplyTo());
        assertArrayEquals(data, msg.getData());
        assertEquals(sequence, msg.getSequence());
        assertEquals(redelivered, msg.isRedelivered());
        assertEquals(crc32, msg.getCrc32());
        assertNotNull(msg.getInstant());
    }

    @Test
    public void testMessageSetters() {
        final String subject = "foo";
        final String reply = "bar";
        final byte[] data = "Hello World".getBytes();

        Message msg = new Message();
        msg.setSubject(subject);
        msg.setReplyTo(reply);
        msg.setData(data);

        assertEquals(subject, msg.getSubject());
        assertEquals(reply, msg.getReplyTo());
        assertArrayEquals(data, msg.getData());
    }

    // /**
    // * Test method for {@link io.nats.streaming.Message#getTime()}.
    // */
    // @Test
    // public void testGetInstant() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.Message#setSubscription(io.nats.streaming.Subscription)}.
    // */
    // @Test
    // public void testSetSubscription() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.Message#getSubscription()}.
    // */
    // @Test
    // public void testGetSubscription() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.Message#getSequence()}.
    // */
    // @Test
    // public void testGetSequence() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.Message#getSubject()}.
    // */
    // @Test
    // public void testGetSubject() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.Message#getReplyTo()}.
    // */
    // @Test
    // public void testGetReplyTo() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.Message#getData()}.
    // */
    // @Test
    // public void testGetData() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.Message#getTimestamp()}.
    // */
    // @Test
    // public void testGetTimestamp() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.Message#isRedelivered()}.
    // */
    // @Test
    // public void testIsRedelivered() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // /**
    // * Test method for {@link io.nats.streaming.Message#getCRC32()}.
    // */
    // @Test
    // public void testGetCRC32() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    /**
     * Test method for {@link io.nats.streaming.Message#ack()}.
     */
    @Test
    public void testAckBadSubscription() {
        SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
        SubscriptionOptions mockSubOpts = mock(SubscriptionOptions.class);
        when(mockSubOpts.isManualAcks()).thenReturn(true);
        when(mockSub.getOptions()).thenReturn(mockSubOpts);

        Message msg = new Message();
        msg.setSubject("foo");
        msg.setReplyTo("bar");
        msg.setData(null);
        msg.setSubscription(mockSub);
        boolean exThrown = false;
        try {
            msg.ack();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals(NatsStreaming.ERR_BAD_SUBSCRIPTION, e.getMessage());
            exThrown = true;
        }
        assertTrue("Should have thrown exception", exThrown);
    }

    @Test
    public void testAckSuccess() throws IOException, TimeoutException {
        SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
        StreamingConnectionImpl subConn = mock(StreamingConnectionImpl.class);
        when(mockSub.getConnection()).thenReturn(subConn);
        when(subConn.getNatsConnection()).thenReturn(mock(io.nats.client.Connection.class));
        SubscriptionOptions subOpts = new SubscriptionOptions.Builder().manualAcks().build();
        when(mockSub.getOptions()).thenReturn(subOpts);
        Message msg = new Message();
        msg.setSubject("foo");
        msg.setReplyTo("bar");
        msg.setData(null);
        msg.setSubscription(mockSub);
        msg.ack();
    }

    @Test
    public void testAckNullPointerEx() throws IOException, TimeoutException {
        thrown.expect(NullPointerException.class);
        SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
        StreamingConnectionImpl subConn = mock(StreamingConnectionImpl.class);
        when(mockSub.getConnection()).thenReturn(subConn);
        when(subConn.getNatsConnection()).thenReturn(mock(io.nats.client.Connection.class));
        when(mockSub.getOptions()).thenReturn(null);
        Message msg = new Message();
        msg.setSubject("foo");
        msg.setReplyTo("bar");
        msg.setData(null);
        msg.setSubscription(mockSub);
        msg.ack();
    }

    /**
     * Test method for {@link io.nats.streaming.Message#ack()}.
     * 
     * @throws TimeoutException if timeout occurs
     * @throws IOException if I/O exception occurs
     */
    @Test
    public void testAckWhenManualAcksIsFalse() throws IOException, TimeoutException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(StreamingConnectionImpl.ERR_MANUAL_ACK);

        SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
        StreamingConnectionImpl subConn = mock(StreamingConnectionImpl.class);
        when(mockSub.getConnection()).thenReturn(subConn);
        io.nats.client.Connection mockNatsConn = mock(io.nats.client.Connection.class);
        when(subConn.getNatsConnection()).thenReturn(mockNatsConn);
        SubscriptionOptions mockSubOpts = mock(SubscriptionOptions.class);
        when(mockSub.getOptions()).thenReturn(mockSubOpts);

        Message msg = new Message();
        msg.setSubject("foo");
        msg.setReplyTo("bar");
        msg.setData(null);
        msg.setSubscription(mockSub);
        msg.ack();
    }

    @Test
    public void testImmutable() {
        final String subject = "foo";
        final String reply = "bar";
        final byte[] data = "Hello World".getBytes();
        final long sequence = 1234567890;
        final boolean redelivered = true;
        final int crc32 = 9898989;

        long timestamp = getTimeNanos();

        MsgProto msgp = MsgProto.newBuilder().setSubject(subject).setReply(reply)
                .setData(ByteString.copyFrom(data)).setTimestamp(timestamp).setSequence(sequence)
                .setRedelivered(redelivered).setCRC32(crc32).build();
        Message msg = new Message(msgp);

        boolean exThrown = false;
        try {
            msg.setSubject(subject);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals(e.getMessage(), Message.ERR_MSG_IMMUTABLE);
            exThrown = true;
        }
        assertTrue("Should have thrown exception", exThrown);

        exThrown = false;
        try {
            msg.setReplyTo(reply);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals(e.getMessage(), Message.ERR_MSG_IMMUTABLE);
            exThrown = true;
        }
        assertTrue("Should have thrown exception", exThrown);

        exThrown = false;
        try {
            msg.setData("test".getBytes());
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals(e.getMessage(), Message.ERR_MSG_IMMUTABLE);
            exThrown = true;
        }
        assertTrue("Should have thrown exception", exThrown);

        exThrown = false;
        try {
            byte[] payload = "test".getBytes();
            msg.setData(payload, 0, payload.length);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals(e.getMessage(), Message.ERR_MSG_IMMUTABLE);
            exThrown = true;
        }
        assertTrue("Should have thrown exception", exThrown);

    }

    /**
     * Test method for {@link io.nats.streaming.Message#toString()}.
     */
    @Test
    public void testToString() {
        final String subject = "foo";
        final String reply = "bar";
        final byte[] data = "Hello World 1234567890 1234567890 1234567890".getBytes();

        MsgProto msgp = MsgProto.newBuilder().setSubject(subject).setReply(reply)
                .setData(ByteString.copyFrom(data)).build();
        Message msg = new Message(msgp);
        assertNotNull(msg.toString());

        msg = new Message();
        msg.setSubject(subject);
        try {
            assertNotNull(msg.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
