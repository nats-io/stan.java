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

import com.google.protobuf.ByteString;
import io.nats.streaming.protobuf.MsgProto;
import org.junit.Test;

public class MessageTests {

    static final String clusterName = "my_test_cluster";
    static final String clientName = "me";

    /**
     * Test method for {@link io.nats.streaming.Message#Message()}.
     */
    @Test
    public void testMessage() {
        new Message();
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

        long timestamp = System.nanoTime();

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

    @Test
    public void testImmutable() {
        final String subject = "foo";
        final String reply = "bar";
        final byte[] data = "Hello World".getBytes();
        final long sequence = 1234567890;
        final boolean redelivered = true;
        final int crc32 = 9898989;

        long timestamp = System.nanoTime();

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
