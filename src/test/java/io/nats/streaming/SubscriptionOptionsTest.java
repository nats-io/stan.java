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
import static org.junit.Assert.assertTrue;

import io.nats.streaming.protobuf.StartPosition;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;

public class SubscriptionOptionsTest {

    private static SubscriptionOptions testOpts;

    /**
     * Setup for all cases in this test.
     * 
     * @throws Exception if something goes wrong
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        testOpts = buildSubscriptionOptions();
    }

    private static SubscriptionOptions buildSubscriptionOptions() {
        return new SubscriptionOptions.Builder().ackWait(Duration.ofMillis(500))
                .durableName("foo").manualAcks().maxInFlight(10000)
                .startAtSequence(12345).build();
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionOptions#getDurableName()}.
     */
    @Test
    public void testGetDurableName() {
        assertNotNull(testOpts.getDurableName());
        assertEquals("foo", testOpts.getDurableName());
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionOptions#getMaxInFlight()}.
     */
    @Test
    public void testGetMaxInFlight() {
        assertEquals(10000, testOpts.getMaxInFlight());
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionOptions#getAckWait()}.
     */
    @Test
    public void testGetAckWait() {
        assertEquals(500, testOpts.getAckWait().toMillis());

        SubscriptionOptions opts =
                new SubscriptionOptions.Builder().ackWait(1, TimeUnit.SECONDS).build();
        assertEquals(1000, opts.getAckWait().toMillis());
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionOptions#getStartAt()}.
     */
    @Test
    public void testGetStartAt() {
        assertEquals(StartPosition.SequenceStart, testOpts.getStartAt());

        SubscriptionOptions opts =
                new SubscriptionOptions.Builder().startWithLastReceived().build();
        assertEquals(StartPosition.LastReceived, opts.getStartAt());

        opts = new SubscriptionOptions.Builder().deliverAllAvailable().build();
        assertEquals(StartPosition.First, opts.getStartAt());
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionOptions#getStartSequence()}.
     */
    @Test
    public void testGetStartSequence() {
        assertEquals(12345, testOpts.getStartSequence());
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionOptions#getStartTime()}.
     */
    @Test
    public void testGetStartTime() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 2016);
        cal.set(Calendar.MONTH, Calendar.MARCH);
        cal.set(Calendar.DAY_OF_MONTH, 13);
        cal.set(Calendar.HOUR_OF_DAY, 12);
        cal.set(Calendar.MINUTE, 12);
        cal.set(Calendar.SECOND, 12);
        cal.set(Calendar.MILLISECOND, 121);

        Instant startTime = cal.getTime().toInstant();
        SubscriptionOptions opts = new SubscriptionOptions.Builder().startAtTime(startTime).build();


        assertEquals(cal.getTimeInMillis(), opts.getStartTime().toEpochMilli());
    }

    /**
     * Test method for
     * {@link io.nats.streaming.SubscriptionOptions#getStartTime(java.util.concurrent.TimeUnit)}.
     */
    @Test
    public void testGetStartTimeTimeUnit() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 2016);
        cal.set(Calendar.MONTH, Calendar.MARCH);
        cal.set(Calendar.DAY_OF_MONTH, 13);

        Instant startTime = cal.getTime().toInstant();
        SubscriptionOptions opts = new SubscriptionOptions.Builder().startAtTime(startTime).build();

        assertEquals(cal.getTimeInMillis(), opts.getStartTime(TimeUnit.MILLISECONDS));
    }

    /**
     * Test method for {@link io.nats.streaming.SubscriptionOptions#isManualAcks()}.
     */
    @Test
    public void testIsManualAcks() {
        assertTrue(testOpts.isManualAcks());
    }

    @Test
    public void testStartAtTimeDelta() {
        long delta = 50000;
        TimeUnit unit = TimeUnit.MILLISECONDS;

        // Add some leeway to reduce flakiness
        Instant before = Instant.now().minusMillis(delta+100);
        SubscriptionOptions opts =
                new SubscriptionOptions.Builder().startAtTimeDelta(delta, unit).build();
        Instant after = Instant.now().minusMillis(delta-100);
        assertEquals(StartPosition.TimeDeltaStart, opts.getStartAt());
        assertTrue(opts.getStartTime().isAfter(before) && opts.getStartTime().isBefore(after));
    }

    @Test
    public void testHashcodeEqualsAndToString(){
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        SubscriptionOptions copy = new SubscriptionOptions.Builder().build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());

        opts = new SubscriptionOptions.Builder().durableName("hello").build();
        copy = new SubscriptionOptions.Builder().durableName("hello").build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());

        opts = new SubscriptionOptions.Builder().subscriptionTimeout(Duration.ofMillis(500)).build();
        copy = new SubscriptionOptions.Builder().subscriptionTimeout(Duration.ofMillis(500)).build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());

        opts = new SubscriptionOptions.Builder().ackWait(Duration.ofMillis(500)).build();
        copy = new SubscriptionOptions.Builder().ackWait(Duration.ofMillis(500)).build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());
        
        opts = new SubscriptionOptions.Builder().durableName("foo").build();
        copy = new SubscriptionOptions.Builder().durableName("foo").build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());
        
        opts = new SubscriptionOptions.Builder().manualAcks().build();
        copy = new SubscriptionOptions.Builder().manualAcks().build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());
        
        opts = new SubscriptionOptions.Builder().maxInFlight(10000).build();
        copy = new SubscriptionOptions.Builder().maxInFlight(10000).build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());
        
        opts = new SubscriptionOptions.Builder().startAtSequence(12345).build();
        copy = new SubscriptionOptions.Builder().startAtSequence(12345).build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());
        
        opts = new SubscriptionOptions.Builder().startAtTime(Instant.now()).build();
        copy = new SubscriptionOptions.Builder().startAtTime(Instant.now()).build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());
        
        opts = new SubscriptionOptions.Builder().startAtTimeDelta(Duration.ofMinutes(1)).build();
        copy = new SubscriptionOptions.Builder().startAtTimeDelta(Duration.ofMinutes(1)).build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());
        
        opts = new SubscriptionOptions.Builder().dispatcher("one").build();
        copy = new SubscriptionOptions.Builder().dispatcher("one").build();
        assertEquals(opts.hashCode(), copy.hashCode());
        assertEquals(opts, copy);
        assertNotNull(opts.toString());
    }
}
