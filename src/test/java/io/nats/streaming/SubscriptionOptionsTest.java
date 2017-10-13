/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.nats.streaming.protobuf.StartPosition;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class SubscriptionOptionsTest {

    private static SubscriptionOptions testOpts;

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

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

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

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
        SubscriptionOptions opts =
                new SubscriptionOptions.Builder().startAtTimeDelta(delta, unit).build();
        Instant expected = Instant.now().minusMillis(delta);
        assertEquals(StartPosition.TimeDeltaStart, opts.getStartAt());
        String.format("Start time: expected %s but was %s", opts.getStartTime(), expected);
        assertTrue(opts.getStartTime().equals(expected));
    }

    @Test
    public void testEquals(){
        assertEquals(buildSubscriptionOptions(), buildSubscriptionOptions());
    }

    @Test
    public void testHashcode(){
        assertEquals(buildSubscriptionOptions().hashCode(), buildSubscriptionOptions().hashCode());
    }

}
