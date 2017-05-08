/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.nats.streaming.SubscriptionOptions.Builder;
import io.nats.streaming.protobuf.StartPosition;
import io.nats.streaming.protobuf.SubscriptionOptionsMsg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
        testOpts = new SubscriptionOptions.Builder().ackWait(Duration.ofMillis(500))
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
    public void testSubscriptionOptionsMsg() throws ClassNotFoundException, IOException {
    	final Builder builder = 
    		new SubscriptionOptions.Builder()
    			.ackWait(Duration.ofMillis(500))
                .durableName("foo")
                .manualAcks()
                .maxInFlight(10000)
                .startAtSequence(12345)
                .startAtTime(Instant.now());
    	
    	final SubscriptionOptions orgOptions = builder.build();    	
    	final SubscriptionOptionsMsg optionsMsg = builder.buildMsg();
    	SubscriptionOptionsMsg deserializedOptionsMsg = (SubscriptionOptionsMsg) serializeDeserialize(optionsMsg);
    	final SubscriptionOptions newOptions = SubscriptionOptions.Builder.build(deserializedOptionsMsg);
    	
    	assertTrue(equals(orgOptions, newOptions));
    }

	protected Object serializeDeserialize(Object object)
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
   
	protected boolean equals(SubscriptionOptions obj1, SubscriptionOptions obj2) {
		if (obj1 == obj2)
			return true;
		if (obj2 == null)
			return false;
		if (obj1.getClass() != obj2.getClass())
			return false;
		SubscriptionOptions other = (SubscriptionOptions) obj2;
		if (obj1.getAckWait() == null) {
			if (other.getAckWait() != null)
				return false;
		} else if (!obj1.getAckWait().equals(other.getAckWait()))
			return false;
		if (obj1.getDurableName() == null) {
			if (other.getDurableName() != null)
				return false;
		} else if (!obj1.getDurableName().equals(other.getDurableName()))
			return false;
		if (obj1.isManualAcks() != other.isManualAcks())
			return false;
		if (obj1.getMaxInFlight() != other.getMaxInFlight())
			return false;
		if (obj1.startAt != other.startAt)
			return false;
		if (obj1.startSequence != other.startSequence)
			return false;
		if (obj1.getStartTime() == null) {
			if (other.getStartTime() != null)
				return false;
		} else if (!obj1.getStartTime().equals(other.getStartTime()))
			return false;
		return true;
	}
    
}
