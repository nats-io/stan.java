// Copyright 2015-2018 The NATS Authors and Logimethods
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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class SubscriptionOptionsBuilderTest {

    private static SubscriptionOptions.Builder testOptsBuilder;

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    /**
     * Setup for all cases in this test.
     * 
     * @throws Exception if something goes wrong
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        testOptsBuilder = new SubscriptionOptions.Builder().ackWait(Duration.ofMillis(500))
                .durableName("foo").manualAcks().maxInFlight(10000)
                .startAtSequence(12345);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    /**
     * Test method for {@link java.io.Serializable}.
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    @Test
    public void testSerializable() throws ClassNotFoundException, IOException {
        final SubscriptionOptions.Builder serializedTestOpts = (SubscriptionOptions.Builder) UnitTestUtilities.serializeDeserialize(testOptsBuilder);
        
        assertTrue(equals(testOptsBuilder, serializedTestOpts));
    }

	protected static boolean equals(SubscriptionOptions.Builder obj1, SubscriptionOptions.Builder obj2) {
		if (obj1 == obj2)
			return true;
		if (obj1.ackWait == null) {
			if (obj2.ackWait != null)
				return false;
		} else if (!obj1.ackWait.equals(obj2.ackWait))
			return false;
		if (obj1.durableName == null) {
			if (obj2.durableName != null)
				return false;
		} else if (!obj1.durableName.equals(obj2.durableName))
			return false;
		if (obj1.manualAcks != obj2.manualAcks)
			return false;
		if (obj1.maxInFlight != obj2.maxInFlight)
			return false;
		if (obj1.startAt != obj2.startAt)
			return false;
		if (obj1.startSequence != obj2.startSequence)
			return false;
		if (obj1.startTime == null) {
			if (obj2.startTime != null)
				return false;
		} else if (!obj1.startTime.equals(obj2.startTime))
			return false;
		if (obj1.startTimeAsDate == null) {
			if (obj2.startTimeAsDate != null)
				return false;
		} else if (!obj1.startTimeAsDate.equals(obj2.startTimeAsDate))
			return false;
		return true;
	}
    
}
