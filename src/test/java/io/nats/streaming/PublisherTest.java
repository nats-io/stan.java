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

import static io.nats.streaming.UnitTestUtilities.runServer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.nats.streaming.examples.Publisher;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(IntegrationTest.class)
public class PublisherTest {
    private static final String clusterId = UnitTestUtilities.testClusterName;

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

    @Test
    public void testPublisherStringArraySync() throws Exception {
        List<String> argList = new ArrayList<String>();
        argList.addAll(Arrays.asList("-c", clusterId));
        argList.addAll(Arrays.asList("foo", "Hello World!"));

        String[] args = new String[argList.size()];
        args = argList.toArray(args);

        try (NatsStreamingServer srv = runServer(clusterId)) {
            new Publisher(args).run();
        }
    }

    @Test
    public void testPublisherStringArrayAsync() throws Exception {
        String clusterId = UnitTestUtilities.testClusterName;
        List<String> argList = new ArrayList<String>();
        argList.addAll(Arrays.asList("-c", clusterId));
        argList.add("-a");
        argList.addAll(Arrays.asList("foo", "Hello World!"));

        String[] args = new String[argList.size()];
        args = argList.toArray(args);

        try (NatsStreamingServer srv = runServer(clusterId)) {
            new Publisher(args).run();
        }
    }


    @Test
    public void testParseArgsBadFlags() {
        List<String> argList = new ArrayList<String>();
        String[] flags = new String[] { "-s", "-c", "-id" };
        boolean exThrown = false;

        for (String flag : flags) {
            try {
                exThrown = false;
                argList.clear();
                argList.addAll(Arrays.asList(flag, "foo", "Hello World!"));
                String[] args = new String[argList.size()];
                args = argList.toArray(args);
                new Publisher(args);
            } catch (IllegalArgumentException e) {
                assertEquals(String.format("%s requires an argument", flag), e.getMessage());
                exThrown = true;
            } finally {
                assertTrue("Should have thrown exception", exThrown);
            }
        }
    }

    @Test
    public void testParseArgsNotEnoughArgs() {
        List<String> argList = new ArrayList<String>();
        boolean exThrown = false;

        try {
            exThrown = false;
            argList.clear();
            argList.addAll(Arrays.asList("foo"));
            String[] args = new String[argList.size()];
            args = argList.toArray(args);
            new Publisher(args);
        } catch (IllegalArgumentException e) {
            assertEquals("must supply at least subject and msg", e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Should have thrown exception", exThrown);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMainFails() throws Exception {
        try (NatsStreamingServer srv = runServer(clusterId)) {
            Publisher.main(new String[] { "foobar" });
        }
    }

    @Test
    public void testMainSuccess() throws Exception {
        try (NatsStreamingServer srv = runServer(clusterId)) {
            Publisher.main(new String[] { "-c", clusterId, "foo", "bar" });
        }
    }

}
