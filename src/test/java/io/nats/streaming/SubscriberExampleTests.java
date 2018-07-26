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
import static org.junit.Assert.assertTrue;

import io.nats.streaming.examples.Publisher;
import io.nats.streaming.examples.Subscriber;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class SubscriberExampleTests {
    private static final String clusterName = "test-cluster";

    @Test
    public void testSubscriberStringArray() throws Exception {
        List<String> argList = new ArrayList<String>();
        argList.addAll(Arrays.asList("-c", clusterName));
        argList.add("foo");

        String[] args = new String[argList.size()];
        args = argList.toArray(args);

        new Subscriber(args);
    }

    @Test
    public void testParseArgsBadFlags() {
        List<String> argList = new ArrayList<String>();
        String[] flags = new String[] { "-s", "-c", "-id", "-q", "--seq", "--since", "--durable" };
        boolean exThrown = false;

        for (String flag : flags) {
            try {
                exThrown = false;
                argList.clear();
                argList.addAll(Arrays.asList(flag, "foo"));
                String[] args = new String[argList.size()];
                args = argList.toArray(args);
                new Subscriber(args);
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
            String[] args = new String[argList.size()];
            args = argList.toArray(args);
            new Subscriber(args);
        } catch (IllegalArgumentException e) {
            assertEquals("must supply at least a subject name", e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Should have thrown exception", exThrown);
        }
    }

    @Test(timeout = 5000)
    public void testMainSuccess() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Publisher.main(new String[] { "-s", srv.getURI(), "-c", clusterName, "foo", "bar" });
            Subscriber.main(new String[] {"-s", srv.getURI(), "-c", clusterName, "--all", "--count", "1", "foo" });
        }
    }

    @Test(expected=IOException.class)
    public void testMainFailsNoServers() throws Exception {
        Subscriber.main(new String[] { "-s", "nats://enterprise:4242", "foobar" });
    }

    @Test(expected=IOException.class)
    public void testMainFailsTimeout() throws Exception {
        try (NatsStreamingTestServer srv = new NatsStreamingTestServer(clusterName, false)) {
            Subscriber.main(new String[] {"-s", srv.getURI(),"-c", "nonexistent-cluster", "foobar"});
        }
    }
}
