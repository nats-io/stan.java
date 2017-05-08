/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import static io.nats.streaming.UnitTestUtilities.runServer;
import static io.nats.streaming.UnitTestUtilities.testClusterName;
import static org.junit.Assert.fail;

import ch.qos.logback.classic.Logger;
import io.nats.client.NUID;
import io.nats.streaming.examples.StanBench;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.LoggerFactory;

public class StanBenchTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = (Logger) LoggerFactory.getLogger(StanBenchTest.class);

    static final LogVerifier verifier = new LogVerifier();

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
    public void testStanBenchStringArray() {
        try (NatsStreamingServer srv = runServer(testClusterName)) {
            final String urls = "nats://localhost:4222";
            final String clientId = NUID.nextGlobal();
            final String clusterId = "my_test_cluster";
            final int count = 1000;
            final int numPubs = 1;
            final int numSubs = 1;
            final int msgSize = 256;
            final boolean secure = false;
            final boolean ignoreOld = true;
            final boolean async = true;
            final String subject = "foo";

            List<String> argList = new ArrayList<String>();
            argList.addAll(Arrays.asList("-s", urls));
            argList.addAll(Arrays.asList("-c", clusterId));
            argList.addAll(Arrays.asList("-id", clientId));
            argList.addAll(Arrays.asList("-np", Integer.toString(numPubs)));
            argList.addAll(Arrays.asList("-ns", Integer.toString(numSubs)));
            argList.addAll(Arrays.asList("-n", Integer.toString(count)));
            argList.addAll(Arrays.asList("-ms", Integer.toString(msgSize)));

            if (secure) {
                argList.add("-tls");
            }

            if (ignoreOld) {
                argList.add("-io");
            }

            if (async) {
                argList.add("-a");
            }

            argList.add(subject);

            String[] args = new String[argList.size()];
            args = argList.toArray(args);

            final StanBench bench = new StanBench(args);
            try {
                bench.run();
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testStanBenchProperties() {
        try (NatsStreamingServer srv = runServer(testClusterName)) {
            Properties props = new Properties();
            String client = NUID.nextGlobal();
            props.setProperty("bench.stan.servers", "nats://localhost:4222");
            props.setProperty("bench.stan.cluster.id", "my_test_cluster");
            props.setProperty("bench.streaming.client.id", client);
            props.setProperty("bench.stan.secure", "false");
            props.setProperty("bench.stan.msg.count", "1000");
            props.setProperty("bench.stan.msg.size", "0");
            props.setProperty("bench.stan.secure", "false");
            props.setProperty("bench.stan.pubs", "1");
            props.setProperty("bench.stan.subs", "0");
            props.setProperty("bench.stan.subject", "foo");
            props.setProperty("bench.stan.pub.maxpubacks", "1000");
            props.setProperty("bench.stan.sub.ignoreold", Boolean.toString(true));
            props.setProperty("bench.streaming.async", Boolean.toString(true));

            final StanBench bench = new StanBench(props);
            try {
                bench.run();
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }
    }

    // @Test
    // public void testRun() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testInstallShutdownHook() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testUsage() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testMain() {
    // fail("Not yet implemented"); // TODO
    // }
}
