/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming.examples;

import io.nats.benchmark.Benchmark;
import io.nats.benchmark.Sample;
import io.nats.client.AsyncSubscription;
import io.nats.client.ClosedCallback;
import io.nats.client.ConnectionEvent;
import io.nats.client.DisconnectedCallback;
import io.nats.client.ExceptionHandler;
import io.nats.client.NATSException;
import io.nats.client.NUID;
import io.nats.client.Nats;
import io.nats.streaming.AckHandler;
import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A utility class for measuring NATS performance.
 *
 */
public class StanBench {

    // Default test values
    private int numMsgs = 100000;
    private int numPubs = 1;
    private int numSubs = 0;
    private boolean async = false;
    private int size = 128;
    private boolean ignoreOld = false;
    private int maxPubAcksInFlight = 1000;
    private String clientId = "benchmark";
    private String clusterId = "test-cluster";

    private String urls = Nats.DEFAULT_URL;
    private String subject;
    private final AtomicInteger published = new AtomicInteger();
    private final AtomicInteger received = new AtomicInteger();
    private String csvFileName;

    private io.nats.client.Options natsOptions;

    private Thread shutdownHook;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private boolean secure;
    private Benchmark bench;

    private static final String usageString =
            "\nUsage: nats-bench [-s server] [--tls] [-c clusterid] [-id clientid] [-np #pubs] "
                    + "[-ns #subs] [-n #msg] [-mpa #pubacks] [-ms size] "
                    + "[-io] [-a] [-csv file] <subject>\n\nOptions:\n"
                    + "    -s   <urls>                     NATS Streaming server URLs (separated by"
                    + " comma)\n"
                    + "    -cid                            NATS Streaming cluster ID\n"
                    + "    -id                             Benchmark process base client ID\n"
                    + "    -tls                            Use TLS secure connection\n"
                    + "    -np                             Number of concurrent publishers\n"
                    + "    -ns                             Number of concurrent subscribers\n"
                    + "    -n                              Number of messages to publish\n"
                    + "    -a                              Async message publishing\n"
                    + "    -ms                             Message size in bytes\n"
                    + "    -io                             Subscribers ignore old messages\n"
                    + "    -ms                             Message size in bytes\n"
                    + "    -mpa                            Max number of published acks in flight\n"
                    + "    -csv                            Save bench data to csv file\n";

    /**
     * Main constructor for StanBench.
     * 
     * @param args configuration parameters
     */
    public StanBench(String[] args) {
        if (args == null || args.length < 1) {
            usage();
            return;
        }
        parseArgs(args);
    }

    /**
     * Properties-based constructor for StanBench.
     * 
     * @param properties configuration properties
     */
    public StanBench(Properties properties) {
        urls = properties.getProperty("bench.stan.servers", urls);
        clientId = properties.getProperty("bench.streaming.client.id", clientId);
        clusterId = properties.getProperty("bench.stan.cluster.id", clusterId);
        secure = Boolean.parseBoolean(
                properties.getProperty("bench.stan.secure", Boolean.toString(secure)));
        numMsgs = Integer.parseInt(
                properties.getProperty("bench.stan.msg.count", Integer.toString(numMsgs)));
        maxPubAcksInFlight = Integer.parseInt(properties.getProperty("bench.stan.pub.maxpubacks",
                Integer.toString(maxPubAcksInFlight)));
        size = Integer
                .parseInt(properties.getProperty("bench.stan.msg.size", Integer.toString(numSubs)));
        numPubs = Integer
                .parseInt(properties.getProperty("bench.stan.pubs", Integer.toString(numPubs)));
        numSubs = Integer
                .parseInt(properties.getProperty("bench.stan.subs", Integer.toString(numSubs)));
        csvFileName = properties.getProperty("bench.stan.csv.filename", null);
        subject = properties.getProperty("bench.stan.subject", NUID.nextGlobal());
        async = Boolean.parseBoolean(
                properties.getProperty("bench.stan.pub.async", Boolean.toString(async)));
        ignoreOld = Boolean.parseBoolean(
                properties.getProperty("bench.stan.sub.ignoreold", Boolean.toString(ignoreOld)));
    }

    class Worker implements Runnable {
        final Phaser phaser;
        final int numMsgs;
        final int size;
        final String workerClientId;

        Worker(Phaser phaser, int numMsgs, int size, String workerClientId) {
            Thread.currentThread().setName(workerClientId);
            this.phaser = phaser;
            this.numMsgs = numMsgs;
            this.size = size;
            this.workerClientId = workerClientId;
        }

        @Override
        public void run() {}
    }

    class SubWorker extends Worker {
        private final boolean ignoreOld;

        SubWorker(Phaser phaser, int numMsgs, int size, boolean ignoreOld, String subId) {
            super(phaser, numMsgs, size, subId);
            this.ignoreOld = ignoreOld;
        }

        @Override
        public void run() {
            try {
                runSubscriber();
            } catch (Exception e) {
                e.printStackTrace();
                phaser.arrive();
            }
        }

        public void runSubscriber() throws Exception {
            final io.nats.client.Connection nc = Nats.connect(urls, natsOptions);
            nc.setDisconnectedCallback(new DisconnectedCallback() {
                @Override
                public void onDisconnect(ConnectionEvent ev) {
                    System.err.printf("Subscriber disconnected after %d msgs", received.get());
                }
            });
            nc.setClosedCallback(new ClosedCallback() {
                @Override
                public void onClose(ConnectionEvent ev) {
                    System.err.printf("Subscriber connection closed after %d msgs", received.get());
                }
            });
            nc.setExceptionHandler(new ExceptionHandler() {
                @Override
                public void onException(NATSException ex) {
                    System.err.printf("Subscriber connection exception", ex);
                    AsyncSubscription sub = (AsyncSubscription) ex.getSubscription();
                    System.err.printf("Sent=%d, Received=%d", published.get(), received.get());
                    System.err.printf("Messages dropped (total) = %d", sub.getDropped());
                    System.exit(-1);
                }
            });
            io.nats.streaming.Options opts = new io.nats.streaming.Options.Builder()
                    .natsConn(nc)
                    .build();

            final StreamingConnection sc = NatsStreaming.connect(clusterId, clientId, opts);
            final SubscriptionOptions sopts;

            if (ignoreOld) {
                sopts = new SubscriptionOptions.Builder().deliverAllAvailable().build();
            } else {
                sopts = new SubscriptionOptions.Builder().build();
            }

            final long start = System.nanoTime();

            final Subscription sub = sc.subscribe(subject, new MessageHandler() {
                @Override
                public void onMessage(Message msg) {
                    received.incrementAndGet();
                    if (received.get() >= numMsgs) {
                        bench.addSubSample(new Sample(numMsgs, size, start, System.nanoTime(), nc));
                        System.out.printf("Subscriber connection stats: " + nc.getStats());
                        phaser.arrive();
                        nc.setDisconnectedCallback(null);
                        nc.setClosedCallback(null);
                        try {
                            sc.close();
                        } catch (IOException | TimeoutException e) {
                            System.err.printf(
                                    "streaming-bench: "
                                            + "exception thrown during subscriber connection close",
                                    e);
                        } catch (InterruptedException e) {
                            System.err.printf("Interrupted during subscriber connection close", e);
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }, sopts);

            phaser.arrive();
            while (received.get() < numMsgs) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    class PubWorker extends Worker {
        private final boolean async;
        private final int maxPubAcks;

        PubWorker(Phaser phaser, int numMsgs, int size, boolean async, String pubId,
                int maxPubAcks) {
            super(phaser, numMsgs, size, pubId);
            this.async = async;
            this.maxPubAcks = maxPubAcks;
        }

        @Override
        public void run() {
            try {
                runPublisher();
                phaser.arrive();
            } catch (Exception e) {
                e.printStackTrace();
                phaser.arrive();
            }
        }

        public void runPublisher() throws Exception {
            final io.nats.client.Connection nc = Nats.connect(urls, natsOptions);
            Options pubOpts;
            if (maxPubAcksInFlight > 0) {
                pubOpts = new Options.Builder()
                        .maxPubAcksInFlight(maxPubAcksInFlight)
                        .natsConn(nc)
                        .build();
            } else {
                pubOpts = new Options.Builder()
                        .natsConn(nc)
                        .build();
            }
            try (StreamingConnection sc =
                         NatsStreaming.connect(clusterId, workerClientId, pubOpts)) {

                byte[] msg = null;
                if (size > 0) {
                    msg = new byte[size];
                }

                final long start = System.nanoTime();

                if (async) {
                    CountDownLatch latch = new CountDownLatch(1);
                    AckHandler acb = new AckHandler() {
                        public void onAck(String nuid, Exception ex) {
                            if (published.incrementAndGet() >= numMsgs) {
                                latch.countDown();
                            }
                        }
                    };
                    for (int i = 0; i < numMsgs; i++) {
                        try {
                            sc.publish(subject, msg, acb);
                        } catch (Exception e) {
                            System.err.printf("streaming-bench: error during publish", e);
                        }
                    }
                    latch.await();
                } else {
                    for (int i = 0; i < numMsgs; i++) {
                        try {
                            sc.publish(subject, msg);
                            published.incrementAndGet();
                        } catch (Exception e) {
                            System.err.printf("streaming-bench: error during publish", e);
                        }
                    }
                }

                bench.addPubSample(new Sample(numMsgs, size, start, System.nanoTime(), nc));
                System.out.printf("Publisher connection stats: \n" + nc.getStats());
            } // StreamingConnection
        }
    }

    /**
     * Runs the benchmark.
     * 
     * @throws Exception if an exception occurs
     */
    public void run() throws Exception {
        ExecutorService exec = Executors.newCachedThreadPool();
        final Phaser phaser = new Phaser();

        installShutdownHook();

        phaser.register();
        if (secure) {
            natsOptions = new io.nats.client.Options.Builder().secure().noReconnect().build();
        } else {
            natsOptions = new io.nats.client.Options.Builder().noReconnect().build();
        }

        bench = new Benchmark("NATS Streaming", numSubs, numPubs);

        // Run Subscribers first
        for (int i = 0; i < numSubs; i++) {
            phaser.register();
            String subId = String.format("%s-sub-%d", clientId, i);
            exec.execute(new SubWorker(phaser, numMsgs, size, ignoreOld, subId));
        }

        // Wait for subscribers threads to initialize
        phaser.arriveAndAwaitAdvance();

        // Now publishers
        List<Integer> pubCounts = io.nats.benchmark.Utils.msgsPerClient(numMsgs, numPubs);
        for (int i = 0; i < numPubs; i++) {
            phaser.register();
            String pubId = String.format("%s-pub-%d", clientId, i);
            exec.execute(new PubWorker(phaser, pubCounts.get(i), size, async, pubId,
                    maxPubAcksInFlight));
        }

        System.out.printf("Starting benchmark [msgs=%d, msgsize=%d, pubs=%d, subs=%d]\n", numMsgs,
                size, numPubs, numSubs);

        // Wait for subscribers and publishers to finish
        phaser.arriveAndAwaitAdvance();

        // We're done. Clean up and report.
        Runtime.getRuntime().removeShutdownHook(shutdownHook);

        bench.close();
        System.out.println(bench.report());

        if (csvFileName != null) {
            List<String> csv = bench.csv();
            Path csvFile = Paths.get(csvFileName);
            Files.write(csvFile, csv, Charset.forName("UTF-8"));
        }
    }

    private void installShutdownHook() {
        shutdownHook = new Thread(new Runnable() {
            @Override
            public void run() {
                System.err.println("\nCaught CTRL-C, shutting down gracefully...\n");
                shutdown.set(true);
                System.err.printf("Sent=%d\n", published.get());
                System.err.printf("Received=%d\n", received.get());

            }
        });

        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }

    private void parseArgs(String[] args) {
        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        subject = argList.get(argList.size() - 1);
        argList.remove(argList.size() - 1);

        if (subject.startsWith("-")) {
            usage();
        }

        // Anything left is flags + args
        Iterator<String> it = argList.iterator();
        while (it.hasNext()) {
            String arg = it.next();
            switch (arg) {
                case "-s":
                case "--server":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    urls = it.next();
                    it.remove();
                    continue;
                case "--tls":
                    secure = true;
                    it.remove();
                    continue;
                case "-np":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    numPubs = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-ns":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    numSubs = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-n":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    numMsgs = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-a":
                    async = true;
                    it.remove();
                    continue;
                case "-ms":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    size = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-io":
                    ignoreOld = true;
                    it.remove();
                    continue;
                case "-mpa":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    maxPubAcksInFlight = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-id":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    clientId = it.next();
                    it.remove();
                    continue;
                case "-c":
                case "-cid":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    clusterId = it.next();
                    it.remove();
                    continue;
                case "-csv":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    csvFileName = it.next();
                    it.remove();
                    continue;

                default:
                    System.err.printf("Unexpected token: '%s'\n", arg);
                    usage();
                    break;
            }
        }
    }

    private static Properties loadProperties(String configPath) {
        try {
            InputStream is = new FileInputStream(configPath);
            Properties prop = new Properties();
            prop.load(is);
            return prop;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The main program executive.
     * 
     * @param args command line arguments
     */
    public static void main(String[] args) {
        try {
            if (args.length == 1 && args[0].endsWith(".properties")) {
                Properties properties = loadProperties(args[0]);
                new StanBench(properties).run();
            } else {
                new StanBench(args).run();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }

}
