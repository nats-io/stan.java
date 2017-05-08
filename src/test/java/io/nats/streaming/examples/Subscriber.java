/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming.examples;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Subscriber {
    private String url;
    private String subject;
    private String clusterId = "test-cluster";
    private String clientId = "test-client";
    private final SubscriptionOptions.Builder builder = new SubscriptionOptions.Builder();
    private String qgroup;
    private String durable;
    private int count = 0;
    private boolean unsubscribe;

    private static final String usageString = "\nUsage: java Subscriber [options] <subject>\n\nOptions:\n"
            + "    -s,  --server   <urls>           NATS Streaming server URL(s)\n"
            + "    -c,  --cluster  <cluster name>   NATS Streaming cluster name\n"
            + "    -id, --clientid <client ID>      NATS Streaming client ID               \n\n"
            + "Subscription Options:                                             \n"
            + "     -q, --qgroup   <name>           Queue group\n"
            + "         --seq      <seqno>          Start at seqno\n"
            + "         --all                       Deliver all available messages\n"
            + "         --last                      Deliver starting with last published message\n"
            + "         --since    <duration>       Deliver messages in last interval "
            + "(e.g. 1s, 1hr)\n" + "                   (format: 00d00h00m00s00ns)\n"
            + "         --durable  <name>           Durable subscriber name\n"
            + "         --unsubscribe               Unsubscribe the durable on exit\n"
            + "         --count    <num>            Number of messages to receive";

    public Subscriber(String[] args) {
        parseArgs(args);
    }

    private static void usage() {
        System.err.println(usageString);
    }

    private void run() throws Exception {
        Options opts = null;
        if (url != null) {
            opts = new Options.Builder().natsUrl(url).build();
        }

        final CountDownLatch done = new CountDownLatch(1);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger delivered = new AtomicInteger(0);


        Thread hook = null;

        try (final StreamingConnection sc = NatsStreaming.connect(clusterId, clientId, opts)) {
            try {
                final Subscription sub = sc.subscribe(subject, qgroup, new MessageHandler() {
                    public void onMessage(Message msg) {
                        try {
                            start.await();
                        } catch (InterruptedException e) {
                            /* NOOP */
                        }
                        System.out.printf("[#%d] Received on [%s]: '%s'\n",
                                delivered.incrementAndGet(), msg.getSubject(), msg);
                        if (delivered.get() == count) {
                            done.countDown();
                        }
                    }
                }, builder.build());
                hook = new Thread() {
                    public void run() {
                        System.err.println("\nCaught CTRL-C, shutting down gracefully...\n");
                        try {
                            if (durable == null || durable.isEmpty() || unsubscribe) {
                                sub.unsubscribe();
                            }
                            sc.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        done.countDown();
                    }
                };
                Runtime.getRuntime().addShutdownHook(hook);
                System.out.printf("Listening on [%s], clientID=[%s], qgroup=[%s] durable=[%s]\n",
                        sub.getSubject(), clientId, sub.getQueue(),
                        sub.getOptions().getDurableName());
                start.countDown();
                done.await();
                if (durable == null || durable.isEmpty() || unsubscribe) {
                    sub.unsubscribe();
                }
                sc.close();
            } finally {
                Runtime.getRuntime().removeShutdownHook(hook);
            }
        }
    }

    private void parseArgs(String[] args) {
        if (args == null || args.length < 1) {
            throw new IllegalArgumentException("must supply at least a subject name");
        }

        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        // The last arg should be subject
        // get the subject and remove it from args
        subject = argList.remove(argList.size() - 1);

        // Anything left is flags + args
        Iterator<String> it = argList.iterator();
        while (it.hasNext()) {
            String arg = it.next();
            switch (arg) {
                case "-s":
                case "--server":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    url = it.next();
                    it.remove();
                    continue;
                case "-c":
                case "--cluster":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    clusterId = it.next();
                    it.remove();
                    continue;
                case "-id":
                case "--clientid":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    clientId = it.next();
                    it.remove();
                    continue;
                case "-q":
                case "--qgroup":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    qgroup = it.next();
                    it.remove();
                    continue;
                case "--seq":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    builder.startAtSequence(Long.parseLong(it.next()));
                    it.remove();
                    continue;
                case "--all":
                    builder.deliverAllAvailable();
                    it.remove();
                    continue;
                case "--last":
                    builder.startWithLastReceived();
                    it.remove();
                    continue;
                case "--since":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    try {
                        builder.startAtTimeDelta(parseDuration(it.next()));
                    } catch (ParseException e) {
                        throw new IllegalArgumentException(e.getMessage());
                    }
                    it.remove();
                    continue;
                case "--durable":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    builder.durableName(it.next());
                    it.remove();
                    continue;
                case "-u":
                case "--unsubscribe":
                    unsubscribe = true;
                    it.remove();
                    continue;
                case "--count":
                    if (!it.hasNext()) {
                        throw new IllegalArgumentException(arg + " requires an argument");
                    }
                    it.remove();
                    count = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                default:
                    throw new IllegalArgumentException(String.format("Unexpected token: '%s'", arg));
            }
        }
    }

    private static final Pattern pattern =
            Pattern.compile("(\\d+)d\\s*(\\d+)h\\s*(\\d+)m\\s*(\\d+)s\\s*(\\d+)ns");

    /**
     * Parses a duration string of the form "98d 01h 23m 45s" into milliseconds.
     * 
     * @throws ParseException if the duration can't be parsed
     */
    private static Duration parseDuration(String duration) throws ParseException {
        Matcher matcher = pattern.matcher(duration);

        long nanoseconds = 0L;

        if (matcher.find() && matcher.groupCount() == 4) {
            int days = Integer.parseInt(matcher.group(1));
            nanoseconds += TimeUnit.NANOSECONDS.convert(days, TimeUnit.DAYS);
            int hours = Integer.parseInt(matcher.group(2));
            nanoseconds += TimeUnit.NANOSECONDS.convert(hours, TimeUnit.HOURS);
            int minutes = Integer.parseInt(matcher.group(3));
            nanoseconds += TimeUnit.NANOSECONDS.convert(minutes, TimeUnit.MINUTES);
            int seconds = Integer.parseInt(matcher.group(4));
            nanoseconds += TimeUnit.NANOSECONDS.convert(seconds, TimeUnit.SECONDS);
            long nanos = Long.parseLong(matcher.group(5));
            nanoseconds += nanos;
        } else {
            throw new ParseException("Cannot parse duration " + duration, 0);
        }

        return Duration.ofNanos(nanoseconds);
    }

    /**
     * Subscribes to a subject.
     * 
     * @param args the subject, cluster info, and subscription options
     */
    public static void main(String[] args) throws Exception {
        try {
            new Subscriber(args).run();
        } catch (IllegalArgumentException e) {
            System.out.flush();
            System.err.println(e.getMessage());
            Subscriber.usage();
            System.err.flush();
            throw e;
        }
    }
}
