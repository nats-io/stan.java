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

package io.nats.streaming.examples;

import io.nats.streaming.AckHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

public class Publisher {
    private String urls;
    private String subject;
    private String payloadString;
    private String clusterId = "test-cluster";
    private String clientId = "test-client";
    private boolean async;

    private static final String usageString =
            "\nUsage: java Publisher [options] <subject> <message>\n\nOptions:\n"
                    + "    -s, --server   <urls>           NATS Streaming server URL(s)\n"
                    + "    -c, --cluster  <cluster name>   NATS Streaming cluster name\n"
                    + "    -id,--clientid <client ID>      NATS Streaming client ID\n"
                    + "    -a, --async                     Asynchronous publish mode";

    /**
     * Main constructor for Publisher.
     * 
     * @param args the command line arguments
     * @throws IOException if something goes wrong
     * @throws TimeoutException if the connection times out
     */
    public Publisher(String[] args) {
        parseArgs(args);
    }

    private static void usage() {
        System.err.println(usageString);
    }

    public void run() throws Exception {
        Options opts = NatsStreaming.defaultOptions();
        if (urls != null) {
            opts = new Options.Builder().natsUrl(urls).build();
        }

        try (StreamingConnection sc = NatsStreaming.connect(clusterId, clientId, opts)) {
            final CountDownLatch latch = new CountDownLatch(1);
            final String[] guid = new String[1];
            byte[] payload = payloadString.getBytes();

            AckHandler acb = new AckHandler() {
                @Override
                public void onAck(String nuid, Exception ex) {
                    System.out.printf("Received ACK for guid %s\n", nuid);
                    if (ex != null) {
                        System.err.printf("Error in server ack for guid %s: %s", nuid,
                                ex.getMessage());
                    }
                    if (!guid[0].equals(nuid)) {
                        System.err.printf(
                                "Expected a matching guid in ack callback, got %s vs %s\n", nuid,
                                guid[0]);
                    }
                    System.out.flush();
                    latch.countDown();
                }
            };

            if (!async) {
                try {
                    //noinspection ConstantConditions
                    sc.publish(subject, payload);
                } catch (Exception e) {
                    System.err.printf("Error during publish: %s\n", e.getMessage());
                    throw (e);
                }
                System.out.printf("Published [%s] : '%s'\n", subject, payloadString);
            } else {
                try {
                    //noinspection ConstantConditions
                    guid[0] = sc.publish(subject, payload, acb);
                    latch.await();
                } catch (IOException e) {
                    System.err.printf("Error during async publish: %s\n", e.getMessage());
                    throw (e);
                }

                if (guid[0].isEmpty()) {
                    String msg = "Expected non-empty guid to be returned.";
                    System.err.println(msg);
                    throw new IOException(msg);
                }
                System.out.printf("Published [%s] : '%s' [guid: %s]\n", subject, payloadString,
                        guid[0]);
            }

        } catch (IOException e) {
            if (e.getMessage().equals(io.nats.client.Nats.ERR_NO_SERVERS)) {
                String err = String.format(
                        "Can't connect: %s.\nMake sure a NATS Streaming Server is running at: %s",
                        e.getMessage(), urls);
                throw new IOException(err);
            } else {
                throw (e);
            }
        }
    }

    private void parseArgs(String[] args) {
        if (args == null || args.length < 2) {
            throw new IllegalArgumentException("must supply at least subject and msg");
        }

        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        // The last two args should be subject and payloadString
        // get the payloadString and remove it from args
        payloadString = argList.remove(argList.size() - 1);

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
                    urls = it.next();
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
                case "-a":
                case "--async":
                    async = true;
                    it.remove();
                    continue;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unexpected token: '%s'", arg));
            }
        }
    }

    /**
     * Publishes a message to a subject.
     * 
     * @param args the subject, message payloadString, and other arguments
     * @throws Exception if something goes awry
     */
    public static void main(String[] args) throws Exception {
        try {
            new Publisher(args).run();
        } catch (IllegalArgumentException e) {
            System.out.flush();
            System.err.println(e.getMessage());
            Publisher.usage();
            System.err.flush();
            throw e;
        }
    }
}
