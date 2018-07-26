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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class NatsStreamingTestServer implements AutoCloseable {
    private static final String STAN_SERVER = "nats-streaming-server";

    // Use a new port each time, we increment and get so start at the normal port
    private static AtomicInteger portCounter = new AtomicInteger(io.nats.client.Options.DEFAULT_PORT + 1);

    private int port;
    private boolean debug;
    private String configFilePath;
    private String clusterId = "test_cluster";
    private Process process;
    private String cmdLine;
    private String[] customArgs;

    public NatsStreamingTestServer(String clusterId, boolean debug) {
        this.port = nextPort();
        this.debug = debug;
        this.clusterId = clusterId;
        start();
    }

    // These aren't all used right now, but keep code from Nats tests in case we
    // need it
    public NatsStreamingTestServer(int port, boolean debug) {
        this.port = port;
        this.debug = debug;
        start();
    }

    public NatsStreamingTestServer(String configFilePath, int port, boolean debug) {
        this.configFilePath = configFilePath;
        this.debug = debug;
        this.port = port;
        start();
    }

    public NatsStreamingTestServer(String[] customArgs, boolean debug) {
        this.port = NatsStreamingTestServer.nextPort();
        this.debug = debug;
        this.customArgs = customArgs;
        start();
    }

    public static int nextPort() {
        return NatsStreamingTestServer.portCounter.incrementAndGet();
    }

    public void start() {
        ArrayList<String> cmd = new ArrayList<String>();

        String stan = System.getenv("stan_path");

        if(stan == null){
            stan = NatsStreamingTestServer.STAN_SERVER;
        }

        cmd.add(stan);

        // Rewrite the port to a new one, so we don't reuse the same one over and over
        if (this.configFilePath != null) {
            Pattern pattern = Pattern.compile("port: (\\d+)");
            Matcher matcher = pattern.matcher("");
            BufferedReader read = null;
            File tmp = null;
            BufferedWriter write = null;
            String line;

            try {
                tmp = File.createTempFile("nats_streaming_java_test", ".conf");
                write = new BufferedWriter(new FileWriter(tmp));
                read = new BufferedReader(new FileReader(this.configFilePath));

                while ((line = read.readLine()) != null) {
                    matcher.reset(line);

                    if (matcher.find()) {
                        line = line.replace(matcher.group(1), String.valueOf(this.port));
                    }

                    write.write(line);
                    write.write("\n");
                }
            } catch (Exception exp) {
                System.out.println("%%% Error parsing config file for port.");
                return;
            } finally {
                if (read != null) {
                    try {
                        read.close();
                    } catch (Exception e) {
                        throw new IllegalStateException("Failed to read config file");
                    }
                }
                if (write != null) {
                    try{
                        write.close();
                    } catch (Exception e) {
                        throw new IllegalStateException("Failed to update config file");
                    }
                }
            }

            cmd.add("--config");
            cmd.add(tmp.getAbsolutePath());
        } else {
            cmd.add("--port");
            cmd.add(String.valueOf(port));
        }

        if (this.customArgs != null) {
            cmd.addAll(Arrays.asList(this.customArgs));
        }

        if (this.clusterId != null) {
            cmd.add("-cluster_id");
            cmd.add(this.clusterId);
        }
            
        if (debug) {
            cmd.add("-DV");
        }

        this.cmdLine = String.join(" ", cmd);

        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);

            if (debug) {
                System.out.println("%%% Starting [" + this.cmdLine + "] with redirected IO");
                pb.inheritIO();
            } else {
                String errFile = null;
                String osName = System.getProperty("os.name");

                if (osName != null && osName.contains("Windows")) {
                    // Windows uses the "nul" file.
                    errFile = "nul";
                } else {                
                    errFile = "/dev/null";
                }

                pb.redirectError(new File(errFile));
            }

            this.process = pb.start();

            int tries = 10;
            // wait at least 1x and maybe 10
            do {
                try {
                    Thread.sleep(1000);
                } catch (Exception exp) {
                    //Give the server time to get going
                }
                tries--;
            } while(!this.process.isAlive() && tries > 0);

            if (debug) {
                System.out.println("%%% Started [" + this.cmdLine + "]");
            }
        } catch (IOException ex) {
            System.out.println("%%% Failed to start [" + this.cmdLine + "] with message:");
            System.out.println("\t" + ex.getMessage());
            System.out.println("%%% Make sure that nats-streaming-server is installed and in your PATH.");
            System.out.println("%%% See https://github.com/nats-io/nats-streaming-server for information on installing the server");

            throw new IllegalStateException("Failed to run [" + this.cmdLine +"]");
        }
    }

    public int getPort() {
        return this.port;
    }

    public String getURI() {
        return getURIForPort(this.port);
    }

    public static String getURIForPort(int port) {
        return "nats://localhost:" + port;
    }

    public void shutdown() {
        if (this.process == null) {
            return;
        }

        this.process.destroy();
        
        if (this.debug) {
            System.out.println("%%% Shut down ["+ this.cmdLine +"]");
        }

        this.process = null;
    }

    /**
     * Synonomous with shutdown.
     */
    public void close() {
        shutdown();
    }
}

