/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class NatsStreamingServer implements Runnable, AutoCloseable {
    private static final String STAN_SERVER = "nats-streaming-server";

    // Enable this for additional server debugging info.
    private boolean debug = false;

    private Process proc;
    private ProcessStartInfo psInfo;

    class ProcessStartInfo {
        final List<String> arguments = new ArrayList<String>();

        public ProcessStartInfo(String command) {
            this.arguments.add(command);
        }

        public void addArgument(String arg) {
            this.arguments.addAll(Arrays.asList(arg.split("\\s+")));
        }

        String[] getArgsAsArray() {
            return arguments.toArray(new String[arguments.size()]);
        }

        String getArgsAsString() {
            String stringVal = new String();
            for (String s : arguments) {
                stringVal = stringVal.concat(s + " ");
            }
            return stringVal.trim();
        }

        public String toString() {
            return getArgsAsString();
        }
    }

    public NatsStreamingServer() {
        this(null, -1, false);
    }

    public NatsStreamingServer(String id) {
        this(id, -1, false);
    }

    public NatsStreamingServer(boolean debug) {
        this(null, -1, debug);
    }

    public NatsStreamingServer(int port) {
        this(null, port, false);
    }

    public NatsStreamingServer(int port, boolean debug) {
        this.debug = debug;
        psInfo = this.createProcessStartInfo();

        if (port > 1023) {
            psInfo.addArgument("-p " + String.valueOf(port));
        }

        start();
    }

    public NatsStreamingServer(String id, boolean debug) {
        this(id, -1, debug);
    }

    private NatsStreamingServer(String id, int port, boolean debug) {
        this.debug = debug;
        psInfo = this.createProcessStartInfo();

        if (id != null) {
            psInfo.addArgument("-cluster_id " + id);
        }
        if (port > 1023) {
            psInfo.addArgument("-p " + String.valueOf(port));
        }
        start();
    }

    private ProcessStartInfo createProcessStartInfo() {
        String execPath = Paths.get("target", "/", STAN_SERVER).toAbsolutePath().toString();
        psInfo = new ProcessStartInfo(execPath);

        if (debug) {
            psInfo.addArgument("-DV");
        }

        return psInfo;
    }

    private void start() {
        try {
            ProcessBuilder pb = new ProcessBuilder(psInfo.arguments);
            pb.directory(new File("target"));
            if (debug) {
                System.err.println("Inheriting IO, psInfo =" + psInfo);
                pb.inheritIO();
            } else {
                pb.redirectError(new File("/dev/null"));
                pb.redirectOutput(new File("/dev/null"));
            }
            proc = pb.start();
            if (debug) {
                System.out.println("Started [" + psInfo + "]");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        if (proc == null) {
            return;
        }

        proc.destroy();
        if (debug) {
            System.out.println("Stopped [" + psInfo + "]");
        }

        proc = null;
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        this.shutdown();
    }
}

