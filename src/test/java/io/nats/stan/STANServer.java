/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class STANServer implements Runnable, AutoCloseable {
    static final String STAN_SERVER = "./nats-streaming-server";
    // Enable this for additional server debugging info.
    boolean debug = false;

    ProcessBuilder pb;
    Process proc;
    ProcessStartInfo psInfo;

    class ProcessStartInfo {
        List<String> arguments = new ArrayList<String>();

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

    public STANServer() {
        this(null, -1, false);
    }

    public STANServer(String id) {
        this(id, -1, false);
    }

    public STANServer(boolean debug) {
        this(null, -1, debug);
    }

    public STANServer(int port) {
        this(null, port, false);
    }

    public STANServer(String id, boolean debug) {
        this(id, -1, debug);
    }

    public STANServer(String id, int port, boolean debug) {
        this.debug = debug;
        psInfo = this.createProcessStartInfo();

        if (id != null) {
            psInfo.addArgument("--cluster_id " + id);
        }
        if (port > 1023) {
            psInfo.addArgument("-p " + String.valueOf(port));
        }
        start();
    }

    // private String buildConfigFileName(String configFile) {
    // return new String("../src/test/resources/" + configFile);
    // }

    // public STANServer(String configFile, boolean debug)
    // {
    // this.debug = debug;
    // psInfo = this.createProcessStartInfo();
    // psInfo.addArgument("-config " + buildConfigFileName(configFile));
    // start();
    // }

    private ProcessStartInfo createProcessStartInfo() {
        psInfo = new ProcessStartInfo(STAN_SERVER);

        if (debug) {
            // TODO
            // psInfo.addArgument("-DV");
        }

        return psInfo;
    }

    public void start() {
        try {
            pb = new ProcessBuilder(psInfo.arguments);
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

