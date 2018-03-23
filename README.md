![](https://raw.githubusercontent.com/nats-io/nats-site/master/src/img/large-logo.png)
# NATS Streaming Java Client
NATS Streaming is an extremely performant, lightweight reliable streaming platform powered by NATS.

[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/nats-io/java-nats-streaming.svg?branch=master)](http://travis-ci.org/nats-io/java-nats-streaming)
[![Coverage Status](https://coveralls.io/repos/github/nats-io/java-nats-streaming/badge.svg?branch=master&t=YxbrCO)](https://coveralls.io/github/nats-io/java-nats-streaming?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.nats/java-nats-streaming/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.nats/java-nats-streaming)
[![Javadoc](http://javadoc.io/badge/io.nats/java-nats-streaming.svg)](http://javadoc.io/doc/io.nats/java-nats-streaming)

[![Dependency Status](https://www.versioneye.com/user/projects/57c07fd1968d6400336022f2/badge.svg?style=flat-square)](https://www.versioneye.com/user/projects/57c07fd1968d6400336022f2)
[![Reference Status](https://www.versioneye.com/java/io.nats:java-nats-streaming/reference_badge.svg?style=flat-square)](https://www.versioneye.com/java/io.nats:java-nats-streaming/references)

## Installation

### Maven Central

#### Releases

Current stable release (click for pom info): [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.nats/java-nats-streaming/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.nats/java-nats-streaming)

#### Snapshots

Snapshot releases from the current `master` branch are uploaded to Sonatype OSSRH (OSS Repository Hosting) with each successful Travis CI build. 
If you don't already have your pom.xml configured for using Maven snapshots, you'll need to add the following repository to your pom.xml:

```xml
<profiles>
  <profile>
     <id>allow-snapshots</id>
        <activation><activeByDefault>true</activeByDefault></activation>
     <repositories>
       <repository>
         <id>snapshots-repo</id>
         <url>https://oss.sonatype.org/content/repositories/snapshots</url>
         <releases><enabled>false</enabled></releases>
         <snapshots><enabled>true</enabled></snapshots>
       </repository>
     </repositories>
   </profile>
</profiles>

```
#### Building from source code (this repository)
First, download and install the parent POM:
```
git clone git@github.com:nats-io/nats-parent-pom.git
cd nats-parent-pom
mvn install
```

Now clone, compile, and install in your local maven repository (or copy the artifacts from the `target/` directory to wherever you need them):
```
git clone git@github.com:/nats-io/java-nats-streaming.git
cd java-nats-streaming
mvn install
```

## Platform Notes
### Linux
We use RNG to generate unique inbox names. A peculiarity of the JDK on Linux (see [JDK-6202721] (https://bugs.openjdk.java.net/browse/JDK-6202721) and [JDK-6521844](https://bugs.openjdk.java.net/browse/JDK-6521844)) causes Java to use `/dev/random` even when `/dev/urandom` is called for. The net effect on java-nats-streaming is that client connection startup will be very slow. The standard workaround is to add this to your JVM options:

`-Djava.security.egd=file:/dev/./urandom`

## Basic Usage

```java

// Create a connection factory
StreamingConnectionFactory cf = new StreamingConnectionFactory("test-cluster", "bar");

// A StreamingConnection is a logical connection to the NATS streaming
// server.  This API creates an underlying core NATS connection for
// convenience and simplicity.  In most cases one would create a secure
// core NATS connection and pass it in via
// StreamingConnectionFactory.setNatsConnection(Connection nc)
StreamingConnection sc = cf.createConnection();

// This simple synchronous publish API blocks until an acknowledgement
// is returned from the server.  If no exception is thrown, the message
// has been stored in NATS streaming.
sc.publish("foo", "Hello World".getBytes());

// Use a countdown latch to wait for our subscriber to receive the
// message we published above.
final CountDownLatch doneSignal = new CountDownLatch(1);

// Simple Async Subscriber that retrieves all available messages.
Subscription sub = sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", new String(m.getData()));
        doneSignal.countDown();
    }
}, new SubscriptionOptions.Builder().deliverAllAvailable().build());

doneSignal.await();

// Unsubscribe to clean up
sub.unsubscribe();

// Close the logical connection to NATS streaming
sc.close();
```

### Subscription Start (i.e. Replay) Options 

NATS Streaming subscriptions are similar to NATS subscriptions, but clients may start their subscription at an earlier point in the message stream, allowing them to receive messages that were published before this client registered interest. 
The options are described with examples below:

```java

// Subscribe starting with most recently published value
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());
    }
}, new SubscriptionOptions.Builder().startWithLastReceived().build());

// Receive all stored values in order
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());
    }
}, new SubscriptionOptions.Builder().deliverAllAvailable().build());

// Receive messages starting at a specific sequence number
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());
    }
}, new SubscriptionOptions.Builder().startAtSequence(22).build());

// Subscribe starting at a specific time
Instant startTime = Instant.now().minus(30, ChronoUnit.MINUTES);
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());
    }
}, new SubscriptionOptions.Builder().startAtTime(startTime).build());

// Subscribe starting a specific amount of time in the past (e.g. 30 seconds ago)
Duration ago = Duration.ofSeconds(90);
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());
    }
}, new SubscriptionOptions.Builder().startAtTimeDelta(ago).build());
```

### Durable Subscriptions

Replay of messages offers great flexibility for clients wishing to begin processing at some earlier point in the data stream. 
However, some clients just need to pick up where they left off from an earlier session, without having to manually track their position in the stream of messages. 
Durable subscriptions allow clients to assign a durable name to a subscription when it is created. 
Doing this causes the NATS Streaming server to track the last acknowledged message for that clientID + durable name, so that only messages since the last acknowledged message will be delivered to the client.

```java
StreamingConnection sc = new StreamingConnectionFactory("test-cluster", "client-123").createConnection();

// Subscribe with a durable name
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());
    }
}, new SubscriptionOptions.Builder().durableName("my-durable").build());

// The client receives message sequence 1-40, then disconnects.
sc.close();

// Meanwhile more messages are published to subject "foo"

// Here the client reconnects with same clientID "client-123"
sc = new StreamingConnectionFactory("test-cluster", "client-123").createConnection();

// client re-subscribes to "foo" with same durable name "my-durable"
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());
    }
}, new SubscriptionOptions.Builder().durableName("my-durable").build());

// client receives messages 41-current
```

### Wildcard Subscriptions

NATS Streaming subscriptions **do not** support wildcards.

## Advanced Usage 

### Asynchronous Publishing

The basic publish API (`Publish(subject, payload)`) is synchronous; it does not return control to the caller until the NATS Streaming server has acknowledged receipt of the message. To accomplish this, a [NUID](https://github.com/nats-io/nuid) is generated for the message on creation, and the client library waits for a publish acknowledgement from the server with a matching NUID before it returns control to the caller, possibly with an error indicating that the operation was not successful due to some server problem or authorization error.

Advanced users may wish to process these publish acknowledgements manually to achieve higher publish throughput by not waiting on individual acknowledgements during the publish operation. An asynchronous publish API is provided for this purpose:

```java
// The ack handler will be invoked when a publish acknowledgement is received
AckHandler ackHandler = new AckHandler() {
    public void onAck(String guid, Exception err) {
        if (err != null) {
            System.err.printf("Error publishing msg id %s: %s\n", guid, err.getMessage());
        } else {
            System.out.printf("Received ack for msg id %s\n", guid);
        }
    }
};

// This returns immediately.  The result of the publish can be handled in the ack handler.
String guid = sc.publish("foo", "Hello World".getBytes(), ackHandler);
```

### Message Acknowledgements and Redelivery

NATS Streaming offers At-Least-Once delivery semantics, meaning that once a message has been delivered to an eligible subscriber, if an acknowledgement is not received within the configured timeout interval, NATS Streaming will attempt redelivery of the message. 
This timeout interval is specified by the subscription option `AckWait`, which defaults to 30 seconds.

By default, messages are automatically acknowledged by the NATS Streaming client library after the subscriber's message handler is invoked. However, there may be cases in which the subscribing client wishes to accelerate or defer acknowledgement of the message. 
To do this, the client must set manual acknowledgement mode on the subscription, and individually acknowledge messages.
For example:

```java
// Subscribe with manual ack mode, and set AckWait to 60 seconds
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());

        // You must manually ack when manualAcks() are set.
        try {
            m.ack();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}, new SubscriptionOptions.Builder().setManualAcks(true), setAckWait(Duration.ofSeconds(60)));
```

## Rate limiting/matching

A classic problem of publish-subscribe messaging is matching the rate of message producers with the rate of message consumers. 
Message producers can often outpace the speed of the subscribers that are consuming their messages. 
This mismatch is commonly called a "fast producer/slow consumer" problem, and may result in dramatic resource utilization spikes in the underlying messaging system as it tries to buffer messages until the slow consumer(s) can catch up.

### Publisher rate limiting

NATS Streaming provides a connection option called `MaxPubAcksInFlight` that effectively limits the number of unacknowledged messages that a publisher may have in-flight at any given time. When this maximum is reached, further `PublishAsync()` calls will block until the number of unacknowledged messages falls below the specified limit. ex:

```java
StreamingConnectionFactory cf = new StreamingConnectionFactory("test-cluster", "client-123");
cf.setMaxPubAcksInFlight(25);

StreamingConnection sc = cf.createConnection();

AckHandler ah = new AckHandler() {
    public void onAck(String guid, Exception e) {
        // process the ack
    }
};

for (int i = 1; i < 1000; i++) {
    // If the server is unable to keep up with the publisher, the number of oustanding acks will eventually
    // reach the max and this call will block

    String guid = sc.publish("foo", "Hello World".getBytes(), ah);
    // track the guid in application code to resend, log, etc. if an error is identified in the ack handler.
```

### Subscriber rate limiting

Rate limiting may also be accomplished on the subscriber side, on a per-subscription basis, using a subscription option called `MaxInFlight`. 
This option specifies the maximum number of outstanding acknowledgements (messages that have been delivered but not acknowledged) that NATS Streaming will allow for a given subscription. 
When this limit is reached, NATS Streaming will suspend delivery of messages to this subscription until the number of unacknowledged messages falls below the specified limit. ex:

```java

// Subscribe with manual ack mode and a max in-flight limit of 25
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received message : %s\n", m.getData());

        // You must manually ack when manualAcks() are set.  If acks fail or take too long,
        // message delivery will suspend one the number of unacknowledged messages reaches 25
        // due to the max in flight value.
        try {
            m.ack();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}, new SubscriptionOptions.Builder().manualAcks().maxInFlight(25).build());
```

## License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.