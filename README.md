![](https://raw.githubusercontent.com/nats-io/nats-site/master/src/img/large-logo.png)
# NATS Streaming Java Client
NATS Streaming is an extremely performant, lightweight reliable streaming platform powered by NATS.

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.org/nats-io/java-nats-streaming.svg?branch=master)](http://travis-ci.org/nats-io/java-nats-streaming)
[![Coverage Status](https://coveralls.io/repos/github/nats-io/java-nats-streaming/badge.svg?branch=master&t=YxbrCO)](https://coveralls.io/github/nats-io/java-nats-streaming?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.nats/java-nats-streaming/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.nats/java-nats-streaming)
[![Javadocs](http://javadoc.io/badge/io.nats/java-nats-streaming.svg)](http://javadoc.io/doc/io.nats/java-nats-streaming)

## Installation

Install the client jar:


```
git clone git@github.com:/nats-io/java-nats-streaming.git

mvn install
```

Load the following dependency in your project's pom.xml:

```
  <dependencies>
    ...
    <dependency>
      <groupId>io.nats</groupId>
      <artifactId>java-nats-streaming</artifactId>
      <version>0.1.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```

## Documentation

The javadoc can be accessed [here](http://nats-io.github.io/java-nats-streaming/).

## Basic Usage

```java

ConnectionFactory cf = new ConnectionFactory(clusterID, clientID);
Connection sc = cf.createConnection();

// Simple Synchronous Publisher
sc.publish("foo", "Hello World".getBytes()); // does not return until an ack has been received from NATS Streaming server

// Simple Async Subscriber
Subscription sub = sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());
    }
});

// Unsubscribe
sub.unsubscribe();

// Close connection
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
Connection sc = new ConnectionFactory("test-cluster", "client-123").createConnection();

// Subscribe with durable name
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());
    }
}, new SubscriptionOptions.Builder().setDurableName("my-durable"));
...
// client receives message sequence 1-40 
...
// client disconnects for an hour
...
// client reconnects with same clientID "client-123"
sc = new ConnectionFactory("test-cluster", "client-123").createConnection();

// client re-subscribes to "foo" with same durable name "my-durable"
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received a message: %s\n", m.getData());
    }
}, new SubscriptionOptions.Builder().setDurableName("my-durable"));
...
// client receives messages 41-current
```

### Wildcard Subscriptions

NATS Streaming subscriptions **do not** support wildcards.

## Advanced Usage 

### Asynchronous Publishing

The basic publish API (`Publish(subject, payload)`) is synchronous; it does not return control to the caller until the NATS Streaming server has acknowledged receipt of the message. To accomplish this, a [NUID](https://github.com/nats-io/nuid) is generated for the message on creation, and the client library waits for a publish acknowledgement from the server with a matching NUID before it returns control to the caller, possibly with an error indicating that the operation was not successful due to some server problem or authorization error.

Advanced users may wish to process these publish acknowledgements manually to achieve higher publish throughput by not waiting on individual acknowledgements during the publish operation. An asynchronous publish API is provided for this purpose:

```java
    // will be invoked when a publish acknowledgement is received
    AckHandler ackHandler = new AckHandler() { {
        public void onAck(String ackedNuid, Exception err) {
            if (err != null) {
                log.error("Error publishing msg id %s: %s\n, ackedNuid, err.getMessage());
            } else {
                log.info("Received ack for msg id %s\n", ackedNuid);
            }
        }
    }
    
    // can also use publish(subj, replysubj, payload, ah)
    String nuid = sc.publish("foo", "Hello World".getBytes(), ackHandler) // returns immediately
```

### Message Acknowledgements and Redelivery

NATS Streaming offers At-Least-Once delivery semantics, meaning that once a message has been delivered to an eligible subscriber, if an acknowledgement is not received within the configured timeout interval, NATS Streaming will attempt redelivery of the message. 
This timeout interval is specified by the subscription option `AckWait`, which defaults to 30 seconds.

By default, messages are automatically acknowledged by the NATS Streaming client library after the subscriber's message handler is invoked. However, there may be cases in which the subscribing client wishes to accelerate or defer acknowledgement of the message. 
To do this, the client must set manual acknowledgement mode on the subscription, and invoke `Ack()` on the `Msg`. ex:

```java
// Subscribe with manual ack mode, and set AckWait to 60 seconds
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        m.ack(); // ack message before performing I/O intensive operation
        ...
        System.out.printf("Received a message: %s\n", m.getData());
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
ConnectionFactory cf = new ConnectionFactory(clusterID, clientID);
cf.setMaxPubAcksInFlight(25);
Connection sc = cf.createConnection();

AckHandler ah = new MessageHandler() {
    public void onAck(String nuid, Exception e) {
      // process the ack
      ...
    }
}

for (int i = 1; i < 1000; i++) {
    // If the server is unable to keep up with the publisher, the number of oustanding acks will eventually 
    // reach the max and this call will block
    String guid = sc.publish("foo", "Hello World".getBytes(), ah); 
}
```

### Subscriber rate limiting

Rate limiting may also be accomplished on the subscriber side, on a per-subscription basis, using a subscription option called `MaxInFlight`. 
This option specifies the maximum number of outstanding acknowledgements (messages that have been delivered but not acknowledged) that NATS Streaming will allow for a given subscription. 
When this limit is reached, NATS Streaming will suspend delivery of messages to this subscription until the number of unacknowledged messages falls below the specified limit. ex:

```java
// Subscribe with manual ack mode and a max in-flight limit of 25
int i = 0;
sc.subscribe("foo", new MessageHandler() {
    public void onMessage(Message m) {
        System.out.printf("Received message #%d: %s\n", ++i, m.getData())
        ...
        // Does not ack, or takes a very long time to ack
        ...
        // Message delivery will suspend when the number of unacknowledged messages reaches 25
    }
}, new SubscriptionOptions.Builder().setManualAcks(true).setMaxInFlight(25).build());

```

## License

(The MIT License)

Copyright (c) 2012-2016 Apcera Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
