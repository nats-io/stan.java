# Change Log

## Version 2.1.4

* [Fixed] - #121 - Set protobuf dependency to 3.6.1 to avoid breaking version.
* [Fixed] - #119 - Fixed issue with client ids in stan bench when multiple subscribers are used.

## Version 2.1.3

* [Added] Methods to include error and connection listener with factory #113
* [Fixed] Protocol buffer build issues #112
* [Fixed] Made use of ossh password optional #111
* [Fixed] Typo in readme pointing to java repo #110

## Version 2.1.2

* [Removed] Dependency on guava

## Version 2.1.1

* [Changed] Dependency on java nats to 2.1.0
* [Fixed] A couple issues with versions in jars

## Version 2.1.0

* [Added] Added named dispatchers to subscriptions to allow application control of callback threads
* [#100] Split internal callback dispatchers to prevent application code blocking heartbeats

## Version 2.0.3

* [#93] Made it easier to set the error and connection listener on a stream connection
* [#98] Added try-catch around subscription message callback

## Version 2.0.2

* [#92] Updated version of protobuf library
* [#94 CHANGE] Subscriber close() was documented to be close(false), but was calling close(true). The code now reflects the documentation. Old code expecting close() to remove a durable subscriber will need to be updated.

## Version 2.0.0

* [BREAKING CHANGE] Moved build to gradle
* [BREAKING CHANGE] Moved to 2.0.0 of the NATs client
* [BREAKING CHANGE] Removed some deprecated fields from options
* [BREAKING CHANGE] Exception behavior has changed on some methods, TimeoutExceptions have been added

## Version 0.5.0-SNAPSHOT

_2017-02-10_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.4.1...HEAD)

* [CHANGED] Top-level package is now `io.nats.streaming` (vs. `io.nats.stan`)
* [CHANGED] Methods that used to throw TimeoutException will return `null` instead, if they have non-void return types.
* [ADDED] Simplified connect API (See `NatsStreaming.connect(...)`)
* [ADDED] `Subscription::close(boolean unsubscribe)` to allow durable subscribers to close without unsubscribing.
* [ADDED] Set NATS connection name to Streaming clientID
* Miscellaneous FindBugs, PMD and style fixes

## Version 0.4.1

_2016-11-01_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.4.0...0.4.1)

* Bump jnats dependency to `jnats-0.7.3.jar`, which solves a thread exit problem in NATS.

## Version 0.4.0

_2016-10-30_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.3.0...0.4.0)

* Bump jnats dependency to `jnats-0.7.1.jar`, thus improving synchronous publish performance significantly.
* Added benchmark utility to examples.
* Updated functional/integration test coverage to match `go-nats-streaming` tests.
* Repository structure changes

## Version 0.3.0

_2016-09-14_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.2.1...0.3.0)

* [#29] Added `getNatsConnection()` public API for getting underlying NATS connection.
* Removed the `publish*WithReply` variants, as request-reply isn't well-served by streaming. Use the underlying NATS connection to do this.

## Version 0.2.1

_2016-09-12_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.2.0...0.2.1)

* [#26](/../../issues/#26) Fixed an issue where the heartbeat subscription was not being unsubscribed during connection close.
* Updated README examples and installation info

## Version 0.2.0

_2016-09-10_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.1.2...0.2.0)

* Fix `ConnectionFactory::createConnection` to return `Connection` interface vs. `ConnectionImpl`
* Resolved several test issues, including locating/running server binary for integration tests

## Version 0.1.2

_2016-08-29_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.1.1...0.1.2)

* Filter nats_checkstyle.xml from jar/bundle

## Version 0.1.1

_2016-08-26_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/v0.1.0...0.1.1)

* Updated dependencies and build configuration

## Version 0.1.0

_2016-08-26_ Initial public release of java-nats-streaming, now available on Maven Central.
