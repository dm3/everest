# everest

A Clojure/Postgres-based event store/library/toolkit.

When you should use it?

* You don't need to process hundreds of GB of events (Postgres will deal
  perfectly fine with tables that fit in RAM)
* You are OK with what Postgres offers for HA (getting simpler with each release)
* You believe in event-based integration
* You are a fan of event sourcing/event driven integration

Alternative Event Store implementations:

* [Rill](https://github.com/rill-event-sourcing/rill) - also based on Postgres,
  more opinionated, covers less ground
* [NEventStore](https://github.com/NEventStore) - if you prefer .NET

Alternatives if Postgres doesn't cut your needs:

* [Kafka](http://kafka.apache.org/)
* [EventStore](https://geteventstore.com/)

## TODO

* Tests with embedded Postgres
* More examples
    - use with Component
    - auto load handlers
    - event sourcing
    - recover from failure
    - transactional handlers
    - complicated handler state

## Design

Everest is split into several modules:

* Core `[everest]`
  - core event store protocols
  - in-memory event store implementation
  - implementation-agnostic subscription mechanism
* PG `[everest.module/pg]` - relational event store implementation using
  Postgres
* PG Schema `[everest.module/pg-schema]` - schema for the JDBC module with
  Flyway-based migrations
* JSON `[everest.module/json]` - Cheshire (Jackson)-based store middleware for
  serialization/deserialization into JSON
* DSL `[everest.module/dsl]` - experimental DSL to compose event handlers

### Event Store

Everest event store interface is highly influenced by Greg Young's excellent
[EventStore](https://geteventstore.com/). It supports the following operations:

* `read-event` to read a single event
* `read-stream-forward` to read all events in the given stream starting from
  the given position
* `read-stream-backward` to read all events in the given stream starting from
  the given position towards the beginning
* `delete-stream!` to delete the given stream and all of the events
* `append-events!` to append a number of events to the given stream

There is a special `:all` stream which returns all of the events in the store.
It is not possible to delete or append to the `:all` stream.

Delete and append operations expect an `expected-version` argument which will
check the following:

* if `:everest.stream.version/any` - no check is performed
* if `:everest.stream.version/not-present` - the stream should not exist
* if a number - the last event in the stream should be at the given position

### Subscriptions

One great advantage of having the events is being able to react to them as they
appear. Everest event store supports subscriptions through the
`everest.store/subscribe` function which returns a stream of events for the
given stream starting right after the provided position.

### Store and Subscription Middleware

Everest supports a concept of store middleware which allows wrapping the events
coming in and out of the event store with custom functions. This fits the
Clojure mindset very well and is very convenient for pluggable event
serialization, validation and coercion, discussed further below.

### Event Serialization

Everest provides a JSON serialization middleware,
`everest.json.store.middleware/wrap-cheshire-json`, which will serialize the
events going into the store and deserialize ones coming out.  It plays
especially well with Postgres' *json/jsonb* database types.

We can create our own middleware easily by wrapping e.g.
[Nippy](https://github.com/ptaoussanis/nippy) or [Avro](https://avro.apache.org/).

### Event Schemas and Versioning

A common need when working with data in a dynamic language is making sure data
follows the specification. Clojure has a de-facto standard for dynamic
validation and coercion: [clojure.spec](https://clojure.org/guides/spec).
There's no module in Everest to provide Spec-based event validation, but it
would be quite easy to implement!

Furthermore, events evolve. Although a contract for an event shouldn't change
drastically, it may change in a non-compatible way which could require
migration. Obviously, we should try to evolve the contracts in a
backwards-compatible manner. If that isn't possible, we need to implement
custom logic which will migrate the events to the newest versions upon loading.

We can choose from the following migration/versioning strategies:

1. Do nothing. Handle upgrades in the application code of aggregates,
   projections and processes.
2. Define upcasters in the components which own the events.  Projections and
   processes in other components will have to work with old event versions.
3. Upcasters in contract libraries. Package event definitions into separate
   libraries and include them as dependencies in consuming projects.  This will
   ensure the upcasters will run every time the events are read.
4. Upcaster migrations. Write an upcaster in the component which owns the event
   and run it on the event store to update the events. This will work in
   Postgres, but only for upcasters that do not split the event into multiple
   ones while upcasting. These kinds of upcasters would have to append more
   events which would get propagated to the subscriptions. This could
   theoretically be dealt with by adding some sort of metadata which would be
   used in subscriptions to filter out such upcasted events.

If you need to break your contracts - solution #3 is preferable.

For more insight into event versioning:

* https://abdullin.com/post/event-sourcing-versioning/
* http://files.movereem.nl/2017saner-eventsourcing.pdf
* https://leanpub.com/esversioning/read

Currently Everest doesn't provide any abstractions to deal with upcasting, as
the scenarios for upcasting are quite different, e.g.:

* Upcast an event by supplying a default value based on the `version` defined
  in its metadata
* Upcast an event by looking up data in the external sources
* Upcast an event by capturing the state seen earlier in the stream
* Split an event into multiple events

### Event Handlers

Once events are in the store, we want to read and react to them. This is
achieved by subscribing to the new events using the subscription mechanism
provided by Everest. However, just getting a hold of an event stream is too low
level. Usually we want to do one of the few basic things:

* Process a number of events and write the results into some other representation
* Raise more events in response to processed events
* Interact with the outside world in response to processed events
* Wrap the above interactions into a state machine which reacts to events (also
  known as a *Process Manager*)

An event handler will most surely want to track its position in the event
stream so that it doesn't have to process all of the events every time it
starts up. Position tracking is also much simpler to reason about if the position
is saved atomically with the result of processing the event. This way
idempotency is guaranteed and there's no trouble in performing a live back up
of the position together with the result.

Thus, an event handler will want to access an event together with some kind of
context, be it a [component](https://github.com/stuartsierra/component) or just
a map holding the handler's state. Also, you will probably want to run the
handlers in such a manner so as not to interfere with the workings of other
handlers. If a handler is slow or failing, it should not affect the other
handlers.

All of the cross-cutting concerns that affect many handlers should be expressed
as *event-handler middleware*. An event is just a map which can be `assoc`ed
with additional stuff at any point in the middleware chain. The same way it works
with [Ring](https://github.com/ring-clojure/ring):

* event ~= request
* event handler = [Ring handler](https://github.com/ring-clojure/ring/wiki/Concepts#handlers)
* event handler middleware = [Ring handler middleware](https://github.com/ring-clojure/ring/wiki/Concepts#middleware)

Although the concept of an event handler is very simple, handling some of the
common cases is quite tricky and Everest aims to provide support for some of
the patterns.

#### Projections and Other Simple Event Handlers

Projections are the simplest type of event handlers. They consume an event and
produce a result - storing it either in a persistent storage or memory - then
wait for another event.

Everest provides some generic projection middleware:

* `everest.handler.middleware/wrap-position` - tracks the current position of
  the handler
* `everest.handler.middleware/wrap-state` - tracks the state returned from the
  handler

These middlewares can be parameterized with the
`:handler.middleware/state-store` which controls where the handler state will
be stored. Everest provides a JDBC-based setup for position/state tracking.

Position updates only:

```clojure
(def handler-state-store (memory.handler.store/create))

(everest.handler.middleware/wrap-position
  (fn [event]
    (println "Hi: " (:name event)))
  {:handler.middleware/state-store handler-state-store})
```

Position and state updates:

```clojure
(everest.handler.middleware/wrap-state
  (fn [event]
    (update event :handler/state (fnil inc 0)))
  {:handler.middleware/state-store handler-state-store
   :handler.middleware/state-key :handler/state}) ;; default value
```

The above functionality depends on the JDBC-specific middleware:

* `wrap-connection` - opens the connection, attaches it to the event and
  executes the handler
* `wrap-tx` - runs the wrapped handler inside of the transaction

So you would actually use it like so:

```clojure
(-> event-handler
    (wrap-state {:get-state (get-fn 'HandlerId)
                 :upsert-state (upsert-fn 'HandlerType 'HandlerId)})
    (wrap-tx)
    (wrap-connection {:db dbspec}))
```

Of course, clojure data cannot be stored in the JDBC-backed storage without
serialization. Serialization is also dealt with via middleware, which we'll
touch upon a bit later.

## License

Copyright © 2017 Vadim Platonov

Distributed under the MIT License.
