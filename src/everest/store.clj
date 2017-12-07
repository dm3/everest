(ns everest.store
  (:require [everest.store.proto :as proto]))

(defn read-event
  "Reads a single event from the `stream-id` at the specified `position`.

  Supported parameters:
    * `stream-id` - name of the stream or `:all` for the 'all' stream
    * `position` - a positive number, `nil`/zero for the first event or `:everest.event.position/last` for the last event"
  [store stream-id position]
  (proto/-read-event store stream-id position))

(defn read-stream-forward
  "Reads a `cnt` of events from the position `start` of the specified stream
  `stream-id` forward.

  Parameters may have the following values:
    * `stream-id` - a name of the stream or `:all` for the 'all' stream
    * `start` - a positive number or `nil`/zero
    * `cnt` - a positive number or `nil`/zero if all events should be read

  Result is a stream slice, a map with the following structure:
    * `:everest.stream-slice/stream` - the name of the stream being read from
    * `:everest.stream-slice/from` - the position of the first event read or `nil` if no events were read
    * `:everest.stream-slice/next` - the possible position of the next event (the next event in the
      stream will have a position >= the `next` position) or `nil` if no events were read
    * `:everest.stream-slice/direction` - either
      - `:everest.read-direction/forward` or
      - `:everest.read-direction/backward`
    * `:everest.stream-slice/events` - a seq of events that were read

  An Event is a map of the following:
    * `:everest.event/stream` - the name of the stream this event belongs to. This might
      be different from the stream id that was queried if the query `stream-id` is `:all`
    * `:everest.event/position` - the position of this event
    * `:everest.event/timestamp` - date when the event was written
    * `:everest.event/type` - type of the event
    * `:everest.event/data`, `:everest.event/metadata` - event data and metadata"
  [store stream-id start cnt]
  (proto/-read-stream-forward store stream-id start cnt))

(defn read-stream-backward
  "Reads a `cnt` of events from the position `start` of the specified stream
  `stream-id` backward.

  Please see the documentation of `read-stream-forward`."
  [store stream-id start cnt]
  (proto/-read-stream-backward store stream-id start cnt))

(defn delete-stream!
  "Deletes the specified stream and all of the events that were written into it.

  Cannot delete the 'all' stream. Will fail if the current version of the
  stream doesn't match the `expected-version`."
  [store stream-id expected-version]
  (proto/-delete-stream! store stream-id expected-version))

(defn append-events!
  "Appends the provided seq of `events` to the stream identified by the
  `stream-id`. Returns a map containing:
  * `everest.append/next-expected-version` - the expected version which, when used, will
  successfully append events to the stream

  Cannot append to the 'all' stream. Will fail if the current version of the
  stream doesn't match the `expected-version`."
  [store stream-id expected-version events]
  (proto/-append-events! store stream-id expected-version events))

(defn subscribe
  "Subscribe to the specified stream/events from the `start` position,
  exclusive (the reason for that being the usual use case for subscriptions
  when you want to catch up from the last position you have seen and
  processed successfully).  This is a catch up subscription which will read
  all of the events from `start` to the current head and subscribe to the
  further updates.

  Returns a (manifold) stream of events.

  The `start` position might be:
    * `:all` or 0 - stream will be read from the beginning
    * number > 0 - the position of the last event that you processed succesfully
      (or any other position within a stream) - this is a catch up subscription
    * `:everest.subscription.events/new` or `nil` - stream will receive only the newly appended events

  You can pass in additional `opts` which include:
    * `:read-batch-size` - batch size to be used when reading events for catch up
    * `:catch-up-executor` - executor where the catch up work will happen"
  [subscription-source stream-id start opts]
  (proto/-subscribe subscription-source stream-id start opts))
