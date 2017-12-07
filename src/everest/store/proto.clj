(ns everest.store.proto)

(defprotocol IEventStore
  (-read-event [this stream-id position])
  (-read-stream-forward [this stream-id start cnt])
  (-read-stream-backward [this stream-id start cnt])
  (-delete-stream! [this stream-id expected-version])
  (-append-events! [this stream-id expected-version events]))

(defprotocol ISubscriptionSource
  (-subscribe [this stream-id start opts]))
