(ns everest.dummy.store
  (:require [everest.store.proto :as store.proto]))

(defn create []
  (reify store.proto/IEventStore
    (-read-event [this stream-id position])
    (-read-stream-forward [this stream-id start cnt])
    (-read-stream-backward [this stream-id start cnt])
    (-delete-stream! [this stream-id expected-version])
    (-append-events! [this stream-id expected-version events])))
