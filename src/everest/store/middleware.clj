(ns everest.store.middleware
  (:require [manifold.stream :as m.s]
            [everest.store.proto :as store.proto]))

(deftype DecoratedEventStore [store event-fn-in single-event-fn-out event-fn-out]
  store.proto/IEventStore
  (-read-event [_ stream-id pos]
    (single-event-fn-out (store.proto/-read-event store stream-id pos)))
  (-read-stream-forward [_ stream-id start cnt]
    (update (store.proto/-read-stream-forward store stream-id start cnt)
            :everest.stream-slice/events #(mapcat event-fn-out %)))
  (-read-stream-backward [_ stream-id start cnt]
    (update (store.proto/-read-stream-backward store stream-id start cnt)
            :everest.stream-slice/events #(mapcat event-fn-out %)))
  (-delete-stream! [_ stream-id expected-version]
    (store.proto/-delete-stream! store stream-id expected-version))
  (-append-events! [_ stream-id expected-version events]
    (store.proto/-append-events! store stream-id expected-version
      (mapv event-fn-in events))))

(alter-meta! #'->DecoratedEventStore assoc :private true)

;; TODO: inefficient - split the `wrap-store` into explicit wrap-store-1-to-1/1-to-many?
(defn- wrap-stream-event-fn-out [f]
  (fn [e]
    (when-let [e (f e)]
      (if (sequential? e)
        e
        (vector e)))))

(defn wrap-store
  "Wraps the given `IEventStore` by piping all of the events going in through
  `event-fn-in` and the ones coming out through `event-fn-out`.  The callbacks
  are functions of one argument which expect and event and produce an event,
  several events or no events (`nil` or empty seq) synchronously.

  `event-fn-in` can only produce a single event when given an event.

  If the middleware returns multiple events in place of a single one, the
  functions of `EventStore` will work as follows:

    * `read-event` will return whatever `event-fn-out` returns
    * `read-stream-*` concatenates the events produced by `event-fn-out`

  Example:

    (defn log
      [store]
      (wrap-store store
        #(do (log/debug :writing %) %)
        #(do (log/debug :reading %) %)))
  "
  [store event-fn-in event-fn-out]
  (->> (wrap-stream-event-fn-out event-fn-out)
       (DecoratedEventStore. store event-fn-in event-fn-out)))
