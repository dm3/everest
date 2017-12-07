(ns everest.handler.dsl
  (:require [everest.event :as event]
            [everest.spec :refer [verify!]]
            [manifold.stream :as m.s]
            [manifold.time :as m.t]))

(defn handler [handler-type & handler-fns]
  (fn [e]
    (let [fns (->> handler-fns (filter #((::match %) e)))]
      (verify! (>= 1 (count fns))
        "More than one handling function found for handler %s, event %s!"
        handler-type e)
      (if-let [{::keys [fn]} (first fns)]
        (fn e)))))

(defn on [event-type f]
  (let [event-type' (name event-type)]
    {::type ::handler
     ::match #(= event-type' (:everest.event/type %))
     ::fn f}))

(defn emit
  "Returns a function that will emit a transient event of the given
  `event-type` into a manifold stream.

  To be used together with `every-ms`, e.g.:

    (every-ms 5000
      (emit {:everest.event/type 'CheckCondition}))"
  [event]
  (fn [stream]
    @(m.s/put! stream (event/transient-event event))))

(defn every-ms
  "Creates a handler that registers the function `f` upon startup. It then runs
  the function every `period-ms` with initial `delay-ms`.

  `f` must accept a manifold stream, which is the stream of events handled by
  this handler.

  For example, in order to produce a handler which will receive a
  `CheckCondition` event every 5 seconds, do the following:

    (every-ms 5000 0
      (emit {:everest.event/type 'CheckCondition}))"
  [period-ms delay-ms f]
  {::type ::handler
   ::match event/subscription-started?
   ::fn (fn [{:everest.event/keys [this-stream] :as e}]
          (let [timer (m.t/every period-ms delay-ms #(f this-stream))]
            (m.s/on-closed this-stream timer)))})
