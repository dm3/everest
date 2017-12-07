(ns everest.spec
  (:require [clojure.string :as string]
            [everest.event.proto :as event.proto]))

(defn ex [msg & args]
  (throw (ex-info (apply format msg args) {})))

(defn verify! [v msg & args]
  (assert v (apply format msg args)))

(defn normalize-identifier [id]
  (if (= :all id)
    id
    (string/lower-case (name id))))

(defn check-version [v]
  (when-not (or (#{:everest.stream.version/not-present
                   :everest.stream.version/any
                   :everest.stream.version/empty} v)
                (number? v))
    (throw (ex "Invalid stream version: %s!" v)))
  v)

(defn version->position [v]
  (let [result (case v
                 :everest.stream.version/any -2
                 :everest.stream.version/not-present -1
                 :everest.stream.version/empty 0
                 v)]
    (when-not (number? result)
      (throw (ex "Cannot convert stream version %s to a position!" result)))
    (long result)))

(defn init-position [p direction]
  (if (nil? p)
    (case direction
      :everest.read-direction/forward 1
      :everest.read-direction/backward :everest.event.position/last)
    p))

(defn check-position [p]
  (if (or (identical? :everest.event.position/last p)
          (satisfies? event.proto/IPosition p))
    p
    (throw (ex "Expected a position, got: %s!" p))))

(defn check-limit [l]
  (if (or (nil? l) (zero? l))
    nil
    (if (or (not (number? l)) (neg? l))
      (throw (ex "Limit must be a number >= 0, was: %s!" l))
      l)))

(defn check-events [es]
  (when-not (seq es)
    (throw (ex "no events found to append!")))
  es)
