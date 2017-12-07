(ns everest.pg.store.types
  (:require [everest.spec :refer [ex]]))

(deftype JsonObject [^String data])
(deftype JsonbObject [^String data])
(deftype BinaryEvent [^String type, ^bytes data, ^bytes metadata])
(deftype JsonEvent [^String type, ^String data, ^String metadata])

(defn ->pg-event
  [{:everest.pg.store/keys [event-data-type]}
   {:everest.event/keys [type data metadata]}]
  (when-not (and type data metadata)
    (throw (ex "Event must contain at least type [was %s], data [was %s] and metadata [was %s]!"
               type data metadata)))
  (case event-data-type
    (:json :jsonb) (->JsonEvent type data metadata)
    :bytea (->BinaryEvent type data metadata)))
