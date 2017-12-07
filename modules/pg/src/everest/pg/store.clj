(ns everest.pg.store
  (:require [clojure.string :as string]
            [jdbc.core :as jdbc]
            [everest.event :as event]
            [everest.spec :as spec :refer [ex]]
            [everest.store.proto :as store.proto]
            [everest.pg.util :as util]
            [everest.pg.dynamic :as dyn :refer (*connection*)]
            [everest.pg.store.types :as types]

            [everest.pg.jdbc-ext])
  (:import [org.postgresql.util PGobject]))

(defn- where-stream [[sql args :as stmt] stream-id]
  (if-not (= :all stream-id)
    [(str sql " and lower(stream_id) = ?") (conj args stream-id)]
    stmt))

(defn- limit-by [[sql args :as stmt] limit]
  (if limit
    [(str sql " limit ?") (conj args limit)]
    stmt))

(defn- order-by-position [[sql args] order]
  [(str sql " order by position " order) args])

(defn- unwrap [[sql args]]
  (vec (concat [sql] args)))

(def ^:private event-columns
  "stream_id, position, timestamp, type, data, metadata")

(defn- with-connection [db f]
  (if *connection*
    (f *connection*)
    (with-open [conn (jdbc/connection db)]
      (f conn))))

(defn- where-position [[sql args :as stmt] start direction]
  (let [sql (str sql " where position ")]
    (cond (and (= :everest.read-direction/backward direction)
               (= :everest.event.position/last start))
          [(str sql "<= (select max(position) from events)") args]

          (number? start)
          [(str sql (if (= :everest.read-direction/forward direction) ">=" "<=") " ?")
           [start]]

          :else (throw (ex "Cannot query events going %s from position '%s'!"
                           (name direction) start)))))

(defn- from-pg-object [v]
  (if (instance? PGobject v)
    (let [^PGobject o v]
      (if (#{"json" "jsonb"} (.getType o))
        (.getValue o)
        o))
    v))

(defn- ->event [{:keys [stream-id position timestamp type data metadata]}]
  {:everest.event/type type
   :everest.event/stream stream-id
   :everest.event/position position
   :everest.event/timestamp timestamp
   :everest.event/data (from-pg-object data)
   :everest.event/metadata (from-pg-object metadata)})

(defn- read-stream* [db stream-id start cnt direction]
  (->>
    (with-connection db
      #(jdbc/fetch %
         (-> [(format "select %s from events" event-columns) []]
             (where-position start direction)
             (where-stream stream-id)
             (order-by-position (if (= direction :everest.read-direction/forward) "asc" "desc"))
             (limit-by cnt)
             unwrap)
         {:identifiers util/->identifier}))
    (map ->event)
    (event/events->stream-slice direction stream-id)))

(defn- read-event* [db stream-id position]
  (-> (with-connection db
        #(jdbc/fetch-one %
           (unwrap
             (if (identical? :everest.event.position/last position)
               (-> [(format "select %s from events where 1 = 1" event-columns) []]
                   (where-stream stream-id)
                   (order-by-position "desc")
                   (limit-by 1))
               (-> [(format "select %s from events where position = ?" event-columns) [position]]
                   (where-stream stream-id))))
           {:identifiers util/->identifier}))
      ->event))

(defn- delete-stream!* [db stream-id expected-version]
  (if (= :all stream-id)
    (throw (ex "cannot delete the all stream!"))
    (if (identical? :everest.stream.version/not-present expected-version)
      (throw (ex "Cannot delete a stream which isn't expected to exist!"))
      (with-connection db
        #(jdbc/execute %
           (if (identical? :everest.stream.version/any expected-version)
             ["delete from streams where id = ?" stream-id]
             ["delete from streams where id = ? and position = ?"
              stream-id (spec/version->position expected-version)]))))))

(defn- append-events!* [db stream-id expected-version events]
  (if (= :all stream-id)
    (throw (ex "Cannot append to the all stream!"))
    (with-connection db
      (fn [conn]
        {:everest.append/next-expected-version
         (:result
           (jdbc/fetch-one conn
             ["select append_events(?, ?, ?) as result;"
              (spec/version->position expected-version) stream-id
              (into-array (class (first events)) events)]))}))))

(def ^:private +default-config
  {:everest.pg.store/event-data-type :jsonb})

(deftype JdbcEventStore [db config]
  store.proto/IEventStore
  (-read-event [_ stream-id position]
    (read-event* db
      (spec/normalize-identifier stream-id)
      (spec/check-position (spec/init-position position :everest.read-direction/forward))))
  (-read-stream-forward [_ stream-id start cnt]
    (read-stream* db
      (spec/normalize-identifier stream-id)
      (spec/check-position (spec/init-position start :everest.read-direction/forward))
      (spec/check-limit cnt)
      :everest.read-direction/forward))
  (-read-stream-backward [_ stream-id start cnt]
    (read-stream* db
      (spec/normalize-identifier stream-id)
      (spec/check-position (spec/init-position start :everest.read-direction/backward))
      (spec/check-limit cnt)
      :everest.read-direction/backward))
  (-delete-stream! [_ stream-id expected-version]
    (delete-stream!* db
      (spec/normalize-identifier stream-id)
      (spec/check-version expected-version)))
  (-append-events! [_ stream-id expected-version events]
    (append-events!* db
      (spec/normalize-identifier stream-id)
      (spec/check-version expected-version)
      (map #(types/->pg-event config %)
           (spec/check-events events)))))

(defn create-store [db config]
  (JdbcEventStore. db (merge +default-config config)))

(alter-meta! #'->JdbcEventStore assoc :private true)
