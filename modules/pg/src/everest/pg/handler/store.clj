(ns everest.pg.handler.store
  (:require [clojure.string :as string]
            [jdbc.core :as jdbc]
            [everest.pg.util :as util]
            [everest.pg.dynamic :refer [*connection*]]
            [everest.store.subscription :as store.subscription]
            [everest.store :as store]
            [everest.spec :refer [ex verify!]]))

(defn upsert!* [conn handler-type handler-id new-position state]
  ;; TODO: migrate to upsert
  ;;   * keep tests with hsql? - probably not
  ;;   * setup postgres integration test
  (let [cnt (jdbc/execute conn
              ["UPDATE handler_positions SET position = ?, state = ? WHERE handler_id = ?"
               new-position state handler-id])]
    (when (zero? cnt)
      (jdbc/execute conn
        ["INSERT INTO handler_positions (handler_id, handler_type, state, position)
         VALUES (?, ?, ?, ?)"
         handler-id (name handler-type) state new-position]))))

(defn- col->id [n]
  (->> n util/->identifier string/lower-case (keyword "handler")))

(def ^:private select+
  "SELECT handler_id as id, handler_type as type, position, state FROM handler_positions")

(defn get-state* [conn handler-id]
  (jdbc/fetch-one conn
    [(str select+ " WHERE handler_id = ?") handler-id]
    {:identifiers col->id}))

(defn get-state
  "Gets the `{:position, :state}` map of the handler identified by the
  `handler-id`."
  [db handler-id]
  (with-open [conn (jdbc/connection db)]
    (get-state* conn handler-id)))

(defn get-all-states
  "Gets all of the `#:handler{:position, :state, :id, :type}` for the given
  `handler-type` or for all types if not specified."
  ([db]
   (with-open [conn (jdbc/connection db)]
     (jdbc/fetch conn
       [(str select+ " ORDER BY handler_type")])))
  ([db handler-type]
   (with-open [conn (jdbc/connection db)]
     (jdbc/fetch conn
       ;; names of the columns should change rather than using aliases.
       ;; However, we want to maintain backward compat.
       [(str select+ " WHERE handler_type = ? ORDER BY handler_id") (name handler-type)]
       {:identifiers col->id}))))

;; Modification

(defn update! [db {:pg.handler/keys [type id position state]}]
  (if-not (get-state db id)
    (verify! (and type id position)
             "All of type/id/position must be present to initialize a handler, got: %s/%s/%s"
             type id position)
    (verify! (or position state)
             "Either position or state must be present to update a handler, got: %s/%s"
             position state))
  (with-open [conn (jdbc/connection db)]
    (upsert!* conn type id position state)))

(defn delete! [db {:pg.handler/keys [id]}]
  (with-open [conn (jdbc/connection db)]
    (jdbc/execute conn
      ["DELETE FROM handler_positions WHERE handler_id = ?" id])))

(defn subscribe [source db stream-id handler-id opts]
  (let [{:handler/keys [position]} (get-state db handler-id)]
    (store/subscribe source stream-id position opts)))
