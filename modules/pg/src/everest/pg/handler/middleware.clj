(ns everest.pg.handler.middleware
  (:require [jdbc.core :as jdbc]
            [everest.handler :as handler]
            [everest.handler.middleware :as handler.middleware]
            [everest.pg.dynamic :refer [*connection*]]
            [everest.pg.handler.store :as handler.store]
            [everest.pg.store.types :as types]
            [everest.event :as event]
            [everest.spec :refer [ex verify!]])
  (:import [everest.pg.store.types JsonObject JsonbObject]))

(defn wrap-connection
  "Opens a connection using the provided `db` spec which might (and should) be
  a connection pool, a JDBC URL or a connection description as specified in the
  [clojure.jdbc](http://funcool.github.io/clojure.jdbc/latest/) docs.

  Accepts a:
    * `db` - dbspec or pool
    * `connection-key` - a key under which the connection will be `assoc`ed to
      the event. None by default."
  [handler {:handler.middleware/keys [db connection-key]}]
  (verify! db "Expected a db spec or a connection pool!")
  (fn [e]
    (with-open [conn (jdbc/connection db)]
      (binding [*connection* conn]
        (let [e (if connection-key (assoc e connection-key conn) e)]
          (handler (vary-meta e assoc :everest.jdbc/connection conn)))))))

(defn- with-connection [e f]
  (if-let [conn (:everest.jdbc/connection (meta e))]
    (f conn)
    (throw (ex "No JDBC connection found for event: %s [%s]!"
               (:everest.event/type e) (:everest.event/position e)))))

(defn wrap-tx
  "Wraps the event handler into a transaction. Must be run inside the
  `wrap-connection` handler."
  [handler {:handler.middleware/keys [tx-options tx-during-catch-up?]
            :or {tx-options {}, tx-during-catch-up? true}}]
  (let [!live? (volatile! false)]
    (fn [e]
      (when (event/live-processing-started? e)
        (vreset! !live? true))
      (if (or tx-during-catch-up? @!live?)
        (with-connection e
          (fn [conn]
            (jdbc/atomic conn tx-options (handler e))))
        (handler e)))))

(defn- unwrap [x]
  (when x
    (cond (instance? JsonObject x) (.data ^JsonObject x)
          (instance? JsonbObject x) (.data ^JsonbObject x)
          :else x)))

(defn wrap-json-state
  "Wraps the result of the handler invocation so that it would be treated as a
  Json data type by the Postgres JDBC driver.

  Accepts the following options:
    * `state-key` - `:state` by default, where the state is stored
    * `json-type` - `:jsonb` by default, either `:json` or `:jsonb`"
  [handler {:handler.middleware/keys [state-key json-type]
            :or {state-key :handler/state, json-type :jsonb}}]
  (fn [e]
    (let [result (handler (update e state-key unwrap))]
      (if (handler/handled? result)
        (when-not (nil? result)
          (case json-type
            :json (types/->JsonObject result)
            :jsonb (types/->JsonbObject result)))
        (handler/skip)))))

(defn- upsert [type id position state]
  (verify! position "No position provided to upsert: %s/%s!" type id)
  (handler.store/upsert!* *connection* type id position state))

(defn wrap-position
  [handler {:handler.middleware/keys [handler-type handler-id] :as opts}]
  (let [f (handler.middleware/wrap-position handler
            (merge opts
              {:handler.middleware/upsert-state #(upsert handler-type handler-id %1 %2)}))]
    (fn [e]
      (f (assoc e :handler/id handler-id :handler/type handler-type)))))

(defn wrap-state
  [handler {:handler.middleware/keys [handler-type handler-id] :as opts}]
  (let [f (handler.middleware/wrap-state handler
            (merge opts
              {:handler.middleware/get-state
               (fn []
                 (let [state (handler.store/get-state* *connection* handler-id)]
                   (verify! state
                            "Handler %s/%s isn't initialized. Please, initialize the handler before starting!"
                            handler-type handler-id)
                   state))
               :handler.middleware/upsert-state
               (fn [pos state]
                 (upsert handler-type handler-id pos state))}))]
    (fn [e]
      (f (assoc e :handler/id handler-id :handler/type handler-type)))))
