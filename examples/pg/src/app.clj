(ns app
  (:require [everest.json.handler.middleware :as json.handler.middleware]
            [everest.json.store.middleware :as json.store.middleware]
            [everest.pg.store :as pg.store]
            [everest.pg.schema :as pg.schema]
            [everest.pg.handler.store :as pg.handler.store]
            [everest.pg.handler.middleware :as pg.handler.middleware]
            [everest.pg.notification.poll-notify :as pg.poll-notify]
            [everest.handler.middleware :as handler.middleware]
            [everest.handler :as handler]
            [everest.store :as store]
            [everest.store.middleware :as store.middleware]
            [everest.store.subscription :as subscriptions]

            [clojure.tools.logging :as log]
            [clojure.pprint :as pprint :refer [pprint]]
            [embedded-pg :as pg]
            [hikari-cp.core :as hikari]
            [manifold.stream :as m.s]
            [mount.core :as mount :refer [defstate]]))

(def +pg-config {:host "localhost", :port 3334, :db-name "everest"
                 :user "everest", :password "everest"})

(def +hikari-config
  {:datasource-classname "org.postgresql.ds.PGSimpleDataSource"
   :username "everest"
   :password "everest"
   :database-name "everest"
   :server-name "localhost"
   :port-number 3334
   :pool-name "everest"})

(defstate pg+
  :start (doto (pg/make {:version "9.5.7-1"})
           (pg/start! +pg-config))
  :stop (pg/stop! pg+))

(defstate pool+
  :start (hikari/make-datasource +hikari-config)
  :stop (hikari/close-datasource pool+))

(defstate pg-schema+
  :start (pg.schema/migrate! pool+ {:type "bytea"})
  :stop (pg.schema/drop! pool+))

(defn- wrap-edn-utf8-bytes [store]
  (let [generate-fn #(-> % pr-str (.getBytes "UTF-8"))
        parse-fn #(if (zero? (alength %))
                    {}
                    (-> % (String.) read-string))]
    (store.middleware/wrap-store store
      (fn [e]
        (-> e
            (update :everest.event/data generate-fn)
            (update :everest.event/metadata generate-fn)))
      (fn [e]
        (-> e
            (update :everest.event/data parse-fn)
            (update :everest.event/metadata parse-fn))))))

(defstate store+
  :start (-> (pg.store/create-store pool+ {:everest.pg.store/event-data-type :bytea})
             (wrap-edn-utf8-bytes)
             #_(json.store.middleware/wrap-cheshire-json {})))

(defstate notifications+
  :start (pg.poll-notify/create-notification-source pool+ {})
  :stop (.close notifications+))

(defstate subscriptions+
  :start (subscriptions/create-subscription-source store+
           (pg.poll-notify/notifications notifications+) {})
  :stop (.close subscriptions+))

(defn- event-handler [handler-name handler-fn]
  (-> handler-fn
      (handler.middleware/wrap-state-atom {})
      (json.handler.middleware/wrap-cheshire-json {})
      (pg.handler.middleware/wrap-json-state {})
      (pg.handler.middleware/wrap-state
        {:handler.middleware/catch-up-upsert-batch-size 500
         :handler.middleware/handler-type (name handler-name)
         :handler.middleware/handler-id (name handler-name)})
      (pg.handler.middleware/wrap-tx
        {:handler.middleware/tx-during-catch-up? false})
      (pg.handler.middleware/wrap-connection
        {:handler.middleware/db pool+})))

(defn- init-handler! [id pos]
  (pg.handler.store/update! pool+
    {:pg.handler/type id
     :pg.handler/id (name id)
     :pg.handler/position pos}))

(defstate catch-up-executor+
  :start (java.util.concurrent.Executors/newFixedThreadPool 2)
  :stop (.shutdownNow catch-up-executor+))

(defn- run-with [sub handler]
  (.execute ^java.util.concurrent.Executor catch-up-executor+
    #(m.s/consume handler sub)))

(defn- init-and-subscribe-handler! [f stream type pos]
  (let [type (name type)
        _ (init-handler! type pos)
        sub (pg.handler.store/subscribe subscriptions+ pool+ stream type {})]
    (run-with sub (event-handler type f))
    sub))

(defstate handler-all+
  :start (-> (fn [{:everest.event/keys [stream type position] :as event}]
               (let [{:handler/keys [id state]} event]
                 (log/info id stream "->" type "@" position ">" @state)
                 (swap! state (fnil inc 0))))
             (init-and-subscribe-handler! :all 'TestHandlerAll 0))
  :stop (m.s/close! handler-all+))

(defstate handler-stream+
  :start (-> (fn [{:everest.event/keys [stream type position] :as event}]
               (let [{:handler/keys [id state]} event]
                 (log/info id stream "->" type "@" position ">" @state)
                 (swap! state (fnil inc 0))))
             (init-and-subscribe-handler! "test-stream" 'TestHandlerStream 5))
  :stop (m.s/close! handler-stream+))

(defn- read-all-events []
  (log/info "EVENTS IN STORE>>\n"
            (with-out-str
              (-> (store/read-stream-forward store+ :all nil 10)
                  (pprint)))))

(defn main []
  (try (mount/start)
       (let [all-sub (store/subscribe subscriptions+
                       :all :everest.subscription.events/all {})]
         (m.s/consume (fn [e] (log/info "GOT EVENT>>" (with-out-str (pprint e)))) all-sub)
         (read-all-events)
         (->> (for [i (range 1)]
                {:everest.event/type 'TestEvent
                 :everest.event/data {:string "string"
                                      :int i
                                      :double 1.0
                                      :boolean true
                                      :date (java.util.Date.)}
                 :everest.event/metadata {:user "test-user"}})
              (store/append-events! store+ "test-stream" :everest.stream.version/any))
         ;; allow the notifications to propagate
         (Thread/sleep 1000)
         (read-all-events))
       (finally
         (mount/stop))))
