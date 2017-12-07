(ns everest.pg.notification.poll-notify
  (:require [clojure.tools.logging :as log]
            [jdbc
             [core :as jdbc]
             [proto :as jdbc-proto]]
            [manifold
             [stream :as m.s]
             [time :as m.t]
             [bus :as m.b]])
  (:import [org.postgresql PGConnection PGNotification]))

(deftype PostgresPushNotificationSource [notification-bus cancel]
  java.io.Closeable
  (close [_] (cancel)))

(alter-meta! #'->PostgresPushNotificationSource assoc :private true)

(defn- notification->map [^PGNotification n]
  {:everest.pg.notification/name      (.getName n)
   :everest.pg.notification/pid       (.getPID n)
   :everest.pg.notification/parameter (.getParameter n)})

(defn- try-parse-long [x]
  (try
    (Long/parseLong x)
    (catch NumberFormatException _
      nil)))

(defn- pg-listen!
  "Listens to the Postgres everest notifications submitted via `pg_notify` and
  puts the notifications in the `sink`. Will stop listening if any exception is
  thrown."
  [db {:everest.pg.notification/keys [poll-interval-ms notification-buffer-size]}]
  (let [bus (m.b/event-bus #(m.s/stream notification-buffer-size))
        conn-ref (atom nil)
        cancel (m.t/every poll-interval-ms
                 (fn []
                   (try
                     (when-not @conn-ref
                       (let [conn (jdbc/connection db)]
                         (reset! conn-ref conn)
                         (jdbc/execute conn "LISTEN new_events_after")))
                     (let [conn @conn-ref
                           ^PGConnection pg-conn
                           (.unwrap (jdbc-proto/connection conn) PGConnection)]
                       ;; (jdbc/fetch) doesn't close the statement, nor
                       ;; ResultSet. This, together with the fact the the
                       ;; notification connection is not released for a long
                       ;; time creates a leak of open PreparedStatement
                       ;; objects.
                       (with-open [^java.sql.PreparedStatement stmt
                                   (.prepareStatement pg-conn "select 1")]
                         (.executeQuery stmt))
                       (when-let [{:everest.pg.notification/keys [parameter] :as n}
                                  (->> (.getNotifications pg-conn)
                                       (map notification->map)
                                       (filter #(= (:everest.pg.notification/name %) "new_events_after"))
                                       (last))]
                         @(m.b/publish! bus :all
                            {:everest.pg.notification/probable-next-position
                             (try-parse-long parameter)})))
                     (catch Throwable t
                       (log/error t "Error while listening to PG notifications!")
                       (try
                         (when-let [conn @conn-ref]
                           (reset! conn-ref nil)
                           (jdbc/execute conn "UNLISTEN new_events_after")
                           (.close conn))
                         (catch Exception e
                           (log/error e "Could not unlisten and close connection!")))))))]
    {:notification-bus bus
     :cancel-pg-listener cancel}))

(def ^:private +default-config
  {:everest.pg.notification/poll-interval-ms 500
   :everest.pg.notification/notification-buffer-size 100})

(defn create-notification-source
  "Creates a notification source using Postgres LISTEN/NOTIFY.

  Should be used in a `with-open` or `close`d after use."
  [db config]
  (let [{:keys [cancel-pg-listener notification-bus]}
        (pg-listen! db (merge +default-config config))]
    (PostgresPushNotificationSource. notification-bus cancel-pg-listener)))

(defn notifications
  "Given a notification `source` returns a (manifold) stream of notification
  messages containing:

    * `everest.pg.notification/probable-next-position` - indication of a next
      position in the stream that should be read from in order to get latest
      events. As the name indicates the actual position might have advanced farther
      since the notification has been sent."
  [^PostgresPushNotificationSource source]
  (m.b/subscribe (.notification-bus source) :all))

(defn close! [^PostgresPushNotificationSource source]
  (.close source))
