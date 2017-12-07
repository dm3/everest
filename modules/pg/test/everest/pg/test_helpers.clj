(ns everest.pg.test-helpers
  (:require [jdbc.core :as jdbc]
            [everest.pg.dynamic :as dynamic]))

(def +hsql-config {:subprotocol "hsqldb"
                   :subname "mem:test"
                   :username "sa"
                   :password ""})

(def ^:dynamic *connection*)

(defn drop-table [conn tbl]
  (jdbc/execute conn (format "DROP TABLE %s" tbl)))

(defn wrap-connection [handler]
  (fn [e]
    (binding [dynamic/*connection* *connection*]
      (handler e))))

(defn table-contents [tbl]
  (jdbc/fetch *connection* (format "SELECT * FROM %s" tbl)))

(defn event= [a b]
  (= (dissoc a :everest.event/position :everest.event/timestamp :everest.event/stream)
     (dissoc b :everest.event/position :everest.event/timestamp :everest.event/stream)))

(defn with-table-fixture [table sql]
  (fn [f]
    (with-open [conn (jdbc/connection +hsql-config)]
      (jdbc/execute conn sql)
      (binding [*connection* conn]
        (try
          (f)
          (finally
            (with-open [conn (jdbc/connection +hsql-config)]
              (drop-table conn table))))))))
