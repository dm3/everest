(ns everest.pg.schema
  (:require [clojure.java.io :as io])
  (:import [javax.sql DataSource]
           [org.flywaydb.core Flyway]))

(def ^:private +allowed-types #{"bytea" "json" "jsonb"})

(def ^:private +empty-type-value
  {"bytea" "''"
   "json" "'{}'"
   "jsonb" "'{}'"})

(defn- check-type [t]
  (when-not (+allowed-types t)
    (throw (ex-info (format "Can only store event data/metadata in %s, got %s!" +allowed-types t) {})))
  t)

(defn migrate!
  "Initializes the Everest in a new database `schema` authorized for `user`.

  Performs all of the migrations in the `pg-schema` package.

    (migrate! jdbc-data-source {})"
  ([db] (migrate! db {}))
  ([db {:keys [schema user type] :or {user "everest", type "jsonb"}}]
   (let [^Flyway flyway (Flyway.)
         schema-with-dot (when schema (str schema "."))]
     (if (instance? DataSource db)
       (.setDataSource flyway db)
       (.setDataSource flyway (:url db) (:username db) (:password db) (into-array String [])))
     (doto flyway
       (.setPlaceholders {"schema" schema-with-dot
                          "user" user
                          "type" (check-type type)
                          "emptyTypeValue" (get +empty-type-value type)}))
     (when schema
       (.setSchemas flyway (into-array String [schema])))
     (.migrate flyway))))

(defn drop!
  "Drops the Everest objects in the `schema`.

  *WARNING*: this will delete all the data!"
  ([db] (drop! db {}))
  ([db {:keys [schema]}]
   (let [^Flyway flyway (Flyway.)]
     (if (instance? DataSource db)
       (.setDataSource flyway db)
       (.setDataSource flyway (:url db) (:username db) (:password db) (into-array String [])))
     (when schema
       (.setSchemas flyway (into-array String [schema])))
     (.clean flyway))))
