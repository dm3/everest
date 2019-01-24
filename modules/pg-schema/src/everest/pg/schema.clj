(ns everest.pg.schema
  (:require [clojure.java.io :as io])
  (:import [javax.sql DataSource]
           [org.flywaydb.core Flyway ]
           [org.flywaydb.core.api.configuration FluentConfiguration]))

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
   (let [^FluentConfiguration config (Flyway/configure)
         schema-with-dot (when schema (str schema "."))]
     (if (instance? DataSource db)
       (.dataSource config db)
       (.dataSource config (:url db) (:username db) (:password db) (into-array String [])))
     (.placeholders config {"schema" schema-with-dot
                            "user" user
                            "type" (check-type type)
                            "emptyTypeValue" (get +empty-type-value type)})
     (when schema
       (.schemas config (into-array String [schema])))
     (-> config .load .migrate))))

(defn drop!
  "Drops the Everest objects in the `schema`.

  *WARNING*: this will delete all the data!"
  ([db] (drop! db {}))
  ([db {:keys [schema]}]
   (let [^FluentConfiguration config (Flyway/configure)]
     (if (instance? DataSource db)
       (.dataSource config db)
       (.dataSource config (:url db) (:username db) (:password db) (into-array String [])))
     (when schema
       (.schemas config (into-array String [schema])))
     (-> config .load .clean))))
