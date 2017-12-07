(ns embedded-pg
  (:import [ru.yandex.qatools.embed.postgresql EmbeddedPostgres]
           [de.flapdoodle.embed.process.distribution IVersion]
           [ru.yandex.qatools.embed.postgresql.util SocketUtil]))

(defn- ->iversion [version]
  (if (instance? IVersion version)
    version
    (reify IVersion
      (asInDownloadPath [_]
        (str version)))))

(defn ^EmbeddedPostgres make
  "See [IVersion definitions](https://github.com/yandex-qatools/postgresql-embedded/blob/master/src/main/java/ru/yandex/qatools/embed/postgresql/distribution/Version.java) for more info.

  * version - a version string or an IVersion, e.g. 9.5.7-1
  * data-dir - name of the directory e.g. /tmp/data"
  [{:keys [version data-dir]}]
  (EmbeddedPostgres. (->iversion version) data-dir))

(defn start!
  [^EmbeddedPostgres pg
   {:keys [runtime-config host port db-name user password params]
    :or {runtime-config (EmbeddedPostgres/defaultRuntimeConfig)
         host "localhost", port (SocketUtil/findFreePort)
         db-name "postgres", user "postgres", password "postgres"
         params ["-E"  "SQL_ASCII" "--locale=C" "--lc-collate=C" "--lc-ctype=C"]}}]
  (.start pg host port db-name user password params))

(defn stop! [^EmbeddedPostgres pg]
  (.stop pg))
