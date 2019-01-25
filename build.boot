(def project 'everest/everest)
(def version "0.4.0-alpha4")

(set-env! :resource-paths #{"src"}
          :source-paths   #{"test"}
          :dependencies   '[[org.clojure/clojure          "1.9.0-RC2"]
                            [manifold                     "0.1.7-alpha6"]
                            [org.clojure/tools.namespace  "0.2.11" :scope "test"]
                            [adzerk/boot-test             "1.1.2"  :scope "test"]
                            [adzerk/bootlaces             "0.1.13" :scope "test"]])

(task-options!
 pom {:project     project
      :version     version
      :description "A Clojure/Postgres event store"
      :url         "https://github.com/dm3/everest"
      :scm         {:url "https://github.com/dm3/everest"}
      :license     {"MIT License" ""}})


(require '[adzerk.boot-test :as bt]
         '[adzerk.bootlaces :as b]
         '[boot.parallel :as par]
         '[clojure.java.io :as io]
         '[clojure.tools.namespace.repl :as tnr])

(b/bootlaces! version :dont-modify-paths? true)

(defn latest-parent-dependency []
  [project version])

(def +modules
  {'dsl       {:resource-paths #{"modules/dsl/src"}
               :source-paths   #{"modules/dsl/test"}}
   'pg-schema {:resource-paths #{"modules/pg-schema/resources" "modules/pg-schema/src"}
               :dependencies   '[[org.flywaydb/flyway-core "4.2.0"]]}
   'pg        {:resource-paths #{"modules/pg/src"}
               :source-paths   #{"modules/pg/test"}
               :dependencies   '[[funcool/clojure.jdbc      "0.9.0"]
                                 [org.postgresql/postgresql "42.1.4"]
                                 [org.hsqldb/hsqldb         "2.4.0" :scope "test"]]}
   'json      {:resource-paths #{"modules/json/src"}
               :source-paths   #{"modules/json/test"}
               :dependencies   '[[cheshire "5.8.0"]]}})

(defn env-modules!
  ([] (env-modules! +modules))
  ([modules]
   (doseq [[_ env] modules, [k v] env]
     (merge-env! k v))))

(defn env-module! [m]
  (env-modules! (select-keys +modules [m])))

;; Test

(deftask test []
  (env-modules!)
  (bt/test))

;; Build

(deftask jar-module [m module VAL sym "module"]
  (assert (get +modules module)
          (format "module %s doesn't exist!" module))
  (set-env! :resource-paths #{}
            :source-paths #{}
            :dependencies [(latest-parent-dependency)])
  (let [group-id (-> (str project) (clojure.string/split #"\/") first)
        artifact (str (-> (str project) (clojure.string/split #"\/") last) "." (name module))]
    (task-options!
     pom {:project (symbol group-id artifact)
          :description (format "Everest module '%s'" module)})
    (env-module! module)
    (comp (pom) (jar) (install))))

(deftask install-main-module []
  (comp (pom) (jar) (install)))

(deftask local-install []
  ;; need to run these tasks in serial, and can't work out how
  ;; to do this without spawning new boot processes..
  ;; given the order of post-wrap and comp we need this order for main to
  ;; be installed first! (as the other modules depend upon it!)
  (apply comp (concat (map (fn [[m _]]
                             (with-post-wrap fileset
                               (dosh "boot" "jar-module" "-m" (name m))))
                           +modules)
                      [(with-post-wrap fileset
                         (dosh "boot" "install-main-module"))])))

;;; Release

(deftask push-release []
  (push :repo "clojars"
        :ensure-release true
        :ensure-clean false))

(deftask push-main-release []
  (comp (pom) (jar) (install) (push-release)))

(deftask push-module-release [m module VAL sym "module"]
  (comp (jar-module :module module)
        (push-release)))

(deftask push-releases []
  ;; need to run these tasks in serial, and can't work out how
  ;; to do this without spawning new boot processes..
  ;; given the order of post-wrap and comp we need this order for main to
  ;; be pushed first! (as the other modules depend upon it!)
  (apply comp (concat (map (fn [[m _]]
                             (with-post-wrap fileset
                               (dosh "boot" "push-module-release" "-m" (name m))))
                           +modules)
                      [(with-post-wrap fileset
                         (dosh "boot" "push-main-release"))])))

;; Repl

(defn init-repl []
  (set! *warn-on-reflection* true))

(deftask repl-dev []
  (env-modules!)
  (repl :eval `(init-repl)))

;; Examples

(def +examples
  {'pg {:resource-paths #{"examples/pg/src"
                          "examples/pg/resources"}
        :checkouts      [['everest                         version]
                         ['everest.module/pg               version]
                         ['everest.module/pg-schema        version]
                         ['everest.module/json             version]]
        :dependencies   [['everest                         version]
                         ['everest.module/pg               version]
                         ['everest.module/pg-schema        version]
                         ['everest.module/json             version]
                         ['mount                           "0.1.11"]
                         ['hikari-cp                       "1.7.6"]
                         ['ru.yandex.qatools.embed/postgresql-embedded "2.4"]
                         ['ch.qos.logback/logback-classic  "1.2.3"]]}})

(defn env-example! [e]
  (doseq [[k v] (get +examples e)]
    (merge-env! k v)))

(deftask repl-example [e example VAL sym "example"]
  (set-env! :resource-paths #{}
            :source-paths   #{}
            :dependencies   [])
  (env-example! example)
  (repl))
