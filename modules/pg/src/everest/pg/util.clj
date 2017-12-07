(ns everest.pg.util
  (:require [clojure.string :as string]))

(defn ->identifier [x]
  (-> x
      (string/lower-case)
      (.replace \_ \-)))
