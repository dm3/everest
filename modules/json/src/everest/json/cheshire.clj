(ns everest.json.cheshire
  (:require [cheshire.core :as json]))

(defn parse-string-fn
  [{:keys [parse-strict? key-fn] :or {parse-strict? true, key-fn keyword}}]
  (let [f (if parse-strict? json/parse-string-strict json/parse-string)]
    (fn [x] (f x key-fn))))

(defn generate-string-fn [opts]
  (fn [x] (json/generate-string x
            (merge {:date-format "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"} opts))))
