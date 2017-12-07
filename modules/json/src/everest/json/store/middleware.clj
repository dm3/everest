(ns everest.json.store.middleware
  (:require [everest.json.cheshire :as json]
            [everest.store.middleware :as store.middleware]))

(defn wrap-cheshire-json
  "Wraps the store into Json-serializing middleware using Cheshire.

  Accepts the following options:

    * `:parse-strict?` - the parsing mode, if true (default) - top level arrays will not be parsed lazily
    * `:key-fn` - the function to be applied when deserializing object keys, defaults to `keyword`
    * `:date-format` - format string used for date serialization, defaults to `yyyy-MM-dd'T'HH:mm:sss'Z'`"
  [store opts]
  (let [parse-fn (json/parse-string-fn opts)
        generate-fn (json/generate-string-fn opts)]
    (store.middleware/wrap-store store
      (fn [e]
        (-> e
            (update :everest.event/data generate-fn)
            (update :everest.event/metadata generate-fn)))
      (fn [e]
        (-> e
            (update :everest.event/data parse-fn)
            (update :everest.event/metadata parse-fn))))))
