(ns everest.json.handler.middleware
  (:require [everest.json.cheshire :as json]
            [everest.handler :as handler]))

(defn wrap-cheshire-json
  "Wraps the handler by serializing and deserializing the state under the
  specified `state-key`. Returns the serialized state.

  Accepts the following options:

    * `:state-key` - `:handler/state` by default
    * `:parser` - parser options:
      + `:parse-strict?` - the parsing mode, if true (default) - top level
        arrays will not be parsed lazily
      + `:key-fn` - the function to be applied when deserializing object
        keys, defaults to `keyword`
      + `:date-format` - format string used for date serialization, defaults
        to `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`"
  [handler {:handler.middleware/keys [state-key parser]
            :or {state-key :handler/state}}]
  (let [parse-fn (json/parse-string-fn parser)
        generate-fn (json/generate-string-fn parser)]
    (fn [e]
      (let [state (handler (update e state-key parse-fn))]
        (if (handler/handled? state)
          (when-not (nil? state)
            (generate-fn state))
          (handler/skip))))))
