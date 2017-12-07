(ns everest.memory.store
  (:require [everest.store.proto :as store.proto]
            [everest.spec :as spec :refer [ex]]
            [everest.event :as event]))

(defn- read-forward* [stream-id events start cnt]
  (let [start (spec/check-position (spec/init-position start :everest.read-direction/forward))
        cnt (spec/check-limit cnt)]
    (->> events
         (drop-while #(< (:everest.event/position %) start))
         (take (or cnt Long/MAX_VALUE))
         (event/events->stream-slice :everest.read-direction/forward stream-id))))

(defn- read-backward* [stream-id events start cnt]
  (let [start (spec/check-position (spec/init-position start :everest.read-direction/backward))
        cnt (spec/check-limit cnt)]
    (->> (reverse events)
         (take-while #(or (= :everest.event.position/last start)
                          (<= (:everest.event/position %) start)))
         (take (or cnt Long/MAX_VALUE))
         (event/events->stream-slice :everest.read-direction/backward stream-id))))

(defn- all-events [streams]
  (->> streams
       (reduce-kv (fn [all _ events]
                    (concat all events)) [])
       (sort-by :everest.event/position)))

(defn- read* [f streams stream-id start cnt]
  (let [stream-id (spec/normalize-identifier stream-id)]
    (if (= :all stream-id)
      (f stream-id (all-events streams) start cnt)
      (when-let [events (get streams stream-id)]
        (f stream-id events start cnt)))))

(defn- ->stored-event [stream-id timestamp {:keys [pos es] :as res} e]
  (let [new-pos (inc pos)
        e (assoc e :everest.event/position new-pos
                   :everest.event/stream stream-id)]
    {:es (conj es (if timestamp (assoc e :everest.event/timestamp timestamp) e))
     :pos new-pos}))

(defn- append-events [streams stream-id pos-ref events {:keys [timestamps?]}]
  (let [timestamp (java.util.Date.)]
    (dosync
      (let [{:keys [pos es]}
            (reduce (partial ->stored-event stream-id (when timestamps? timestamp))
                    {:pos @pos-ref, :es []} events)]
        (ref-set pos-ref pos)
        (alter streams update stream-id (fnil concat []) es)))))

(defn create
  ([] (create {}))
  ([{:keys [timestamps?] :as opts :or {timestamps? false}}]
   (let [streams (ref {})
         last-position (ref 0)]
     (append-events streams "$$internal" last-position
                    [{:everest.event/type "everest-initialized"
                      :everest.event/data ""
                      :everest.event/metadata ""}] opts)

     (reify store.proto/IEventStore
       (-read-event [_ stream-id position]
         (let [position (-> position
                            (spec/init-position :everest.read-direction/forward)
                            (spec/check-position))
               stream-id (spec/normalize-identifier stream-id)
               events (if (= :all stream-id)
                        (all-events @streams)
                        (get @streams stream-id))]
           (if (= :everest.event.position/last position)
             (last events)
             (->> events
                  (filter #(= (:everest.event/position %) position))
                  (first)))))

       (-read-stream-forward [_ stream-id start cnt]
         (read* read-forward* @streams stream-id start cnt))

       (-read-stream-backward [_ stream-id start cnt]
         (read* read-backward* @streams stream-id start cnt))

       (-delete-stream! [_ stream-id expected-version]
         (let [stream-id (spec/normalize-identifier stream-id)
               expected-version (spec/check-version expected-version)]
           (when (#{-1 :everest.stream.version/not-present} expected-version)
             (throw (ex "Cannot delete while expecting no stream to exist!")))
           (when (= stream-id :all)
             (throw (ex "Cannot delete the :all stream!")))
           (dosync
             (alter streams
                    #(if-let [s (get % stream-id)]
                       (cond
                         (#{-2 :everest.stream.version/any} expected-version)
                         (dissoc % stream-id)

                         (= (spec/version->position expected-version)
                            (:everest.event/position (last s)))
                         (dissoc % stream-id)

                         :else (throw (ex "Wrong expected version: %s, got: %s" expected-version
                                          (:everest.event/position (last s)))))
                       %)))))

       (-append-events! [_ stream-id expected-version events]
         (let [stream-id (spec/normalize-identifier stream-id)
               expected-version (spec/check-version expected-version)
               events (spec/check-events events)]
           (when (= stream-id :all)
             (throw (ex "Cannot append to the :all stream!")))
           (dosync
             (if-let [s (get @streams stream-id)]
               (cond
                 (#{-1 :everest.stream.version/not-present} expected-version)
                 (throw (ex "Expected no stream, got: %s" (pr-str s)))

                 (#{-2 :everest.stream.version/any} expected-version)
                 (append-events streams stream-id last-position events opts)

                 (= (spec/version->position expected-version)
                    (:everest.event/position (last s)))
                 (append-events streams stream-id last-position events opts)

                 :else (throw (ex "Wrong expected version: %s <> %s!"
                                  expected-version (:everest.event/position (last s)))))
               (cond
                 (#{-1 :everest.stream.version/not-present,
                    -2 :everest.stream.version/any} expected-version)
                 (append-events streams stream-id last-position events opts)

                 :else (throw (ex "Wrong expected version: %s! Stream %s does not exist!"
                                  expected-version stream-id))))
             {:everest.append/next-expected-version @last-position})))))))
