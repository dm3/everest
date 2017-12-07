(ns everest.store.subscription
  (:require [clojure.tools.logging :as log]
            [manifold
             [stream :as m.s]
             [deferred :as m.d]
             [time :as m.t]
             [bus :as m.b]
             [executor :as m.e]]
            [everest.store :as store]
            [everest.event :as event]
            [everest.event.proto :as event.proto]
            [everest.store.proto :as store.proto]
            [everest.spec :as spec :refer [ex verify!]]))

(def ^:private +default-opts
  {:read-batch-size 1000
   :max-publish-time-ms (m.t/minutes 5)})

(defn- read-all-stream [store position cnt]
  (store/read-stream-forward store :all position cnt))

(defn- read-events-until
  "Reads events from `start` to `stop` inclusive. If `stop` is nil - reads
  until the end of stream."
  [f store start stop {:keys [read-batch-size]}]
  (log/debugf "Reading events from %s to %s..." start (if (nil? stop) "end" stop))
  (verify! (not (and stop (neg? (compare stop start))))
    "Cannot read events where stop < start: %s < %s!" stop start)
  (if (= start stop)
    (m.d/success-deferred start)
    (let [read-all #(read-all-stream store % read-batch-size)
          nxt #(:everest.stream-slice/next %)
          pos #(:everest.event/position %)
          continue? #(or (not stop) (not (neg? (compare stop (nxt %)))))
          up-to-stop #(not (neg? (compare stop (pos %))))
          failed #(ex-info "Could not process the read-events result" {:result %})]
      (m.d/loop [[previous-start result] [start (read-all start)]]
        (if-let [events (seq (:everest.stream-slice/events result))]
          (if (continue? result)
            (-> (f events)
                (m.d/chain
                  (fn [ok?]
                    (if ok?
                      (m.d/recur [(nxt result) (read-all (nxt result))])
                      (throw (failed result))))))
            (let [[to-take [r :as to-drop]] (split-with up-to-stop events)]
              (-> (f to-take)
                  (m.d/chain
                    (fn [ok?]
                      (if ok?
                        (pos r)
                        (throw (failed result))))))))
          previous-start)))))

(defn- read-events-until-deferred [stream store start stop opts]
  (read-events-until #(m.s/put-all! stream %) store start stop opts))

(defn- read-and-publish [bus store start opts]
  (let [f (fn [es]
            (m.d/loop [e (first es), more (rest es)]
              (if e
                (-> (m.b/publish! bus ::all e)
                    (m.d/chain
                      (fn [_] (m.d/recur (first more) (rest more)))))
                true)))]
    (read-events-until f store start nil opts)))

(defn- subscribe* [store bus stream-id start opts]
  (let [dst (m.s/stream)
        subscribe-to-new-only? (= start :everest.subscription.events/new)
        live-stream #(m.b/subscribe bus ::all)
        matches? #(or (= :all stream-id)
                      (#{stream-id :everest.stream/transient} (:everest.event/stream %)))]

    (m.s/on-closed dst
      #(log/debugf "Subscription to stream %s from %s is closed." stream-id start))

    (if subscribe-to-new-only?
      (-> (m.s/put-all! dst
            [(event/subscription-started) (event/live-processing-started)])
          (m.d/chain
            (fn [_] (m.s/connect (live-stream) dst))))

      (let [!caught-up? (volatile! false)
            catch-up #(read-events-until-deferred dst store %1 %2 opts)
            caught-up! (fn []
                         ;; TODO: live-processing-started event will not arrive until
                         ;; an event appears in the live stream.
                         ;; Ideally, it'd arrive as soon as we're done catching up.
                         (-> (m.s/put! dst (event/live-processing-started))
                             (m.d/chain (fn [_] (vreset! !caught-up? true)))))]

        (-> (m.s/put! dst (event/subscription-started))
            (m.d/chain
              (fn [_] (catch-up start nil))
              (fn [caught-up-to-pos]
                (m.s/connect-via (live-stream)
                  (fn [{:everest.event/keys [position] :as e}]
                    (if @!caught-up?
                      (m.s/put! dst e)
                      (cond (neg? (compare position caught-up-to-pos))
                            (m.d/success-deferred true)

                            (= position caught-up-to-pos)
                            (-> (caught-up!)
                                (m.d/chain
                                  (fn [_] (m.s/put! dst e))))

                            (pos? (compare position caught-up-to-pos))
                            ;; this puts all events up to and including `e` into `dst`
                            (-> (catch-up caught-up-to-pos position)
                                (m.d/chain
                                  (fn [result]
                                    (-> (caught-up!)
                                        (m.d/chain
                                          (fn [did-put?]
                                            (and did-put? (boolean result)))))))))))
                  dst))) )))

    (let [closeable-result (m.s/stream)]
      (-> (->> (m.s/filter matches? dst)
               (m.s/map #(assoc % :everest.event/this-stream dst)))
          (m.s/connect closeable-result))
      closeable-result)))

;; Impl

(defn- check-position [pos]
  (cond (and (number? pos) (not (neg? pos))) pos

        (or (nil? pos) (= pos :everest.subscription.events/new))
        :everest.subscription.events/new

        :everest.subscription.events/all -1
        :else (throw (ex "Invalid subscription position: %s!" pos))))

(defn- stop-bus! [bus]
  (doseq [[_ ss] (m.b/topic->subscribers bus), s ss]
    ;; TODO: events might still arrive after the subscription/stopped event
    ;; as there's nothing taking a write lock of the streams during the `stop-bus!` call
    (-> (m.s/put! s (event/subscription-stopped))
        (m.d/chain (fn [_] (m.s/close! s))))))

(deftype SubscriptionSource [bus store]
  store.proto/ISubscriptionSource
  (-subscribe [_ stream-id start opts]
    (let [stream-id (spec/normalize-identifier stream-id)
          position (check-position start)]
      (subscribe* store bus
        stream-id
        (case position
          :everest.subscription.events/new position
          (event.proto/-next position))
        (merge opts +default-opts))))

  java.io.Closeable
  (close [_]
    (stop-bus! bus)))

(def ^:private +default-config
  (merge +default-opts
    {:subscription-buffer-size 500}))

(defn create-subscription-source
  "Creates a subscription source given an event `store`.

  This new events will be read only when a notification appears in the
  `notification-stream`.

  Currently the subscriptions returned from the source do not get closed when
  the source is abandoned. They will continue to be open but no new events will arrive."
  [store notification-stream config]
  (let [config (merge +default-config config)
        last-event (store/read-event store :all :everest.event.position/last)
        ;; Manifold Bus is a simple way to keep track of subscriptions.
        ;; However, it does not allow to have subscriptions which work with a
        ;; different consumption rate as the slowest subscription will delay
        ;; all the faster ones.
        ;; TODO: implement a subscription manager for better control.
        bus (m.b/event-bus #(m.s/stream (:subscription-buffer-size config)))]

    ;; subscription starts as soon as `notifications` start appearing. The events are
    ;; published into the bus. Publish is a noop when there are no active subscriptions.
    (m.d/loop [last-position (:everest.event/position last-event)]
      (-> (m.s/take! notification-stream ::done)
          (m.d/chain
            (fn [notification]
              (if-not (identical? notification ::done)
                (read-and-publish bus store last-position config)
                (stop-bus! bus)))
            (fn [next-position]
              (if next-position
                (m.d/recur next-position)
                (stop-bus! bus))))
          (m.d/catch
            (fn [e]
              (log/error e "Could not process a notification, stopping...")
              (stop-bus! bus)))))

    (SubscriptionSource. bus store)))
