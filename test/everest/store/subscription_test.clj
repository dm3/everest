(ns everest.store.subscription-test
  (:require [clojure.test :refer :all]
            [manifold.deferred :as m.d]
            [manifold.stream :as m.s]
            [everest.store :as store]
            [everest.store.proto :as store.proto]
            [everest.memory.store :as mem-store]

            [everest.store.subscription :as sut]))

(def sub-opts
  {:read-batch-size 100})

(defn- test-event [d & {:keys [pos stream]}]
  (let [e {:everest.event/type (str "test-" d)
           :everest.event/data (format "test=%s" d)
           :everest.event/metadata "test"}]
    (if pos
      (assoc e :everest.event/position pos
               :everest.event/stream stream)
      e)))

(defn- next-from [m s]
  (let [e @(m.s/try-take! s ::closed 10 ::timeout)]
    (if (vector? m)
      ((juxt :everest.event/position :everest.event/type) e)
      e)))

(defmacro is=next-event [m s]
  `(is (= ~m (next-from ~m ~s))))

(defn- internal_read-events [& args]
  (apply #'sut/read-events-until-deferred args))

(defn- append-any! [store stream events]
  (store/append-events! store stream
    :everest.stream.version/any events))

(def ^:private +!notification-cnt (atom 0))

(defn- notify! [n]
  @(m.s/put! n {::test-id (swap! +!notification-cnt inc)}))

(defn- test-store-subscription []
  (let [n (m.s/stream)
        store (mem-store/create)
        sub (sut/create-subscription-source store n {})
        !result (atom [])
        next-raw (fn [] (let [e (first @!result)]
                          (swap! !result (comp vec rest))
                          e))]
    {:no-next-event? #(empty? @!result)
     :next-event-raw next-raw
     :next-event (fn [] ((juxt :everest.event/position :everest.event/type)
                         (next-raw)))
     :into-result (fn [sub] (m.s/consume #(swap! !result conj %) sub) sub)
     :sub sub
     :store store
     :n n}))

(deftest subscribes
  (testing "reads"
    (let [store (mem-store/create)
          stream (m.s/stream)
          !results (atom [])]
      (m.s/consume #(swap! !results conj %) stream)

      (is (= 2 @(internal_read-events stream store 0 nil sub-opts)))
      (is (= 1 (count @!results)))
      (reset! !results [])

      (testing "additional events to the end"
        (append-any! store "test-stream"
          (map #(test-event (str "event-" %)) (range 10)))

        (is (= 12 @(internal_read-events stream store 2 nil (assoc sub-opts :read-batch-size 2))))
        (is (= 10 (count @!results)))
        (is (= 2 (:everest.event/position (first @!results))))
        (is (= 11 (:everest.event/position (last @!results))))
        (reset! !results []))

      (testing "doesn't read any more events when already at end"
        (is (= 12 @(internal_read-events stream store 12 nil (assoc sub-opts :read-batch-size 2)))))

      (testing "part of events"
        (append-any! store "test-stream"
          (map #(test-event (str "event-" %)) (range 10)))

        (is (= 19 @(internal_read-events stream store 12 18 (assoc sub-opts :read-batch-size 5))))
        (is (= 7 (count @!results)))
        (is (= 12 (:everest.event/position (first @!results))))
        (is (= 18 (:everest.event/position (last @!results))))
        (reset! !results []))))

  (testing "subscribes to events in :all stream"
    (let [{:keys [n store sub into-result next-event no-next-event?]}
          (test-store-subscription)]

      (testing ", gets initial event in subscription"
        (let [s (-> (store/subscribe sub :all :everest.subscription.events/all sub-opts)
                    (into-result))]

          (is (= [nil :everest.subscription/started] (next-event)))
          (is (= [1 "everest-initialized"] (next-event)))

          (m.s/close! s)))

      (testing ", processes events when no subscriptions present"
        (append-any! store "test-stream" [(test-event "a")])
        (notify! n))

      (testing ", gets new events live"
        (let [s (-> (store/subscribe sub :all :everest.subscription.events/new sub-opts)
                    (into-result))]

          (is (= [nil :everest.subscription/started] (next-event)))
          (is (= [nil :everest.subscription/live-processing-started] (next-event)))
          (is (no-next-event?))

          (append-any! store "test-stream" [(test-event "a")])
          (is (no-next-event?))

          (notify! n)
          (is (= [3 "test-a"] (next-event)))

          (append-any! store "test-stream" [(test-event "a")])
          (append-any! store "test-stream" [(test-event "b")])
          (append-any! store "test-stream" [(test-event "c")])
          (is (no-next-event?))

          (notify! n)
          (is (= [4 "test-a"] (next-event)))
          (is (= [5 "test-b"] (next-event)))
          (is (= [6 "test-c"] (next-event)))

          (testing "doesn't matter if multiple notifications received for events already consumed"
            (notify! n)
            (notify! n)

            (is (no-next-event?)))

          (m.s/close! s)))

      (testing ", multiple subscriptions"
        (let [s1 (-> (store/subscribe sub :all :everest.subscription.events/new sub-opts)
                     (into-result))
              s2 (-> (store/subscribe sub :all :everest.subscription.events/new sub-opts)
                     (into-result))]

          (is (= [nil :everest.subscription/started] (next-event)))
          (is (= [nil :everest.subscription/live-processing-started] (next-event)))
          (is (= [nil :everest.subscription/started] (next-event)))
          (is (= [nil :everest.subscription/live-processing-started] (next-event)))

          (append-any! store "test-stream" [(test-event "a")])

          (notify! n)
          (is (= [7 "test-a"] (next-event)))
          (is (= [7 "test-a"] (next-event)))

          (m.s/close! s1)
          (m.s/close! s2)))

      (testing ", subscribes from a given position"
        (let [s (-> (store/subscribe sub :all 6 sub-opts)
                    (into-result))]

          (is (= [nil :everest.subscription/started] (next-event)))
          (is (= [7 "test-a"] (next-event)))
          (is (no-next-event?))

          (append-any! store "test-stream" [(test-event "b")])
          (notify! n)

          (is (= [nil :everest.subscription/live-processing-started] (next-event)))
          (is (= [8 "test-b"] (next-event)))

          (m.s/close! s)))))

  (testing "subscribes to events in a specific stream"
    (let [{:keys [n store sub into-result next-event no-next-event?]}
          (test-store-subscription)]

      (testing ", from the beginning; initial event filtered out"
        (let [s (-> (store/subscribe sub "test-stream" :everest.subscription.events/new sub-opts)
                    (into-result))]

          (append-any! store "test-stream" [(test-event "a")])
          (notify! n)

          (is (= [nil :everest.subscription/started] (next-event)))
          (is (= [nil :everest.subscription/live-processing-started] (next-event)))
          (is (= [2 "test-a"] (next-event)))

          (m.s/close! s)))

      (testing "from the specified position"
        (append-any! store "other-stream"       [(test-event "b1")])
        (append-any! store "yet-another-stream" [(test-event "b2")])
        (append-any! store "test-stream"        [(test-event "b3")])
        (notify! n)

        (let [s (-> (store/subscribe sub "test-stream" 3 sub-opts)
                    (into-result))]

          (is (= [nil :everest.subscription/started] (next-event)))
          (is (= [5 "test-b3"] (next-event)))

          (m.s/close! s))))))

(deftest attaches-writable-stream-to-events
  (let [{:keys [n store sub into-result next-event-raw next-event no-next-event?]} (test-store-subscription)]
    (let [s (-> (store/subscribe sub :all 0 sub-opts)
                (into-result))
          event (next-event-raw)]

      (is (= [1 "everest-initialized"] (next-event)))

      (notify! n)
      @(m.s/put! (:everest.event/this-stream event)
        {:everest.event/type :transient-event
         :everest.event/stream :everest.stream/transient})

      (is (= [nil :transient-event] (next-event)))

      (is (no-next-event?))

      (m.s/close! s))))

(deftest read-before-subscribe
  (let [{:keys [n store sub into-result next-event no-next-event?]} (test-store-subscription)]

    (append-any! store "test-stream" [(test-event "a")])

    (let [s (-> (store/subscribe sub :all 0 sub-opts)
                (into-result))]

      (is (= [nil :everest.subscription/started] (next-event)))
      (is (= [1 "everest-initialized"] (next-event)))
      (is (= [2 "test-a"] (next-event)))

      (append-any! store "test-stream" [(test-event "b")])
      (notify! n)

      (is (= [nil :everest.subscription/live-processing-started] (next-event)))
      (is (= [3 "test-b"] (next-event)))
      (is (no-next-event?))

      (m.s/close! s))))

(defn- in-thread-named [tname f]
  (let [l (java.util.concurrent.CountDownLatch. 1)]
    (doto (Thread. #(try (f) (finally (.countDown l))))
      (.setName tname)
      (.start))
    l))

(deftest threading
  (let [n (m.s/stream)
        store (mem-store/create)
        sub (sut/create-subscription-source store n {})

        !s1 (promise)

        !result (atom [])
        next-event #(let [e (first @!result)] (swap! !result rest) e)]

    (-> (in-thread-named "SUBSCRIBE"
          #(deliver !s1 (store/subscribe sub :all 0 {})))
        (.await))

    (-> (in-thread-named "CONSUME"
          (fn []
            (m.s/consume
              (fn [e]
                (swap! !result conj
                  [(.getName (Thread/currentThread)) (:everest.event/type e)]))
              @!s1)))
        (.await))

    (is (= ["CONSUME" :everest.subscription/started] (next-event)))

    (-> (in-thread-named "APPEND"
          #(append-any! store "test-stream" [(test-event "a")]))
        (.await))
    (-> (in-thread-named "NOTIFY"
          (fn [] @(m.s/put! n {::test-id ::notification})))
        (.await))

    (is (= ["NOTIFY" "test-a"] (next-event)))
    (is (= ["NOTIFY" :everest.subscription/live-processing-started] (next-event)))))
