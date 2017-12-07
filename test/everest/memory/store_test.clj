(ns everest.memory.store-test
  (:require [clojure.test :refer :all]
            [everest.event :as event]
            [everest.store :as store]
            [everest.memory.store :as sut]))

(defn- test-event [d & {:keys [pos stream]}]
  (let [e {:everest.event/type "test"
           :everest.event/data (format "test=%s" d)
           :everest.event/metadata "test"}]
    (if pos
      (assoc e :everest.event/position pos
               :everest.event/stream stream)
      e)))

(def initial-event
  {:everest.event/position 1
   :everest.event/type "everest-initialized"
   :everest.event/stream "$$internal"
   :everest.event/data ""
   :everest.event/metadata ""})

(defn- forward [& args]
  (:everest.stream-slice/events (apply store/read-stream-forward args)))

(defn- backward [& args]
  (:everest.stream-slice/events (apply store/read-stream-backward args)))

(deftest memory-event-store
  (let [store (sut/create)]
    (testing "initialized"
      (is (= (store/read-event store :all :everest.event.position/last)
             (store/read-event store :all 1)
             (store/read-event store :all nil)
             (store/read-event store "$$internal" 1)
             (first (forward store :all nil nil))
             initial-event)))

    (testing "append and delete events"
      (is (thrown? Exception (store/append-events! store :all :everest.stream.version/any [(test-event 1)])))
      (is (thrown? Exception (store/append-events! store "new" 1 [(test-event 1)])))

      (store/append-events! store "test-stream-1" :everest.stream.version/not-present [(test-event 1)])
      (store/append-events! store "test-stream-2" :everest.stream.version/any [(test-event 2)])

      (testing ", then read forward"
        (is (= (rest (forward store :all nil nil))
               (concat (forward store "test-stream-1" nil nil)
                       (forward store "test-stream-2" nil nil))
               [(test-event 1 :pos 2 :stream "test-stream-1")
                (test-event 2 :pos 3 :stream "test-stream-2")])))

      (testing ", then read backwards"
        (is (= (take 2 (backward store :all nil nil))
               (concat (backward store "test-stream-2" nil nil)
                       (backward store "test-stream-1" nil nil))
               [(test-event 2 :pos 3 :stream "test-stream-2")
                (test-event 1 :pos 2 :stream "test-stream-1")])))

      (store/append-events! store "test-stream-2" 3 [(test-event 3)])
      (is (thrown? Exception (store/append-events! store "test-stream-2" 3 [(test-event 3)])))

      (testing ", reads forward from further position"
        (is (= (event/stream-slice
                 {:direction :everest.read-direction/forward, :from 4, :next 5
                  :stream "test-stream-2"
                  :events [(test-event 3 :pos 4 :stream "test-stream-2")]})
               (store/read-stream-forward store "test-stream-2" 4 nil))))

      (testing ", reads forward the specified number of events"
        (is (= (event/stream-slice
                 {:direction :everest.read-direction/forward, :from 3, :next 4
                  :stream "test-stream-2"
                  :events [(test-event 2 :pos 3 :stream "test-stream-2")]})
               (store/read-stream-forward store "test-stream-2" 1 1))))

      (testing ", reads backward"
        (is (= (event/stream-slice
                 {:direction :everest.read-direction/backward, :from 4, :next 2
                  :stream "test-stream-2"
                  :events [(test-event 3 :pos 4 :stream "test-stream-2")
                           (test-event 2 :pos 3 :stream "test-stream-2")]})
               (store/read-stream-backward store "test-stream-2" 4 nil)
               (store/read-stream-backward store "test-stream-2" nil nil)
               (store/read-stream-backward store "test-stream-2" :everest.event.position/last nil)))))

    (testing "deletes stream"
      (let [{:everest.append/keys [next-expected-version]}
            (store/append-events! store "test-stream-3" :everest.stream.version/not-present [(test-event 5)])]
        (is (= 5 next-expected-version
               (:everest.stream-slice/from
                 (store/read-stream-forward store "test-stream-3" nil nil))))
        (is (thrown? Exception (store/delete-stream! store "test-stream-3" 1)))

        (is (store/delete-stream! store "test-stream-3" next-expected-version))
        (is (nil? (store/read-stream-forward store "test-stream-3" nil nil)))))))
