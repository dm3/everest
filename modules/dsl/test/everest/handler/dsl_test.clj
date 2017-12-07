(ns everest.handler.dsl-test
  (:require [clojure.test :refer :all]
            [everest.event :as event]
            [everest.handler.dsl :as sut]
            [manifold.stream :as m.s]))

(deftest dsl-handler
  (let [!events (atom [])
        next-event #(let [e (first @!events)] (swap! !events rest) e)
        h (sut/handler :MyHandler
            (sut/on :MyEvent
              (fn [e] (swap! !events conj e))))]

    (h {:everest.event/type "AnotherEvent"})
    (is (not (next-event)))

    (h {:everest.event/type "MyEvent"})
    (is (= {:everest.event/type "MyEvent"} (next-event)))))

(deftest dsl-handler-emit
  (let [!events (atom [])
        period-ms 50
        delay-ms  50
        s (m.s/stream)
        h (sut/handler :MyHandler
            (sut/every-ms period-ms delay-ms
              (sut/emit {:everest.event/type "TimerEvent"}))
            (sut/on :TimerEvent
              (fn [e] (swap! !events conj (:everest.event/type e)))))]

    (m.s/consume h s)

    (is (= [] @!events))

    (testing "no events before subscription starts"
      (Thread/sleep period-ms)
      (is (= [] @!events)))

    (testing "timer starts after subscription start event"
      (m.s/put! s (assoc (event/subscription-started)
                         :everest.event/this-stream s))
      (Thread/sleep (+ (/ period-ms 2) period-ms))

      (is (= ["TimerEvent"] @!events))
      (reset! !events []))

    (testing "stops when stream closed"
      (m.s/close! s)
      (Thread/sleep period-ms)

      (is (= [] @!events)))))
