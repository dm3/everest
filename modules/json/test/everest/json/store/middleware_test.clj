(ns everest.json.store.middleware-test
  (:require [clojure.test :refer :all]
            [everest.store :as store]
            [everest.memory.store :as mem-store]
            [everest.json.store.middleware :as sut]))

(defn- store []
  (mem-store/create))

(defn- date [y m d]
  (let [cal (doto (java.util.GregorianCalendar.)
              (.clear)
              (.set y (dec m) d)
              (.setTimeZone (java.util.TimeZone/getTimeZone "UTC")))]
    (.getTime cal)))

(deftest cheshire-middleware
  (testing "serializes and deserializes with defaults"
    (let [store (sut/wrap-cheshire-json (store) {})
          data {:int 1, :string "2", :boolean true, :vector [1 2], :map {:x "y"}}
          metadata {:date (date 2015 1 1)}]
      (store/append-events! store "test-stream" :everest.stream.version/any
        [{:everest.event/data data,
          :everest.event/metadata metadata,
          :everest.event/type "e-1"}])
      (is (= {:everest.event/position 2
              :everest.event/type "e-1"
              :everest.event/stream "test-stream"
              :everest.event/data data
              :everest.event/metadata {:date "2015-01-01T00:00:00.000Z"}}
             (store/read-event store "test-stream" :everest.event.position/last)))))

  (testing "serializes and deserializes with overrides"
    (let [store (sut/wrap-cheshire-json (store) {:key-fn name
                                                 :parse-strict? false
                                                 :date-format "yyyy-MM-dd"})
          data {:int 1, :string "2"}
          metadata {:date (date 2015 1 1)}]
      (store/append-events! store "test-stream" :everest.stream.version/any
        [{:everest.event/data data,
          :everest.event/metadata metadata,
          :everest.event/type "e-1"}])
      (is (= {:everest.event/position 2
              :everest.event/type "e-1"
              :everest.event/stream "test-stream"
              :everest.event/data {"int" 1, "string" "2"}
              :everest.event/metadata {"date" "2015-01-01"}}
             (store/read-event store "test-stream" :everest.event.position/last))))))
