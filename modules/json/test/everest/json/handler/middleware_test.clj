(ns everest.json.handler.middleware-test
  (:require [clojure.test :refer :all]
            [everest.handler :as handler]
            [everest.json.handler.middleware :as sut]))

(defn- wrap-default [h]
  (sut/wrap-cheshire-json h {}))

(deftest cheshire-middleware
  (testing "serializes empty state"
    (let [res ((wrap-default (constantly {:x 1})) {})]
      (is (= res "{\"x\":1}"))))

  (testing "updates and serializes past state"
    (let [prev "{\"x\":1}"
          res ((wrap-default (fn [{:handler/keys [state]}] (update state :x inc)))
               {:handler/state prev})]
      (is (= res "{\"x\":2}"))))

  (testing "handles not-handled result"
    (let [res ((wrap-default (fn [_] (handler/skip))) {:state "1"})]
      (is (= res (handler/skip))))))
