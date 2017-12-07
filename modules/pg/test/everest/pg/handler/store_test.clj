(ns everest.pg.handler.store-test
  (:require [clojure.test :refer :all]
            [everest.pg.test-helpers :as th :refer :all]
            [everest.pg.dynamic :as dyn]
            [everest.pg.handler.store :as sut]))

(def create-table-sql "
  CREATE TABLE IF NOT EXISTS handler_positions (
      handler_id varchar(100) PRIMARY KEY,
      handler_type varchar(100) NOT NULL,
      position int NOT NULL,
      state varchar(10000)
  )")

(use-fixtures :each (with-table-fixture "handler_positions" create-table-sql))

(defn- upsert-fn [type id]
  #(sut/upsert!* dyn/*connection* type id %1 %2))

(deftest inserts-and-updates
  (binding [dyn/*connection* th/*connection*]
    (let [upsert-state (upsert-fn "HandlerType" "handler-1")
          upsert-state-2 (upsert-fn "HandlerType" "handler-2")
          upsert-state-3 (upsert-fn "HandlerType3" "handler-3")]
      (testing "creates a position entry"
        (is (empty? (table-contents "handler_positions")))

        (upsert-state 1 nil)
        (is (= [{:position 1
                 :handler_id "handler-1"
                 :handler_type "HandlerType"
                 :state nil}]
               (table-contents "handler_positions")))

        (upsert-state 2 nil)
        (is (= [{:position 2
                 :handler_id "handler-1"
                 :handler_type "HandlerType"
                 :state nil}]
               (table-contents "handler_positions"))))

      (testing "creates an entry for another handler"
        (upsert-state-2 5 nil)
        (is (= [{:position 2
                 :handler_id "handler-1"
                 :handler_type "HandlerType"
                 :state nil}
                {:position 5
                 :handler_id "handler-2"
                 :handler_type "HandlerType"
                 :state nil}]
               (table-contents "handler_positions"))))

      (testing "inserts some state"
        (upsert-state-3 10 "some-state")
        (is (= {:handler/id "handler-3" :handler/type "HandlerType3"
                :handler/position 10, :handler/state "some-state"}
               (sut/get-state* th/*connection* "handler-3")))

        (upsert-state-3 11 "some-more-state")
        (is (= {:handler/id "handler-3" :handler/type "HandlerType3"
                :handler/position 11, :handler/state "some-more-state"}
               (sut/get-state* th/*connection* "handler-3"))))

      (testing "gets all"
        (is (= [{:handler/position 2, :handler/id "handler-1", :handler/type "HandlerType", :handler/state nil}
                {:handler/position 5, :handler/id "handler-2", :handler/type "HandlerType", :handler/state nil}]
               (sut/get-all-states th/*connection* "HandlerType")))))))
