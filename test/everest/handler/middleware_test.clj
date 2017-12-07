(ns everest.handler.middleware-test
  (:require [clojure.test :refer :all]
            [everest.event :as event]
            [everest.handler.store.proto :as handler.store.proto]
            [everest.handler :as handler]
            [everest.handler.middleware :as sut]))

(defn store [!state-atom]
  (reify handler.store.proto/IHandlerStateStore
    (-get [_ id]
      (get @!state-atom id))
    (-set [_ id state]
      (swap! !state-atom assoc id state))
    (-remove [_ id]
      (swap! !state-atom dissoc id))))

(defn- transient-event []
  (event/transient-event {:timestamp 0}))

(defn- wrap-handler-id [f id]
  (fn [e]
    (let [r (f (assoc e :handler/id id))]
      (if (map? r)
        (dissoc r :handler/id)
        r))))

(defn- wrap-with-id [wrap-f f id state]
  (-> (wrap-f f {:handler.middleware/state-store (store state)})
      (wrap-handler-id id)))

(deftest updates-handler-position
  (let [state (atom {})
        handler (wrap-with-id sut/wrap-position identity "handler-1" state)]
    (testing "doesn't update if a transient event"
      (handler (transient-event))
      (is (empty? @state)))

    (testing "returns handler's result"
      (is (= (transient-event)
             (handler (transient-event)))))

    (testing "creates a position entry"
      (is (empty? @state))

      (handler {:everest.event/position 1})
      (is (= {"handler-1" {:handler/position 1}}
             @state))

      (handler {:everest.event/position 2})
      (is (= {"handler-1" {:handler/position 2}}
             @state)))

    (testing "creates an entry for another handler"
      (let [handler-2 (wrap-with-id sut/wrap-position identity "handler-2" state)]
        (handler-2 {:everest.event/position 5})
        (is (= {"handler-1" {:handler/position 2}
                "handler-2" {:handler/position 5}}
               @state))))))

(deftest fails-handler-positions
  (testing "doesn't update when handler throws"
    (let [state (atom {})
          handler (wrap-with-id sut/wrap-position
                    #(throw (Exception. (str %))) "projection-1" state)]
      (try (handler {:everest.event/position 1}) (catch Exception _))
      (is (empty? @state)))))

(deftest inserts-and-updates-handler-state
  (let [initial-state {:handler/position 0, :handler/state nil}
        state (atom {"projection-1" initial-state})
        handler (wrap-with-id sut/wrap-state
                  (constantly "initial-state") "projection-1" state)]
    (testing "updates state without position on transient event"
      (handler (transient-event))
      (is (= {:handler/position 0, :handler/state "initial-state"}
             (get @state "projection-1"))))

    (testing "inserts new state"
      (handler {:everest.event/position 1})
      (is (= {:handler/position 1, :handler/state "initial-state"}
             (get @state "projection-1"))))

    (testing "updates existing state"
      (handler {:everest.event/position 2})

      (is (= {:handler/position 2, :handler/state "initial-state"}
             (get @state "projection-1"))))

    (testing "doesn't update state when special return"
      ((wrap-with-id sut/wrap-state
         (fn [e] (handler/skip)) "projection-1" state)
       {:everest.event/position 3})

      (is (= {:handler/position 3, :handler/state "initial-state"}
             (get @state "projection-1"))))

    (testing "updates state on a transient event"
      ((wrap-with-id sut/wrap-state
         (fn [e] "new-state") "projection-1" state)
       {:everest.event/position 4})

      (is (= {:handler/position 4, :handler/state "new-state"}
             (get @state "projection-1"))))

    (let [handler (wrap-with-id sut/wrap-state
                    (fn [e]
                      (cond (= (:handler/state e) "new-state") "next-state"
                            (= (:handler/state e) "next-state") "next-next-state"
                            :else (throw (Exception. "fail"))))
                    "projection-1" state)]
      (testing "picks up position with the new handler"
        (handler {:everest.event/position 4})
        (is (= {:handler/position 4, :handler/state "next-state"}
               (get @state "projection-1")))

        (handler {:everest.event/position 5})
        (is (= {:handler/position 5, :handler/state "next-next-state"}
               (get @state "projection-1")))))))

(deftest wrap-state-atom
  (testing "propagates not-handled token"
    (let [wrapped (sut/wrap-state-atom
                    (fn [_] (handler/skip)) {})]
      (is (not (handler/handled? (wrapped {:handler/state {:x 1}}))))))

  (testing "wraps empty state into an atom"
    (let [wrapped (sut/wrap-state-atom
                    (fn [e] (reset! (:handler/state e) :handled)) {})]
      (is (= :handled (wrapped {})))))

  (testing "wraps non-empty state into an atom"
    (let [wrapped (sut/wrap-state-atom
                    (fn [e] (swap! (:handler/state e) inc)) {})]
      (is (= 2 (wrapped {:handler/state 1})))))

  (testing "doesn't hide nils"
    (let [wrapped (sut/wrap-state-atom
                    (fn [e] (reset! (:handler/state e) nil)) {})]
      (is (nil? (wrapped {:handler/state 'x})))))

  (testing "returns same state when not modified"
    (let [wrapped (sut/wrap-state-atom (fn [_] nil) {})]
      (is (= 'x (wrapped {:handler/state 'x})))))

  (testing "honors the state-key parameter"
    (let [wrapped (sut/wrap-state-atom
                    (fn [e] (swap! (:state-atom e) inc))
                    {:handler.middleware/state-key :state-atom})]
      (is (= 2 (wrapped {:state-atom 1}))))))
