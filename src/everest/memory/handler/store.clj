(ns everest.memory.handler.store
  (:require [everest.handler.store.proto :as store.proto]))

(defn create
  "Creates an in-memory handler state store."
  ([] (create {}))
  ([{:keys [initial-state] :or {initial-state {}}}]
   (let [!store (atom initial-state)]
     (reify store.proto/IHandlerStateStore
       (-get [_ id]
         (get @!store id))
       (-set [_ id state]
         (swap! !store assoc id state))
       (-remove [_ id]
         (swap! !store dissoc id))))))
