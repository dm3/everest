(ns everest.handler.store
  (:refer-clojure :exclude [set! get])
  (:require [everest.handler.store.proto :as store.proto]))

(defn get [store handler-id]
  (store.proto/-get store handler-id))

(defn set! [store handler-id state]
  (store.proto/-set store handler-id state))

(defn remove! [store handler-id]
  (store.proto/-remove store handler-id))
