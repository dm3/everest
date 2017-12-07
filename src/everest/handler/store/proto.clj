(ns everest.handler.store.proto)

(defprotocol IHandlerStateStore
  (-get    [_ id])
  (-set    [_ id state])
  (-remove [_ id]))
