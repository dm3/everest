(ns everest.pg.dynamic)

(def ^{:doc
       "Holds the connection used by the pg.store, `wrap-connection`
       middleware and other everest.jdbc functionality in order to provide
       transactions spanning the whole handler chain."
       :dynamic true}
  *connection* nil)

(defmacro with-unbound-connection
  "Use this when there's a situation where you inherit the bindings of a thread
  that has the `*connection*` bound.

  For example, suppose you have an event handler with the PG Connection
  middleware that spawns `future` and calls EventStore functions inside. The
  `*connection*` binding will be copied into the thread running the Future.
  However, the connection itself will probably already be closed as the event
  handler execution is done.

    (def handler
      (-> (fn [_]
            (future
              (dynamic/with-unbound-connection
                (store/read-stream-forward ...))))
          (wrap-connection {:handler.middleware/db ...})))"
  [& forms]
  `(binding [*connection* nil]
     ~@forms))
