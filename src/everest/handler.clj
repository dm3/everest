(ns everest.handler
  "An event handler is a function of `Event -> Result` where a `Result` is an arbitrary value.")

(defn skip
  "Return the result of `(skip)` from a handler function to indicate that the
  event was skipped by the handler.

  This is effectively a signal to outer handler middleware to skip any
  unnecessary work."
  [] :everest.handler.result/not-handled)

(defn handled?
  "True if the given `result` of a handlers execution indicates that the event
  was handled (i.e. not skipped) by the handler."
  [result]
  (not (identical? :everest.handler.result/not-handled result)))
