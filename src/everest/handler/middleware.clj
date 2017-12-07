(ns everest.handler.middleware
  (:require [everest.event :as event]
            [everest.handler :as handler]
            [everest.handler.store :as store]))

(defn wrap-position
  "Updates the handler's position after the handler is done with the event.

  Accepts the following options:

    * `state-store` - an instance of a `IHandlerStateStore`

  Events must have a `:handler/id` attached to identify the handler."
  [handler {:handler.middleware/keys [state-store]}]
  (fn [{:everest.event/keys [position stream] :as e}]
    (let [result (handler e)]
      (when-not (event/transient? e)
        (store/set! state-store (:handler/id e) {:handler/position position}))
      result)))

(defn wrap-state
  "Updates the handler's state and position when the handler returns new state.
  If a handler skips the event (see `handler/handled?`), the state is not
  updated.

  Accepts the following options:

    * `state-store` - an instance of a `IHandlerStateStore`. Has to return at least
      `#:handler{:state any?, :position position?}`.
    * `state-key` - `:handler/state` by default. The key under which the current state
      will be stored in the event map.
    * `catch-up-upsert-batch-size` - number of events to appear when catching up before
      state is updated
    * `upsert-batch-size` - number of events to appear when processing live before
      state is updated

  This middleware doesn't automatically make the actions performed in the
  handler idempotent with respect to state and position tracking. You must
  ensure that yourself by attaching an event position or other identifier to
  the actions you perform in the handler."
  [handler {:handler.middleware/keys [state-key state-store
                                      catch-up-upsert-batch-size upsert-batch-size]
            :or {state-key :handler/state
                 catch-up-upsert-batch-size 1
                 upsert-batch-size 1}}]
  (let [!state      (volatile! ::none)
        !caught-up? (volatile! false)
        !seen       (volatile! 0)
        should-upsert? #(zero? (mod @!seen %))]
    (fn [{:everest.event/keys [position] :as e}]
      (vswap! !seen inc)

      (when (identical? ::none @!state)
        (let [result (store/get state-store (:handler/id e))]
          (vreset! !state result)))

      (when (event/live-processing-started? e)
        (vreset! !caught-up? true))

      (let [old-state (:handler/state @!state)
            new-state (handler (assoc e state-key old-state))
            handled? (handler/handled? new-state)
            position (if (event/transient? e) (:handler/position @!state) position)]

        (when (and (not position) new-state)
          (throw (ex-info (str "Cannot update the state when no known position is present! "
                               "Please make sure that `state-store` returns a valid position!")
                          {:result new-state})))

        (let [state (if handled? new-state old-state)
              handler-state {:handler/state state, :handler/position position}]
          (when (or (and @!caught-up? (should-upsert? upsert-batch-size))
                    (and (not @!caught-up?) (should-upsert? catch-up-upsert-batch-size)))
            (store/set! state-store (:handler/id e) handler-state))
          (vreset! !state handler-state))

        new-state))))

(defn wrap-state-atom
  "Wraps the state under the `state-key` in an atom. Instead of returning the
  state you will have to `swap!` or `reset!` the atom.

  Depends on the state present under the `state-key` (`:handler/state` by
  default)."
  [handler {:handler.middleware/keys [state-key] :or {state-key :handler/state}}]
  (fn [e]
    (let [state-atom (atom (get e state-key))
          result (handler (assoc e state-key state-atom))]
      (if (handler/handled? result)
        @state-atom
        (handler/skip)))))
