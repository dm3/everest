(ns everest.event.proto)

(defprotocol IPosition
  (-next [this])
  (-prev [this]))

(extend-type Number
  IPosition
  (-next [this]
    (inc this))
  (-prev [this]
    (dec this)))
