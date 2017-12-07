(ns everest.pg.jdbc-ext
  "This namespace must be required explicitly if you use Postgres"
  (:require [jdbc.proto :as jdbc.p]
            [everest.pg.store.types])
  (:import [org.postgresql.util PGobject]))

(def ^{:type chars, :private true}
  hex-arr (.toCharArray "0123456789ABCDEF"))

(defn- ^String to-hex-string [^bytes bts]
  (let [^chars res (char-array (* 2 (alength bts)))]
    (loop [idx 0]
      (let [v (bit-and (aget bts idx) 0xFF)]
        (aset res (* 2 idx) (aget hex-arr (unsigned-bit-shift-right v 4)))
        (aset res (inc (* 2 idx)) (aget hex-arr (bit-and v 0x0F))))
      (if (< (inc idx) (alength bts))
        (recur (inc idx))
        (String. res)))))

(defn- wrap-hex [^String s]
  (str "\"\\\\x" s "\""))

;; http://www.unicode.org/versions/Unicode5.1.0/
(defn- escape-json-unicode-into [^StringBuilder b, c]
  (if (or (<= (int \u0000) (int c) (int \u001F))
          (<= (int \u007F) (int c) (int \u009F))
          (<= (int \u2000) (int c) (int \u20FF)))
    (let [^String s (Integer/toHexString (int c))]
      (.append b "\\u")
      (loop [idx 0]
        (.append b \0)
        (when (> (inc idx) (- 4 (.length s)))
          (recur (inc idx))))
      (.append b (.toUpperCase s)))
    (.append b c)))

(defn- ^String escape-json [^String json-str]
  (let [^StringBuilder b (StringBuilder.)]
    (loop [idx 0]
      ;; we might encounter character from surrogate pairs (\uD800 to \uDBFF
      ;; for low and \uDC00 to \uDFFF for high) currently we don't do anything
      ;; as such a character doesn't need escaping.
      (let [c (.charAt json-str idx)]
        (case c
          (\backspace \newline \tab \return \" \\ \formfeed)
          (.append b (char-escape-string c))

          \/ (.append b "\\/")

          (escape-json-unicode-into b c))
      (if (< (inc idx) (.length json-str))
        (recur (inc idx))
        (.toString b))))))

(defn- wrap-text [txt]
  (str "\"" txt "\""))

(defn- ^"[Ljava.lang.String;" to-string-array [^"[Leverest.pg.store.types.JsonEvent;" event-arr]
  (let [^"[Ljava.lang.String;" res (object-array (alength event-arr))]
    (loop [idx 0]
      (let [^everest.pg.store.types.JsonEvent event (aget event-arr idx)]
        (aset res idx
              (str "(" (.type event) ","
                       (wrap-text (escape-json (.data event))) ","
                       (wrap-text (escape-json (.metadata event))) ")"))
        (if (< (inc idx) (alength event-arr))
          (recur (inc idx))
          res)))))

(defn- ^"[Ljava.lang.String;" to-hex-encoded-byte-array [^"[Leverest.pg.store.types.BinaryEvent;" event-arr]
  (let [^"[Ljava.lang.String;" res (object-array (alength event-arr))]
    (loop [idx 0]
      (let [^everest.pg.store.types.BinaryEvent event (aget event-arr idx)]
        (aset res idx
              (str "(" (.type event) ","
                       (wrap-hex (to-hex-string (.data event))) ","
                       (wrap-hex (to-hex-string (.metadata event))) ")"))
        (if (< (inc idx) (alength event-arr))
          (recur (inc idx))
          res)))))

(extend-protocol jdbc.p/ISQLType
  (class (into-array everest.pg.store.types.JsonEvent []))

  (set-stmt-parameter! [this conn stmt index]
    (let [value (jdbc.p/as-sql-type this conn)]
      (->> (.createArrayOf conn "event" value)
           (.setArray stmt index))))

  (as-sql-type [this conn] (to-string-array this)))

(extend-protocol jdbc.p/ISQLType
  (class (into-array everest.pg.store.types.BinaryEvent []))

  (set-stmt-parameter! [this conn stmt index]
    (let [value (jdbc.p/as-sql-type this conn)]
      (->> (.createArrayOf conn "event" value)
           (.setArray stmt index))))

  (as-sql-type [this conn] (to-hex-encoded-byte-array this)))

(extend-protocol jdbc.p/ISQLType
  everest.pg.store.types.JsonObject

  (set-stmt-parameter! [this conn stmt index]
    (.setObject stmt index (jdbc.p/as-sql-type this conn)))

  (as-sql-type [this conn]
    (doto (PGobject.)
      (.setType "json")
      (.setValue (.data this)))))

(extend-protocol jdbc.p/ISQLType
  everest.pg.store.types.JsonbObject

  (set-stmt-parameter! [this conn stmt index]
    (.setObject stmt index (jdbc.p/as-sql-type this conn)))

  (as-sql-type [this conn]
    (doto (PGobject.)
      (.setType "jsonb")
      (.setValue (.data this)))))
