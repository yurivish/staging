(ns toolkit.datapotamus.counters
  "Flow counters — sent/recv/completed.

   Shared across the inject thread and every proc thread, so the fields
   are `LongAdder`s (low-contention concurrent bumps). A defrecord keeps
   the shape fixed and gives direct field access so the hot path avoids
   map lookups and per-call type hints."
  (:import [java.util.concurrent.atomic LongAdder]))

(defrecord Counters [^LongAdder sent ^LongAdder recv ^LongAdder completed])

(defn make []
  (->Counters (LongAdder.) (LongAdder.) (LongAdder.)))

(defn record-event!
  "Apply an event's counter delta in place. Called on the proc-fn publish
   path for every emitted event."
  [^Counters c ev]
  (case (:kind ev)
    :recv     (.increment (.-recv c))
    :success  (.increment (.-completed c))
    :failure  (.increment (.-completed c))
    :send-out (when (:port ev) (.increment (.-sent c)))
    nil))

(defn record-inject!
  "Count a message injected into the flow from outside. The inject path
   has no corresponding event to dispatch on — it just ticks `:sent`."
  [^Counters c]
  (.increment (.-sent c)))

(defn snapshot
  "Plain-map view of the current counts. Each `.sum` is read
   independently, so the three values may come from slightly different
   moments; callers that need a consistent triple should treat the
   snapshot as eventually-consistent."
  [^Counters c]
  {:sent      (.sum (.-sent c))
   :recv      (.sum (.-recv c))
   :completed (.sum (.-completed c))})

(defn balanced?
  "True iff some work has been sent and sent = recv = completed.

   Reads each LongAdder's sum independently — not atomic across the
   three. Under concurrent writes a transient skew can produce a false
   negative; it cannot produce a false positive for long, because the
   counters are monotonic and balance is a steady state. Callers use
   this as a steady-state quiescence check, not a mid-flight consistency
   claim."
  [^Counters c]
  (let [s  (.sum (.-sent c))
        r  (.sum (.-recv c))
        co (.sum (.-completed c))]
    (and (pos? s) (= s r) (= r co))))
