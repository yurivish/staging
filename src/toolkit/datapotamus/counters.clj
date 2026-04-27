(ns toolkit.datapotamus.counters
  "Flow counters — sent/recv/completed.

   Shared across the inject thread and every proc thread. The three counts
   live together inside one atom so a single deref gives an atomic
   snapshot — quiescence detection has to read all three coherently or it
   can false-positive at a moment when the three sums happen to coincide
   while real work is still in flight, drop `done-p`, and lose mid-flight
   messages.")

(defrecord Counters [^long sent ^long recv ^long completed])

(defn make []
  (atom (->Counters 0 0 0)))

(defn- balanced-record? [^Counters c]
  (and (pos? (.-sent c))
       (= (.-sent c) (.-recv c))
       (= (.-recv c) (.-completed c))))

(defn balanced?
  "True iff some work has been sent and sent = recv = completed. Atomic
   across the three fields — `@counters` is one snapshot."
  [counters]
  (balanced-record? @counters))

(defn record-event!
  "Apply an event's counter delta atomically. Returns `true` iff this
   event advanced `completed` and the flow is now balanced (potentially
   quiescent). The completion path checks balance against the post-swap
   value, so the answer reflects the same atomic snapshot the increment
   produced."
  [counters ev]
  (case (:kind ev)
    :recv     (do (swap! counters update :recv unchecked-inc)
                  false)
    :send-out (do (when (:port ev)
                    (swap! counters update :sent unchecked-inc))
                  false)
    (:success :failure)
    (balanced-record? (swap! counters update :completed unchecked-inc))
    false))

(defn record-inject!
  "Count a message injected into the flow from outside. The inject path
   has no corresponding event to dispatch on — it just ticks `:sent`."
  [counters]
  (swap! counters update :sent unchecked-inc))

(defn snapshot
  "Plain-map view of the current counts, read atomically."
  [counters]
  (let [c @counters]
    {:sent      (.-sent ^Counters c)
     :recv      (.-recv ^Counters c)
     :completed (.-completed ^Counters c)}))
