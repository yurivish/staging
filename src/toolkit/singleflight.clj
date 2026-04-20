(ns toolkit.singleflight)
;; Inspired by https://cs.opensource.google/go/x/sync/+/master:singleflight/singleflight.go

(defn group
  "Creates a new singleflight group — a namespace in which concurrent callers
   sharing a key will share one execution of fn.

   Example:
     (def g (group))
     (def fetch #(do (Thread/sleep 100) (rand-int 1000)))
     ;; Three threads calling do! at once see the same value; fetch runs once.
     (let [fs (repeatedly 3 #(future (do! g \"user:42\" fetch)))]
       (mapv deref fs))
     ;; => [713 713 713]"
  []
  (atom {}))

(defn do!
  "Runs (f) for key; concurrent calls with the same key share one execution.
   Returns the value; exceptions propagate to all waiters."
  [g key f]
  (let [d       (delay (f))
        [old _] (swap-vals! g #(if (contains? % key) % (assoc % key d)))
        leader? (not (contains? old key))
        target  (if leader? d (get old key))]
    (try
      @target
      (finally
        (when leader?
          ;; identity guard: if `forget` cleared our entry mid-flight and a new
          ;; leader installed its own delay under this key, leave it alone.
          (swap! g (fn [m] (if (identical? (get m key) d) (dissoc m key) m))))))))

(defn forget [g key]
  (swap! g dissoc key))
