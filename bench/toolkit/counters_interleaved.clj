(ns toolkit.counters-interleaved
  "Interleaved A/B benchmark for `ctrs/record-event!` — inlined balance
   check vs. calling `balanced?`.

   Methodology:
     - Both implementations live in this ns as plain fns.
     - For each trial, randomize whether A or B runs first (cancels out
       any machine-state drift within a trial).
     - Between every run: force GC + sleep to settle.
     - Report per-variant medians AND paired deltas (B−A within each
       trial) so variance is visible.
     - Rebind `ctrs/record-event!` via `alter-var-root`; works because
       `flow.clj` calls it through the var (no direct linking)."
  (:require [toolkit.datapotamus-bench :as bench]
            [toolkit.datapotamus.counters :as ctrs])
  (:import [java.util.concurrent.atomic LongAdder]
           [java.lang.management ManagementFactory]))

(set! *warn-on-reflection* true)

;; ---- The two implementations under test --------------------------------

(defn record-event-called!
  "Un-inlined: delegates balance check to `balanced?`."
  [^toolkit.datapotamus.counters.Counters c ev]
  (case (:kind ev)
    :recv     (do (.increment ^LongAdder (.-recv c)) false)
    :send-out (do (when (:port ev) (.increment ^LongAdder (.-sent c))) false)
    (:success :failure)
    (do (.increment ^LongAdder (.-completed c)) (ctrs/balanced? c))
    false))

(defn record-event-inlined!
  "Inlined: duplicates balanced?'s body inside the completion branch."
  [^toolkit.datapotamus.counters.Counters c ev]
  (case (:kind ev)
    :recv     (do (.increment ^LongAdder (.-recv c)) false)
    :send-out (do (when (:port ev) (.increment ^LongAdder (.-sent c))) false)
    (:success :failure)
    (do (.increment ^LongAdder (.-completed c))
        (let [s  (.sum ^LongAdder (.-sent c))
              r  (.sum ^LongAdder (.-recv c))
              co (.sum ^LongAdder (.-completed c))]
          (and (pos? s) (= s r) (= r co))))
    false))

;; ---- Bind-and-run ------------------------------------------------------

(defn- heap-used ^long []
  (let [rt (Runtime/getRuntime)] (- (.totalMemory rt) (.freeMemory rt))))

(defn- settle! []
  (loop [m (heap-used) n 0]
    (System/runFinalization)
    (System/gc)
    (let [m' (heap-used)
          p  (.. (ManagementFactory/getMemoryMXBean) getObjectPendingFinalizationCount)]
      (when (and (or (pos? p) (> m m')) (< n 100))
        (recur m' (inc n))))))

(defn- run-with [impl opts]
  (alter-var-root #'ctrs/record-event! (constantly impl))
  (settle!)
  (bench/run-bench opts))

(defn- thp [result]
  (double (or (get-in result [:summary :throughput-msgs-per-s]) 0.0)))

(defn- median [xs]
  (let [v (vec (sort xs)) n (count v)]
    (cond
      (zero? n) 0.0
      (odd? n)  (double (nth v (quot n 2)))
      :else     (/ (+ (double (nth v (dec (quot n 2))))
                      (double (nth v (quot n 2)))) 2.0))))

(defn- fmt-rate ^String [^double r]
  (cond (> r 1e6) (format "%6.2fM/s" (/ r 1e6))
        (> r 1e3) (format "%6.2fK/s" (/ r 1e3))
        :else     (format "%6.1f /s" r)))

(defn- fmt-pct ^String [^double f]
  (format "%+.2f%%" (* 100.0 f)))

;; ---- Interleaved driver ------------------------------------------------

(defn interleaved-sweep
  "For each scenario, run `trials` paired runs. Each trial:
     - flip a coin
     - run first impl, then the other (random order)
   Report: per-variant median thp, paired (inlined − called) delta stats."
  [scenarios trials]
  (println)
  (println (format "=== Interleaved A/B: inlined vs called — %d trials/scenario ===" trials))
  (doseq [[label opts] scenarios]
    (let [inlined-thps (atom [])
          called-thps  (atom [])
          deltas       (atom [])] ; (inlined − called) / called
      (dotimes [_ trials]
        (let [inlined-first? (< (rand) 0.5)
              [a a-tag b]    (if inlined-first?
                               [record-event-inlined! :inlined record-event-called!]
                               [record-event-called!  :called  record-event-inlined!])
              ta (thp (run-with a opts))
              tb (thp (run-with b opts))
              [tin tcl] (if (= a-tag :inlined) [ta tb] [tb ta])]
          (swap! inlined-thps conj tin)
          (swap! called-thps  conj tcl)
          (swap! deltas       conj (if (pos? tcl) (/ (- tin tcl) tcl) 0.0))))
      (let [in-med (median @inlined-thps)
            cl-med (median @called-thps)
            d-med  (median @deltas)
            d-min  (apply min @deltas)
            d-max  (apply max @deltas)]
        (println (format "  %-20s  inlined med=%s  called med=%s"
                         label (fmt-rate in-med) (fmt-rate cl-med)))
        (println (format "%24sΔ(inlined−called)/called  med=%s  range=[%s .. %s]"
                         "" (fmt-pct d-med) (fmt-pct d-min) (fmt-pct d-max)))))))

;; ---- Entry point -------------------------------------------------------

(def scenarios
  "Same shapes as smoke-sweep minus redundant noop C=1 (single-proc,
   expected wash). Shorter warmup/duration per run so we can afford more
   trials."
  (let [base {:warmup-s 1 :duration-s 2}]
    [["noop        C=4 "  (merge base {:step (bench/noop-step)        :load {:kind :closed :concurrency 4}})]
     ["noop        C=16"  (merge base {:step (bench/noop-step)        :load {:kind :closed :concurrency 16}})]
     ["chain-3     C=16"  (merge base {:step (bench/chain-n-step 3)   :load {:kind :closed :concurrency 16}})]
     ["chain-10    C=16"  (merge base {:step (bench/chain-n-step 10)  :load {:kind :closed :concurrency 16}})]
     ["workers-4   C=16"  (merge base {:step (bench/workers-w-step 4) :load {:kind :closed :concurrency 16}})]]))

(defn -main [& [trials-str]]
  (let [trials (Long/parseLong (or trials-str "15"))]
    (interleaved-sweep scenarios trials)))
