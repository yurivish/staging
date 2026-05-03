(ns workers-bench
  "Microbenchmarks for the `stealing-workers` and `round-robin-workers`
   combinators, plus broadcast-roundtrip and trace-overhead probes.

   Each benchmark is a top-level fn that prints a tabular result. Run
   individually from the REPL or:

     clojure -M -e '(load-file \"/work/dev/workers_bench.clj\") (workers-bench/bench-fan-out-vs-k)'
     clojure -M -e '(load-file \"/work/dev/workers_bench.clj\") (workers-bench/bench-stealing-vs-round-robin-skewed)'
     clojure -M -e '(load-file \"/work/dev/workers_bench.clj\") (workers-bench/bench-flush-and-drain-roundtrip)'
     clojure -M -e '(load-file \"/work/dev/workers_bench.clj\") (workers-bench/bench-trace-overhead)'

   Numbers are wall-clock from `(System/nanoTime)` deltas — no statistical
   harness, no warm-up runs. Use them as relative reference points across
   changes, not absolute SLAs."
  (:require [clojure.core.async.flow :as-alias flow]
            [toolkit.datapotamus.combinators.workers :as cw]
            [toolkit.datapotamus.flow :as df]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.pubsub :as pubsub]))

(defn- ms-since [^long t0]
  (long (/ (- (System/nanoTime) t0) 1e6)))

;; ============================================================================
;; #1. Fan-out scaling vs K — fixed tree (depth 5, fanout 5 → 3906 nodes),
;;     pure compute, K ∈ {1 2 4 8 16 32}.
;; ============================================================================

(defn- fan-out-inner []
  (step/handler-map
   {:ports   {:ins {:in ""} :outs {:out "" :work ""}}
    :on-data (fn [ctx _ {:keys [id depth]}]
               (let [kids (when (pos? depth)
                            (mapv (fn [i]
                                    {:id (+ (* id 10) i)
                                     :depth (dec depth)})
                                  (range 5)))]
                 (cond-> {:out [(msg/child ctx {:id id})]}
                   (seq kids)
                   (assoc :work (msg/children ctx kids)))))}))

(defn bench-fan-out-vs-k
  "Run a fixed 3906-node recursive tree through `stealing-workers` for
   K ∈ {1 2 4 8 16 32}. Prints wall-ms per K. Tells you (a) parallel
   scaling efficiency, (b) where added workers stop helping."
  []
  (println "fan-out-vs-k — depth=5 fanout=5 (3906 nodes), pure compute")
  (println "K\twall-ms\tspeedup-vs-K=1")
  (let [baseline (atom nil)]
    (doseq [k [1 2 4 8 16 32]]
      (let [wf (cw/stealing-workers :pool k (fan-out-inner))
            t0 (System/nanoTime)
            res (df/run-seq wf [{:id 1 :depth 5}])
            ms (ms-since t0)
            n (count (mapcat identity (:outputs res)))]
        (assert (= 3906 n) (str "expected 3906 nodes, got " n))
        (when (= k 1) (reset! baseline ms))
        (println (format "%d\t%d\t%.2fx" k ms (double (/ @baseline ms))))))))

;; ============================================================================
;; #2. Stealing vs round-robin under skewed load — bimodal item costs
;;     (50% take 1ms, 50% take 50ms), K=8. Stealing should approach
;;     total/K wall-time; round-robin gets stuck on its slow allocations.
;; ============================================================================

(defn- skewed-inner []
  (step/handler-map
   {:ports   {:ins {:in ""} :outs {:out ""}}
    :on-data (fn [ctx _ d]
               (Thread/sleep (long (if (odd? d) 1 50)))
               {:out [(msg/child ctx d)]})}))

(defn bench-stealing-vs-round-robin-skewed
  "N=200 items, 50% fast (1ms) / 50% slow (50ms), K=8. Compare wall-time
   for stealing-workers vs round-robin-workers. Stealing should win
   substantially because fast workers re-stack onto the slow queue."
  []
  (println "stealing-vs-round-robin-skewed — N=200, K=8, bimodal 1ms/50ms")
  (println "combinator         \twall-ms")
  (let [n      200
        inputs (vec (range n))
        ;; serial-bias: alternate odd/even so round-robin to mod-K=8 will
        ;; cluster slow items unevenly across workers.
        run    (fn [ctor]
                 (let [wf  (ctor :pool 8 (skewed-inner))
                       t0  (System/nanoTime)
                       res (df/run-seq wf inputs)]
                   (assert (= :completed (:state res)))
                   (ms-since t0)))]
    (println (format "stealing-workers   \t%d" (run cw/stealing-workers)))
    (println (format "round-robin-workers\t%d" (run cw/round-robin-workers)))))

;; ============================================================================
;; #3. flush-and-drain roundtrip — empty-pipeline cycles.
;; ============================================================================

(defn bench-flush-and-drain-roundtrip
  "1000 cycles of `cast! → await-quiescent!` on an empty pipeline. Prints
   mean and p99 in µs per cycle. Tells you the per-cycle overhead;
   useful when sizing periodic-flush rates for long-running flows."
  []
  (println "flush-and-drain roundtrip — 1000 cycles on a 2-step pipeline")
  (let [agg-step {:procs
                  {:agg (step/handler-map
                         {:ports {:ins {:in ""} :outs {:out ""}}
                          :on-init (fn [] {})
                          :on-data (fn [_ s _] [s msg/drain])
                          :signal-select #{::flow/flush}
                          :on-broadcast (fn [_ s _ _] [s {}])})}
                  :conns [] :in :agg :out :agg}
        wf       (step/serial agg-step (step/sink))
        h        (df/start! wf {})
        n        1000
        samples  (long-array n)]
    (try
      (dotimes [i n]
        (let [t0 (System/nanoTime)]
          (df/cast! h ::flow/flush)
          (df/await-quiescent! h)
          (aset samples i (- (System/nanoTime) t0))))
      (let [ns->us  (fn [x] (long (/ x 1000)))
            sorted  (vec (sort (seq samples)))
            mean-ns (long (/ (reduce + 0 (map long sorted)) n))
            p99-ns  (long (nth sorted (long (* n 0.99))))]
        (println (format "mean\t%dµs" (ns->us mean-ns)))
        (println (format "p99\t%dµs" (ns->us p99-ns))))
      (finally
        (df/stop! h)))))

;; ============================================================================
;; #4. Trace overhead — same workload, with and without a pubsub listener.
;; ============================================================================

(defn bench-trace-overhead
  "Run a fan-out tree (depth 4, fanout 5 → 781 nodes), once with no
   pubsub listener and once with a listener that just `swap! conj`s
   every event. Prints the wall-time ratio. Tells you whether always-on
   tracing has meaningful runtime cost. Includes one warmup run before
   timing — JIT noise dominates the first invocation otherwise."
  []
  (println "trace-overhead — depth=4 fanout=5 (781 nodes), K=8")
  (let [run (fn [opts]
              (let [wf (cw/stealing-workers :pool 8 (fan-out-inner))
                    t0 (System/nanoTime)
                    res (df/run-seq wf [{:id 1 :depth 4}] opts)]
                (assert (= :completed (:state res)))
                (ms-since t0)))
        ;; Warmup — both code paths once to settle JIT.
        _ (run {})
        _ (let [ps (pubsub/make)]
            (pubsub/sub ps [:>] (fn [_ _ _] nil))
            (run {:pubsub ps}))
        ;; Timed runs.
        no-listener (run {})
        ps (pubsub/make)
        events (atom [])
        _ (pubsub/sub ps [:>] (fn [_ ev _] (swap! events conj ev)))
        with-listener (run {:pubsub ps})]
    (println (format "no-listener  \t%dms" no-listener))
    (println (format "with-listener\t%dms\t(%.2fx)"
                     with-listener
                     (double (/ with-listener (max 1 no-listener)))))
    (println (format "events captured: %d" (count @events)))))

(comment
  (bench-fan-out-vs-k)
  (bench-stealing-vs-round-robin-skewed)
  (bench-flush-and-drain-roundtrip)
  (bench-trace-overhead))
