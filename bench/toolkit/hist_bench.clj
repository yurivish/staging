(ns toolkit.hist-bench
  "Recording-cost microbench for the h2-backed histogram container.

   Measures:
     - h2/encode alone — pure arithmetic cost (criterium).
     - Dense.record under N-way contention (1, 2, 4, 8, 16 threads) — the
       AtomicLongArray.incrementAndGet path.
     - Sparse.record under the same contention levels — the
       ConcurrentHashMap + LongAdder path.
     - Dense.record with all writers hitting the SAME bin (worst-case
       single-cache-line contention).

   The single-thread results set the per-op floor. Contention results show
   how quickly the atomic scales — a good library makes aggregate
   throughput grow with thread count up to the cache-coherence wall.

   Run: clojure -M:bench hist

   Parameters (mutated at top of file):
     ops-per-thread — records per thread per measurement pass
     warmup-ops     — records per thread before measurement
     thread-counts  — contention levels to sweep"
  (:require [criterium.core :as crit]
            [toolkit.bench :as b]
            [toolkit.h2 :as h2]
            [toolkit.hist :as hist])
  (:import [java.util.concurrent CountDownLatch Executors ExecutorService]))

(set! *warn-on-reflection* true)

(def ^:private ^:const ops-per-thread 2000000)
(def ^:private ^:const warmup-ops     200000)
(def ^:private thread-counts [1 2 4 8 16])

;; Precompute value stream so the hot loop does only an array load,
;; an encode, and an atomic increment. The LCG-style stride spreads
;; successive encodes across different bins (different cache lines)
;; so by default we measure the typical-case, not worst-case.
(def ^:private ^:const stream-size 65536)
(def ^:private value-stream
  (let [a (long-array stream-size)]
    (dotimes [i stream-size]
      (aset a i (bit-and (unchecked-multiply (inc (long i)) 2654435761) 0x3fffffff)))
    a))

(defn- fmt-ns ^String [^double ns]
  (cond
    (< ns 1e3) (format "%6.1f ns" ns)
    (< ns 1e6) (format "%6.2f us" (/ ns 1e3))
    :else      (format "%6.2f ms" (/ ns 1e6))))

(defn- fmt-ops ^String [^double ops]
  (cond
    (> ops 1e9) (format "%6.2fG/s" (/ ops 1e9))
    (> ops 1e6) (format "%6.2fM/s" (/ ops 1e6))
    (> ops 1e3) (format "%6.2fK/s" (/ ops 1e3))
    :else       (format "%6.1f /s" ops)))

(defn- run-pool
  "Submit `threads` Runnables to `pool`, gated on a start latch; wait for
   completion. Returns elapsed nanoseconds from just-before start to
   all-done."
  ^long [^ExecutorService pool ^long threads runnable-factory]
  (let [start (CountDownLatch. 1)
        done  (CountDownLatch. (int threads))]
    (dotimes [t threads]
      (let [task (runnable-factory t)]
        (.submit pool ^Runnable
          (fn []
            (try
              (.await start)
              (task)
              (finally (.countDown done)))))))
    (let [t0 (System/nanoTime)]
      (.countDown start)
      (.await done)
      (unchecked-subtract (System/nanoTime) t0))))

(defn- parallel-throughput
  "Run `task-factory` on `threads` threads in parallel.
   `task-factory` takes thread-id and returns a thunk that records
   `ops-per-thread` values. Returns {:threads :ops :per-op-ns :ops-per-s}."
  [threads task-factory]
  (let [pool ^ExecutorService (Executors/newFixedThreadPool threads)]
    (try
      ;; Warmup — JIT, class-init, any per-pool state.
      (run-pool pool threads
                (fn [_t] #(let [vs value-stream]
                            (dotimes [i warmup-ops]
                              (task-factory i vs)))))
      ;; Measurement
      (let [elapsed (run-pool pool threads
                              (fn [_t] #(let [vs value-stream]
                                          (dotimes [i ops-per-thread]
                                            (task-factory i vs)))))
            total   (* (long threads) (long ops-per-thread))]
        {:threads threads
         :ops     total
         :elapsed-ns elapsed
         :per-op-ns  (/ (double elapsed) (double total))
         :ops-per-s  (* 1e9 (/ (double total) (double elapsed)))})
      (finally (.shutdown pool)))))

(defn- row
  "Print one contention row."
  [label {:keys [^long threads ^double per-op-ns ^double ops-per-s]}]
  (println (format "  %-28s  threads=%2d  per-op=%s   total=%s"
                   label threads (fmt-ns per-op-ns) (fmt-ops ops-per-s))))

;; ---- Scenarios -----------------------------------------------------

(defn- encode-scenario []
  (b/header "h2/encode — pure arithmetic cost")
  ;; Batch 1000 encodes per thunk so criterium overhead (~100ns per
  ;; quick-benchmark sample boundary) doesn't dominate a sub-10ns op.
  (let [batch 1000
        ^longs vs value-stream
        mask (unchecked-dec stream-size)
        thunk (fn []
                (let [^longs vs vs
                      acc      (long-array 1)]
                  (dotimes [i batch]
                    (let [v (aget vs (bit-and i mask))]
                      (aset acc 0 (unchecked-add (aget acc 0) (h2/encode 2 4 v)))))
                  (aget acc 0)))
        result (crit/quick-benchmark* thunk {})
        mean-s (double (first (:mean result)))
        per-op (/ (* 1e9 mean-s) (double batch))]
    (println (format "  %-28s  per-op=%s   %s"
                     "h2/encode (a=2 b=4)"
                     (fmt-ns per-op)
                     (fmt-ops (/ 1e9 per-op))))))

(defn- dense-scenario []
  (b/header "Dense.record — typical-case (values spread across bins)")
  (let [h (hist/dense {:a 2 :b 4 :max-v 0x3fffffff})]
    (doseq [n thread-counts]
      (row "dense (spread)"
           (parallel-throughput n
             (fn [^long i ^longs vs]
               (hist/record-dense! h (aget vs (bit-and i (unchecked-dec stream-size))))))))))

(defn- dense-same-bin-scenario []
  (b/header "Dense.record — worst-case (all writers hit the same bin)")
  (let [h (hist/dense {:a 2 :b 4 :max-v 0x3fffffff})]
    (doseq [n thread-counts]
      (row "dense (same-bin)"
           (parallel-throughput n
             (fn [^long _i ^longs _vs]
               (hist/record-dense! h 1000)))))))

(defn- sparse-scenario []
  (b/header "Sparse.record — typical-case (ConcurrentHashMap + LongAdder)")
  (let [h (hist/sparse {:a 2 :b 4})]
    (doseq [n thread-counts]
      (row "sparse (spread)"
           (parallel-throughput n
             (fn [^long i ^longs vs]
               (hist/record-sparse! h (aget vs (bit-and i (unchecked-dec stream-size))))))))))

(defn run []
  (b/banner "toolkit.hist — recording microbench")
  (println (format "  ops-per-thread=%,d  warmup=%,d  threads=%s"
                   ops-per-thread warmup-ops (pr-str thread-counts)))
  (println (format "  JVM: %s %s  procs=%d"
                   (System/getProperty "java.vm.name")
                   (System/getProperty "java.version")
                   (.availableProcessors (Runtime/getRuntime))))
  (encode-scenario)
  (dense-scenario)
  (sparse-scenario)
  (dense-same-bin-scenario))

(defn -main [& _]
  (run)
  (shutdown-agents))
