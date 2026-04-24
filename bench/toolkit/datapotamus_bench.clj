(ns toolkit.datapotamus-bench
  "Benchmark harness for Datapotamus pipelines.

   Wraps any user `step` with a timing sink, drives it with a load
   generator, records per-message end-to-end latency into an h2-encoded
   histogram, and reports throughput + latency distribution.

   Public entry: `run-bench`. See docstring for options.

   Phase-2 skeleton scope:
     - closed-loop and :count load modes (open-loop/rate in phase 3)
     - E2E latency only (per-stage latency needs nanoTime trace events)
     - drains the flow's events atom periodically to bound memory
     - assumes the user step has a single `:out` endpoint (serial-able)"
  (:require [toolkit.datapotamus.combinators :as dc]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]
            [toolkit.hist :as hist])
  (:import [java.util.concurrent Semaphore]
           [java.util.concurrent.atomic AtomicBoolean AtomicLong AtomicReference]
           [toolkit.hist Dense]))

(set! *warn-on-reflection* true)

;; ---- Payload stamping ----------------------------------------------

(defn- stamp-payload
  "Wrap `payload` with inject-time metadata for E2E latency measurement."
  [payload]
  {:bench/t-inject-ns (System/nanoTime)
   :bench/payload     payload})

;; ---- Sink ----------------------------------------------------------

(defn- mk-sink
  "Terminal step that records E2E latency into the current hist (read
   from an AtomicReference so warmup→measurement can swap without
   rebuilding the pipeline) and releases a permit for closed-loop flow."
  [^AtomicReference hist-ref ^AtomicLong done-ctr ^Semaphore sem]
  (step/step
   ::sink
   {:ins {:in ""} :outs {}}
   (fn [_ctx _s d]
     (when (map? d)
       (when-let [t0 (:bench/t-inject-ns d)]
         (hist/record-dense! ^Dense (.get hist-ref)
                             (unchecked-subtract (System/nanoTime) (long t0)))))
     (.incrementAndGet done-ctr)
     (when sem (.release sem))
     {})))

;; ---- Event-atom drainer --------------------------------------------

(defn- start-event-drainer
  "The flow's main-sub appends every event to an atom; at benchmark
   rates this dominates memory. Periodically reset it to `[]`. Returns
   a no-arg stop fn."
  [handle ^long period-ms]
  (let [events-atom (:toolkit.datapotamus.flow/events handle)
        running     (AtomicBoolean. true)
        th          ^Thread (.start (Thread/ofPlatform)
                              (reify Runnable
                                (run [_]
                                  (while (.get running)
                                    (reset! events-atom [])
                                    (try (Thread/sleep period-ms)
                                         (catch InterruptedException _ nil))))))]
    (fn []
      (.set running false)
      (.interrupt th)
      (.join th 1000))))

;; ---- Closed-loop injector -----------------------------------------

(defn- start-closed-loop-injector
  "Spawn a platform thread that keeps `concurrency` msgs in flight by
   acquiring one permit per inject. Returns a stop fn that flips the
   stop flag, releases extra permits to unblock acquire, and joins."
  [handle ^Semaphore sem concurrency input-fn ^AtomicLong sent-ctr]
  (let [stop  (AtomicBoolean. false)
        th    ^Thread (.start (Thread/ofPlatform)
                        (reify Runnable
                          (run [_]
                            (while (not (.get stop))
                              (try
                                (.acquire sem)
                                (when-not (.get stop)
                                  (flow/inject! handle {:data (stamp-payload (input-fn))})
                                  (.incrementAndGet sent-ctr))
                                (catch InterruptedException _ nil))))))]
    (fn []
      (.set stop true)
      (.release sem (int concurrency))
      (.interrupt th)
      (.join th 1000))))

;; ---- Summary + reporting ------------------------------------------

(defn- with-rates
  "Augment a hist summary with throughput + send/completion counters."
  [snap sent completed measured-ns]
  (merge (hist/summary snap)
         {:throughput-msgs-per-s (when (pos? measured-ns)
                                   (* 1e9 (/ (double completed) (double measured-ns))))
          :sent-msgs             sent
          :completed-msgs        completed
          :measured-duration-ns  measured-ns}))

;; ---- Core run fn --------------------------------------------------

(defn run-bench
  "Run one benchmark. Returns {:summary :snapshot :config :sent :completed :elapsed-ns}.

   Options:
     :step         — pipeline step (required). Must have a single :out.
     :input-fn     — fn of no args returning per-msg payload. Default: tiny map.
     :load         — {:kind :closed :concurrency N}  (closed-loop, N in flight)
                   | {:kind :count  :count N}        (fire N msgs, wait for quiescence)
     :warmup-s     — seconds of inject-then-discard (closed-loop only; default 2)
     :duration-s   — seconds of measurement (closed-loop only; default 10)
     :hist-max-ns  — upper bound of the latency histogram (default 10s)
     :drain-ms     — events-atom drain period (default 50; tight to bound
                     the GC burst caused by main-sub's event accumulation)
     :subscribers  — {pattern handler} passed to flow/start! for custom
                     pubsub subscribers. Handlers run synchronously on
                     the publisher's thread; a slow handler stalls the
                     emitting proc.

   Pipeline shape: (serial step ::sink). ::sink records E2E latency and
   releases a semaphore permit per message (closed-loop).

   Warmup vs measurement: the sink reads its current histogram from an
   AtomicReference; the harness swaps in a fresh one after warmup so
   measurement stats exclude JIT / steady-state buildup."
  [{:keys [step input-fn load warmup-s duration-s hist-max-ns drain-ms subscribers]
    :or   {input-fn    (fn [] {:n 42})
           warmup-s    2
           duration-s  10
           hist-max-ns 10000000000
           drain-ms    50
           subscribers {}}}]
  (when-not step (throw (ex-info "run-bench: :step required" {})))
  (when-not load (throw (ex-info "run-bench: :load required" {})))
  (let [kind         (:kind load)
        sent-ctr     (AtomicLong. 0)
        done-ctr     (AtomicLong. 0)
        warmup-hist  (hist/dense {:a 2 :b 4 :max-v hist-max-ns})
        hist-ref     (AtomicReference. warmup-hist)
        sem          (case kind
                       :closed (Semaphore. (int (:concurrency load 1)))
                       (Semaphore. 0))
        sink         (mk-sink hist-ref done-ctr
                              (when (= :closed kind) sem))
        pipeline     (step/serial step sink)
        handle       (flow/start! pipeline {:subscribers subscribers})
        stop-drainer (start-event-drainer handle drain-ms)
        wall-start   (System/nanoTime)]
    (try
      (case kind
        :closed
        (let [stop-inject
              (start-closed-loop-injector handle sem (:concurrency load 1) input-fn sent-ctr)]
          (try
            (Thread/sleep (long (* 1000 (long warmup-s))))
            (let [measurement-hist (hist/dense {:a 2 :b 4 :max-v hist-max-ns})
                  pre-sent (.get sent-ctr)
                  pre-done (.get done-ctr)]
              (.set hist-ref measurement-hist)
              (let [t0 (System/nanoTime)]
                (Thread/sleep (long (* 1000 (long duration-s))))
                (let [t1 (System/nanoTime)]
                  (stop-inject)
                  (flow/await-quiescent! handle 5000)
                  (let [snap (hist/snapshot measurement-hist)
                        post-sent (.get sent-ctr)
                        post-done (.get done-ctr)]
                    {:config    {:load load :warmup-s warmup-s :duration-s duration-s}
                     :snapshot  snap
                     :summary   (with-rates snap
                                            (- post-sent pre-sent)
                                            (- post-done pre-done)
                                            (- t1 t0))
                     :sent      (.get sent-ctr)
                     :completed (.get done-ctr)
                     :elapsed-ns (- (System/nanoTime) wall-start)}))))
            (finally (stop-inject))))

        :count
        (let [n (int (:count load))]
          (dotimes [_ n]
            (flow/inject! handle {:data (stamp-payload (input-fn))})
            (.incrementAndGet sent-ctr))
          (flow/await-quiescent! handle (max 60000 (* 10 n)))
          (let [snap (hist/snapshot warmup-hist)]
            {:config    {:load load}
             :snapshot  snap
             :summary   (with-rates snap (.get sent-ctr) (.get done-ctr)
                                    (- (System/nanoTime) wall-start))
             :sent      (.get sent-ctr)
             :completed (.get done-ctr)
             :elapsed-ns (- (System/nanoTime) wall-start)})))
      (finally
        (stop-drainer)
        (flow/stop! handle)))))

;; ---- Canonical workloads (seed; will grow in Phase 3) --------------

(defn noop-step
  "Single identity stage. Baseline for harness overhead + pubsub cost."
  []
  (step/step ::noop identity))

(defn chain-n-step
  "Linear chain of N identity stages. Each stage pays one full proc
   recv → handler → send-out round-trip; pubsub events + channel
   transfers scale linearly with N."
  [n]
  (when-not (pos? n) (throw (ex-info "chain-n-step: n must be positive" {:n n})))
  (apply step/serial
         (map (fn [i] (step/step (keyword (str "chain-" i)) identity))
              (range n))))

(defn workers-w-step
  "W parallel identity workers behind a round-robin router + join.
   Throughput should scale with W up to the router/join bottleneck."
  [w]
  (dc/workers w (step/step :inner identity)))

;; ---- Reporting ----------------------------------------------------

(defn- fmt-ns [ns]
  (let [ns (double ns)]
    (cond
      (< ns 1e3) (format "%6.1f ns" ns)
      (< ns 1e6) (format "%6.2f us" (/ ns 1e3))
      (< ns 1e9) (format "%6.2f ms" (/ ns 1e6))
      :else      (format "%6.2f s " (/ ns 1e9)))))

(defn- fmt-rate [^double r]
  (cond
    (> r 1e6) (format "%6.2fM/s" (/ r 1e6))
    (> r 1e3) (format "%6.2fK/s" (/ r 1e3))
    :else     (format "%6.1f /s" r)))

(defn print-result [label result]
  (let [s (:summary result)]
    (println (format "  %-24s  sent=%d  completed=%d  throughput=%s"
                     label
                     (long (or (:sent-msgs s) (:sent result)))
                     (long (or (:completed-msgs s) (:completed result)))
                     (fmt-rate (double (or (:throughput-msgs-per-s s) 0.0)))))
    (println (format "%26sE2E latency: p50=%s  p90=%s  p99=%s  p999=%s  max=%s"
                     "" (fmt-ns (or (:p50 s) 0)) (fmt-ns (or (:p90 s) 0))
                     (fmt-ns (or (:p99 s) 0)) (fmt-ns (or (:p999 s) 0))
                     (fmt-ns (or (:max s) 0))))))

;; ---- Sweep runner --------------------------------------------------

(defn sweep
  "Run a sequence of `[label opts]` pairs through run-bench. Prints each
   result as it finishes (so a crash mid-sweep preserves earlier data).
   Returns a vector of {:label :result :error} maps."
  [scenarios]
  (mapv (fn [[label opts]]
          (try
            (let [r (run-bench opts)]
              (print-result label r)
              {:label label :result r})
            (catch Throwable t
              (println (format "  %-24s  ERROR: %s" label (ex-message t)))
              {:label label :error (ex-message t)})))
        scenarios))

;; ---- Scenario entry -----------------------------------------------

(def ^:private smoke-sweep
  "A small cross-section of topologies × concurrencies. 3s measurement
   per config (enough to stabilize p99 for these shapes)."
  (let [base {:warmup-s 1 :duration-s 3}]
    [["noop        C=1 "  (merge base {:step (noop-step)         :load {:kind :closed :concurrency 1}})]
     ["noop        C=4 "  (merge base {:step (noop-step)         :load {:kind :closed :concurrency 4}})]
     ["noop        C=16"  (merge base {:step (noop-step)         :load {:kind :closed :concurrency 16}})]
     ["chain-3     C=16"  (merge base {:step (chain-n-step 3)    :load {:kind :closed :concurrency 16}})]
     ["chain-10    C=16"  (merge base {:step (chain-n-step 10)   :load {:kind :closed :concurrency 16}})]
     ["workers-4   C=16"  (merge base {:step (workers-w-step 4)  :load {:kind :closed :concurrency 16}})]]))

(defn littles-law-sweep
  "Closed-loop concurrency sweep on a fixed topology. Little's law:
   throughput × mean-latency ≈ concurrency. A stable ratio across C
   indicates the measurement stack is consistent; drift indicates
   either saturation or measurement bias. Prints per-row + a
   Little's-law column."
  ([] (littles-law-sweep (chain-n-step 3) "chain-3"))
  ([step label]
   (println)
   (println (format "=== toolkit.datapotamus — Little's law sweep on %s ===" label))
   (println "  concurrency × throughput × mean-latency should ≈ concurrency")
   (doseq [c [1 2 4 8 16 32]]
     (let [r (run-bench {:step step
                         :load {:kind :closed :concurrency c}
                         :warmup-s 1 :duration-s 3})
           s (:summary r)
           thp (double (or (:throughput-msgs-per-s s) 0.0))
           mean-s (/ (double (or (:mean s) 0)) 1e9)
           little (* thp mean-s)]
       (println (format "  C=%-3d  thp=%s  p50=%s  mean=%s  L=%.2f (target %d)"
                        c
                        (fmt-rate thp)
                        (fmt-ns (or (:p50 s) 0))
                        (fmt-ns (or (:mean s) 0))
                        little c))))))

(defn slow-subscriber-experiment
  "Plan H.9 — demonstrates that a slow pubsub subscriber stalls the
   emitting pipeline, since pubsub delivery is synchronous on the
   publisher's thread. Runs :noop at C=4 with each subscriber variant."
  []
  (println)
  (println "=== toolkit.datapotamus — slow-subscriber throughput collapse ===")
  (println "  Synchronous pubsub: a slow subscriber stalls the emitting proc.")
  (let [base {:step (noop-step)
              :load {:kind :closed :concurrency 4}
              :warmup-s 1 :duration-s 3}]
    (doseq [[label subs]
            [["no extra subscriber"          {}]
             ["1 noop subscriber"            {[:>] (fn [_ _ _] nil)}]
             ["1 slow subscriber (100 us)"
              {[:>] (fn [_ _ _]
                      (java.util.concurrent.locks.LockSupport/parkNanos 100000))}]
             ["1 slow subscriber (1 ms)"
              {[:>] (fn [_ _ _]
                      (try (Thread/sleep 1)
                           (catch InterruptedException _ nil)))}]]]
      (let [r (run-bench (assoc base :subscribers subs))]
        (print-result label r)))))

(defn run
  "Entry invoked by `clojure -M:bench datapotamus`. Runs the smoke sweep.
   For deeper sweeps, call `littles-law-sweep` or
   `slow-subscriber-experiment` directly."
  []
  (println)
  (println "=== toolkit.datapotamus — harness smoke sweep ===")
  (sweep smoke-sweep))

(defn -main [& _] (run))
