(ns render-pipelines-with-stats
  "Run a variety of small pipelines, capture their final state, and
   write each one's tree (with stats columns) to
   /work/dev/data/pipelines_with_stats.md. None of these need LLM calls or
   external services; the variable-latency fixtures use small
   `Thread/sleep` calls to produce realistic-looking distributions.

   Run:
     clojure -J--add-opens=java.base/java.nio=ALL-UNNAMED \\
             -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \\
             -M -e '(load-file \"/work/dev/render_pipelines_with_stats.clj\")'"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [toolkit.datapotamus.combinators.aggregate :as ca]
            [toolkit.datapotamus.combinators.core :as cc]
            [toolkit.datapotamus.combinators.workers :as cw]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.render.capture :as capture]
            [toolkit.datapotamus.render.stats :as stats]
            [toolkit.datapotamus.step :as step]
            [toolkit.pubsub :as pubsub]))

;; --- Latency helpers --------------------------------------------------------

(defn- with-jittered-delay
  "Wrap a 1-arg fn so each call sleeps `base-µs` plus a small,
   data-derived jitter. Produces realistic-looking p50/p99/p100 spread."
  [f base-µs jitter-µs]
  (fn [x]
    (let [sleep-ns (long (* 1000 (+ base-µs (mod (hash x) jitter-µs))))]
      (java.util.concurrent.locks.LockSupport/parkNanos sleep-ns))
    (f x)))

;; --- Pipeline fixtures ------------------------------------------------------

(defn- tiny-chain []
  (step/serial
   (step/step :double inc)
   (step/step :triple #(* 3 %))
   (step/step :format str)
   (step/sink)))

(defn- variable-latency-chain []
  (step/serial
   (step/step :ingest    (with-jittered-delay identity   100  500))
   (step/step :transform (with-jittered-delay #(* 2 %)   500 2000))
   (step/step :validate  (with-jittered-delay (fn [x] x) 200 1000))
   (step/step :emit      (with-jittered-delay str        100  200))
   (step/sink)))

(defn- round-robin-pool [k base-µs jitter-µs]
  (step/serial
   (cw/round-robin-workers
    :pool k
    (step/step :work (with-jittered-delay inc base-µs jitter-µs)))
   (step/sink)))

(defn- stealing-recursive-pool []
  (let [inner (step/handler-map
               {:ports {:ins {:in ""} :outs {:out "" :work ""}}
                :on-data (fn [ctx _ d]
                           (java.util.concurrent.locks.LockSupport/parkNanos 200000)
                           (if (pos? d)
                             {:work [(msg/child ctx (dec d))]}
                             {:out [(msg/child ctx d)]}))})]
    (step/serial
     (cw/stealing-workers :pool 4 inner)
     (step/sink))))

(defn- heterogeneous-parallel []
  (step/serial
   (step/step :prep (with-jittered-delay inc 200 500))
   (cc/parallel :specialists
                {:solver  (step/step :solve   (with-jittered-delay inc 1000 3000))
                 :facts   (step/step :facts   (with-jittered-delay inc  300  500))
                 :skeptic (step/step :skeptic (with-jittered-delay inc  600 1500))})
   (step/sink)))

(defn- multi-element-branch-parallel []
  (step/serial
   (step/step :split (with-jittered-delay inc 200 400))
   (cc/parallel :group
                {:stories  (step/serial (step/step :rate-limit (with-jittered-delay inc 100  200))
                                        (step/step :bucket     (with-jittered-delay inc 300  600)))
                 :comments (step/serial (step/step :window     (with-jittered-delay inc 400  900))
                                        (step/step :explode    (with-jittered-delay inc 200  500)))})
   (step/sink)))

(defn- nested-parallel []
  (step/serial
   (step/step :seed (with-jittered-delay inc 100 200))
   (cc/parallel :outer
                {:left  (cc/parallel :inner-left
                                     {:a (step/step :a (with-jittered-delay inc 150 300))
                                      :b (step/step :b (with-jittered-delay inc 250 500))})
                 :right (cc/parallel :inner-right
                                     {:c (step/step :c (with-jittered-delay inc 400 800))
                                      :d (step/step :d (with-jittered-delay inc  80 200))})})
   (step/sink)))

(defn- filter-pipeline []
  (step/serial
   (step/step :gen   (with-jittered-delay inc 100 200))
   ;; `:filter` keeps even numbers, drops odd via empty port-map.
   (step/step :filter
              {:ins {:in ""} :outs {:out ""}}
              (fn [ctx _ x]
                (java.util.concurrent.locks.LockSupport/parkNanos 100000)
                (if (even? x)
                  {:out [(msg/child ctx x)]}
                  {})))
   (step/step :downstream (with-jittered-delay inc 300 600))
   (step/sink)))

(defn- batched-aggregator []
  (step/serial
   (step/step :tag (with-jittered-delay #(assoc {:v %} :group (mod % 3)) 100 200))
   (ca/batch-by-group :group (fn [g rows] {:group g :n (count rows)
                                            :sum  (reduce + (map :v rows))}))
   (step/sink)))

;; --- Run helper -------------------------------------------------------------

(defn- run-and-capture [stepmap inputs pipeline-name]
  (let [ps      (pubsub/make)
        watcher (stats/make)
        _       (stats/attach! ps watcher)
        h       (flow/start! stepmap {:pubsub ps :flow-id (str pipeline-name)})]
    (try
      (doseq [v inputs] (flow/inject! h {:data v}))
      (flow/inject! h {})
      (flow/await-quiescent! h)
      (capture/capture h stepmap watcher pipeline-name)
      (finally
        (flow/stop! h)))))

;; --- Markdown writer --------------------------------------------------------

(defn- entry [pipeline-name description stepmap inputs]
  (let [snap  (run-and-capture stepmap inputs pipeline-name)
        lines (capture/render-snapshot snap)
        n     (count inputs)]
    (str "## " pipeline-name "\n\n"
         description "\n\n"
         "Inputs: " n " messages\n\n"
         "```\n"
         (str/join "\n" lines)
         "\n```\n")))

(defn -main
  ([] (-main "/work/dev/data/pipelines_with_stats.md"))
  ([out-path]
   (let [doc
         (str "# Datapotamus pipelines — captured run with stats\n\n"
              "_Auto-generated by `dev/render_pipelines_with_stats.clj`._\n\n"
              "Each section runs a small fixture pipeline against a known\n"
              "input batch, captures the final state via `render.capture`,\n"
              "and renders the snapshot. Each line shows: tree structure +\n"
              "`recv`, `done`, `mean`, `p50`, `p99`, `p100` (max). The\n"
              "fixtures use small `LockSupport/parkNanos` calls per step to\n"
              "produce realistic-looking distributions; they need no\n"
              "external services.\n\n"

              (entry 'tiny-chain
                     "Three-step serial chain. No artificial delay."
                     (tiny-chain) (range 100))

              "\n"
              (entry 'variable-latency-chain
                     "Four-step chain with per-step jittered delays. Demonstrates p50/p99/p100 spread per step."
                     (variable-latency-chain) (range 200))

              "\n"
              (entry 'round-robin-workers-k4
                     "Round-robin distribution across 4 identical workers, with per-worker jittered delay."
                     (round-robin-pool 4 500 1500) (range 200))

              "\n"
              (entry 'round-robin-workers-k16
                     "Same pattern with k=16 — exercises K× block aggregation across more members."
                     (round-robin-pool 16 200 800) (range 500))

              "\n"
              (entry 'stealing-workers-recursive
                     (str "Stealing-workers with k=4 and a recursive inner. Each input is "
                          "decremented down to 0 via `:work` feedback. K× blocks pool stats "
                          "across class members.")
                     (stealing-recursive-pool) [3 5 4 2 6 7 4 3 5 4])

              "\n"
              (entry 'heterogeneous-parallel
                     (str "Three named specialists with different per-branch delays — "
                          "no K× block aggregation since branches are heterogeneous.")
                     (heterogeneous-parallel) (range 100))

              "\n"
              (entry 'multi-element-branch-parallel
                     "Two parallel arms, each a 2-step chain. Bracket rail with in-branch FT."
                     (multi-element-branch-parallel) (range 100))

              "\n"
              (entry 'nested-parallel
                     "Two outer arms, each containing its own 2-port `c/parallel` block."
                     (nested-parallel) (range 100))

              "\n"
              (entry 'filter-pipeline
                     "Middle step drops odd inputs via empty port-map. Downstream `recv` < upstream."
                     (filter-pipeline) (range 100))

              "\n"
              (entry 'batched-aggregator
                     "`ca/batch-by-group` aggregates by `:group` key, emits one row per group on input-done."
                     (batched-aggregator) (range 60))
              "\n")]
     (io/make-parents out-path)
     (spit out-path doc)
     (println (format "Wrote %s" out-path)))))

(-main)
