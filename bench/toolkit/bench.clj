(ns toolkit.bench
  "Benchmark harness for toolkit data structures.

   Usage:
     clojure -M:bench            — full suite
     clojure -M:bench stree      — stree scenarios only
     clojure -M:bench sublist    — sublist scenarios only
     clojure -M:bench hist       — h2 histogram recording microbench
     clojure -M:bench datapotamus  — datapotamus pipeline harness smoke test
     clojure -M:bench littles      — Little's law sweep (closed-loop concurrency × chain-3)
     clojure -M:bench slow-sub     — slow-subscriber throughput collapse (synchronous pubsub)

   Each scenario produces a table row per dataset size: mean time,
   relative stddev (%), and ops/sec. Scaling is visible by reading down
   the n= column: flat rows confirm sub-linear behavior, steep rows flag
   a hotspot to investigate."
  (:require [criterium.core :as crit]))

(set! *warn-on-reflection* true)

(defn- fmt-time ^String [^double ns]
  (cond
    (< ns 1e3) (format "%7.1f ns" ns)
    (< ns 1e6) (format "%7.2f us" (/ ns 1e3))
    (< ns 1e9) (format "%7.2f ms" (/ ns 1e6))
    :else      (format "%7.2f s " (/ ns 1e9))))

(defn- fmt-ops ^String [^double ns]
  (let [ops (/ 1e9 ns)]
    (cond
      (> ops 1e6) (format "%6.2fM ops/s" (/ ops 1e6))
      (> ops 1e3) (format "%6.2fK ops/s" (/ ops 1e3))
      :else       (format "%7.1f  ops/s" ops))))

(defn bench-case
  "Runs criterium/quick-benchmark* on `thunk`, prints one table row, and
   returns a map with :label, :mean-ns, :stddev-ns, :raw (full result).

   Criterium's result shape: `:mean` is `[mean-seconds ci-pair]` and
   `:variance` is `[variance-seconds^2 ci-pair]`; we pull the point
   estimates and convert mean to ns, variance to stddev-ns."
  [label thunk]
  (let [result   (crit/quick-benchmark* thunk {})
        mean-s   (double (first (:mean result)))
        variance (double (first (:variance result)))
        stddev-s (Math/sqrt variance)
        mean-ns   (* 1e9 mean-s)
        stddev-ns (* 1e9 stddev-s)
        pct       (if (pos? mean-ns) (* 100.0 (/ stddev-ns mean-ns)) 0.0)]
    (println (format "  %-20s  mean=%s  +/-%5.1f%%   %s"
                     label (fmt-time mean-ns) pct (fmt-ops mean-ns)))
    (flush)
    {:label label :mean-ns mean-ns :stddev-ns stddev-ns :raw result}))

(defn header
  "Prints a scenario section header."
  [title]
  (println)
  (println (str "-- " title " " (apply str (repeat (max 2 (- 60 (count title))) \-))))
  (flush))

(defn banner
  "Prints a top-level suite banner."
  [title]
  (println)
  (println (str "=== " title " ==="))
  (flush))

(defn -main
  "Entrypoint. First arg selects a suite; default runs both."
  [& args]
  (let [which (or (first args) "all")]
    (when-not (#{"stree" "sublist" "hist" "datapotamus" "littles" "slow-sub" "all"} which)
      (binding [*out* *err*]
        (println "usage: clojure -M:bench [stree|sublist|hist|datapotamus|littles|slow-sub]"))
      (System/exit 1))
    (when (#{"stree" "all"} which)
      ((requiring-resolve 'toolkit.stree-bench/run)))
    (when (#{"sublist" "all"} which)
      ((requiring-resolve 'toolkit.sublist-bench/run)))
    (when (#{"hist" "all"} which)
      ((requiring-resolve 'toolkit.hist-bench/run)))
    (when (#{"datapotamus" "all"} which)
      ((requiring-resolve 'toolkit.datapotamus-bench/run)))
    (when (= "littles" which)
      ((requiring-resolve 'toolkit.datapotamus-bench/littles-law-sweep)))
    (when (= "slow-sub" which)
      ((requiring-resolve 'toolkit.datapotamus-bench/slow-subscriber-experiment)))
    (shutdown-agents)))
