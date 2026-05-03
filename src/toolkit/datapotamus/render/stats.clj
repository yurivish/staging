(ns toolkit.datapotamus.render.stats
  "Per-step stats records and aggregation. A stats record is a plain
   map with counter fields and a `:latency` histogram snapshot:

     {:recv      Long
      :sent      Long
      :completed Long
      :queued    Long           ; from `flow/ping` at snapshot time
      :latency   <hist/snapshot> | nil}

   The histogram is the *source of truth* for sample count and
   quantiles — no pre-aggregated p50/p99 fields. `quantile` and
   `sample-count` derive from `:latency` on demand. Merging two
   records sums counters and merges histograms via
   `hist/merge-snapshots`, so `K× <name>` lines aggregate cleanly.

   `make` + `attach!` set up a combined per-step watcher (counters +
   latency histograms). `stats-map-from-flow` reads from that watcher,
   the flow's graph (for queue depths via `flow/ping`), and the
   flow's topology to produce a `{path → stats-record}` map. Pure /
   non-disruptive at snapshot time."
  (:require [clojure.core.async.flow :as caf]
            [clojure.datafy :as dat]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.watchers.latency :as latency]
            [toolkit.hist :as hist]
            [toolkit.pubsub :as pubsub])
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic LongAdder]))

;; --- Per-record derivations -------------------------------------------------

(defn sample-count
  "Number of latency samples in a stats record (0 if no histogram)."
  ^long [stats-record]
  (if-let [snap (:latency stats-record)]
    (hist/total-count snap)
    0))

(defn quantile
  "Value at quantile `p ∈ [0,1]` derived from the record's histogram.
   nil if no samples. Delegates to `hist/percentile`."
  [stats-record p]
  (when-let [snap (:latency stats-record)]
    (hist/percentile snap p)))

;; --- Aggregation ------------------------------------------------------------

(defn- merge-latencies
  "Merge a seq of histogram snapshots (some may be nil). Returns nil if
   none were present, otherwise the merged snapshot."
  [snaps]
  (let [present (filter some? snaps)]
    (when (seq present)
      (reduce hist/merge-snapshots present))))

(defn merge-stats
  "Combine N stats records. Counter fields sum; latency histograms
   merge. Used to aggregate K class members into one displayed row."
  [records]
  (let [sum-key (fn [k] (apply + (map #(or (k %) 0) records)))]
    {:recv      (sum-key :recv)
     :sent      (sum-key :sent)
     :completed (sum-key :completed)
     :queued    (sum-key :queued)
     :latency   (merge-latencies (map :latency records))}))

;; --- Per-step counter accumulator ------------------------------------------

(defn- bump! [^ConcurrentHashMap m k]
  (let [existing (.get m k)
        ^LongAdder adder
        (or existing
            (let [na (LongAdder.)]
              (or (.putIfAbsent m k na) na)))]
    (.increment adder)))

(defn- counter-snapshot
  "Read all counter values into a plain `{[step-id field] long}` map."
  [^ConcurrentHashMap counts]
  (into {}
        (map (fn [^java.util.Map$Entry e]
               [(.getKey e) (.sum ^LongAdder (.getValue e))]))
        (.entrySet counts)))

;; --- Combined watcher (counters + latency) ---------------------------------

(defn make
  "Fresh stats watcher. Bundles a per-step counter table and a
   latency histogram (from `watchers/latency/make`)."
  ([] (make nil))
  ([hist-opts]
   {:counts          (ConcurrentHashMap.)
    :latency-watcher (latency/make hist-opts)}))

(defn attach!
  "Subscribe `watcher` to events on `pubsub`. Returns a zero-arg
   unsub fn that detaches both subscriptions."
  [pubsub watcher]
  (let [unsub-latency (latency/attach! pubsub (:latency-watcher watcher))
        unsub-counts
        (pubsub/sub pubsub [:>]
                    (fn [subj ev _]
                      (when-let [sid (:step-id ev)]
                        (case (first subj)
                          "recv"     (bump! (:counts watcher) [sid :recv])
                          "send-out" (bump! (:counts watcher) [sid :sent])
                          ("success" "failure")
                          (bump! (:counts watcher) [sid :completed])
                          nil))))]
    (fn []
      (unsub-counts)
      (unsub-latency))))

(defn- queue-depths-from-graph
  "Per-path queue depth (sum of input-channel buffer counts) via
   `clojure.core.async.flow/ping`. Returns `{path → long}`. Missing
   paths from the ping result default to 0."
  [graph]
  (when graph
    (try
      (let [p (caf/ping graph)]
        (reduce-kv
         (fn [acc path proc]
           (let [info (dat/datafy proc)
                 ins  (:clojure.core.async.flow/ins info)
                 q    (reduce
                       (fn [s [_port port-info]]
                         (+ s (long (or (get-in port-info [:buffer :count]) 0))))
                       0
                       ins)]
             (cond-> acc (pos? q) (assoc path q))))
         {}
         p))
      ;; A stopped flow throws on ping; a captured-after-stop snapshot
      ;; gets queued = 0 for every path.
      (catch Throwable _ {}))))

(defn stats-map-from-flow
  "Snapshot a flow's per-step stats. `flow-handle` is from `flow/start!`;
   `stepmap` is the original (pre-instrumentation) stepmap so paths
   reflect user-visible structure; `watcher` is from `make` and must
   already be `attach!`'d. Returns `{path → stats-record}` keyed by
   the same path vectors used in `step/topology` output."
  [flow-handle stepmap watcher]
  (let [topology       (step/topology stepmap)
        all-paths      (mapv :path (:nodes topology))
        counts         (counter-snapshot (:counts watcher))
        latency-by-sid (latency/snapshot (:latency-watcher watcher))
        queues         (queue-depths-from-graph
                        (:toolkit.datapotamus.flow/graph flow-handle))]
    (into {}
          (for [path all-paths
                :let [tip (last path)]]
            [path {:recv      (or (get counts [tip :recv]) 0)
                   :sent      (or (get counts [tip :sent]) 0)
                   :completed (or (get counts [tip :completed]) 0)
                   :queued    (or (get queues path) 0)
                   :latency   (get latency-by-sid tip)}]))))
