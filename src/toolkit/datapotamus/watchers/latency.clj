(ns toolkit.datapotamus.watchers.latency
  "Per-step latency watcher. On each `:recv`, stash the message's start
   time; on `:success`/`:failure`, look it up and record the duration into
   that step's H2 histogram (`toolkit.hist`).

   The watcher is fully mutable — `ConcurrentHashMap`s for the in-flight
   table and the per-step histograms, atomic bins inside each histogram —
   so it doesn't fit `pubsub/watch`'s pure-reducer atom contract. Use
   `pubsub/sub` directly via `attach!`.

   Designed to be attached at or before flow start. A success/failure
   without a matching recv in the in-flight table is silently dropped."
  (:require [toolkit.pubsub :as pubsub]
            [toolkit.hist :as hist])
  (:import [java.util.concurrent ConcurrentHashMap]))

(defrecord Watcher [^ConcurrentHashMap in-flight ^ConcurrentHashMap hists hist-opts])

(defn make
  "Fresh watcher. `opts` are passed to `hist/dense` for each per-step
   histogram (default covers 0..10^11 ns with ~6% relative error)."
  ([] (make nil))
  ([hist-opts] (->Watcher (ConcurrentHashMap.) (ConcurrentHashMap.) hist-opts)))

(defn- ensure-hist [^ConcurrentHashMap hists step-id hist-opts]
  (or (.get hists step-id)
      (let [h (hist/dense (or hist-opts {}))]
        (or (.putIfAbsent hists step-id h) h))))

(defn- record-event! [^Watcher w [kind & _] {:keys [step-id msg-id at]}]
  (case kind
    "recv"      (.put ^ConcurrentHashMap (.-in-flight w) msg-id at)
    ("success"
     "failure") (when-let [t0 (.remove ^ConcurrentHashMap (.-in-flight w) msg-id)]
                  (hist/record-dense! (ensure-hist (.-hists w) step-id (.-hist-opts w))
                                      (- (long at) (long t0))))
    nil))

(defn attach!
  "Subscribe `watcher` to events on `pubsub`. Returns the zero-arg unsub fn.
   Opts: `{:tap (fn [subj msg])}` — called after each event for side
   effects on the same subscription."
  ([pubsub watcher] (attach! pubsub watcher nil))
  ([pubsub watcher {:keys [tap]}]
   (pubsub/sub pubsub [:>]
               (if tap
                 (fn [subj msg _]
                   (record-event! watcher subj msg)
                   (tap subj msg))
                 (fn [subj msg _]
                   (record-event! watcher subj msg))))))

(defn snapshot
  "Per-step `{step-id snap}` map of frozen histogram snapshots. Each snap
   is a value compatible with `hist/percentile`, `hist/summary`, etc."
  [^Watcher w]
  (into {}
        (map (fn [^java.util.Map$Entry e]
               [(.getKey e) (hist/snapshot (.getValue e))]))
        (.entrySet ^ConcurrentHashMap (.-hists w))))

(defn in-flight
  "Snapshot view of pending `{msg-id recv-at}`. Useful for debugging."
  [^Watcher w]
  (into {} ^ConcurrentHashMap (.-in-flight w)))

(defn percentile
  "Convenience: `{step-id value-at-p}` from `(snapshot watcher)`. Sample
   each step's histogram at `p ∈ [0,1]`. Use `(snapshot watcher)` directly
   if you need multiple stats — it caches the freeze."
  [^Watcher w p]
  (into {}
        (map (fn [[sid snap]] [sid (hist/percentile snap p)]))
        (snapshot w)))
