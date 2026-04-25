(ns toolkit.datapotamus.latency
  "Per-step latency tracking. On each `:recv`, stash the message's start
   time; on `:success`/`:failure`, look it up and record the duration into
   that step's H2 histogram (`toolkit.hist`).

   The tracker is fully mutable — `ConcurrentHashMap`s for the in-flight
   table and the per-step histograms, atomic bins inside each histogram —
   so it doesn't fit `pubsub/watch`'s pure-reducer atom contract. Use
   `pubsub/sub` directly via `attach!`.

   Designed to be attached at or before flow start. A success/failure
   without a matching recv in the in-flight table is silently dropped."
  (:require [toolkit.pubsub :as pubsub]
            [toolkit.hist :as hist])
  (:import [java.util.concurrent ConcurrentHashMap]))

(defrecord Tracker [^ConcurrentHashMap in-flight ^ConcurrentHashMap hists hist-opts])

(defn make
  "Fresh tracker. `opts` are passed to `hist/dense` for each per-step
   histogram (default covers 0..10^11 ns with ~6% relative error)."
  ([] (make nil))
  ([hist-opts] (->Tracker (ConcurrentHashMap.) (ConcurrentHashMap.) hist-opts)))

(defn- ensure-hist [^ConcurrentHashMap hists step-id hist-opts]
  (or (.get hists step-id)
      (let [h (hist/dense (or hist-opts {}))]
        (or (.putIfAbsent hists step-id h) h))))

(defn- record-event! [^Tracker t [kind & _] {:keys [step-id msg-id at]}]
  (case kind
    "recv"      (.put ^ConcurrentHashMap (.-in-flight t) msg-id at)
    ("success"
     "failure") (when-let [t0 (.remove ^ConcurrentHashMap (.-in-flight t) msg-id)]
                  (hist/record-dense! (ensure-hist (.-hists t) step-id (.-hist-opts t))
                                      (- (long at) (long t0))))
    nil))

(defn attach!
  "Subscribe `tracker` to events on `pubsub`. Returns the zero-arg unsub fn.
   Opts: `{:tap (fn [subj msg])}` — called after each event for side
   effects on the same subscription."
  ([pubsub tracker] (attach! pubsub tracker nil))
  ([pubsub tracker {:keys [tap]}]
   (pubsub/sub pubsub [:>]
               (if tap
                 (fn [subj msg _]
                   (record-event! tracker subj msg)
                   (tap subj msg))
                 (fn [subj msg _]
                   (record-event! tracker subj msg))))))

(defn snapshot
  "Per-step `{step-id snap}` map of frozen histogram snapshots. Each snap
   is a value compatible with `hist/percentile`, `hist/summary`, etc."
  [^Tracker t]
  (into {}
        (map (fn [^java.util.Map$Entry e]
               [(.getKey e) (hist/snapshot (.getValue e))]))
        (.entrySet ^ConcurrentHashMap (.-hists t))))

(defn in-flight
  "Snapshot view of pending `{msg-id recv-at}`. Useful for debugging."
  [^Tracker t]
  (into {} ^ConcurrentHashMap (.-in-flight t)))

(defn percentile
  "Convenience: `{step-id value-at-p}` from `(snapshot tracker)`. Sample
   each step's histogram at `p ∈ [0,1]`. Use `(snapshot tracker)` directly
   if you need multiple stats — it caches the freeze."
  [^Tracker t p]
  (into {}
        (map (fn [[sid snap]] [sid (hist/percentile snap p)]))
        (snapshot t)))
