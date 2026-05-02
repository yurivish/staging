(ns toolkit.datapotamus.recorder
  "Event-sourced trace recorder. Subscribes to a Datapotamus run's
   pubsub and accumulates everything that happens into a single map —
   the artefact a retrospective UI loads to walk the run end-to-end.

   Same subscribe-and-reduce pattern as `toolkit.datapotamus.obs.viz`,
   but it keeps the full event stream (not aggregated counters).
   Use `viz` for live dashboards, `recorder` for after-the-fact
   inspection.

   Public API:
     (start-recorder! pubsub run-meta) → {:trace <atom> :stop (fn [])}

   The returned atom holds:
     {:run         <run-meta you passed in>
      :events      [<every event in arrival order>]
      :lifecycle   [<recv|success|failure|send-out|split|merge|inject>]
      :llm-calls   [<:status events whose payload contains :llm-call>]
      :status      [<other :status events>]}"
  (:require [toolkit.pubsub :as pubsub]))

(def ^:private lifecycle-kinds
  #{:inject :recv :success :failure :send-out :split :merge :run-started :flow-error})

(defn- reduce-event [trace _subj ev]
  (let [kind (:kind ev)]
    (cond-> (update trace :events conj ev)
      (and (= :status kind) (contains? (:data ev) :llm-call))
      (update :llm-calls conj ev)

      (and (= :status kind) (not (contains? (:data ev) :llm-call)))
      (update :status conj ev)

      (contains? lifecycle-kinds kind)
      (update :lifecycle conj ev))))

(defn start-recorder!
  "Subscribe to all events on `pubsub`. Returns
   `{:trace <atom>, :stop (fn []) → final-trace-map}`.

   `run-meta` is merged into the initial trace under `:run` and stays
   intact through the run. Call `(:stop handle)` at run end; it
   unsubscribes and returns the deref'd trace map for convenience."
  [pubsub run-meta]
  (let [trace (atom {:run       run-meta
                     :events    []
                     :lifecycle []
                     :llm-calls []
                     :status    []})
        unsub (pubsub/watch pubsub [:>] trace reduce-event)]
    {:trace trace
     :stop  (fn []
              (unsub)
              @trace)}))

(defn assoc-final
  "Merge run-end metadata (timings, totals, registries-by-task,
   prompts-by-stage, …) into a finalized trace map. Convenience
   so callers don't have to build the assoc-fold themselves."
  [trace m]
  (update trace :run merge m))
