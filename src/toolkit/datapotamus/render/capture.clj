(ns toolkit.datapotamus.render.capture
  "Snapshot a running pipeline's structure + per-step stats into a
   serializable EDN map; load that map back later and render it like
   it were a live flow.

   The captured snapshot is the substrate for two views:
     - **Post-hoc analysis**: capture at end of run; render anytime.
     - **Time-travel** (future): capture at multiple instants, diff
       across runs.

   `:latency` is preserved as a histogram snapshot (a plain map
   from `hist/snapshot`), not pre-computed quantiles, so consumers
   can derive any percentile or merge across class members later
   without losing fidelity."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [toolkit.datapotamus.render :as render]
            [toolkit.datapotamus.render.stats :as stats]
            [toolkit.datapotamus.shape :as shape]
            [toolkit.datapotamus.step :as step])
  (:import [java.time Instant]))

(defn capture
  "Snapshot a running flow. Returns
     {:pipeline    <symbol>
      :captured-at <Instant>
      :topology    {:nodes :edges}
      :stats       {path → stats-record}}
   Pure / non-disruptive — safe to call any time, including after
   `flow/await-quiescent!`. Pass the original stepmap (the one given
   to `flow/start!`) so topology is derived from the user-visible
   structure rather than the framework-instrumented form."
  [flow-handle stepmap watcher pipeline-name]
  {:pipeline    pipeline-name
   :captured-at (Instant/now)
   :topology    (step/topology stepmap)
   :stats       (stats/stats-map-from-flow flow-handle stepmap watcher)})

(defn spit-capture
  "Write a snapshot to `path` as EDN."
  [path snapshot]
  (with-open [w (io/writer path)]
    (binding [*out* w
              *print-length* nil
              *print-level*  nil]
      (pr snapshot))))

(defn slurp-capture
  "Read a snapshot from `path`. Inverse of `spit-capture`."
  [path]
  (with-open [r (io/reader path)]
    (edn/read {:readers {'object identity}}
              (java.io.PushbackReader. r))))

(defn render-snapshot
  "Render a captured snapshot — same output shape as
   `render/render-with-stats` against a live flow."
  ([snapshot] (render-snapshot snapshot nil))
  ([{:keys [topology stats]} opts]
   (let [tree (shape/decompose topology)]
     (render/render-with-stats tree stats opts))))

(defn print-snapshot
  "Print a captured snapshot's rendered tree to *out*."
  ([snapshot] (print-snapshot snapshot nil))
  ([snapshot opts]
   (run! println (render-snapshot snapshot opts))
   nil))
