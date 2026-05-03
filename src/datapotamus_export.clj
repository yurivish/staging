(ns datapotamus-export
  "Snapshot every Datapotamus pipeline in this repo to a single JSON file
   for offline visualization. Each entry carries `:name`, `:description`
   (the namespace ns-form docstring), `:topology` (flat
   `{:nodes :edges}` with port specs spliced onto each leaf), and
   `:tree` (hierarchical `viz/from-step` form).

   Run:
     clojure -X:export-pipelines
     clojure -X:export-pipelines :out '\"/tmp/pipelines.json\"'"
  (:require [toolkit.os-guard]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [toolkit.datapotamus.obs.viz :as viz]
            [toolkit.datapotamus.shape :as shape]
            [toolkit.datapotamus.step :as step]))

;; Every pipeline whose stepmap we can build without real domain inputs.
;; `:invoke` controls how we call `build-flow`:
;;   :no-args      — `(build-flow)`
;;   :opts-map     — try `(build-flow)`, fall back to `(build-flow {})`
;;   :usernames    — `(build-flow ["dang"])` (polite-crawler shape)
;; `:skip` records pipelines we deliberately don't materialize, with a
;; reason string the visualizer can render.
(def pipelines
  [{:ns 'hn.core                     :invoke :no-args}
   {:ns 'hn-alarm.core               :invoke :opts-map}
   {:ns 'hn-buzzword-obituaries.core :invoke :opts-map}
   {:ns 'hn-compose.core             :invoke :opts-map}
   {:ns 'hn-crank-index.core         :invoke :opts-map}
   {:ns 'hn-density.core             :invoke :opts-map}
   {:ns 'hn-drift.core               :invoke :opts-map}
   {:ns 'hn-emotion.core             :invoke :opts-map}
   {:ns 'hn-firehose.core            :invoke :opts-map}
   {:ns 'hn-join.core                :invoke :opts-map}
   {:ns 'hn-mind-changed.core        :invoke :opts-map}
   {:ns 'hn-polite-crawler.core      :invoke :usernames}
   {:ns 'hn-reply-posture.core       :invoke :opts-map}
   {:ns 'hn-self-contradiction.core  :invoke :opts-map}
   {:ns 'hn-self-correct.core        :invoke :opts-map}
   {:ns 'hn-shape.core               :invoke :opts-map}
   {:ns 'toolkit.hn-sota.core        :invoke :opts-map}
   {:ns 'hn-steelman.core            :invoke :opts-map}
   {:ns 'hn-tempo.core               :invoke :opts-map}
   {:ns 'hn-title-editorialize.core  :invoke :opts-map}
   {:ns 'hn-topic-graveyard.core     :invoke :opts-map}
   {:ns 'hn-typing.core              :invoke :opts-map}
   {:ns 'hn-voice-fingerprint.core   :invoke :opts-map}
   {:ns 'distill.core                :invoke :opts-map}
   {:ns 'doublespeak.core            :invoke :opts-map}
   {:ns 'podcast.flow                :invoke :no-args}
   {:ns 'research.core               :invoke :no-args}])

(defn- ns-docstring [ns-sym]
  (some-> (find-ns ns-sym) meta :doc str/trim not-empty))

(defn- build-stepmap [{:keys [ns invoke]}]
  (require ns)
  (let [v (some-> (ns-resolve ns 'build-flow) deref)]
    (when-not v
      (throw (ex-info (str ns "/build-flow not found") {:ns ns})))
    (case invoke
      :no-args   (v)
      :opts-map  (try (v) (catch clojure.lang.ArityException _ (v {})))
      :usernames (v ["dang"]))))

(defn- proc-at [stepmap path]
  (get-in stepmap (interleave (repeat :procs) path)))

(defn- enrich-leaves
  "For each :leaf node, splice in the proc's :ports map so consumers
   can draw input/output port handles."
  [stepmap nodes]
  (mapv (fn [{:keys [path kind] :as n}]
          (if (= kind :leaf)
            (assoc n :ports (:ports (proc-at stepmap path)))
            n))
        nodes))

(defn- pipeline->entry [{:keys [ns skip] :as spec}]
  (if skip
    {:name (str ns) :skipped true :reason skip}
    (try
      (let [stepmap (build-stepmap spec)
            topo    (step/topology stepmap)
            {:keys [nodes edges]} topo]
        {:name        (str ns)
         :description (or (ns-docstring ns) "TODO: add ns docstring")
         :topology    {:nodes (enrich-leaves stepmap nodes) :edges edges}
         :tree        (viz/from-step stepmap)
         :shape       (shape/decompose topo)})
      (catch Throwable t
        {:name (str ns) :error (.getMessage t)}))))

(defn- json-friendly
  "Convert keyword keys and values to plain strings so the JSON has no
   leading colons (consumers are JS, not Clojure)."
  [v]
  (walk/postwalk #(cond-> % (keyword? %) name) v))

(defn dump
  "Walk the registry, snapshot each pipeline's static topology, write a
   single JSON file. Default output: /work/dev/data/pipelines.json."
  [{:keys [out] :or {out "/work/dev/data/pipelines.json"}}]
  (let [entries (mapv pipeline->entry pipelines)]
    (io/make-parents out)
    (with-open [w (io/writer out)]
      (json/write (json-friendly entries) w :indent true))
    (let [n-skip  (count (filter :skipped entries))
          n-err   (count (filter :error entries))
          n-ok    (- (count entries) n-skip n-err)]
      (println (format "Wrote %d entries (%d ok, %d skipped, %d errors) → %s"
                       (count entries) n-ok n-skip n-err out))
      (doseq [e entries :when (:error e)]
        (println (format "  ERROR  %-32s  %s" (:name e) (:error e)))))))
