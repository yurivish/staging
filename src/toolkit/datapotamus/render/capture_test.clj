(ns toolkit.datapotamus.render.capture-test
  "End-to-end behavior contract for `toolkit.datapotamus.render.capture`.
   Written before implementation. Runs a tiny pipeline, captures its
   final state, serializes/deserializes the snapshot, and renders.

   The captured snapshot must contain enough information to:
     1. Reconstruct the pipeline tree (via `:topology`).
     2. Re-derive any stats column (via `:stats` map containing
        `:latency` histogram snapshots, not pre-computed quantiles).

   These tests hit a real flow but use the most minimal possible
   topology (3-step serial chain) so behavior is deterministic and
   easy to reason about."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.render :as render]
            [toolkit.datapotamus.render.capture :as capture]
            [toolkit.datapotamus.render.stats :as stats]
            [toolkit.datapotamus.step :as step]
            [toolkit.pubsub :as pubsub]))

(defn- tiny-pipeline []
  (step/serial
   (step/step :double inc)
   (step/step :triple #(* 3 %))
   (step/step :format str)
   (step/sink)))

(defn- run-and-capture
  "Run `n` messages through `(tiny-pipeline)`, attach a stats watcher,
   wait for quiescence, capture. Returns the captured snapshot."
  [n]
  (let [sm      (tiny-pipeline)
        ps      (pubsub/make)
        watcher (stats/make)
        _       (stats/attach! ps watcher)
        h       (flow/start! sm {:pubsub ps :flow-id "capture-test"})]
    (try
      (doseq [v (range n)] (flow/inject! h {:data v}))
      (flow/inject! h {})                         ; input-done
      (flow/await-quiescent! h)
      (capture/capture h sm watcher 'tiny-pipeline)
      (finally
        (flow/stop! h)))))

;; --- Snapshot shape ---------------------------------------------------------

(deftest capture-includes-pipeline-topology-and-stats
  (testing "capture/capture returns a snapshot map with the four required keys."
    (let [snap (run-and-capture 10)]
      (is (= 'tiny-pipeline (:pipeline snap)))
      (is (some? (:captured-at snap)))
      (is (map? (:topology snap)))
      (is (vector? (:nodes (:topology snap))))
      (is (vector? (:edges (:topology snap))))
      (is (map? (:stats snap)))
      ;; Each :stats value is a stats record keyed by step path.
      (is (every? vector? (keys (:stats snap)))))))

(deftest capture-records-recv-per-step
  (testing "After 10 messages, every step's :recv is at least 10. (Input-done also counts as a recv, so total may be 11.)"
    (let [snap (run-and-capture 10)]
      (doseq [step-path [[:double] [:triple] [:format]]]
        (let [recv (:recv (get (:stats snap) step-path))]
          (is (>= recv 10)
              (str "expected :recv ≥ 10 at " step-path ", got: " recv)))))))

(deftest capture-stores-latency-as-histogram-not-quantiles
  (testing "A captured stats record's :latency is a histogram snapshot, NOT a {:p50 ...} map. Quantiles must be derivable on demand from this stored histogram."
    (let [snap (run-and-capture 10)
          rec  (get (:stats snap) [:double])]
      (is (some? (:latency rec)))
      ;; The kind tag distinguishes histogram snapshots; quantile
      ;; pre-aggregates would never have :kind.
      (is (contains? #{:dense :sparse} (:kind (:latency rec)))
          (str "expected :latency to be a hist snapshot map; got: "
               (pr-str (:latency rec)))))))

;; --- EDN round-trip ---------------------------------------------------------

(deftest snapshot-roundtrips-through-file
  (testing "spit-capture writes and slurp-capture reads a snapshot losslessly."
    (let [orig (run-and-capture 10)
          tmp  (java.io.File/createTempFile "dp-cap-" ".edn")]
      (try
        (capture/spit-capture (.getCanonicalPath tmp) orig)
        (let [loaded (capture/slurp-capture (.getCanonicalPath tmp))]
          (is (= (:pipeline orig) (:pipeline loaded)))
          (is (= (:topology orig) (:topology loaded)))
          ;; Stats records must round-trip — same sample-count and
          ;; same quantiles after serialization.
          (doseq [[path orig-rec] (:stats orig)
                  :let [loaded-rec (get-in loaded [:stats path])]]
            (is (= (:recv orig-rec) (:recv loaded-rec))
                (str "recv mismatch at " path))
            (is (= (stats/sample-count orig-rec)
                   (stats/sample-count loaded-rec))
                (str "sample-count mismatch at " path))
            (when (some? (:latency orig-rec))
              (is (= (stats/quantile orig-rec 0.5)
                     (stats/quantile loaded-rec 0.5))
                  (str "p50 mismatch at " path)))))
        (finally
          (.delete tmp))))))

;; --- Rendering with the captured snapshot -----------------------------------

(deftest render-snapshot-shows-counts
  (testing "render-snapshot emits one line per step, each showing a non-zero done count."
    (let [snap (run-and-capture 10)
          out  (capture/render-snapshot snap)]
      (is (sequential? out))
      ;; One line per step, each with a done-count column.
      (doseq [name ["double" "triple" "format"]]
        (is (some #(and (str/includes? % name)
                        (re-find #"done\s+\d+" %))
                  out)
            (str "expected '" name "' line with done count; got: "
                 (pr-str out)))))))

(deftest render-with-stats-pads-tree-section-and-aligns-columns
  (testing "Tree section is padded so stat columns line up across rows."
    (let [snap (run-and-capture 10)
          out  (capture/render-snapshot snap)
          ;; Find lines that have stats columns (heuristic: contain '│').
          stat-lines (filter #(str/includes? % "│") out)
          ;; Position of the column separator should be the same across
          ;; lines — that's what alignment means.
          first-pipe (when (seq stat-lines)
                       (.indexOf ^String (first stat-lines) (int \│)))]
      (is (seq stat-lines))
      (is (every? #(= first-pipe (.indexOf ^String % (int \│))) stat-lines)
          "all stat-bearing lines should have the column separator at the same column"))))
