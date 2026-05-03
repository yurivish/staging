(ns toolkit.datapotamus.render.stats-test
  "Behavior contract for `toolkit.datapotamus.render.stats`. These tests
   are written *before* the implementation (TDD red phase). They will
   fail to load until the namespace exists; once it does, they specify
   exactly what it must do.

   Design intent that motivates this contract:
   - Stats records carry the **full latency histogram snapshot**, not
     pre-computed quantiles. Quantiles are derived on demand from the
     stored histogram.
   - Sample count is also derived from the histogram (no separate
     counter). Two histograms merged via `hist/merge-snapshots` give a
     single histogram covering the union of samples; sample-count and
     all quantiles fall out from that.
   - Aggregating K class members for a `K× <name>` line means: counter
     fields sum; latency histograms merge.

   Note: the `quantile-near?` helper accommodates histogram bin
   granularity (~6% relative error with default a=2 b=4 params)."
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus.render.stats :as stats]
            [toolkit.hist :as hist]))

;; --- Helpers ----------------------------------------------------------------

(defn- record-many [opts values]
  (let [h (hist/dense opts)]
    (doseq [v values] (hist/record-dense! h (long v)))
    (hist/snapshot h)))

(defn- mk-record
  "Build a stats record with `recv` messages, all completed, latency
   histogram populated from `latency-values` (or nil)."
  ([recv] (mk-record recv nil))
  ([recv latency-values]
   {:recv      recv
    :sent      recv
    :completed recv
    :latency   (when (seq latency-values)
                 (record-many {} latency-values))}))

(defn- quantile-near?
  "Within ±tol of expected — accommodates histogram bin width."
  [expected actual tol]
  (and (some? actual)
       (>= (long actual) (- (long expected) (long tol)))
       (<= (long actual) (+ (long expected) (long tol)))))

;; --- Sample count from histogram --------------------------------------------

(deftest empty-record-zero-samples-nil-quantile
  (testing "Stats record with no latency histogram has sample-count 0 and nil quantiles."
    (let [r (mk-record 0)]
      (is (zero? (stats/sample-count r)))
      (is (nil? (stats/quantile r 0.5))))))

(deftest single-sample-derives-itself
  (testing "Recording one value of v ns: sample-count = 1; quantile ≈ v."
    (let [r (mk-record 1 [1000])]
      (is (= 1 (stats/sample-count r)))
      ;; Histogram bin width is small near 1000 ns; allow 100ns tolerance.
      (is (quantile-near? 1000 (stats/quantile r 0.5) 100)))))

(deftest hundred-uniform-samples-derive-quantiles
  (testing "100 samples uniform over [0, 100000) ns: p50 ≈ 50_000, p99 ≈ 99_000."
    (let [values (mapv #(* % 1000) (range 100))   ; 0, 1000, 2000, ..., 99000
          r      (mk-record 100 values)]
      (is (= 100 (stats/sample-count r)))
      ;; ~6% relative error in default histogram: ±6000 around the target.
      (is (quantile-near? 50000 (stats/quantile r 0.5) 6000))
      (is (quantile-near? 99000 (stats/quantile r 0.99) 6000))
      (is (quantile-near?  1000 (stats/quantile r 0.01) 1000)))))

;; --- Merging stats records --------------------------------------------------

(deftest merge-sums-counter-fields
  (testing "Merging two records sums :recv, :sent, :completed."
    (let [a (mk-record 5 [10 20 30 40 50])
          b (mk-record 7 [11 22 33])
          m (stats/merge-stats [a b])]
      (is (= 12 (:recv m)))
      (is (= 12 (:sent m)))
      (is (= 12 (:completed m))))))

(deftest merge-pools-histogram-samples
  (testing "Merging two records combines latency histograms; sample-count is the sum."
    (let [a (mk-record 5 [100 200 300 400 500])
          b (mk-record 5 [150 250 350 450 550])
          m (stats/merge-stats [a b])]
      (is (= 10 (stats/sample-count m))
          "merged histogram contains all 10 samples"))))

(deftest merge-handles-records-with-no-latency
  (testing "A record with no histogram contributes 0 samples but still merges."
    (let [a (mk-record 3 [100 200 300])         ; has latency
          b (mk-record 4)                       ; no latency
          m (stats/merge-stats [a b])]
      (is (= 7 (:recv m)))
      (is (= 3 (stats/sample-count m))          ; only A contributes samples
          "sample-count comes only from records with latency data"))))

(deftest merged-quantile-reflects-pooled-distribution
  (testing "Merged median lies between the two source medians."
    (let [low  (mk-record 50 (mapv #(* % 1000) (range 50)))           ; 0..49000
          high (mk-record 50 (mapv #(* (+ 50 %) 1000) (range 50)))    ; 50000..99000
          m    (stats/merge-stats [low high])]
      (is (= 100 (stats/sample-count m)))
      ;; Pool of [0, 99000]; p50 ≈ 50000.
      (is (quantile-near? 50000 (stats/quantile m 0.5) 6000)))))

;; --- Round-trip via EDN -----------------------------------------------------

(deftest histogram-snapshot-roundtrips-via-edn
  (testing "A captured stats record (with histogram) round-trips through pr-str / read-string with quantiles preserved."
    (let [r-orig   (mk-record 100 (mapv #(* % 1000) (range 100)))
          edn      (pr-str r-orig)
          r-loaded (clojure.edn/read-string edn)]
      (is (= (:recv r-orig) (:recv r-loaded)))
      (is (= (stats/sample-count r-orig) (stats/sample-count r-loaded)))
      ;; Same quantile from the loaded histogram as from the original.
      (is (= (stats/quantile r-orig  0.5)
             (stats/quantile r-loaded 0.5))))))

;; --- Histogram is the source of truth ---------------------------------------

(deftest sample-count-matches-hist-total-count
  (testing "stats/sample-count delegates to hist/total-count on the snapshot."
    (let [snap   (record-many {} (range 42))
          r      {:recv 42 :sent 42 :completed 42 :latency snap}]
      (is (= (hist/total-count snap)
             (stats/sample-count r))))))

(deftest quantile-matches-hist-percentile
  (testing "stats/quantile delegates to hist/percentile on the snapshot."
    (let [snap (record-many {} (mapv #(* % 1000) (range 100)))
          r    {:recv 100 :sent 100 :completed 100 :latency snap}]
      (doseq [p [0.01 0.5 0.9 0.99]]
        (is (= (hist/percentile snap p)
               (stats/quantile r p))
            (str "quantile mismatch at p=" p))))))
