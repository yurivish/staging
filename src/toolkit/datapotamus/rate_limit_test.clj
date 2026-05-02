(ns toolkit.datapotamus.rate-limit-test
  "Tests for `c/rate-limited` — a passthrough step that paces messages
   through a shared token bucket. Insert it before a worker pool to
   bound the pool's effective RPS regardless of K."
  (:refer-clojure :exclude [run!])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]))

;; ============================================================================
;; A. Passthrough — content unchanged
;; ============================================================================

(deftest a1-passthrough-preserves-data
  (let [wf  (step/serial
             (c/rate-limited {:rps 1000 :burst 1000})
             (step/step :double (fn [n] (* 2 n))))
        res (flow/run-seq wf [1 2 3 4 5])]
    (is (= :completed (:state res)))
    (is (= [[2] [4] [6] [8] [10]] (:outputs res)))))

;; ============================================================================
;; B. Burst — N <= burst completes ~immediately
;; ============================================================================

(deftest b1-burst-absorbs-up-to-capacity
  ;; rps=10, burst=20, sending 20 msgs should complete in < 200ms
  ;; (bucket starts full; all 20 consumed from the burst).
  (let [wf  (step/serial
             (c/rate-limited {:rps 10 :burst 20})
             (step/step :id identity))
        t0  (System/currentTimeMillis)
        res (flow/run-seq wf (vec (range 20)))
        ms  (- (System/currentTimeMillis) t0)]
    (is (= :completed (:state res)))
    (is (< ms 200) (str "burst-only run took " ms "ms; expected <200"))))

;; ============================================================================
;; C. Sustained rate — beyond burst, RPS is bounded
;; ============================================================================

(deftest c1-beyond-burst-paces-to-rate
  ;; rps=20, burst=5, sending 25 msgs.
  ;; First 5 immediate (burst). Remaining 20 at 20/s = 1s minimum.
  ;; Total elapsed: >= 1000ms (with some slack for jitter).
  (let [wf  (step/serial
             (c/rate-limited {:rps 20 :burst 5})
             (step/step :id identity))
        t0  (System/currentTimeMillis)
        res (flow/run-seq wf (vec (range 25)))
        ms  (- (System/currentTimeMillis) t0)]
    (is (= :completed (:state res)))
    (is (>= ms 950) (str "expected ≥ ~1000ms for 25 msgs at 20rps/burst5; got " ms))))

;; ============================================================================
;; D. Shared across a worker pool — global RPS bound
;; ============================================================================

(deftest d1-pool-respects-shared-bucket
  ;; This is the headline test: K=8 stealing-workers all draw through
  ;; the SAME upstream rate-limit gate. Total observed throughput must
  ;; stay below the configured rate, not K × rate.
  (let [worker (step/handler-map
                {:ports {:ins {:in ""} :outs {:out ""}}
                 :on-data (fn [ctx _ d]
                            (Thread/sleep 5)  ;; tiny per-task work
                            {:out [(toolkit.datapotamus.msg/child ctx d)]})})
        wf  (step/serial
             (c/rate-limited {:rps 25 :burst 5})
             (c/stealing-workers :pool 8 worker))
        n   30
        t0  (System/currentTimeMillis)
        res (flow/run-seq wf (vec (range n)))
        ms  (- (System/currentTimeMillis) t0)]
    (is (= :completed (:state res)))
    (testing "if the gate worked, 30 msgs at 25rps with burst 5 takes ≥ ~1000ms"
      (is (>= ms 950) (str "pool finished too fast — gate didn't apply globally; ms=" ms)))))

;; ============================================================================
;; E. Property — total time bounded below by (N - burst) / rps
;; ============================================================================

(defspec p1-time-floor-by-bucket-formula 8
  (prop/for-all [n     (gen/choose 5 25)
                 rps   (gen/choose 20 50)
                 burst (gen/choose 1 5)]
    (let [wf  (step/serial
               (c/rate-limited {:rps rps :burst burst})
               (step/step :id identity))
          t0  (System/currentTimeMillis)
          res (flow/run-seq wf (vec (range n)))
          ms  (- (System/currentTimeMillis) t0)
          floor-ms (max 0 (long (* 1000 (/ (max 0 (- n burst)) (double rps)))))]
      (and (= :completed (:state res))
           ;; 100ms slack for thread start, single-msg refill rounding, etc.
           (>= ms (- floor-ms 100))))))
