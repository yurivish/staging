(ns toolkit.hist-test
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.hist :as h])
  (:import [java.util.concurrent CountDownLatch]))

(deftest empty-histogram-summary-is-nil
  (is (nil? (h/summary (h/snapshot (h/dense)))))
  (is (nil? (h/summary (h/snapshot (h/sparse)))))
  (is (zero? (h/total-count (h/snapshot (h/dense))))))

(deftest dense-uniform-percentiles
  ;; Uniform [0, 10000). With b=4 we expect ~6% relative error in the
  ;; log-linear tail; linear section (0..127) is exact.
  (let [dh (h/dense)]
    (dotimes [i 10000] (h/record-dense! dh i))
    (let [s    (h/snapshot dh)
          stat (h/summary s)]
      (is (= 10000 (:count stat)))
      (is (= 0     (:min stat)))
      ;; p50 ~ 5000 within one bin width (~320)
      (is (<= 4700 (:p50 stat) 5300))
      (is (<= 8900 (:p90 stat) 9100))
      (is (<= 9800 (:p99 stat) 10240))
      ;; mean ~ 4999.5 within 1%
      (is (<= 4950.0 (:mean stat) 5050.0)))))

(deftest dense-and-sparse-agree
  (let [dh (h/dense)
        sh (h/sparse)
        vs (for [i (range 5000)] (bit-shift-left (inc i) 3))]
    (doseq [v vs]
      (h/record-dense!  dh v)
      (h/record-sparse! sh v))
    (let [ds (h/summary (h/snapshot dh))
          ss (h/summary (h/snapshot sh))]
      (doseq [k [:count :min :max :p50 :p90 :p99 :p999]]
        (is (= (get ds k) (get ss k))
            (str "disagreement on " k ": dense=" (get ds k) " sparse=" (get ss k)))))))

(deftest out-of-range-clamps-to-top-bin
  (let [dh (h/dense {:a 2 :b 4 :max-v 1024})]
    (h/record-dense! dh 10)
    (h/record-dense! dh 500)
    (h/record-dense! dh 999999)       ; > max-v, must clamp
    (h/record-dense! dh Long/MAX_VALUE)
    (let [s (h/snapshot dh)]
      (is (= 4 (h/total-count s))))))

(deftest negative-clamps-to-zero
  (let [dh (h/dense)]
    (h/record-dense! dh -5)
    (h/record-dense! dh -1)
    (h/record-dense! dh 0)
    (let [s (h/snapshot dh)]
      (is (= 3 (h/total-count s)))
      (is (= 0 (:min (h/summary s)))))))

(deftest merge-two-snapshots
  (let [a (h/dense) b (h/dense)]
    (dotimes [i 1000] (h/record-dense! a i))
    (dotimes [i 1000] (h/record-dense! b (+ i 1000)))
    (let [m (h/merge-snapshots (h/snapshot a) (h/snapshot b))]
      (is (= 2000 (h/total-count m)))
      (is (<= 950 (:p50 (h/summary m)) 1050)))))

(deftest concurrent-dense-recording-is-lossless
  (let [threads     16
        per-thread  50000
        dh          (h/dense)
        start-latch (CountDownLatch. 1)
        done-latch  (CountDownLatch. threads)]
    (dotimes [t threads]
      (.start (Thread/ofVirtual)
        (reify Runnable
          (run [_]
            (.await start-latch)
            (dotimes [i per-thread]
              (h/record-dense! dh (+ (* t per-thread) i)))
            (.countDown done-latch)))))
    (.countDown start-latch)
    (.await done-latch)
    (is (= (* threads per-thread) (h/total-count (h/snapshot dh))))))

(deftest concurrent-sparse-recording-is-lossless
  (let [threads     8
        per-thread  50000
        sh          (h/sparse)
        start-latch (CountDownLatch. 1)
        done-latch  (CountDownLatch. threads)]
    (dotimes [t threads]
      (.start (Thread/ofVirtual)
        (reify Runnable
          (run [_]
            (.await start-latch)
            (dotimes [i per-thread]
              (h/record-sparse! sh (+ (* t per-thread) i)))
            (.countDown done-latch)))))
    (.countDown start-latch)
    (.await done-latch)
    (is (= (* threads per-thread) (h/total-count (h/snapshot sh))))))

(deftest snapshot-is-edn-serializable
  (let [dh (h/dense)]
    (dotimes [i 1000] (h/record-dense! dh (* i i)))
    (let [s (h/snapshot dh)
          round-trip (-> s pr-str read-string)]
      (is (= (:kind s) (:kind round-trip)))
      (is (= (:a s)    (:a round-trip)))
      (is (= (:b s)    (:b round-trip)))
      (is (= (:bins s) (:bins round-trip))))))

(deftest record-generic-dispatches-by-type
  (let [dh (h/dense)
        sh (h/sparse)]
    (dotimes [i 100]
      (h/record! dh i)
      (h/record! sh i))
    (is (= 100 (h/total-count (h/snapshot dh))))
    (is (= 100 (h/total-count (h/snapshot sh))))))
