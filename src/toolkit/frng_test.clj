(ns toolkit.frng-test
  (:require [clojure.test :refer [deftest is]]
            [toolkit.frng :as frng])
  (:import [java.util Random]))

(defn- make-frng [^long size ^long seed]
  (let [r  (Random. seed)
        bs (byte-array size)]
    (.nextBytes r bs)
    (frng/make bs)))

(deftest int-inclusive-bounds
  (let [f (make-frng 4096 42)]
    (dotimes [_ 200]
      (let [v (frng/int-inclusive f 9)]
        (is (<= 0 v 9))))))

(deftest int-inclusive-zero-max-always-zero
  (let [f (make-frng 64 42)]
    (dotimes [_ 50]
      (is (zero? (frng/int-inclusive f 0))))))

(deftest range-inclusive-bounds
  (let [f (make-frng 4096 42)]
    (dotimes [_ 200]
      (let [v (frng/range-inclusive f 10 20)]
        (is (<= 10 v 20))))))

(deftest weighted-single-key-always-returned
  (let [f (make-frng 1024 42)]
    (dotimes [_ 100]
      (is (= :only (frng/weighted f {:only 5}))))))

(deftest weighted-zero-weight-never-picked
  (let [f     (make-frng 8192 42)
        picks (repeatedly 500 #(frng/weighted f {:a 1 :b 0 :c 1}))]
    (is (not (some #{:b} picks)))
    (is (pos? (count (filter #{:a} picks))))
    (is (pos? (count (filter #{:c} picks))))))

(deftest weighted-rejects-zero-total
  (let [f (make-frng 1024 42)]
    (is (thrown-with-msg? AssertionError #"positive"
          (frng/weighted f {:a 0 :b 0})))))

(deftest weighted-distribution-roughly-matches
  ;; common weight = 99, rare = 1 → expect rare << common
  (let [f          (make-frng 65536 42)
        picks      (repeatedly 2000 #(frng/weighted f {:rare 1 :common 99}))
        rare-count (count (filter #{:rare} picks))]
    (is (<= 0 rare-count 100))
    (is (> (count (filter #{:common} picks)) 1800))))

(deftest swarm-weights-respects-bounds
  (let [f  (make-frng 4096 42)
        ws (frng/swarm-weights f [:a :b :c] 5 15)]
    (is (= #{:a :b :c} (set (keys ws))))
    (doseq [v (vals ws)]
      (is (<= 5 v 15)))))
