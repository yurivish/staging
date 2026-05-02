(ns hn-alarm.core-test
  "Tests for the windowed comment-rate alarm pipeline."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-alarm.core :as core]
            [toolkit.datapotamus.flow :as flow]))

(defn- mk-comment [t id author text]
  {:id id :type "comment" :time t :by author :text text})

;; --- A. Pipeline emits per-window summaries -------------------------------

(deftest a1-emits-window-summaries
  (let [evts [(mk-comment 0   1 "alice" "hi")
              (mk-comment 30  2 "bob"   "hello")
              (mk-comment 75  3 "alice" "ok")
              (mk-comment 200 4 "carol" "thanks")]
        res  (flow/run-seq (core/build-flow {:size-ms 100 :alarm-threshold 100}) evts)
        all  (vec (mapcat identity (:outputs res)))
        wins (filterv #(= :window (:kind %)) all)]
    (is (= :completed (:state res)))
    (testing "windows: [0,100) with 3 comments, [100,200) empty (not emitted), [200,300) with 1"
      (let [by-start (into {} (map (juxt :start identity)) wins)]
        (is (= 2 (count wins)))
        (is (= 3 (:n (get by-start 0))))
        (is (= 1 (:n (get by-start 200))))
        (testing "top-by reflects dominant author per window"
          (is (= "alice" (:top-by (get by-start 0)))))))))

;; --- B. Alarm fires when count exceeds threshold --------------------------

(deftest b1-alarm-on-burst
  ;; 5 comments in [0,100) — alarm threshold=3 → alarm fires.
  ;; Window [200,300) has 1 comment — no alarm.
  (let [evts (concat
              (for [i (range 5)] (mk-comment (* i 10) (inc i) "x" "burst"))
              [(mk-comment 200 100 "y" "calm")])
        res  (flow/run-seq (core/build-flow {:size-ms 100 :alarm-threshold 3}) evts)
        all  (vec (mapcat identity (:outputs res)))
        alarms (filterv #(= :alarm (:kind %)) all)]
    (is (= :completed (:state res)))
    (is (= 1 (count alarms)) "exactly one alarm — for the burst window")
    (is (= 0 (:start (first alarms))))
    (is (= 5 (:n (first alarms))))))

;; --- C. Late events bypass the window aggregation -------------------------

(deftest c1-late-events-counted-via-on-late
  (let [evts [(mk-comment 0   1 "alice" "first")
              (mk-comment 250 2 "bob"   "advances watermark")
              (mk-comment 50  3 "carol" "LATE — window [0,100) closed at watermark>=100")]
        late (atom 0)
        res  (flow/run-seq
              (core/build-flow {:size-ms 100 :alarm-threshold 100
                                :on-late (fn [_] (swap! late inc))})
              evts)]
    (is (= :completed (:state res)))
    (is (= 1 @late) "exactly one late event observed")))
