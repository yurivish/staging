(ns toolkit.datapotamus.window-test
  "Tests for time-window combinators — tumbling and sliding windows
   keyed by event-time, with late-arrival drop semantics. Watermark
   advances with the max event-time seen; windows close when the
   watermark reaches their end."
  (:refer-clojure :exclude [run!])
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]))

(defn- mk-event [t v] {:time t :v v})

;; ============================================================================
;; A. Tumbling — non-overlapping, every event in exactly one window
;; ============================================================================

(deftest a1-tumbling-buckets-by-window
  (let [wf  (c/tumbling-window
             {:size-ms 100
              :time-fn :time
              :on-window (fn [start end items]
                           {:start start :end end
                            :n (count items)
                            :sum (reduce + (map :v items))})})
        events [(mk-event   0 1) (mk-event  50 2) (mk-event  99 3)
                (mk-event 100 4) (mk-event 150 5)
                (mk-event 200 6)]
        res (flow/run-seq wf events)
        ;; Outputs include trailing flush from :on-all-input-done.
        all (vec (mapcat identity (:outputs res)))]
    (is (= :completed (:state res)))
    (testing "three windows: [0,100) sum=6, [100,200) sum=9, [200,300) sum=6"
      (is (= [{:start 0   :end 100 :n 3 :sum 6}
              {:start 100 :end 200 :n 2 :sum 9}
              {:start 200 :end 300 :n 1 :sum 6}]
             (sort-by :start all))))))

(deftest a2-tumbling-empty-windows-not-emitted
  ;; Events at t=0 and t=500. Windows [100,200), [200,300), [300,400),
  ;; [400,500) are empty and must not be emitted.
  (let [wf  (c/tumbling-window
             {:size-ms 100
              :time-fn :time
              :on-window (fn [s e items] {:start s :end e :n (count items)})})
        res (flow/run-seq wf [(mk-event 0 1) (mk-event 500 2)])
        all (vec (mapcat identity (:outputs res)))]
    (is (= :completed (:state res)))
    (is (= 2 (count all)))
    (is (= #{0 500} (set (map :start all))))))

(deftest a3-tumbling-late-events-dropped
  ;; Watermark advances to 250 (window [200,300) opened). Then a late
  ;; event at t=50 arrives — earlier window already closed → drop.
  (let [drops (atom [])
        wf (c/tumbling-window
            {:size-ms     100
             :time-fn     :time
             :on-late     (fn [item] (swap! drops conj item))
             :on-window   (fn [s e items] {:start s :n (count items)})})
        events [(mk-event   0 :a)
                (mk-event 250 :b)   ;; advances watermark to 250 → closes [0,100), [100,200)
                (mk-event  50 :late)  ;; late, dropped
                (mk-event 300 :c)]
        res (flow/run-seq wf events)
        all (vec (mapcat identity (:outputs res)))]
    (is (= :completed (:state res)))
    (testing "late event recorded via on-late, not in any emitted window"
      (is (= [{:time 50 :v :late}] @drops))
      (is (= #{0 200 300} (set (map :start all)))))))

;; ============================================================================
;; B. Sliding — overlapping windows, every event belongs to multiple
;; ============================================================================

(deftest b1-sliding-emits-overlapping-windows
  ;; size=300, slide=100. Events at t=0,100,200,300,400 — each falls
  ;; into multiple windows. Window [start, start+300) for each
  ;; start in {-200 -100 0 100 200 300 400}; only those with at
  ;; least one event get emitted.
  (let [wf  (c/sliding-window
             {:size-ms 300 :slide-ms 100
              :time-fn :time
              :on-window (fn [s e items]
                           {:start s :end e :n (count items)
                            :vs (sort (map :v items))})})
        events (mapv #(mk-event (* % 100) %) (range 5))  ;; t=0..400, v=0..4
        res (flow/run-seq wf events)
        all (sort-by :start (vec (mapcat identity (:outputs res))))]
    (is (= :completed (:state res)))
    (testing "window starting at 100 covers [100,400), so events at t=100,200,300"
      (let [w (first (filter #(= 100 (:start %)) all))]
        (is (= 3 (:n w)))
        (is (= [1 2 3] (:vs w)))))
    (testing "window starting at 200 covers [200,500), events at t=200,300,400"
      (let [w (first (filter #(= 200 (:start %)) all))]
        (is (= 3 (:n w)))
        (is (= [2 3 4] (:vs w)))))))
