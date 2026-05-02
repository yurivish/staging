(ns hn-compose.core-test
  "Tests for the composition pipeline. Combines five primitives:
   `c/tumbling-window`, `c/join-by-key`, `c/rate-limited`, `c/with-backoff`,
   and `c/stealing-workers`. The point is to confirm primitives compose
   without losing tokens or stalling."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-compose.core :as core]
            [toolkit.datapotamus.flow :as flow]))

(defn- mk-evt [t kind by id label]
  ;; Stories use :title; comments use :text — same payload either way.
  (cond-> {:kind kind :time t :by by :id id}
    (= kind :story)   (assoc :title label)
    (= kind :comment) (assoc :text label)))

(deftest end-to-end-window-join
  ;; Within window [0,100): alice=2 comments, bob=1 comment, carol=0
  ;; Stories in [0,100): one by alice, one by bob, one by carol.
  ;; Expected joined output (one per story, all keyed within window 0):
  ;;   alice: 2 recent comments, bob: 1, carol: 0 (joined as a singleton because
  ;;   require-all? is false)
  (let [evts [(mk-evt 10 :comment "alice" 1 "a1")
              (mk-evt 20 :comment "bob"   2 "b1")
              (mk-evt 30 :comment "alice" 3 "a2")
              (mk-evt 40 :story   "alice" 100 "T1")
              (mk-evt 50 :story   "bob"   101 "T2")
              (mk-evt 60 :story   "carol" 102 "T3")
              ;; Anchor the watermark past the window so it closes.
              (mk-evt 200 :comment "z" 999 "anchor")]
        res (flow/run-seq (core/build-flow {:size-ms 100}) evts)
        outs (distinct (mapcat identity (:outputs res)))
        in-w0 (filter #(= 0 (:window-start %)) outs)
        by-author (group-by :author in-w0)]
    (is (= :completed (:state res)))
    (testing "all three stories appear, joined with their author's window count"
      (is (= 3 (count in-w0)))
      (is (= 2 (-> by-author (get "alice") first :n-recent-comments)))
      (is (= 1 (-> by-author (get "bob")   first :n-recent-comments)))
      (is (= 0 (-> by-author (get "carol") first :n-recent-comments)))
      (testing "story details preserved through the join"
        (is (= "T1" (-> by-author (get "alice") first :title)))))))
