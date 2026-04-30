(ns hn-tempo.core-test
  "Unit + end-to-end tests for hn-tempo. No live HN calls."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-tempo.core :as core]
            [toolkit.datapotamus.flow :as flow]))

(defn- mk-tree [comments]
  {:id 1 :type "story" :time 1000 :title "t" :url "u" :kid-trees comments})

(defn- mk-comment [id t & [kids]]
  {:id id :by (str "u" id) :time t :kid-trees (or kids [])})

;; --- burst-analysis --------------------------------------------------------

(deftest burst-empty-or-tiny
  (testing "empty kids → header + n=0, no rate stats"
    (let [r (#'core/burst-analysis (mk-tree []))]
      (is (= 0 (:n_comments r)))
      (is (nil? (:coefficient_of_variation r)))
      (is (some? (:story_id r)))))
  (testing "fewer than 3 comments → no rate stats"
    (let [r (#'core/burst-analysis
             (mk-tree [(mk-comment 2 1100) (mk-comment 3 1200)]))]
      (is (= 2 (:n_comments r)))
      (is (nil? (:coefficient_of_variation r))))))

(deftest burst-uniform-rate-has-low-cv
  (let [evenly (mk-tree (mapv #(mk-comment % (+ 1100 (* 60 %))) (range 10)))
        r      (#'core/burst-analysis evenly)]
    (is (= 10 (:n_comments r)))
    (testing "all gaps are 60s → CV is 0"
      (is (== 0.0 (:coefficient_of_variation r))))
    (is (some? (:peak_5min_count r)))))

(deftest burst-very-bursty-has-high-cv
  ;; 5 comments in the first 50s, then a 1-hour gap, then 5 in 50s.
  (let [bursty (mk-tree (concat (mapv #(mk-comment % (+ 1100 (* 10 %))) (range 5))
                                (mapv #(mk-comment (+ 100 %) (+ 1100 3600 (* 10 %)))
                                      (range 5))))
        r      (#'core/burst-analysis (assoc bursty :kid-trees (vec (:kid-trees bursty))))]
    (is (= 10 (:n_comments r)))
    (testing "huge variance vs. mean → CV well above 1"
      (is (> (:coefficient_of_variation r) 1.0)))))

;; --- tempo-analysis --------------------------------------------------------

(deftest tempo-handles-empty
  (let [r (#'core/tempo-analysis (mk-tree []))]
    (is (nil? (:peak_hour_utc r)))
    (is (nil? (:peak_6h_fraction r)))
    (is (= {} (:hour_histogram r)))))

(deftest tempo-fraction-in-unit-interval
  (let [now      1700000000
        ts       (mapv #(+ now (* 3600 %)) (range 24))
        comments (map-indexed #(mk-comment (+ 100 %1) %2) ts)
        r        (#'core/tempo-analysis (mk-tree (vec comments)))]
    (is (some? (:peak_hour_utc r)))
    (is (<= 0.0 (:peak_6h_fraction r) 1.0))
    (is (= 24 (count (:hour_histogram r))))))

;; --- flow-analysis ---------------------------------------------------------

(deftest flow-shallow
  (let [r (#'core/flow-analysis (mk-tree (mapv #(mk-comment % 1100) (range 5))))]
    (is (= 5 (:n_top_level r)))
    (is (= 0 (:n_deeper r)))
    (is (== 0.0 (:depth_to_breadth r)))
    (is (= 1 (:max_thread_depth r)))))

(deftest flow-deep
  (let [r (#'core/flow-analysis
           (mk-tree [(mk-comment 2 1100
                                 [(mk-comment 3 1200
                                              [(mk-comment 4 1300)])])]))]
    (is (= 1 (:n_top_level r)))
    (is (= 2 (:n_deeper r)))
    (is (== 2.0 (:depth_to_breadth r)))
    (is (= 3 (:max_thread_depth r)))))

;; --- E2E with stubbed get-json ---------------------------------------------

(defn- fixture-lookup [m]
  (fn [url]
    (or (get m url)
        (throw (ex-info (str "fixture missing: " url) {:url url})))))

(deftest end-to-end-parallel-scatter-gather
  (let [m {"https://hacker-news.firebaseio.com/v0/topstories.json" [1]
           "https://hacker-news.firebaseio.com/v0/item/1.json"
           {:id 1 :type "story" :time 1000 :title "t1" :url "u1" :kids [2 3]}
           "https://hacker-news.firebaseio.com/v0/item/2.json"
           {:id 2 :time 1100 :by "alice" :kids []}
           "https://hacker-news.firebaseio.com/v0/item/3.json"
           {:id 3 :time 1200 :by "bob" :kids []}}]
    (with-redefs [core/get-json (fixture-lookup m)]
      (let [res (flow/run-seq (core/build-flow {:n-stories 1 :tree-workers 1})
                              [:tick])
            row (-> res :outputs first first)]
        (is (= :completed (:state res)))
        (testing "row carries header from the burst analyzer"
          (is (= 1 (:story_id row)))
          (is (= "t1" (:title row))))
        (testing "all three analyzer port outputs present"
          (is (some? (:tempo row)))
          (is (some? (:flow row)))
          (is (= 2 (:n_top_level (:flow row))))
          (is (= 0 (:n_deeper (:flow row)))))))))
