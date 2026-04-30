(ns hn-drift.core-test
  "Unit + end-to-end tests for hn-drift. No live LLM calls."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-drift.core :as core]
            [toolkit.datapotamus.flow :as flow]))

(defn- node [id text & [kids]]
  {:id id :time 1100 :by "u" :text text :kid-trees (or kids [])})

;; --- subtree-size / top-comments / clip ------------------------------------

(deftest subtree-size-counts-self-plus-descendants
  (is (= 1 (#'core/subtree-size (node 1 "x"))))
  (is (= 3 (#'core/subtree-size (node 1 "x" [(node 2 "y") (node 3 "z")])))))

(deftest top-comments-picks-by-subtree-size
  (let [tree {:id 100 :type "story" :title "T"
              :kid-trees [(node 2 "small")
                          (node 3 "biggest" [(node 4 "x") (node 5 "y") (node 6 "z")])
                          (node 7 "medium" [(node 8 "a")])]}]
    (testing "K=2 picks the two largest by subtree-size"
      (is (= ["biggest" "medium"] (#'core/top-comments tree 2))))
    (testing "filters out empty text"
      (let [t-with-empty (update tree :kid-trees conj (node 9 ""))]
        (is (not (some empty? (#'core/top-comments t-with-empty 99))))))))

(deftest clip-boundary
  (is (= "abc" (#'core/clip "abc" 5)))
  (is (= "abcde" (#'core/clip "abcde" 5)))
  (testing "longer than n → starts with first n chars + ellipsis"
    (let [c (#'core/clip "abcdefgh" 3)]
      (is (clojure.string/starts-with? c "abc"))
      (is (= 4 (count c))))))

;; --- E2E with stubs --------------------------------------------------------

(defn- fixture-lookup [m]
  (fn [url]
    (or (get m url)
        (throw (ex-info (str "fixture missing: " url) {:url url})))))

(defn- canned-drift
  "Dispatch on title → drift report."
  [title _comments]
  (case title
    "On-topic"   {:drift_score 1
                  :discussion_summary "Comments hew to the title."
                  :drift_target ""}
    "Drifted"    {:drift_score 8
                  :discussion_summary "Comments are arguing about something else."
                  :drift_target "tangent topic"}
    {:drift_score nil :discussion_summary nil :drift_target nil}))

(deftest end-to-end-no-aggregator-1to1-flow
  ;; Two stories, each gets one drift report. No aggregator stage in this
  ;; pipeline — stealing-workers' join is the terminal step before the
  ;; collector.
  (let [m {"https://hacker-news.firebaseio.com/v0/topstories.json" [10 20]
           "https://hacker-news.firebaseio.com/v0/item/10.json"
           {:id 10 :type "story" :time 1000 :title "On-topic" :url "u1"
            :kids [101 102]}
           "https://hacker-news.firebaseio.com/v0/item/101.json"
           {:id 101 :type "comment" :time 1100 :text "yes" :kids []}
           "https://hacker-news.firebaseio.com/v0/item/102.json"
           {:id 102 :type "comment" :time 1110 :text "agree" :kids []}
           "https://hacker-news.firebaseio.com/v0/item/20.json"
           {:id 20 :type "story" :time 2000 :title "Drifted" :url "u2"
            :kids [201]}
           "https://hacker-news.firebaseio.com/v0/item/201.json"
           {:id 201 :type "comment" :time 2100 :text "totally unrelated" :kids []}}]
    (with-redefs [core/get-json    (fixture-lookup m)
                  core/score-drift! canned-drift]
      (let [res (flow/run-seq (core/build-flow {:n-stories 2 :tree-workers 2
                                                :k-comments 5 :llm-workers 2})
                              [:tick])
            rows (vec (sort-by :story_id (first (:outputs res))))]
        (is (= :completed (:state res)))
        (is (= 2 (count rows)))
        (let [r10 (first (filter #(= 10 (:story_id %)) rows))
              r20 (first (filter #(= 20 (:story_id %)) rows))]
          (is (= 1 (:drift_score r10)))
          (is (= 8 (:drift_score r20)))
          (is (= "tangent topic" (:drift_target r20))))))))
