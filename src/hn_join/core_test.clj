(ns hn-join.core-test
  "Tests for the story↔root-comment join pipeline."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-join.core :as core]
            [toolkit.datapotamus.flow :as flow]))

(deftest end-to-end-story-comment-join
  ;; Two stories, each with two root comments.
  (let [m {"https://hacker-news.firebaseio.com/v0/topstories.json" [10 20]
           "https://hacker-news.firebaseio.com/v0/item/10.json"
           {:id 10 :type "story" :title "Story A" :url "ua" :kids [11 12]}
           "https://hacker-news.firebaseio.com/v0/item/11.json"
           {:id 11 :type "comment" :text "A1" :kids []}
           "https://hacker-news.firebaseio.com/v0/item/12.json"
           {:id 12 :type "comment" :text "A2" :kids []}
           "https://hacker-news.firebaseio.com/v0/item/20.json"
           {:id 20 :type "story" :title "Story B" :url "ub" :kids [21]}
           "https://hacker-news.firebaseio.com/v0/item/21.json"
           {:id 21 :type "comment" :text "B1" :kids []}}]
    (with-redefs [core/get-json (fn [url] (or (get m url)
                                              (throw (ex-info "fixture missing" {:url url}))))]
      (let [res (flow/run-seq (core/build-flow {:n-stories 2 :tree-workers 2}) [:tick])
            outs (distinct (mapcat identity (:outputs res)))
            by-id (into {} (map (juxt :story-id identity)) outs)]
        (is (= :completed (:state res)))
        (is (= 2 (count outs)) "one joined row per story")
        (testing "story 10: A1 + A2"
          (let [r (get by-id 10)]
            (is (= "Story A" (:title r)))
            (is (= #{"A1" "A2"} (set (:root-comments r))))))
        (testing "story 20: just B1"
          (let [r (get by-id 20)]
            (is (= "Story B" (:title r)))
            (is (= ["B1"] (:root-comments r)))))))))

(deftest story-with-no-comments-still-emits-row
  ;; require-all? defaults to false so a story without comments still emits.
  (let [m {"https://hacker-news.firebaseio.com/v0/topstories.json" [30]
           "https://hacker-news.firebaseio.com/v0/item/30.json"
           {:id 30 :type "story" :title "lonely" :url "ux" :kids []}}]
    (with-redefs [core/get-json (fn [url] (or (get m url) (throw (ex-info "miss" {:url url}))))]
      (let [res (flow/run-seq (core/build-flow {:n-stories 1 :tree-workers 1}) [:tick])
            outs (distinct (mapcat identity (:outputs res)))]
        (is (= :completed (:state res)))
        (is (= 1 (count outs)))
        (is (= 30 (:story-id (first outs))))
        (is (= [] (:root-comments (first outs))))))))
