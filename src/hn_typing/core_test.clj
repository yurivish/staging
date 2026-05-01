(ns hn-typing.core-test
  "Unit + end-to-end tests for hn-typing. No live LLM calls."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-typing.core :as core]
            [toolkit.datapotamus.flow :as flow]))

(def ^:private fixture
  ;; story 1
  ;;  ├── c2 "P1"  → c5 "K1"
  ;;  ├── c3 "P2"  → c6 "K2"
  ;;  └── c4 "P3"  (leaf — no reply-edge)
  {:id 1 :type "story" :time 1000 :title "T" :url "U"
   :kid-trees [{:id 2 :type "comment" :time 1100 :text "P1"
                :kid-trees [{:id 5 :type "comment" :time 1200 :text "K1"
                             :kid-trees []}]}
               {:id 3 :type "comment" :time 1110 :text "P2"
                :kid-trees [{:id 6 :type "comment" :time 1210 :text "K2"
                             :kid-trees []}]}
               {:id 4 :type "comment" :time 1120 :text "P3"
                :kid-trees []}]})

;; --- subtree-size / comment-edges -----------------------------------------

(deftest subtree-size-counts-self-plus-descendants
  (is (= 1 (#'core/subtree-size {:id 99 :kid-trees []})))
  (testing "story with three top-level + two grandchildren = 6"
    (is (= 6 (#'core/subtree-size fixture)))))

(deftest comment-edges-skips-story-root
  (let [edges (#'core/comment-edges fixture)]
    (testing "exactly the two comment→comment edges; no story→comment edges"
      (is (= 2 (count edges)))
      (is (= #{[2 5] [3 6]}
             (set (map (juxt :parent-id :kid-id) edges)))))
    (testing "kid-subtree-size is 1 for childless kids"
      (is (every? #(= 1 (:kid-subtree-size %)) edges)))))

;; --- summarize-story histogram math ----------------------------------------

(deftest summarize-story-histogram
  (let [par-msgs [{:data {:story-id 7 :title "T" :url "U" :edge_type "agree"}}
                  {:data {:story-id 7 :title "T" :url "U" :edge_type "agree"}}
                  {:data {:story-id 7 :title "T" :url "U" :edge_type "extend"}}
                  {:data {:story-id 7 :title "T" :url "U" :edge_type nil}}]
        s        (#'core/summarize-story 7 par-msgs)]
    (is (= 7 (:story_id s)))
    (is (= 4 (:n_edges_classified s)))
    (is (= 1 (:n_unclassifiable s)))
    (is (= 2 (get-in s [:by_type :agree])))
    (is (= 1 (get-in s [:by_type :extend])))
    (is (= 0 (get-in s [:by_type :tangent])))
    (testing "all 7 categories always present in :by_type"
      (is (= (set (map keyword core/edge-types))
             (set (keys (:by_type s))))))))

(deftest summarize-story-empty
  (let [s (#'core/summarize-story 99 [])]
    (is (= 0 (:n_edges_classified s)))
    (is (= 0 (:n_unclassifiable s)))
    (is (every? zero? (vals (:by_type s))))))

;; --- E2E with stubs --------------------------------------------------------

(defn- fixture-lookup [m]
  (fn [url]
    (or (get m url)
        (throw (ex-info (str "fixture missing: " url) {:url url})))))

(defn- canned-classifier
  "Dispatch on parent-text → label."
  [{:keys [parent-text]}]
  (cond
    (= parent-text "P1") "agree"
    (= parent-text "P2") "disagree"
    :else                nil))

(deftest end-to-end-pipeline
  ;; Same fixture shape as above. n-stories 1, max-edges 5 (>= 2 actual).
  (let [m {"https://hacker-news.firebaseio.com/v0/topstories.json" [1]
           "https://hacker-news.firebaseio.com/v0/item/1.json"
           {:id 1 :type "story" :time 1000 :title "T" :url "U"
            :kids [2 3 4]}
           "https://hacker-news.firebaseio.com/v0/item/2.json"
           {:id 2 :type "comment" :time 1100 :text "P1" :kids [5]}
           "https://hacker-news.firebaseio.com/v0/item/3.json"
           {:id 3 :type "comment" :time 1110 :text "P2" :kids [6]}
           "https://hacker-news.firebaseio.com/v0/item/4.json"
           {:id 4 :type "comment" :time 1120 :text "P3" :kids []}
           "https://hacker-news.firebaseio.com/v0/item/5.json"
           {:id 5 :type "comment" :time 1200 :text "K1" :kids []}
           "https://hacker-news.firebaseio.com/v0/item/6.json"
           {:id 6 :type "comment" :time 1210 :text "K2" :kids []}}]
    (with-redefs [core/get-json       (fixture-lookup m)
                  core/classify-edge! canned-classifier]
      (let [res  (flow/run-seq (core/build-flow {:n-stories 1 :tree-workers 1
                                                 :max-edges 5 :llm-workers 1})
                               [:tick])
            ;; Aggregator emits a cumulative summary on each classified edge;
            ;; the last emission per story is the final summary.
            row  (-> res :outputs first last)]
        (is (= :completed (:state res)))
        (is (= 1 (:story_id row)))
        (is (= 2 (:n_edges_classified row)))
        (testing "P1 classified agree, P2 classified disagree, no nils"
          (is (= 1 (get-in row [:by_type :agree])))
          (is (= 1 (get-in row [:by_type :disagree])))
          (is (= 0 (:n_unclassifiable row))))))))
