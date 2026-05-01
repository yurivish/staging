(ns hn-steelman.core-test
  "Unit + e2e tests for hn-steelman."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-steelman.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Unit: edge selection + sort ------------------------------------------

(deftest comment-edges-flat
  (testing "extract reply edges from a tree (parent must be a comment)"
    (let [tree {:id 1 :type "story" :title "S"
                :kid-trees
                [{:id 2 :type "comment" :text "A" :by "alice"
                  :kid-trees
                  [{:id 3 :type "comment" :text "A1" :by "bob"
                    :kid-trees []}
                   {:id 4 :type "comment" :text "A2" :by "carol"
                    :kid-trees []}]}
                 {:id 5 :type "comment" :text "B" :by "dan"
                  :kid-trees []}]}
          es (core/comment-edges tree)]
      ;; only [2→3] and [2→4] are comment→comment edges; 5 has no kids,
      ;; and 1→2, 1→5 are story→comment (excluded).
      (is (= 2 (count es)))
      (is (every? #(= 2 (:parent-id %)) es))
      (is (= #{3 4} (set (map :kid-id es)))))))

(deftest sort-rows-by-engagement
  (testing "sort rows by engagement priority + confidence desc"
    (let [rows [{:engagement "neutral"   :confidence 0.9}
                {:engagement "strawman"  :confidence 0.8}
                {:engagement "steelman"  :confidence 0.6}
                {:engagement "steelman"  :confidence 0.95}]
          out  (core/sort-rows rows)]
      (is (= "steelman" (-> out first :engagement)))
      (is (= 0.95 (-> out first :confidence)))
      (is (= "strawman" (-> out (nth 2) :engagement))))))

;; --- E2E with stubs --------------------------------------------------------

(def base "https://hacker-news.firebaseio.com/v0")

(defn- fixture-lookup [m]
  (fn [url]
    (or (get m url)
        (throw (ex-info (str "test fixture missing url: " url) {:url url})))))

(defn- stub-edge-classify [m]
  (fn [edge] (or (get m (:kid-id edge)) "agree")))

(defn- stub-judge [m]
  (fn [edge]
    (or (get m (:kid-id edge))
        {:engagement "neutral" :confidence 0.5
         :parent-strongest-reading "" :what-reply-addressed ""
         :gap-summary ""})))

(deftest end-to-end-with-stubs
  (testing "filter→expensive: only disagree/correct/attack reach Sonnet"
    (let [items {(str base "/topstories.json") [1]
                 (str base "/item/1.json")
                 {:id 1 :type "story" :title "S" :url "u" :kids [2]}
                 (str base "/item/2.json")
                 {:id 2 :type "comment" :text "claim" :by "alice"
                  :parent 1 :kids [3 4 5]}
                 (str base "/item/3.json")
                 {:id 3 :type "comment" :text "I disagree because…"
                  :by "bob" :parent 2 :kids []}
                 (str base "/item/4.json")
                 {:id 4 :type "comment" :text "you're wrong here"
                  :by "carol" :parent 2 :kids []}
                 (str base "/item/5.json")
                 {:id 5 :type "comment" :text "great point"
                  :by "dan" :parent 2 :kids []}}
          edge-class {3 "disagree" 4 "correct" 5 "agree"}
          judge      {3 {:engagement "strawman"  :confidence 0.85
                         :parent-strongest-reading "x"
                         :what-reply-addressed "y"
                         :gap-summary "z"}
                      4 {:engagement "steelman"  :confidence 0.9
                         :parent-strongest-reading "x"
                         :what-reply-addressed "y"
                         :gap-summary ""}}]
      (with-redefs [core/get-json            (fixture-lookup items)
                    core/llm-edge-classify!  (stub-edge-classify edge-class)
                    core/llm-steelman-judge! (stub-judge judge)]
        (let [res  (flow/run-seq
                     (core/build-flow {:n-stories 1 :tree-workers 2 :workers 2})
                     [:tick])
              rows (first (:outputs res))]
          (is (= :completed (:state res)))
          (testing "only 2 edges (disagree, correct) reached judge → 2 rows"
            (is (= 2 (count rows))))
          (testing "row carries class, engagement, confidence, story info"
            (let [r (first rows)]
              (is (some? (:class r)))
              (is (some? (:engagement r)))
              (is (= 1 (:story-id r)))
              (is (= "S" (:story-title r)))))
          (testing "rows sorted by engagement priority"
            (is (= "steelman" (-> rows first :engagement)))))))))

(deftest end-to-end-no-qualifying-edges
  (testing "all edges are :agree → filter drops everything → empty result"
    (let [items {(str base "/topstories.json") [1]
                 (str base "/item/1.json")
                 {:id 1 :type "story" :title "S" :url "u" :kids [2]}
                 (str base "/item/2.json")
                 {:id 2 :type "comment" :text "x" :by "u" :parent 1 :kids [3]}
                 (str base "/item/3.json")
                 {:id 3 :type "comment" :text "y" :by "v" :parent 2 :kids []}}]
      (with-redefs [core/get-json            (fixture-lookup items)
                    core/llm-edge-classify!  (constantly "agree")
                    core/llm-steelman-judge! (fn [_] (throw (ex-info "no" {})))]
        (let [res  (flow/run-seq
                     (core/build-flow {:n-stories 1 :tree-workers 2 :workers 2})
                     [:tick])
              rows (first (:outputs res))]
          (is (= :completed (:state res)))
          (is (= [] rows)))))))

(deftest end-to-end-empty-tree
  (testing "story with no comments → no edges, completes cleanly"
    (let [items {(str base "/topstories.json") [1]
                 (str base "/item/1.json")
                 {:id 1 :type "story" :title "S" :url "u" :kids []}}]
      (with-redefs [core/get-json            (fixture-lookup items)
                    core/llm-edge-classify!  (fn [_] (throw (ex-info "no" {})))
                    core/llm-steelman-judge! (fn [_] (throw (ex-info "no" {})))]
        (let [res  (flow/run-seq
                     (core/build-flow {:n-stories 1 :tree-workers 2 :workers 2})
                     [:tick])
              rows (first (:outputs res))]
          (is (= :completed (:state res)))
          (is (= [] rows)))))))
