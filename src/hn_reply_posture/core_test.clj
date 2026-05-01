(ns hn-reply-posture.core-test
  "Unit + e2e tests for hn-reply-posture."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-reply-posture.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Per-user summary ------------------------------------------------------

(deftest summarize-user-shape
  (let [edges [{:edge_type "agree"   :kid-id 1}
               {:edge_type "agree"   :kid-id 2}
               {:edge_type "extend"  :kid-id 3}
               {:edge_type "correct" :kid-id 4}
               {:edge_type nil       :kid-id 5}]
        s     (core/summarize-user "alice" edges)]
    (is (= "alice" (:user-id s)))
    (testing "edge-typed counts only"
      (is (= 4 (:n-edges s))))
    (testing "histogram"
      (is (= 2 (-> s :counts :agree)))
      (is (= 1 (-> s :counts :extend)))
      (is (= 1 (-> s :counts :correct))))
    (testing "all edge classes present in :counts"
      (doseq [t (map keyword core/edge-types)]
        (is (contains? (:counts s) t))))
    (testing "proportions sum to 1"
      (let [total (reduce + 0.0 (vals (:proportions s)))]
        (is (< (Math/abs (- 1.0 total)) 1e-9))))
    (testing "dominant class is :agree"
      (is (= :agree (:dominant-class s))))))

(deftest summarize-user-empty
  (let [s (core/summarize-user "ghost" [])]
    (is (zero? (:n-edges s)))
    (is (nil? (:dominant-class s)))))

;; --- E2E with stubs --------------------------------------------------------

(defn- stub-page [pages-by-user]
  (fn [user page]
    (or (get-in pages-by-user [user page])
        {:hits [] :nb-pages 0})))

(defn- stub-classify [m]
  (fn [edge]
    (or (get m (:kid-id edge))
        "agree")))

(deftest end-to-end-with-stubs
  (testing "filters top-level edges, classifies replies, aggregates per user"
    (let [pages
          {"alice"
           {0 {:hits [{:objectID 100 :author "alice" :parent_id 99
                       :story_id 1   :comment_text "ok agree"
                       :created_at_i 1700000000}
                      {:objectID 101 :author "alice" :parent_id 1
                       :story_id 1   :comment_text "top-level (story)"
                       :created_at_i 1700000100}
                      {:objectID 102 :author "alice" :parent_id 200
                       :story_id 2   :comment_text "you're wrong"
                       :created_at_i 1700000200}]
               :nb-pages 1}}}
          items
          {99  {:id 99 :type "comment" :text "parent99"}
           200 {:id 200 :type "comment" :text "parent200"}}
          classify {100 "agree" 102 "disagree"}]
      (with-redefs [core/algolia-author-page (stub-page pages)
                    core/get-item            (fn [id] (get items id))
                    core/llm-classify-edge!  (stub-classify classify)]
        (let [res  (flow/run-seq
                     (core/build-flow {:user-ids ["alice"] :workers 2})
                     [:tick])
              rows (first (:outputs res))]
          (is (= :completed (:state res)))
          (is (= 1 (count rows)))
          (testing "top-level edge filtered out → 2 reply edges"
            (is (= 2 (-> rows first :n-edges))))
          (testing "histogram"
            (is (= 1 (-> rows first :counts :agree)))
            (is (= 1 (-> rows first :counts :disagree)))))))))

(deftest end-to-end-include-top-level
  (testing ":include-top-level true keeps top-level edges"
    (let [pages
          {"bob"
           {0 {:hits [{:objectID 200 :author "bob" :parent_id 1
                       :story_id 1   :comment_text "first comment"
                       :created_at_i 1700000000}]
               :nb-pages 1}}}
          items {1 {:id 1 :type "story" :title "S" :url "u"}}]
      (with-redefs [core/algolia-author-page (stub-page pages)
                    core/get-item            (fn [id] (get items id))
                    core/llm-classify-edge!  (constantly "tangent")]
        (let [res  (flow/run-seq
                     (core/build-flow {:user-ids ["bob"] :workers 2
                                       :include-top-level true})
                     [:tick])
              rows (first (:outputs res))]
          (is (= :completed (:state res)))
          (is (= 1 (-> rows first :n-edges))))))))

(deftest end-to-end-empty-user
  (with-redefs [core/algolia-author-page (constantly {:hits [] :nb-pages 0})
                core/get-item            (constantly nil)
                core/llm-classify-edge!  (fn [_] (throw (ex-info "no" {})))]
    (let [res  (flow/run-seq
                 (core/build-flow {:user-ids ["ghost"] :workers 2})
                 [:tick])
          rows (first (:outputs res))]
      (is (= :completed (:state res)))
      (is (= 1 (count rows)))
      (is (= "ghost" (-> rows first :user-id)))
      (is (zero? (-> rows first :n-edges))))))
