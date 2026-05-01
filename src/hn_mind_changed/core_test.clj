(ns hn-mind-changed.core-test
  "Unit + e2e tests for hn-mind-changed."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-mind-changed.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Unit: parent walk ------------------------------------------------------

(deftest hydrate-context-walks-up
  (testing "fetches parent and grandparent"
    (let [items {1 {:id 1 :type "story" :title "T" :url "u"}
                 2 {:id 2 :type "comment" :parent 1 :text "P"}
                 3 {:id 3 :type "comment" :parent 2 :text "GP"}
                 4 {:id 4 :type "comment" :parent 3 :text "candidate"
                    :by "alice" :time 1700000000}}
          h     (core/hydrate-context (fn [id] (get items id))
                                      4)]
      (is (= "candidate" (:candidate-text h)))
      (is (= "GP"        (:parent-text h)))
      (is (= "P"         (:grandparent-text h)))
      (is (= 1           (:story-id h)))
      (is (= "T"         (:story-title h)))
      (is (= "alice"     (:candidate-author h))))))

(deftest hydrate-context-handles-direct-reply-to-story
  (testing "candidate is a direct reply to a story"
    (let [items {10 {:id 10 :type "story" :title "S" :url "u"}
                 11 {:id 11 :type "comment" :parent 10 :text "candidate"
                     :by "bob" :time 1700000000}}
          h     (core/hydrate-context (fn [id] (get items id))
                                      11)]
      (is (= "candidate" (:candidate-text h)))
      (is (= "S"         (:story-title h)))
      (is (= 10          (:story-id h)))
      (is (nil?          (:parent-text h)))
      (is (nil?          (:grandparent-text h))))))

;; --- Unit: dedup -----------------------------------------------------------

(deftest dedup-by-id-keeps-one
  (let [hits [{:objectID "1" :comment_text "a"}
              {:objectID "2" :comment_text "b"}
              {:objectID "1" :comment_text "a-dup"}
              {:objectID "3" :comment_text "c"}]
        out  (core/dedup-by-id hits)]
    (is (= 3 (count out)))
    (is (= #{"1" "2" "3"} (set (map :objectID out))))))

;; --- E2E with stubs --------------------------------------------------------

(defn- stub-search [pages-by-phrase]
  (fn [phrase _since _until page]
    (or (get-in pages-by-phrase [phrase page])
        {:hits [] :nb-pages 0})))

(defn- stub-judge-results
  "Map of candidate-id → judge map (or :reject for filter-rejected)."
  [m]
  {:filter (fn [row]
             (let [r (get m (:candidate-id row))]
               (if (= :reject r)
                 {:is-mind-change false :confidence 0.1 :is-sarcasm false}
                 {:is-mind-change true  :confidence 0.9 :is-sarcasm false})))
   :judge  (fn [row]
             (let [r (get m (:candidate-id row))]
               (if (map? r) r
                   {:direction "partial-concession"
                    :what-was-conceded "x"
                    :trigger-type "data-or-citation"
                    :trigger-excerpt "y"
                    :original-position "z"})))})

(deftest end-to-end-with-stubs
  (testing "two phrases × dedup × filter × judge → final rows"
    (let [pages
          {"you're right"
           {0 {:hits [{:objectID "100" :author "alice"
                       :comment_text "I think you're right"
                       :created_at_i 1700000000}
                      {:objectID "200" :author "bob"
                       :comment_text "you're right, ok"
                       :created_at_i 1700000100}]
               :nb-pages 1}}
           "fair point"
           {0 {:hits [{:objectID "100" :author "alice"     ; duplicate
                       :comment_text "I think you're right"
                       :created_at_i 1700000000}
                      {:objectID "300" :author "carol"
                       :comment_text "fair point indeed"
                       :created_at_i 1700000200}]
               :nb-pages 1}}}
          items
          {100 {:id 100 :type "comment" :parent 99 :text "candidate-100"
                :by "alice" :time 1700000000}
           99  {:id 99  :type "comment" :parent 1
                :text "parent-99" :by "p"}
           1   {:id 1   :type "story" :title "S1" :url "u1"}
           200 {:id 200 :type "comment" :parent 199 :text "candidate-200"
                :by "bob"   :time 1700000100}
           199 {:id 199 :type "comment" :parent 1
                :text "parent-199" :by "p"}
           300 {:id 300 :type "comment" :parent 299 :text "candidate-300"
                :by "carol" :time 1700000200}
           299 {:id 299 :type "comment" :parent 1
                :text "parent-299" :by "p"}}
          stubs (stub-judge-results
                  {"100" {:direction "full-reversal"
                          :what-was-conceded "wc"
                          :trigger-type "data-or-citation"
                          :trigger-excerpt "te"
                          :original-position "op"}
                   "200" :reject     ; filter rejects this one
                   "300" {:direction "partial-concession"
                          :what-was-conceded "wc"
                          :trigger-type "reframing"
                          :trigger-excerpt "te"
                          :original-position "op"}})]
      (with-redefs [core/algolia-search-page (stub-search pages)
                    core/get-item            (fn [id] (get items id))
                    core/llm-filter!         (:filter stubs)
                    core/llm-judge!          (:judge  stubs)]
        (let [res  (flow/run-seq
                     (core/build-flow {:phrases ["you're right" "fair point"]
                                       :since "2024-01-01"
                                       :until "2024-12-31"
                                       :filter-workers 2
                                       :judge-workers 2})
                     [:tick])
              ;; Aggregator no longer sorts/dedupes; tests do it manually.
              rows (->> (first (:outputs res))
                        (sort-by :candidate-id)
                        (sort-by (comp #(or % 0) :created_at_i) >))]
          (is (= :completed (:state res)))
          (testing "filter dropped one (200), so 2 rows remain"
            (is (= 2 (count rows))))
          (testing "rows have all judge fields"
            (doseq [r rows]
              (is (string? (:direction r)))
              (is (string? (:trigger-type r)))
              (is (string? (:candidate-text r)))))
          (testing "rejected candidate is absent"
            (is (not-any? #(= "200" (:candidate-id %)) rows)))
          (testing "sorted by created-at descending"
            (is (= ["300" "100"] (mapv :candidate-id rows)))))))))

(deftest end-to-end-no-survivors
  (testing "all candidates filtered out → empty output"
    (let [pages {"x" {0 {:hits [{:objectID "1" :author "a"
                                 :comment_text "c1"
                                 :created_at_i 1700000000}]
                         :nb-pages 1}}}
          items {1 {:id 1 :type "comment" :parent nil :text "candidate"
                    :by "a" :time 1700000000}}]
      (with-redefs [core/algolia-search-page (stub-search pages)
                    core/get-item            (fn [id] (get items id))
                    core/llm-filter!         (constantly
                                              {:is-mind-change false
                                               :confidence 0.1
                                               :is-sarcasm false})
                    core/llm-judge!          (fn [_] (throw
                                                     (ex-info "should not be called" {})))]
        (let [res  (flow/run-seq
                     (core/build-flow {:phrases ["x"]
                                       :since "2024-01-01"
                                       :until "2024-12-31"
                                       :filter-workers 1
                                       :judge-workers 1})
                     [:tick])
              rows (first (:outputs res))]
          (is (= :completed (:state res)))
          (is (= [] rows)))))))

(deftest end-to-end-handles-empty-search
  (testing "no hits → empty output, judge never called"
    (with-redefs [core/algolia-search-page (constantly {:hits [] :nb-pages 0})
                  core/get-item            (constantly nil)
                  core/llm-filter!         (fn [_] (throw (ex-info "no" {})))
                  core/llm-judge!          (fn [_] (throw (ex-info "no" {})))]
      (let [res (flow/run-seq
                  (core/build-flow {:phrases ["a" "b"]
                                    :since "2024-01-01"
                                    :until "2024-12-31"
                                    :filter-workers 2
                                    :judge-workers 2})
                  [:tick])
            rows (first (:outputs res))]
        (is (= :completed (:state res)))
        (is (= [] rows))))))
