(ns toolkit.hn-sota.core-test
  "Smoke test: stubbed Algolia + stubbed HN tree-fetch get-json. Drives
   the full pipeline including DuckDB persistence and lineage walk-back."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [toolkit.hn-sota.algolia :as algolia]
            [toolkit.hn-sota.core :as core]
            [toolkit.hn-sota.lineage :as lineage])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(def ^:private fixture-stories
  [{:id 1 :title "GPT-5 first impressions"   :url "https://example.com/gpt5"
    :points 100 :author "a" :created-at 0}
   {:id 2 :title "Cooking with cast iron"    :url "https://example.com/cooking"
    :points 50  :author "b" :created-at 0}
   {:id 3 :title "Claude 4 Sonnet vs Opus"   :url "https://example.com/claude"
    :points 75  :author "c" :created-at 0}])

(def ^:private fixture-items
  {1  {:id 1  :type "story"   :title "GPT-5 first impressions"
       :url "https://example.com/gpt5" :kids [11 12 13]}
   11 {:id 11 :type "comment" :by "alice"
       :text "GPT-5 is amazing, much better than before. Love it." :kids []}
   12 {:id 12 :type "comment" :by "bob"
       :text "GPT-5 hallucinates a lot. Useless for coding."       :kids []}
   13 {:id 13 :type "comment" :by "carol"
       :text "Unrelated thought about gardens."                    :kids []}
   3  {:id 3  :type "story"   :title "Claude 4 Sonnet vs Opus"
       :url "https://example.com/claude" :kids [31 32]}
   31 {:id 31 :type "comment" :by "dan"
       :text "Claude 4 Sonnet is great for coding."                :kids []}
   32 {:id 32 :type "comment" :by "eve"
       :text "Claude 4 Opus is slow but powerful."                 :kids []}})

(defn- stub-get-json [url]
  ;; tree-fetch builds URLs like ".../v0/item/<id>.json"
  (let [m (re-find #"/item/(\d+)\.json$" url)]
    (when m
      (get fixture-items (Long/parseLong (second m))))))

(defn- tmp-db []
  (let [dir (Files/createTempDirectory "hn-sota-test"
                                       (into-array FileAttribute []))]
    (.deleteOnExit (.toFile dir))
    (str (.resolve dir "trace.duckdb"))))

(deftest smoke
  (let [db  (tmp-db)
        res (with-redefs [algolia/top-stories-24h (fn [_n] fixture-stories)]
              (core/run-once! {:db-path       db
                               :n-stories     3
                               :tree-workers  2
                               :tree-get-json stub-get-json}))]

    (testing "flow completes"
      (is (= :completed (:state res)))
      (is (nil? (:error res))))

    (testing "ranking covers expected models, irrelevant story dropped"
      (let [by-model (into {} (map (juxt :model_id identity)) (:rows res))]
        (is (contains? by-model "gpt-5"))
        (is (contains? by-model "claude-4-sonnet"))
        (is (contains? by-model "claude-4-opus"))
        (is (= 2 (get-in by-model ["gpt-5" :mentions])))
        (is (= 1 (get-in by-model ["claude-4-sonnet" :mentions])))
        (is (= 1 (get-in by-model ["claude-4-opus" :mentions])))
        (is (= 1.0 (get-in by-model ["claude-4-sonnet" :mean_sentiment])))
        ;; gpt-5: one positive ("amazing"+"love"+"better"), one negative
        ;; ("hallucinates"+"useless") → mean 0.0.
        (is (= 0.0 (get-in by-model ["gpt-5" :mean_sentiment])))))

    (testing "lineage walk surfaces contributing comments"
      (let [contribs (lineage/contributors db "gpt-5")]
        (is (= 2 (count contribs)))
        (is (= #{"GPT-5 is amazing, much better than before. Love it."
                 "GPT-5 hallucinates a lot. Useless for coding."}
               (set (map :comment-text contribs))))
        (is (every? #(= "gpt-5" (:model_id %)) contribs))
        (is (every? #(= 1 (:story-id %)) contribs))
        (is (every? #(= "GPT-5 first impressions" (:story-title %)) contribs))))

    (testing "ranking helper round-trips through DuckDB"
      (let [stored (lineage/ranking db)
            ids    (set (map :model_id stored))]
        (is (contains? ids "gpt-5"))
        (is (contains? ids "claude-4-sonnet"))
        (is (contains? ids "claude-4-opus"))))

    (testing "irrelevant story 2 produced no mention rows"
      (let [contribs (mapcat #(lineage/contributors db (:model_id %)) (:rows res))
            story-ids (set (map :story-id contribs))]
        (is (not (contains? story-ids 2)))))))
