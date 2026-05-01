(ns toolkit.hn.tree-fetch-test
  "Unit + end-to-end tests for the toolkit.hn.tree-fetch step."
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]
            [toolkit.hn.tree-fetch :as tree-fetch]))

;; --- Unit tests on private helpers -----------------------------------------

(deftest reassemble-tree-builds-nested-structure
  (testing "linear chain of 3 nodes"
    (let [nodes [{:id 1 :kids [2]} {:id 2 :kids [3]} {:id 3 :kids []}]
          tree  (#'tree-fetch/reassemble-tree 1 nodes)]
      (is (= 1 (:id tree)))
      (is (= 2 (-> tree :kid-trees first :id)))
      (is (= 3 (-> tree :kid-trees first :kid-trees first :id)))
      (is (= [] (-> tree :kid-trees first :kid-trees first :kid-trees)))))
  (testing "branching tree"
    (let [nodes [{:id 1 :kids [2 3]}
                 {:id 2 :kids [4]}
                 {:id 3 :kids []}
                 {:id 4 :kids []}]
          tree  (#'tree-fetch/reassemble-tree 1 nodes)]
      (is (= 2 (count (:kid-trees tree))))
      (is (= [2 3] (mapv :id (:kid-trees tree))))
      (is (= 4 (-> tree :kid-trees first :kid-trees first :id)))))
  (testing "missing kid is silently dropped (e.g., a fetch failure)"
    (let [nodes [{:id 1 :kids [2 3]}
                 {:id 2 :kids []}]   ; 3 is referenced but missing
          tree  (#'tree-fetch/reassemble-tree 1 nodes)]
      (is (= 1 (count (:kid-trees tree))))
      (is (= 2 (-> tree :kid-trees first :id))))))

;; --- E2E with stubbed get-json ---------------------------------------------

(defn- fixture-lookup [m]
  (fn [url]
    (or (get m url)
        (throw (ex-info (str "test fixture missing url: " url) {:url url})))))

(deftest end-to-end-stubbed-fetch
  (testing "single linear chain"
    (let [m {"https://hacker-news.firebaseio.com/v0/item/1.json"
             {:id 1 :time 1000 :title "t" :type "story" :kids [2]}
             "https://hacker-news.firebaseio.com/v0/item/2.json"
             {:id 2 :time 1100 :by "u" :kids [3]}
             "https://hacker-news.firebaseio.com/v0/item/3.json"
             {:id 3 :time 1200 :by "v" :kids []}}
          ;; A trivial source that emits one input — a coll of ids.
          source (step/step :source nil
                            (fn [_ctx _s ids] {:out [(vec ids)]}))
          wf  (step/serial :test
                           source
                           (tree-fetch/step {:k 4 :get-json (fixture-lookup m)}))
          res (flow/run-seq wf [[1]])
          tree (first (first (:outputs res)))]
      (is (= :completed (:state res)))
      (is (some? tree))
      (is (= 1 (:id tree)))
      (is (= [2] (mapv :id (:kid-trees tree))))
      (is (= [3] (mapv :id (-> tree :kid-trees first :kid-trees)))))))

(deftest end-to-end-multiple-roots
  (testing "two stories, each with a small tree, processed concurrently"
    (let [m {"https://hacker-news.firebaseio.com/v0/item/1.json"
             {:id 1 :time 1000 :title "story1" :type "story" :kids [3 4]}
             "https://hacker-news.firebaseio.com/v0/item/2.json"
             {:id 2 :time 2000 :title "story2" :type "story" :kids [5]}
             "https://hacker-news.firebaseio.com/v0/item/3.json"
             {:id 3 :time 1100 :by "a" :kids []}
             "https://hacker-news.firebaseio.com/v0/item/4.json"
             {:id 4 :time 1200 :by "b" :kids []}
             "https://hacker-news.firebaseio.com/v0/item/5.json"
             {:id 5 :time 2100 :by "c" :kids []}}
          source (step/step :source nil
                            (fn [_ctx _s ids] {:out [(vec ids)]}))
          wf  (step/serial :test
                           source
                           (tree-fetch/step {:k 4 :get-json (fixture-lookup m)}))
          res (flow/run-seq wf [[1 2]])
          trees (sort-by :id (first (:outputs res)))]
      (is (= :completed (:state res)))
      (is (= 2 (count trees)))
      (testing "story 1 has 2 kids"
        (is (= [3 4] (mapv :id (:kid-trees (first trees))))))
      (testing "story 2 has 1 kid"
        (is (= [5] (mapv :id (:kid-trees (second trees)))))))))
