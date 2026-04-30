(ns hn-shape.core-test
  "Unit + end-to-end tests for hn-shape. No live HN calls — `get-json`
   is rebound via with-redefs to a fixture map."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-shape.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Hand-built fixture trees ----------------------------------------------

(def ^:private leaf
  {:id 99 :time 1100 :by "x" :kid-trees []})

(def ^:private deep-tree
  ;; 8 deep linear chain rooted at a story.
  (reduce (fn [child i]
            {:id (+ 100 i) :time (+ 1000 i) :by (str "u" i) :kid-trees [child]})
          {:id 200 :time 2000 :by "leaf" :kid-trees []}
          (range 8)))

(def ^:private fixture-tree
  ;; story
  ;; ├── c2 (alice, t=1100)
  ;; │     └── c4 (bob, t=1200)
  ;; └── c3 (carol, t=1300)
  {:id 1 :time 1000 :title "t" :url "u" :type "story"
   :kid-trees [{:id 2 :time 1100 :by "alice"
                :kid-trees [{:id 4 :time 1200 :by "bob" :kid-trees []}]}
               {:id 3 :time 1300 :by "carol" :kid-trees []}]})

;; --- Unit tests ------------------------------------------------------------

(deftest nodes-traverses-dfs
  (testing "leaf has only itself"
    (is (= [leaf] (#'core/nodes leaf))))
  (testing "fixture: 4 nodes total (story + 3 comments)"
    (is (= [1 2 4 3] (mapv :id (#'core/nodes fixture-tree))))))

(deftest max-depth-counts-edges
  (is (= 0 (#'core/max-depth leaf)))
  (is (= 2 (#'core/max-depth fixture-tree)))
  (is (= 8 (#'core/max-depth deep-tree))))

(deftest mean-edge-cases
  (is (nil? (#'core/mean [])))
  (is (= 1.0 (#'core/mean [1])))
  (is (= 2.5 (#'core/mean [1 2 3 4]))))

(deftest percentile-edge-cases
  (is (nil? (#'core/percentile [] 0.5)))
  (is (= 5 (#'core/percentile [5] 0.5)))
  (testing "p50 of [1..10] picks the index-5 element"
    (is (= 6 (#'core/percentile (range 1 11) 0.5))))
  (testing "p100 picks the last element (clamped)"
    (is (= 10 (#'core/percentile (range 1 11) 1.0)))))

(deftest shape-row-over-fixture
  (let [r (#'core/shape-row fixture-tree)]
    (is (= 1 (:story_id r)))
    (is (= "t" (:title r)))
    (is (= 1000 (:submitted_at_unix r)))
    (is (= 3 (:n_comments r)))
    (is (= 2 (:max_depth r)))
    (testing "story has 2 kids; c2 has 1 kid → mean = 1.5"
      (is (= 1.5 (:branching_factor_mean r))))
    (testing "widest depth-1 branch is c2's subtree (c2+c4=2 nodes)"
      (is (= 2 (:widest_branch_size r))))
    (testing "first-reply latency is the smallest delta from story.time"
      (is (= 100 (:first_reply_latency_s r))))
    (is (some? (:p50_reply_latency_s r)))
    (is (some? (:p95_reply_latency_s r)))
    (testing "time-span = 300s = 1/12 hour"
      (is (= (/ 300 3600.0) (:time_span_hours r))))
    (let [tb (:top_branches r)]
      (testing "ranks are 0..n-1 in size-desc order"
        (is (= [0 1] (map :rank tb)))
        (is (= [2 1] (map :size tb)))
        (is (= ["alice" "carol"] (map :first_commenter tb)))))))

(deftest shape-row-handles-empty-kids
  (let [r (#'core/shape-row {:id 9 :time 1000 :title "x" :url "u"
                             :type "story" :kid-trees []})]
    (is (= 0 (:n_comments r)))
    (is (= 0 (:max_depth r)))
    (is (nil? (:branching_factor_mean r)))
    (is (nil? (:widest_branch_size r)))
    (is (nil? (:first_reply_latency_s r)))
    (is (nil? (:time_span_hours r)))
    (is (= [] (:top_branches r)))))

;; --- HTTP-stub helpers + E2E ------------------------------------------------

(defn- fixture-lookup [m]
  (fn [url]
    (or (get m url)
        (throw (ex-info (str "test fixture missing url: " url) {:url url})))))

(deftest fetch-tree-counts-all-nodes
  (let [m {"https://hacker-news.firebaseio.com/v0/item/1.json"
           {:id 1 :kids [2]}
           "https://hacker-news.firebaseio.com/v0/item/2.json"
           {:id 2 :kids [3]}
           "https://hacker-news.firebaseio.com/v0/item/3.json"
           {:id 3 :kids []}}]
    (with-redefs [core/get-json (fixture-lookup m)]
      (let [counter (atom 0)
            tree    (#'core/fetch-tree counter 1)]
        (is (= 3 @counter))
        (is (= 1 (:id tree)))
        (is (= 2 (-> tree :kid-trees first :id)))
        (is (= 3 (-> tree :kid-trees first :kid-trees first :id)))))))

(deftest end-to-end-with-stubs
  (let [m {"https://hacker-news.firebaseio.com/v0/topstories.json" [1 2]
           "https://hacker-news.firebaseio.com/v0/item/1.json"
           {:id 1 :time 1000 :title "t1" :url "u1" :type "story" :kids [3]}
           "https://hacker-news.firebaseio.com/v0/item/2.json"
           {:id 2 :time 2000 :title "t2" :url "u2" :type "story" :kids []}
           "https://hacker-news.firebaseio.com/v0/item/3.json"
           {:id 3 :time 1100 :by "alice" :kids []}}]
    (with-redefs [core/get-json (fixture-lookup m)]
      (let [res  (flow/run-seq (core/build-flow {:n-stories 2 :tree-workers 2})
                               [:tick])
            rows (vec (sort-by :story_id (first (:outputs res))))]
        (is (= :completed (:state res)))
        (is (= 2 (count rows)))
        (testing "story 1 has one comment"
          (is (= 1 (:n_comments (first rows))))
          (is (= "t1" (:title (first rows)))))
        (testing "story 2 has zero comments → empty-tree branch"
          (is (= 0 (:n_comments (second rows))))
          (is (= [] (:top_branches (second rows)))))))))
