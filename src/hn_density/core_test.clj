(ns hn-density.core-test
  "Unit + end-to-end tests for hn-density. No live LLM calls — both
   `get-json` and `score-comment!` are rebound via with-redefs."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-density.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- mean / stdev / clip / quadrant ----------------------------------------

(deftest mean-edge-cases
  (is (nil? (#'core/mean [])))
  (is (= 5.0 (#'core/mean [5])))
  (is (= 5.0 (#'core/mean [3 5 7]))))

(deftest stdev-edge-cases
  (is (nil? (#'core/stdev [])))
  (is (= 0.0 (#'core/stdev [5 5 5])))
  (testing "stdev of [1,5] is 2"
    (is (= 2.0 (#'core/stdev [1 5])))))

(deftest clip-boundary
  (is (nil? (#'core/clip nil 5)))
  (is (= "abc" (#'core/clip "abc" 5)))
  (is (= "abcde" (#'core/clip "abcde" 5)))
  (testing "longer than n → clipped to n + ellipsis"
    (let [c (#'core/clip "abcdef" 5)]
      (is (= 6 (count c)))
      (is (clojure.string/starts-with? c "abcde")))))

(deftest quadrant-table
  (is (= "high-density-high-emotion" (#'core/quadrant 5 5)))
  (is (= "high-density-low-emotion"  (#'core/quadrant 5 4)))
  (is (= "low-density-high-emotion"  (#'core/quadrant 4 5)))
  (is (= "low-density-low-emotion"   (#'core/quadrant 4 4)))
  (testing "any nil arg → nil quadrant"
    (is (nil? (#'core/quadrant nil 5)))
    (is (nil? (#'core/quadrant 5 nil)))
    (is (nil? (#'core/quadrant nil nil)))))

;; --- comment-nodes / top-commenters ----------------------------------------

(def ^:private tree-A
  {:id 1 :type "story"
   :kid-trees [{:id 2 :by "alice"
                :kid-trees [{:id 4 :by "bob" :kid-trees []}]}
               {:id 3 :by "alice" :kid-trees []}]})

(def ^:private tree-B
  {:id 11 :type "story"
   :kid-trees [{:id 12 :by "carol" :kid-trees []}
               {:id 13 :by nil     :kid-trees []}]})

(deftest comment-nodes-walk
  (testing "tree-A has 3 comments (alice, bob, alice)"
    (is (= [2 4 3] (mapv :id (#'core/comment-nodes tree-A)))))
  (testing "story-only tree → empty"
    (is (= [] (#'core/comment-nodes {:id 1 :kid-trees []})))))

(deftest top-commenters-ranks-by-frequency
  (let [parent-msgs [{:data tree-A} {:data tree-B}]
        ranked      (#'core/top-commenters 5 parent-msgs)]
    (testing "alice=2 across A, carol=1 from B, bob=1 from A; nil :by ignored"
      (is (= [["alice" 2] ["bob" 1] ["carol" 1]]
             (map (juxt :user-id :n-in-top-stories)
                  (sort-by (fn [r] [(- (:n-in-top-stories r)) (:user-id r)]) ranked)))))
    (testing "m caps the result"
      (is (= 1 (count (#'core/top-commenters 1 parent-msgs)))))))

;; --- summarize-user --------------------------------------------------------

(deftest summarize-user-math
  (let [rows [{:density 8 :emotion 2 :comment-id 1 :text "x"}
              {:density 4 :emotion 4 :comment-id 2 :text "y"}
              {:density nil :emotion nil :comment-id 3 :text "z"}]
        s    (#'core/summarize-user "alice" rows)]
    (is (= "alice" (:user_id s)))
    (testing "nil scores are filtered out before averaging"
      (is (= 2 (:n_scored_comments s)))
      (is (= 6.0 (:info_density_mean s)))
      (is (= 3.0 (:emotional_intensity_mean s))))
    (is (some? (:info_density_std s)))
    (is (= "high-density-low-emotion" (:quadrant s)))
    (testing "sample_comments capped at 5 and only includes scored rows"
      (is (<= (count (:sample_comments s)) 5))
      (is (every? :density (:sample_comments s))))))

(deftest summarize-user-empty
  (let [s (#'core/summarize-user "ghost" [])]
    (is (= 0 (:n_scored_comments s)))
    (is (nil? (:info_density_mean s)))
    (is (nil? (:emotional_intensity_mean s)))
    (is (nil? (:quadrant s)))
    (is (= [] (:sample_comments s)))))

;; --- E2E with stubs --------------------------------------------------------

(defn- fixture-lookup [m]
  (fn [url]
    (or (get m url)
        (throw (ex-info (str "fixture missing: " url) {:url url})))))

(defn- canned-scores
  "Deterministic scoring keyed on comment text. Lets us assert exact
   means / quadrants in tests."
  [text]
  (cond
    (clojure.string/includes? text "FACT")  {:density 9 :emotion 1}
    (clojure.string/includes? text "RANT")  {:density 2 :emotion 9}
    :else                                   {:density 5 :emotion 5}))

(deftest end-to-end-aggregator-flushes-on-close
  ;; Two stories, three commenters; alice posts in both stories.
  ;; m=3 picks top 3; k=2 takes 2 comments per user.
  (let [m {"https://hacker-news.firebaseio.com/v0/topstories.json" [101 102]
           "https://hacker-news.firebaseio.com/v0/item/101.json"
           {:id 101 :type "story" :time 1000 :title "S1" :kids [201 202]}
           "https://hacker-news.firebaseio.com/v0/item/102.json"
           {:id 102 :type "story" :time 1000 :title "S2" :kids [203]}
           "https://hacker-news.firebaseio.com/v0/item/201.json"
           {:id 201 :by "alice" :type "comment" :time 1100 :text "FACT one" :kids []}
           "https://hacker-news.firebaseio.com/v0/item/202.json"
           {:id 202 :by "bob"   :type "comment" :time 1110 :text "RANT one" :kids []}
           "https://hacker-news.firebaseio.com/v0/item/203.json"
           {:id 203 :by "alice" :type "comment" :time 1120 :text "FACT two" :kids []}
           ;; user histories — over-sampled (4*k) so the filter has slack
           "https://hacker-news.firebaseio.com/v0/user/alice.json"
           {:id "alice" :submitted [301 302 303 304 305 306 307 308]}
           "https://hacker-news.firebaseio.com/v0/user/bob.json"
           {:id "bob"   :submitted [401 402 403 404 405 406 407 408]}
           "https://hacker-news.firebaseio.com/v0/item/301.json"
           {:id 301 :type "comment" :time 900 :text "FACT user-comment a"}
           "https://hacker-news.firebaseio.com/v0/item/302.json"
           {:id 302 :type "comment" :time 901 :text "FACT user-comment b"}
           "https://hacker-news.firebaseio.com/v0/item/303.json"
           {:id 303 :type "story" :time 902 :text "skip"}
           "https://hacker-news.firebaseio.com/v0/item/304.json"
           {:id 304 :type "comment" :time 903 :text "ignore extra"}
           "https://hacker-news.firebaseio.com/v0/item/305.json"
           {:id 305 :type "comment" :time 904 :text "ignore extra"}
           "https://hacker-news.firebaseio.com/v0/item/306.json"
           {:id 306 :type "comment" :time 905 :text "ignore extra"}
           "https://hacker-news.firebaseio.com/v0/item/307.json"
           {:id 307 :type "comment" :time 906 :text "ignore extra"}
           "https://hacker-news.firebaseio.com/v0/item/308.json"
           {:id 308 :type "comment" :time 907 :text "ignore extra"}
           "https://hacker-news.firebaseio.com/v0/item/401.json"
           {:id 401 :type "comment" :time 800 :text "RANT a"}
           "https://hacker-news.firebaseio.com/v0/item/402.json"
           {:id 402 :type "comment" :time 801 :text "RANT b"}
           "https://hacker-news.firebaseio.com/v0/item/403.json"
           {:id 403 :type "comment" :time 802 :text "ignore extra"}
           "https://hacker-news.firebaseio.com/v0/item/404.json"
           {:id 404 :type "comment" :time 803 :text "ignore extra"}
           "https://hacker-news.firebaseio.com/v0/item/405.json"
           {:id 405 :type "comment" :time 804 :text "ignore extra"}
           "https://hacker-news.firebaseio.com/v0/item/406.json"
           {:id 406 :type "comment" :time 805 :text "ignore extra"}
           "https://hacker-news.firebaseio.com/v0/item/407.json"
           {:id 407 :type "comment" :time 806 :text "ignore extra"}
           "https://hacker-news.firebaseio.com/v0/item/408.json"
           {:id 408 :type "comment" :time 807 :text "ignore extra"}}]
    (with-redefs [core/get-json       (fixture-lookup m)
                  core/score-comment! canned-scores]
      (let [res  (flow/run-seq (core/build-flow {:n-stories 2 :m-commenters 2
                                                 :k-comments 2 :tree-workers 2
                                                 :user-workers 2 :llm-workers 2})
                               [:tick])
            rows (vec (sort-by :user_id (first (:outputs res))))]
        (is (= :completed (:state res)))
        (testing "aggregator emitted; without the run-seq patch this would be empty"
          (is (= 2 (count rows))))
        (let [alice (first (filter #(= "alice" (:user_id %)) rows))
              bob   (first (filter #(= "bob"   (:user_id %)) rows))]
          (testing "alice's two FACT comments → high-density / low-emotion"
            (is (= 9.0 (:info_density_mean alice)))
            (is (= 1.0 (:emotional_intensity_mean alice)))
            (is (= "high-density-low-emotion" (:quadrant alice))))
          (testing "bob's two RANTs → low-density / high-emotion"
            (is (= 2.0 (:info_density_mean bob)))
            (is (= 9.0 (:emotional_intensity_mean bob)))
            (is (= "low-density-high-emotion" (:quadrant bob)))))))))
