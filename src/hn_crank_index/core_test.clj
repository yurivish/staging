(ns hn-crank-index.core-test
  "Unit + e2e tests for hn-crank-index."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-crank-index.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Crank computation -----------------------------------------------------

(deftest cranks-of-shape
  (testing "user with consistent intensity except on one topic"
    (let [rows (concat
                 ;; baseline: 20 calm comments at intensity 3
                 (for [i (range 20)]
                   {:topic "general" :intensity 3
                    :comment-id (str "g" i) :text (str "g" i)})
                 ;; spike: 6 hot comments on rust
                 (for [i (range 6)]
                   {:topic "rust async" :intensity 9
                    :comment-id (str "r" i) :text (str "r" i)}))
          out  (core/cranks-of "alice" rows {:min-count 3})]
      (is (= "alice" (:user-id out)))
      (is (= 26 (:n-comments out)))
      (testing "rust async dominates — highest z-score"
        (let [first-topic (-> out :topics first)]
          (is (= "rust async" (:topic first-topic)))
          (is (pos? (:z first-topic)))))
      (testing "n is preserved"
        (let [rust (some #(when (= "rust async" (:topic %)) %) (:topics out))]
          (is (= 6 (:n rust))))))))

(deftest cranks-min-count-filters
  (testing "topics below min-count are excluded"
    (let [rows (concat
                 (for [i (range 10)] {:topic "common" :intensity 5
                                      :comment-id (str "c" i) :text "x"})
                 [{:topic "rare" :intensity 9 :comment-id "r1" :text "x"}])
          out  (core/cranks-of "u" rows {:min-count 3})]
      (is (every? #(>= (:n %) 3) (:topics out)))
      (is (not-any? #(= "rare" (:topic %)) (:topics out))))))

(deftest cranks-zero-stdev-handled
  (testing "all-equal intensity → no division by zero"
    (let [rows (for [i (range 6)] {:topic "x" :intensity 5
                                   :comment-id (str i) :text "t"})
          out  (core/cranks-of "u" rows {:min-count 3})]
      (is (= 0.0 (-> out :baseline :stdev)))
      (is (= 0.0 (-> out :topics first :z))))))

(deftest cranks-shuffle-invariance
  (testing "shuffling rows yields the same z-scores"
    (let [rows (concat
                 (for [i (range 15)] {:topic "general" :intensity (mod i 5)
                                      :comment-id (str "g" i) :text "x"})
                 (for [i (range 8)] {:topic "spicy" :intensity 8
                                     :comment-id (str "s" i) :text "x"}))
          a    (core/cranks-of "u" rows {:min-count 3})
          b    (core/cranks-of "u" (shuffle rows) {:min-count 3})
          tof  (fn [r] (into {} (map (juxt :topic #(double (:z %))) (:topics r))))]
      (is (= (tof a) (tof b))))))

;; --- E2E with stubs --------------------------------------------------------

(defn- stub-page [pages-by-user]
  (fn [user page]
    (or (get-in pages-by-user [user page])
        {:hits [] :nb-pages 0})))

(defn- stub-classify [m]
  (fn [row]
    (or (get m (:comment-id row))
        {:topic "general" :intensity 5})))

(deftest end-to-end-with-stubs
  (let [pages
        {"alice"
         {0 {:hits (concat
                     (for [i (range 8)]
                       {:objectID (str "a" i) :author "alice"
                        :comment_text (str "calm " i) :story_title "S"
                        :created_at_i (+ 1700000000 i)})
                     (for [i (range 4)]
                       {:objectID (str "h" i) :author "alice"
                        :comment_text (str "hot " i) :story_title "S"
                        :created_at_i (+ 1700001000 i)}))
             :nb-pages 1}}}
        classify (into {}
                       (concat
                         (for [i (range 8)] [(str "a" i)
                                             {:topic "general" :intensity 4}])
                         (for [i (range 4)] [(str "h" i)
                                             {:topic "javascript" :intensity 9}])))]
    (with-redefs [core/algolia-author-page (stub-page pages)
                  core/llm-classify!       (stub-classify classify)]
      (let [res  (flow/run-seq
                   (core/build-flow {:user-ids ["alice"]
                                     :workers 2
                                     :min-count 3})
                   [:tick])
            rows (->> (first (:outputs res))
                      (group-by :user-id) vals (mapv last))]
        (is (= :completed (:state res)))
        (is (= 1 (count rows)))
        (testing "topics ranked by z-score with javascript on top"
          (is (= "javascript" (-> rows first :topics first :topic))))
        (testing "n-comments tallies all"
          (is (= 12 (-> rows first :n-comments))))))))

(deftest end-to-end-empty-user
  (with-redefs [core/algolia-author-page (constantly {:hits [] :nb-pages 0})
                core/llm-classify!       (fn [_] (throw (ex-info "no" {})))]
    (let [res  (flow/run-seq
                 (core/build-flow {:user-ids ["ghost"] :workers 2})
                 [:tick])
          rows (->> (first (:outputs res))
                    (group-by :user-id) vals (mapv last))]
      (is (= :completed (:state res)))
      (is (= 1 (count rows)))
      (is (= "ghost" (-> rows first :user-id)))
      (is (= 0 (-> rows first :n-comments)))
      (is (= [] (-> rows first :topics))))))
