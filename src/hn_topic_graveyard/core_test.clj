(ns hn-topic-graveyard.core-test
  "Unit + e2e tests for hn-topic-graveyard."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-topic-graveyard.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Quarter helpers -------------------------------------------------------

(deftest quarter-of
  (is (= "2024-Q1" (core/quarter-of 1704067200)))    ; 2024-01-01
  (is (= "2024-Q2" (core/quarter-of 1712016000)))    ; 2024-04-02
  (is (= "2024-Q4" (core/quarter-of 1735603200))))   ; 2024-12-30

(deftest quarters-between
  (is (= ["2024-Q1" "2024-Q2" "2024-Q3" "2024-Q4" "2025-Q1"]
         (core/quarters-between "2024-Q1" "2025-Q1"))))

;; --- Trajectory classification --------------------------------------------

(deftest classify-graveyard
  (testing "peak ≥ 5 then 4 silent quarters at end → graveyard"
    (let [series [["2014-Q1" 6] ["2014-Q2" 11] ["2014-Q3" 22] ["2014-Q4" 8]
                  ["2015-Q1" 0] ["2015-Q2" 0] ["2015-Q3" 0] ["2015-Q4" 0]]]
      (is (= :graveyard
             (core/classify-trajectory series {:peak-min 5
                                               :silent-quarters 4}))))))

(deftest classify-active
  (testing "latest quarter non-zero → active"
    (let [series [["2024-Q1" 3] ["2024-Q2" 5] ["2024-Q3" 7]]]
      (is (= :active
             (core/classify-trajectory series {:peak-min 5
                                               :silent-quarters 4}))))))

(deftest classify-fading
  (testing "peak ever ≥ peak-min, latest < 25% of peak, not yet silent-quarters zeros"
    (let [series [["2020-Q1" 0] ["2020-Q2" 20] ["2020-Q3" 15]
                  ["2020-Q4" 8]
                  ["2021-Q1" 0] ["2021-Q2" 1] ["2021-Q3" 1]]]
      (is (= :fading
             (core/classify-trajectory series {:peak-min 5
                                               :silent-quarters 4}))))))

(deftest classify-ephemeral
  (testing "never reached peak-min → ephemeral"
    (let [series [["2024-Q1" 1] ["2024-Q2" 2] ["2024-Q3" 0] ["2024-Q4" 1]]]
      (is (= :ephemeral
             (core/classify-trajectory series {:peak-min 5
                                               :silent-quarters 4}))))))

;; --- summarize-user ---------------------------------------------------------

(deftest summarize-user-shape
  (let [rows (concat
               ;; perl: peak count 6 in 2014-Q1, then silent through 2018
               (for [i (range 6)]
                 {:topic "perl" :year-quarter "2014-Q1" :is-substantive true
                  :comment-id (str "p" i) :text (str "perl " i)})
               ;; rust: still active in 2018-Q4
               [{:topic "rust" :year-quarter "2018-Q3" :is-substantive true
                 :comment-id "r1" :text "rust tip 1"}
                {:topic "rust" :year-quarter "2018-Q4" :is-substantive true
                 :comment-id "r2" :text "rust tip 2"}])
        out  (core/summarize-user "alice" rows {:peak-min 5 :silent-quarters 4})]
    (is (= "alice" (:user-id out)))
    (is (= 8 (:n-comments-classified out)))
    (testing "perl is in graveyard"
      (let [perl (some #(when (= "perl" (:topic %)) %) (:graveyard out))]
        (is (some? perl))
        (is (= "2014-Q1" (:peak-quarter perl)))
        (is (= 6 (:peak-count perl)))))
    (testing "rust is not in graveyard (active)"
      (is (not-any? #(= "rust" (:topic %)) (:graveyard out))))))

(deftest summarize-user-non-substantive-filtered
  (testing "non-substantive comments excluded from counts"
    (let [rows (concat
                 (for [i (range 6)] {:topic "perl" :year-quarter "2014-Q1"
                                     :is-substantive true
                                     :comment-id (str "p" i) :text "x"})
                 (for [i (range 5)] {:topic "perl" :year-quarter "2018-Q4"
                                     :is-substantive false
                                     :comment-id (str "n" i) :text "x"})
                 (for [i (range 4)] ; provide silent-quarters of zeros
                   {:topic "rust" :year-quarter (format "201%d-Q1" (+ i 5))
                    :is-substantive true
                    :comment-id (str "r" i) :text "x"}))
          out  (core/summarize-user "u" rows {:peak-min 5 :silent-quarters 4})
          perl (some #(when (= "perl" (:topic %)) %) (:graveyard out))]
      ;; perl had 6 substantive in 2014-Q1, 0 substantive in 2018-Q4
      ;; silent quarters from 2014-Q2 .. 2018-Q4 ≥ 4 → graveyard
      (is (some? perl)))))

;; --- E2E with stubs --------------------------------------------------------

(defn- stub-page [pages-by-user]
  (fn [user page]
    (or (get-in pages-by-user [user page])
        {:hits [] :nb-pages 0})))

(defn- stub-classify [m]
  (fn [row]
    (or (get m (:comment-id row))
        {:topic "general" :is-substantive true})))

(deftest end-to-end-with-stubs
  (let [;; Time-stamps spanning 2014-Q1 to 2018-Q4
        epoch  (fn [year quarter]
                 ;; 2014-Q1 = 2014-01-01 = 1388534400
                 ;; rough approximation
                 (let [yrs  (- year 2014)
                       q-secs (* 90 24 3600)
                       y-secs (* 365 24 3600)]
                   (+ 1388534400 (* yrs y-secs) (* (dec quarter) q-secs))))
        pages
        {"alice"
         {0 {:hits (concat
                     (for [i (range 6)]
                       {:objectID (str "p" i) :author "alice"
                        :comment_text (str "perl " i)
                        :story_title "Perl"
                        :created_at_i (epoch 2014 1)})
                     (for [i (range 4)]
                       {:objectID (str "r" i) :author "alice"
                        :comment_text (str "rust " i)
                        :story_title "Rust"
                        :created_at_i (epoch 2018 4)}))
             :nb-pages 1}}}
        classify
        (into {}
              (concat
                (for [i (range 6)] [(str "p" i)
                                    {:topic "perl" :is-substantive true}])
                (for [i (range 4)] [(str "r" i)
                                    {:topic "rust" :is-substantive true}])))]
    (with-redefs [core/algolia-author-page (stub-page pages)
                  core/llm-classify!       (stub-classify classify)]
      (let [res  (flow/run-seq
                   (core/build-flow {:user-ids ["alice"]
                                     :workers 2
                                     :peak-min 5
                                     :silent-quarters 4})
                   [:tick])
            rows (->> (first (:outputs res))
                      (sort-by :user-id))]
        (is (= :completed (:state res)))
        (is (= 1 (count rows)))
        (testing "perl (peak in 2014, silent in subsequent years) is graveyard"
          (let [g (-> rows first :graveyard)]
            (is (some #(= "perl" (:topic %)) g))))
        (testing "rust (active in 2018-Q4) is not graveyard"
          (let [g (-> rows first :graveyard)]
            (is (not-any? #(= "rust" (:topic %)) g))))))))

(deftest end-to-end-empty-user
  (with-redefs [core/algolia-author-page (constantly {:hits [] :nb-pages 0})
                core/llm-classify!       (fn [_] (throw (ex-info "no" {})))]
    (let [res  (flow/run-seq
                 (core/build-flow {:user-ids ["ghost"] :workers 2})
                 [:tick])
          rows (->> (first (:outputs res))
                    (sort-by :user-id))]
      (is (= :completed (:state res)))
      (is (= 1 (count rows)))
      (is (= "ghost" (-> rows first :user-id)))
      (is (= [] (-> rows first :graveyard)))
      (is (= [] (-> rows first :fading))))))
