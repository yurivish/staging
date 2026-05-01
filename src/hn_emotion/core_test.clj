(ns hn-emotion.core-test
  "Unit + e2e tests for hn-emotion."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-emotion.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Bucket -----------------------------------------------------------------

(deftest year-month-of
  (is (= "2024-01" (core/year-month-of 1704067200)))
  (is (= "2024-04" (core/year-month-of 1712016000)))
  (is (= "2025-12" (core/year-month-of 1767139200))))

;; --- Per-bucket summary ----------------------------------------------------

(deftest summarize-bucket-shape
  (let [rows [{:emotions ["anger" "frustration"] :valence -0.5 :grounding 0.7
               :topic "rust async" :comment-id 1 :text "x"}
              {:emotions ["curiosity"] :valence 0.3 :grounding 0.4
               :topic "rust async" :comment-id 2 :text "y"}
              {:emotions ["anger" "amusement"] :valence -0.2 :grounding 0.6
               :topic "go modules" :comment-id 3 :text "z"}]
        s    (core/summarize-bucket rows)]
    (is (= 3 (:n-comments s)))
    (testing "emotion counts"
      (is (= 2 (get (:emotions s) :anger)))
      (is (= 1 (get (:emotions s) :curiosity)))
      (is (= 1 (get (:emotions s) :frustration))))
    (testing "valence and grounding means"
      (is (< -0.2 (:valence-mean s) -0.1)))
    (testing "top topics — repeated phrase wins"
      (is (= "rust async" (-> s :top-topics first first))))))

(deftest summarize-bucket-empty
  (let [s (core/summarize-bucket [])]
    (is (zero? (:n-comments s)))))

;; --- E2E with stubs --------------------------------------------------------

(defn- stub-page [pages-by-user]
  (fn [user page]
    (or (get-in pages-by-user [user page])
        {:hits [] :nb-pages 0})))

(defn- stub-classify [m]
  (fn [row]
    (or (get m (:comment-id row))
        {:emotions ["neutral"] :valence 0.0 :grounding 0.5 :topic "general"})))

(deftest end-to-end-with-stubs
  (testing "two-user batch with known classifications produces per-month rows"
    (let [pages
          {"alice"
           {0 {:hits [{:objectID "1" :author "alice"
                       :comment_text "Rust async rocks"
                       :story_title "Rust"
                       :created_at_i 1704067200}     ; 2024-01
                      {:objectID "2" :author "alice"
                       :comment_text "Rust async still rocks"
                       :story_title "Rust"
                       :created_at_i 1706745600}     ; 2024-02
                      {:objectID "3" :author "alice"
                       :comment_text "Go modules grumble"
                       :story_title "Go"
                       :created_at_i 1712016000}]    ; 2024-04
               :nb-pages 1}}
           "bob"
           {0 {:hits [{:objectID "10" :author "bob"
                       :comment_text "Excited about k8s"
                       :story_title "k8s"
                       :created_at_i 1704067200}]    ; 2024-01
               :nb-pages 1}}}
          classify
          {"1"  {:emotions ["curiosity"] :valence 0.3 :grounding 0.6
                 :topic "rust async"}
           "2"  {:emotions ["amusement"] :valence 0.2 :grounding 0.5
                 :topic "rust async"}
           "3"  {:emotions ["frustration" "anger"] :valence -0.4 :grounding 0.4
                 :topic "go modules"}
           "10" {:emotions ["joy"] :valence 0.7 :grounding 0.5
                 :topic "kubernetes"}}]
      (with-redefs [core/algolia-author-page (stub-page pages)
                    core/llm-classify!       (stub-classify classify)]
        (let [res  (flow/run-seq
                     (core/build-flow {:user-ids ["alice" "bob"]
                                       :workers 2})
                     [:tick])
              rows (->> (first (:outputs res))
                        (sort-by :user-id))
              by   (into {} (map (juxt :user-id identity)) rows)]
          (is (= :completed (:state res)))
          (is (= 2 (count rows)))
          (testing "alice has months 2024-01, 2024-02, 2024-04"
            (let [ms (mapv :year-month (-> by (get "alice") :months))]
              (is (= ["2024-01" "2024-02" "2024-04"] ms))))
          (testing "bob has one month"
            (is (= ["2024-01"] (mapv :year-month (-> by (get "bob") :months)))))
          (testing "month with two emotion labels increments both counters"
            (let [april (->> by (#(get % "alice")) :months
                             (some #(when (= "2024-04" (:year-month %)) %)))]
              (is (= 1 (get (:emotions april) :anger)))
              (is (= 1 (get (:emotions april) :frustration))))))))))

(deftest end-to-end-empty-user
  (testing "user with no comments → zero months"
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
        (is (= [] (-> rows first :months)))))))
