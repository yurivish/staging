(ns hn-self-contradiction.core-test
  "Unit + e2e tests for hn-self-contradiction."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-self-contradiction.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Unit: pair generation -------------------------------------------------

(deftest pairs-within-group-c-n-2
  (testing "C(n,2) pairs"
    (is (= 0 (count (core/pairs-within-group [{:id 1}]))))
    (is (= 1 (count (core/pairs-within-group [{:id 1} {:id 2}]))))
    (is (= 3 (count (core/pairs-within-group [{:id 1} {:id 2} {:id 3}]))))
    (is (= 6 (count (core/pairs-within-group [{:id 1} {:id 2} {:id 3} {:id 4}])))))
  (testing "no self-pairs, ordered"
    (let [ps (core/pairs-within-group [{:id 1} {:id 2} {:id 3}])
          ids (mapv (juxt (comp :id :a) (comp :id :b)) ps)]
      (is (= [[1 2] [1 3] [2 3]] (sort ids))))))

(deftest pairs-time-gap-filter
  (testing "pairs with insufficient time gap dropped"
    (let [day 86400
          rows [{:id 1 :time 0}                 ; day 0
                {:id 2 :time (* 30 day)}        ; day 30
                {:id 3 :time (* 200 day)}]      ; day 200
          ps (core/pairs-within-group rows {:min-time-gap-days 90})]
      ;; (1,2) gap 30 < 90 → drop
      ;; (1,3) gap 200 ≥ 90 → keep
      ;; (2,3) gap 170 ≥ 90 → keep
      (is (= 2 (count ps))))))

;; --- Unit: collect & sort verdicts ----------------------------------------

(deftest verdict-priority
  (let [rows [{:verdict "scope-shift"      :confidence 0.9}
              {:verdict "real-contradiction" :confidence 0.8}
              {:verdict "real-contradiction" :confidence 0.95}
              {:verdict "not-actually-opposed" :confidence 0.99}
              {:verdict "genuine-update"   :confidence 0.7}]
        sorted (core/sort-pairs rows)]
    (is (= "real-contradiction" (-> sorted first :verdict)))
    (is (= 0.95 (-> sorted first :confidence)))
    (is (= "not-actually-opposed" (-> sorted last :verdict)))))

;; --- E2E with stubs --------------------------------------------------------

(defn- stub-page [pages-by-user]
  (fn [user page]
    (or (get-in pages-by-user [user page])
        {:hits [] :nb-pages 0})))

(defn- stub-tag [m]
  (fn [row]
    (or (get m (:comment-id row))
        {:topic "general" :stance-summary ""
         :is-substantive false})))

(defn- stub-pair-score [m]
  (fn [pair]
    (let [k (sort [(:comment-id (:a pair))
                   (:comment-id (:b pair))])]
      (or (get m k) {:opposed-score 0 :note ""}))))

(defn- stub-pair-judge [m]
  (fn [pair]
    (let [k (sort [(:comment-id (:a pair))
                   (:comment-id (:b pair))])]
      (or (get m k)
          {:verdict "not-actually-opposed" :confidence 0.5
           :summary-a "" :summary-b "" :reconciliation ""}))))

(deftest end-to-end-with-stubs
  (testing "tag → group → pair → filter → judge → final"
    (let [day 86400
          base 1700000000
          pages
          {"alice"
           {0 {:hits [{:objectID "c1" :author "alice"
                       :comment_text "rust async is amazing"
                       :story_title "rust"
                       :created_at_i base}
                      {:objectID "c2" :author "alice"
                       :comment_text "rust async is broken"
                       :story_title "rust"
                       :created_at_i (+ base (* 365 day))}
                      {:objectID "c3" :author "alice"
                       :comment_text "k8s is fine"
                       :story_title "k8s"
                       :created_at_i base}]
               :nb-pages 1}}}
          tags
          {"c1" {:topic "rust async" :stance-summary "rust async amazing"
                 :is-substantive true}
           "c2" {:topic "rust async" :stance-summary "rust async broken"
                 :is-substantive true}
           "c3" {:topic "kubernetes" :stance-summary "k8s ok"
                 :is-substantive true}}
          ;; only c1-c2 pair should reach pair scorer
          scores {(sort ["c1" "c2"]) {:opposed-score 9 :note "clear flip"}}
          judgments {(sort ["c1" "c2"])
                     {:verdict "real-contradiction" :confidence 0.9
                      :summary-a "amazing" :summary-b "broken"
                      :reconciliation "user changed mind"}}]
      (with-redefs [core/algolia-author-page (stub-page pages)
                    core/llm-tag!            (stub-tag tags)
                    core/llm-pair-score!     (stub-pair-score scores)
                    core/llm-pair-judge!     (stub-pair-judge judgments)]
        (let [res  (flow/run-seq
                     (core/build-flow {:user-ids ["alice"]
                                       :workers 2
                                       :min-time-gap-days 90
                                       :filter-threshold 6})
                     [:tick])
              rows (->> (first (:outputs res))
                        (group-by :user-id) vals (mapv last))]
          (is (= :completed (:state res)))
          (is (= 1 (count rows)))
          (let [r (first rows)]
            (is (= "alice" (:user-id r)))
            (is (= 1 (count (:pairs r))))
            (is (= "real-contradiction" (-> r :pairs first :verdict)))))))))

(deftest end-to-end-no-substantive
  (testing "all comments non-substantive → no pairs"
    (with-redefs [core/algolia-author-page
                  (fn [_ _] {:hits [{:objectID "x" :author "u"
                                     :comment_text "joke"
                                     :story_title "s"
                                     :created_at_i 1700000000}]
                             :nb-pages 1})
                  core/llm-tag!
                  (constantly {:topic "joke" :stance-summary "joke"
                               :is-substantive false})
                  core/llm-pair-score! (fn [_] (throw (ex-info "no" {})))
                  core/llm-pair-judge! (fn [_] (throw (ex-info "no" {})))]
      (let [res  (flow/run-seq
                   (core/build-flow {:user-ids ["u"] :workers 2})
                   [:tick])
            rows (->> (first (:outputs res))
                      (group-by :user-id) vals (mapv last))]
        (is (= :completed (:state res)))
        (is (= 1 (count rows)))
        (is (= [] (-> rows first :pairs)))))))

(deftest end-to-end-empty-user
  (with-redefs [core/algolia-author-page (constantly {:hits [] :nb-pages 0})
                core/llm-tag!            (fn [_] (throw (ex-info "no" {})))
                core/llm-pair-score!     (fn [_] (throw (ex-info "no" {})))
                core/llm-pair-judge!     (fn [_] (throw (ex-info "no" {})))]
    (let [res  (flow/run-seq
                 (core/build-flow {:user-ids ["ghost"] :workers 2})
                 [:tick])
          rows (first (:outputs res))]
      (is (= :completed (:state res)))
      (is (= 1 (count rows)))
      (is (= [] (-> rows first :pairs))))))
