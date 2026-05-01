(ns hn-buzzword-obituaries.core-test
  "Unit + e2e tests for hn-buzzword-obituaries."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-buzzword-obituaries.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Bucket generation -----------------------------------------------------

(deftest buckets-monthly
  (let [bs (core/buckets "2024-01-01" "2024-04-01" :month)]
    (is (= 3 (count bs)))
    (is (= ["2024-01" "2024-02" "2024-03"] (mapv :label bs)))))

(deftest buckets-quarterly
  (let [bs (core/buckets "2024-01-01" "2025-01-01" :quarter)]
    (is (= 4 (count bs)))
    (is (= ["2024-Q1" "2024-Q2" "2024-Q3" "2024-Q4"] (mapv :label bs)))))

(deftest buckets-snap-to-bucket-start
  (testing "starting mid-month snaps to start of containing month"
    (let [bs (core/buckets "2024-01-15" "2024-03-01" :month)]
      (is (= "2024-01" (-> bs first :label)))))
  (testing "starting mid-quarter snaps to start of containing quarter"
    (let [bs (core/buckets "2024-02-15" "2024-09-01" :quarter)]
      (is (= "2024-Q1" (-> bs first :label))))))

;; --- Trajectory classification ---------------------------------------------

(deftest classify-trajectory-flat
  (is (= :flat (core/classify-trajectory []))
      "empty series")
  (is (= :flat (core/classify-trajectory
                 (mapv (fn [m] {:label (format "2024-%02d" m) :n 0}) (range 1 13))))
      "all-zero series"))

(deftest classify-trajectory-buried
  (testing "peak >12 months ago, current ≤ 0.1 × peak"
    (let [series (concat
                   [{:label "2020-01" :n 100} {:label "2020-02" :n 1000}
                    {:label "2020-03" :n 500}]
                   (for [m (range 1 16)] {:label (format "2024-%02d" m) :n 5}))]
      (is (= :buried (core/classify-trajectory series))))))

(deftest classify-trajectory-rising
  (testing "recent 6-bucket mean >> peak"
    ;; tiny early series, then sharp rise
    (let [series (concat
                   [{:label "2023-01" :n 5}]
                   (for [m (range 1 8)] {:label (format "2024-%02d" m) :n 100}))]
      (is (= :rising (core/classify-trajectory series))))))

(deftest classify-trajectory-sustained
  (testing "current ≈ peak, peak within last 12 buckets"
    (let [series (for [m (range 1 13)]
                   {:label (format "2024-%02d" m) :n 100})]
      (is (= :sustained (core/classify-trajectory series))))))

(deftest classify-trajectory-fading
  (testing "peak 6+ months ago, current between 10% and 50% of peak"
    (let [series (concat
                   [{:label "2023-01" :n 1000}]
                   (for [m (range 1 10)] {:label (format "2024-%02d" m) :n 300}))]
      (is (= :fading (core/classify-trajectory series))))))

;; --- summarize-term unit ---------------------------------------------------

(deftest summarize-term-shape
  (let [cells [{:term "rust" :scope :both :label "2024-01" :n 50 :lo 0 :hi 0}
               {:term "rust" :scope :both :label "2024-02" :n 60 :lo 0 :hi 0}
               {:term "rust" :scope :both :label "2024-03" :n 55 :lo 0 :hi 0}]
        row   (core/summarize-term "rust" :both cells)]
    (is (= "rust" (:term row)))
    (is (= :both  (:scope row)))
    (is (= 165   (:n-total row)))
    (is (= "2024-02" (-> row :peak :label)))
    (is (= "2024-03" (-> row :current :label)))
    (is (= 3 (count (:series row))))
    (is (keyword? (:trajectory row)))
    (testing "series is sorted by label"
      (is (= ["2024-01" "2024-02" "2024-03"] (mapv :label (:series row)))))))

;; --- E2E with stubbed Algolia ----------------------------------------------

(defn- stub-algolia-count [m]
  (fn [term _scope lo _hi]
    ;; lookup by [term lo] to keep the fixture readable
    (get m [term lo] 0)))

(deftest end-to-end-with-stubs
  (testing "two terms × three months produces two rows with full series"
    (let [bs    (core/buckets "2024-01-01" "2024-04-01" :month)
          fix   (into {} (concat
                           ;; "rust" rises across the period
                           (for [[i b] (map-indexed vector bs)]
                             [["rust" (:lo b)] (* (inc i) 10)])
                           ;; "metaverse" stays flat
                           (for [b bs]
                             [["metaverse" (:lo b)] 1])))]
      (with-redefs [core/algolia-count (stub-algolia-count fix)]
        (let [res (flow/run-seq
                    (core/build-flow {:terms ["rust" "metaverse"]
                                      :since "2024-01-01"
                                      :until "2024-04-01"
                                      :bucket :month
                                      :scope :both
                                      :workers 4})
                    [:tick])
              rows (->> (first (:outputs res))
                        (group-by :term) vals (mapv last))]
          (is (= :completed (:state res)))
          (is (= 2 (count rows)))
          (let [by-term (into {} (map (juxt :term identity)) rows)]
            (testing "rust series increases monotonically"
              (is (= [10 20 30] (mapv :n (-> by-term (get "rust") :series)))))
            (testing "metaverse stays flat"
              (is (= [1 1 1] (mapv :n (-> by-term (get "metaverse") :series)))))
            (testing "n-total is correct"
              (is (= 60 (-> by-term (get "rust") :n-total))))))))))

(deftest end-to-end-with-network-error
  (testing "algolia-count returning nil yields :n 0 in the cell, no crash"
    (with-redefs [core/algolia-count (constantly nil)]
      (let [res (flow/run-seq
                  (core/build-flow {:terms ["x"]
                                    :since "2024-01-01"
                                    :until "2024-03-01"
                                    :bucket :month
                                    :workers 2})
                  [:tick])
            rows (->> (first (:outputs res))
                      (group-by :term) vals (mapv last))]
        (is (= :completed (:state res)))
        (is (= 1 (count rows)))
        (is (= [0 0] (mapv :n (-> rows first :series))))))))
