(ns hn-firehose.core-test
  "Tests for the live HN firehose pipeline. No real HTTP."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-firehose.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- A. Routing — items dispatched to handler by type --------------------

(deftest a1-routes-by-type
  (let [items {1 {:id 1 :type "story"   :title "S1" :url "u1"}
               2 {:id 2 :type "comment" :text "c2"}
               3 {:id 3 :type "job"     :title "J3"}
               4 {:id 4 :type "poll"    :title "P4"}
               5 {:id 5 :type "story"   :title "S5"}}
        max-seq [3 5]  ;; tick 1 max-id=3, tick 2 max-id=5
        tick    (atom 0)
        fetches (atom [])]
    (with-redefs [core/get-max-item! (fn [] (let [i (swap! tick inc)
                                                  v (nth max-seq (dec i) (last max-seq))]
                                              v))
                  core/get-item!     (fn [id]
                                       (swap! fetches conj id)
                                       (or (get items id)
                                           (throw (ex-info "no item" {:id id}))))
                  ;; Stub the per-type handlers so they emit a tag
                  ;; instead of calling LLMs.
                  core/summarize-story!  (fn [item] (str "STORY:" (:id item)))
                  core/classify-comment! (fn [item] (str "COMMENT:" (:id item)))]
      (let [h (core/run-firehose! {:interval-ms 20
                                   :start-id    0
                                   :max-ticks   2
                                   :pool-k      2})]
        (try
          (Thread/sleep 500)  ;; let polling drain
          (let [datas (sort (mapv :data @(:collected h)))]
            (testing "stories and comments emitted; jobs/polls dropped"
              (is (= ["COMMENT:2" "STORY:1" "STORY:5"] datas)
                  (str "ticks=" @(:ticks h)
                       " fetches=" (sort @fetches)
                       " items=" datas))))
          (finally
            ((:stop! h))))))))

;; --- B. Cycle: hostile comment fetches its parent ------------------------

(deftest b1-hostile-comment-pulls-parent-context
  ;; Stub classify to mark comment 2 hostile (parent=1).
  ;; The cycle re-fetches parent and re-classifies it as a "context" item.
  (let [items {1 {:id 1 :type "comment" :text "polite parent" :parent 99}
               2 {:id 2 :type "comment" :text "rude reply"    :parent 1}}
        tick (atom 0)]
    (with-redefs [core/get-max-item! (fn [] (swap! tick inc) 2)
                  core/get-item!     (fn [id] (or (get items id)
                                                  (throw (ex-info "no" {:id id}))))
                  core/summarize-story!  (fn [_] nil)
                  core/classify-comment! (fn [item]
                                           (case (long (:id item))
                                             2 (str "HOSTILE:2 parent=" (:parent item))
                                             1 (str "CONTEXT:1")
                                             nil))]
      (let [h (core/run-firehose! {:interval-ms 5
                                   :start-id    0
                                   :max-ticks   1
                                   :pool-k      2
                                   :hostile?    (fn [s] (clojure.string/starts-with? s "HOSTILE"))})]
        (try
          (Thread/sleep 250)
          (let [datas (set (mapv :data @(:collected h)))]
            (testing "both the hostile classification AND a re-classification of the parent appear"
              (is (contains? datas "HOSTILE:2 parent=1"))
              (is (contains? datas "CONTEXT:1"))))
          (finally
            ((:stop! h))))))))
