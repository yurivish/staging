(ns toolkit.datapotamus.keyed-join-test
  "Tests for `c/join-by-key` — N-input batch join. Each declared input
   port has its own key extractor; the step buffers items per
   (port, key) and emits one `(on-match key port→items)` per key on
   `:on-all-input-done`."
  (:refer-clojure :exclude [run!])
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus.combinators.aggregate :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

(defn- mk-driver
  "Wrap `keyed-join` in a tiny driver step that exposes `:in-a` and
   `:in-b` as the only external ports. The driver routes incoming
   records by their `:port` key into the join's named input ports."
  [keyed-join]
  (let [router
        (step/handler-map
         {:ports {:ins {:in ""} :outs {:a "" :b ""}}
          :on-data (fn [ctx _ d]
                     (case (:port d)
                       :a {:a [(msg/child ctx (:row d))]}
                       :b {:b [(msg/child ctx (:row d))]}))})]
    (step/connect
     (step/connect
      {:procs (merge {:router router} (:procs keyed-join))
       :conns []
       :in    :router
       :out   (:out keyed-join)}
      [:router :a] [(:in keyed-join) :a])
     [:router :b] [(:in keyed-join) :b])))

;; --- A. Both sides present — emits one row per matched key -------------

(deftest a1-two-side-join-emits-per-key
  (let [j   (c/join-by-key
             {:ports    [:a :b]
              :key-fns  {:a :id :b :ref}
              :on-match (fn [k items]
                          {:k k
                           :a (mapv :v (:a items))
                           :b (mapv :v (:b items))})})
        wf  (mk-driver j)
        evs [{:port :a :row {:id 1 :v "a-1"}}
             {:port :a :row {:id 2 :v "a-2"}}
             {:port :b :row {:ref 1 :v "b-1"}}
             {:port :b :row {:ref 2 :v "b-2"}}
             {:port :b :row {:ref 2 :v "b-2-extra"}}]
        res (flow/run-seq wf evs)
        ;; Each msg/merge emission appears in :outputs under EVERY parent
        ;; input slot (lineage attribution). Dedupe by content for the
        ;; per-key uniqueness assertion.
        all (sort-by :k (distinct (mapcat identity (:outputs res))))]
    (is (= :completed (:state res)))
    (is (= 2 (count all)))
    (is (= [{:k 1 :a ["a-1"] :b ["b-1"]}
            {:k 2 :a ["a-2"] :b ["b-2" "b-2-extra"]}]
           all))))

;; --- B. require-all?=true drops singletons -----------------------------

(deftest b1-require-all-drops-keys-missing-a-side
  (let [j   (c/join-by-key
             {:ports        [:a :b]
              :key-fns      {:a :id :b :ref}
              :require-all? true
              :on-match     (fn [k items] {:k k :a (mapv :v (:a items))
                                          :b (mapv :v (:b items))})})
        wf  (mk-driver j)
        evs [{:port :a :row {:id 1 :v "a-1"}}    ;; lonely
             {:port :a :row {:id 2 :v "a-2"}}
             {:port :b :row {:ref 2 :v "b-2"}}
             {:port :b :row {:ref 3 :v "b-3"}}]  ;; lonely
        res (flow/run-seq wf evs)
        all (distinct (mapcat identity (:outputs res)))]
    (is (= :completed (:state res)))
    (is (= 1 (count all)))
    (is (= 2 (-> all first :k)))))

;; --- C. require-all?=false (default) emits singletons too --------------

(deftest c1-default-emits-singletons
  (let [j   (c/join-by-key
             {:ports    [:a :b]
              :key-fns  {:a :id :b :ref}
              :on-match (fn [k items]
                          {:k k :only-a? (empty? (:b items))
                           :only-b? (empty? (:a items))})})
        wf  (mk-driver j)
        evs [{:port :a :row {:id 1 :v "a-1"}}
             {:port :b :row {:ref 2 :v "b-2"}}]
        res (flow/run-seq wf evs)
        all (sort-by :k (distinct (mapcat identity (:outputs res))))]
    (is (= :completed (:state res)))
    (is (= 2 (count all)))
    (is (= [{:k 1 :only-a? true  :only-b? false}
            {:k 2 :only-a? false :only-b? true}]
           all))))
