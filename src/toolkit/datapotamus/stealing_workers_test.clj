(ns toolkit.datapotamus.stealing-workers-test
  "Tests for c/stealing-workers — coordinator-driven work-stealing
   pool with optional recursive feedback. Single coordinator + K
   shims + K worker inners; state transitions in the coordinator are
   serialized by core.async.flow's per-proc invocation, eliminating
   the close-cascade race that affected the prior shared-queue
   implementation."
  (:refer-clojure :exclude [run!])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

;; ============================================================================
;; A. Backward-compat shape — non-recursive inner
;; ============================================================================

(deftest a1-non-recursive-inner-handles-single-input
  (let [inner (step/handler-map
               {:ports   {:ins {:in ""} :outs {:out ""}}
                :on-data (fn [ctx _ d] {:out [(msg/child ctx (* d d))]})})
        wf  (c/stealing-workers :pool 4 inner)
        res (flow/run-seq wf [3])]
    (is (= :completed (:state res)))
    (is (= [[9]] (:outputs res)))))

(deftest a2-non-recursive-multiple-inputs
  (let [inner (step/handler-map
               {:ports   {:ins {:in ""} :outs {:out ""}}
                :on-data (fn [ctx _ d] {:out [(msg/child ctx (* d 10))]})})
        wf  (c/stealing-workers :pool 4 inner)
        res (flow/run-seq wf [1 2 3 4 5])]
    (is (= :completed (:state res)))
    (is (= [[10] [20] [30] [40] [50]] (:outputs res)))))

(deftest a3-non-recursive-k-1-degenerate
  (let [inner (step/handler-map
               {:ports   {:ins {:in ""} :outs {:out ""}}
                :on-data (fn [ctx _ d] {:out [(msg/child ctx (inc d))]})})
        wf  (c/stealing-workers :pool 1 inner)
        res (flow/run-seq wf [1 2 3])]
    (is (= :completed (:state res)))
    (is (= [[2] [3] [4]] (:outputs res)))))

;; ============================================================================
;; B. Recursive — linear countdown
;; ============================================================================

(defn- countdown-inner []
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out "" :work ""}}
    :on-data (fn [ctx _s n]
               (if (zero? n)
                 {:out [(msg/child ctx [:leaf n])]}
                 {:out  [(msg/child ctx [:internal n])]
                  :work [(msg/child ctx (dec n))]}))}))

(deftest b1-countdown-from-3-emits-4-outs
  (let [wf  (c/stealing-workers :pool 4 (countdown-inner))
        res (flow/run-seq wf [3])]
    (is (= :completed (:state res)))
    (testing "outputs include all four levels (3 internal + 1 leaf)"
      (let [outs (set (first (:outputs res)))]
        (is (= #{[:internal 3] [:internal 2] [:internal 1] [:leaf 0]}
               outs))))))

(deftest b2-countdown-from-50-completes
  (let [wf  (c/stealing-workers :pool 4 (countdown-inner))
        res (flow/run-seq wf [50])]
    (is (= :completed (:state res)))
    (is (= 51 (count (first (:outputs res)))))))

;; ============================================================================
;; C. Recursive — branching tree
;; ============================================================================

(defn- branching-inner []
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out "" :work ""}}
    :on-data (fn [ctx _s d]
               (let [kids (:kids d)]
                 (cond-> {:out [(msg/child ctx (:n d))]}
                   (seq kids) (assoc :work (mapv #(msg/child ctx %) kids)))))}))

(deftest c1-binary-tree-of-depth-3
  ;; Depth 3: root + 2 + 4 + 8 = 15 nodes.
  (let [tree (letfn [(node [d]
                       (if (zero? d)
                         {:n [:leaf d]}
                         {:n [:branch d]
                          :kids [(node (dec d)) (node (dec d))]}))]
               (node 3))
        wf   (c/stealing-workers :pool 4 (branching-inner))
        res  (flow/run-seq wf [tree])]
    (is (= :completed (:state res)))
    (is (= 15 (count (first (:outputs res)))))))

(deftest c1b-binary-tree-of-depth-2-k1
  ;; Depth 2: root + 2 + 4 = 7 nodes. K=1 to remove cross-worker concurrency.
  (let [tree (letfn [(node [d]
                       (if (zero? d)
                         {:n [:leaf d]}
                         {:n [:branch d]
                          :kids [(node (dec d)) (node (dec d))]}))]
               (node 2))
        wf   (c/stealing-workers :pool 1 (branching-inner))
        res  (flow/run-seq wf [tree])]
    (is (= :completed (:state res)))
    (is (= 7 (count (first (:outputs res)))))))

(deftest c1c-binary-tree-of-depth-2-k2
  (let [tree (letfn [(node [d]
                       (if (zero? d)
                         {:n [:leaf d]}
                         {:n [:branch d]
                          :kids [(node (dec d)) (node (dec d))]}))]
               (node 2))
        wf   (c/stealing-workers :pool 2 (branching-inner))
        res  (flow/run-seq wf [tree])]
    (is (= :completed (:state res)))
    (is (= 7 (count (first (:outputs res)))))))

(deftest c1d-binary-tree-of-depth-3-k2
  (let [tree (letfn [(node [d]
                       (if (zero? d)
                         {:n [:leaf d]}
                         {:n [:branch d]
                          :kids [(node (dec d)) (node (dec d))]}))]
               (node 3))
        wf   (c/stealing-workers :pool 2 (branching-inner))
        res  (flow/run-seq wf [tree])]
    (is (= :completed (:state res)))
    (is (= 15 (count (first (:outputs res)))))))

;; ============================================================================
;; D. Multiple roots
;; ============================================================================

(deftest d1-three-independent-trees
  (let [wf  (c/stealing-workers :pool 4 (countdown-inner))
        res (flow/run-seq wf [2 4 6])]
    (is (= :completed (:state res)))
    (testing "each input gets the right output count via lineage attribution"
      (is (= [3 5 7] (mapv count (:outputs res)))))))

;; ============================================================================
;; E. Property test — tree size = output count
;; ============================================================================

(def ^:private gen-tree
  (gen/recursive-gen
   (fn [inner]
     (gen/let [n     gen/small-integer
               nkids (gen/choose 0 3)
               kids  (gen/vector inner nkids)]
       {:n n :kids kids}))
   (gen/let [n gen/small-integer]
     {:n n :kids []})))

(defn- count-nodes [tree]
  (inc (reduce + (map count-nodes (:kids tree)))))

(defspec p1-output-count-equals-tree-node-count 20
  (prop/for-all [tree gen-tree]
    (let [wf  (c/stealing-workers :pool 4 (branching-inner))
          res (flow/run-seq wf [tree])]
      (and (= :completed (:state res))
           (= (count-nodes tree) (count (first (:outputs res))))))))

(defspec p2-multiple-trees-each-correct 15
  (prop/for-all [trees (gen/vector gen-tree 1 4)]
    (let [wf  (c/stealing-workers :pool 4 (branching-inner))
          res (flow/run-seq wf (vec trees))]
      (and (= :completed (:state res))
           (= (mapv count-nodes trees)
              (mapv count (:outputs res)))))))
