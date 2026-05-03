(ns toolkit.datapotamus.stealing-workers-test
  "Tests for c/stealing-workers — decentralized work-stealing pool.
   Each worker reads from a shared channel array via alts! priority
   (own first, peers next); recursive `:work` emissions are dispatched
   back into the channel array via a daemon thread that uses non-
   blocking try-puts. No central coordinator, no bidirectional buffer
   dependency, no deadlock under wide recursive fan-out."
  (:refer-clojure :exclude [run!])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.datapotamus.combinators.workers :as c]
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
;; C2. Recursive — work-only intermediate iterations (no :out emit)
;;     Regression: prior to the :ack class, an inner that emitted only
;;     :work on intermediate iterations would leave the worker marked
;;     :busy in the coord and the pool would deadlock once K consecutive
;;     work-only invocations occurred.
;; ============================================================================

(defn- countdown-work-only-inner
  "Same arithmetic as countdown-inner, but emits NOTHING on :out for
   intermediate steps — only the final leaf goes to :out. Each
   intermediate iteration emits only :work."
  []
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out "" :work ""}}
    :on-data (fn [ctx _s n]
               (if (zero? n)
                 {:out [(msg/child ctx [:leaf n])]}
                 {:work [(msg/child ctx (dec n))]}))}))

(deftest c2a-work-only-countdown-no-deadlock
  ;; Single input, K=1 so deadlock is most likely if the worker isn't freed.
  (let [wf  (c/stealing-workers :pool 1 (countdown-work-only-inner))
        res (flow/run-seq wf [3])]
    (is (= :completed (:state res)))
    (is (= [[[:leaf 0]]] (:outputs res)))))

(deftest c2b-work-only-many-inputs-deeper
  ;; Three inputs, K=2 — even with multiple workers all in work-only mode,
  ;; the pool must drain.
  (let [wf  (c/stealing-workers :pool 2 (countdown-work-only-inner))
        res (flow/run-seq wf [5 7 4])]
    (is (= :completed (:state res)))
    (testing "each input contributes exactly its leaf"
      (is (= [[[:leaf 0]] [[:leaf 0]] [[:leaf 0]]] (:outputs res))))))

(deftest c2c-work-only-deeper-than-pool
  ;; Recursion depth (10) > K (3). If the worker freeing was tied to :out,
  ;; depth-3 in is enough to wedge all workers — this verifies it isn't.
  (let [wf  (c/stealing-workers :pool 3 (countdown-work-only-inner))
        res (flow/run-seq wf [10])]
    (is (= :completed (:state res)))
    (is (= [[[:leaf 0]]] (:outputs res)))))

;; ============================================================================
;; C3. Empty inner return (drop) — the wrapper synthesizes a tagged :ack
;;     signal so the worker is freed AND tokens flow forward. Without this,
;;     the framework's auto-signal would carry tokens but never free the
;;     worker, deadlocking the pool once K consecutive drops occur.
;; ============================================================================

(defn- mixed-inner
  "Emits :out for ids in `out-ids`, returns `{}` (drop) for the rest."
  [out-ids]
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out ""}}
    :on-data (fn [ctx _ id]
               (if (out-ids id)
                 {:out [(msg/child ctx (str "OUT:" id))]}
                 {}))}))

(deftest c3a-all-drops-completes
  (let [wf  (c/stealing-workers :pool 2 (mixed-inner #{}))
        res (flow/run-seq wf [1 2 3 4 5])]
    (is (= :completed (:state res)))
    (is (= [[] [] [] [] []] (:outputs res)))))

(deftest c3b-last-only-out-K1
  ;; K=1 forces strict ordering — earlier drops can't be processed in
  ;; parallel. Each drop must free the worker for the next.
  (let [wf  (c/stealing-workers :pool 1 (mixed-inner #{5}))
        res (flow/run-seq wf [1 2 3 4 5])]
    (is (= :completed (:state res)))
    (is (= [[] [] [] [] ["OUT:5"]] (:outputs res)))))

(deftest c3c-mixed-K2
  (let [wf  (c/stealing-workers :pool 2 (mixed-inner #{1 3 5}))
        res (flow/run-seq wf [1 2 3 4 5])]
    (is (= :completed (:state res)))
    (is (= [["OUT:1"] [] ["OUT:3"] [] ["OUT:5"]] (:outputs res)))))

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

;; ============================================================================
;; F. Regression — wide recursive fan-out
;;
;; The original centralized-coord stealing-workers deadlocked under
;; wide recursive `:work` fan-out: workers blocked sending to coord
;; while coord blocked sending to workers. The new decentralized
;; design (shared channels with alts! priority + non-blocking work
;; dispatcher) handles this without any bidirectional buffer cycle.
;; ============================================================================

(deftest f1-wide-recursive-fan-out-no-deadlock
  ;; Each worker emits N=5 :work msgs per invocation, recursing D=4
  ;; levels deep. Total nodes = 1 + 5 + 25 + 125 + 625 = 781.
  (let [inner (step/handler-map
               {:ports   {:ins {:in ""} :outs {:out "" :work ""}}
                :on-data (fn [ctx _ {:keys [id depth]}]
                           (let [kids (when (pos? depth)
                                        (mapv (fn [i]
                                                {:id (+ (* id 10) i)
                                                 :depth (dec depth)})
                                              (range 5)))]
                             (cond-> {:out [(msg/child ctx {:id id})]}
                               (seq kids)
                               (assoc :work (msg/children ctx kids)))))})
        wf  (c/stealing-workers :pool 8 inner)
        t0  (System/currentTimeMillis)
        res (flow/run-seq wf [{:id 1 :depth 4}])
        ms  (- (System/currentTimeMillis) t0)
        n   (count (mapcat identity (:outputs res)))]
    (is (= :completed (:state res)))
    (is (= 781 n) "all 781 nodes emitted")
    (is (< ms 30000) (str "must complete fast; took " ms "ms"))))

;; ============================================================================
;; G. Regression — irrelevant-input signals don't loop on :work
;;
;; If an upstream step returns {} for some input, the framework
;; auto-signals downstream. That signal must NOT be broadcast on the
;; pool's :work feedback port — that would create an infinite signal
;; loop. The wrapper installs a custom :on-signal that emits only
;; on :out for handler-maps that declare :work.
;; ============================================================================

(deftest g1-filter-empty-doesnt-loop-on-work
  (let [filter-step (step/step :gate nil
                               (fn [ctx _ {:keys [keep?]}]
                                 (if keep?
                                   {:out [(msg/pass ctx)]}
                                   {})))
        worker (step/handler-map
                {:ports {:ins {:in ""} :outs {:out "" :work ""}}
                 :on-data (fn [ctx _ {:keys [id depth] :as d}]
                            (let [kids (when (pos? depth)
                                         [{:id (* id 10)
                                           :depth (dec depth)
                                           :keep? true}])]
                              (cond-> {:out [(msg/child ctx d)]}
                                (seq kids)
                                (assoc :work (msg/children ctx kids)))))})
        wf  (step/serial filter-step
                         (c/stealing-workers :pool 4 worker))
        res (flow/run-seq wf [{:keep? false :id 1 :depth 1}
                              {:keep? true  :id 2 :depth 2}
                              {:keep? false :id 3 :depth 1}])]
    (is (= :completed (:state res)))
    (testing "the :keep? false items are filtered, no signal-loop"
      (let [outs (:outputs res)]
        (is (= 3 (count outs)))
        (is (= [] (nth outs 0)))
        (is (= [] (nth outs 2)))
        (is (= #{{:id 2 :depth 2 :keep? true}
                 {:id 20 :depth 1 :keep? true}
                 {:id 200 :depth 0 :keep? true}}
               (set (nth outs 1))))))))

;; ============================================================================
;; H. Step-inner support
;;
;; The wrapper handles step inners by building a 2-proc sub-step
;; (shim handler-map → user step). Step inners cannot use :work
;; recursive feedback (their internal output ports can't be cleanly
;; overridden) — convert to handler-map if you need it.
;; ============================================================================

(deftest h1-step-inner-non-recursive
  (let [square-step (step/step :sq (fn [d] (* d d)))
        wf  (c/stealing-workers :pool 4 square-step)
        res (flow/run-seq wf [1 2 3 4 5])]
    (is (= :completed (:state res)))
    (is (= [[1] [4] [9] [16] [25]] (:outputs res)))))

;; ============================================================================
;; I. Edge cases
;; ============================================================================

(deftest i1-empty-input-completes-cleanly
  ;; run-seq short-circuits on empty coll without starting the flow,
  ;; but the contract still requires a clean :completed result.
  (let [wf  (c/stealing-workers :pool 4 (step/step :sq #(* % %)))
        res (flow/run-seq wf [])]
    (is (= :completed (:state res)))
    (is (= [] (:outputs res)))))
