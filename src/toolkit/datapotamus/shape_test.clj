(ns toolkit.datapotamus.shape-test
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus.combinators.core :as cc]
            [toolkit.datapotamus.shape :as shape]
            [toolkit.datapotamus.step :as step]))

(defn- count-leaves
  "Walk a shape tree, return the number of leaf children."
  [tree]
  (let [walk (fn walk [n]
               (if (= :leaf (:kind n))
                 1
                 (apply + 0 (map walk (:children n)))))]
    (walk tree)))

(defn- topology-leaves [topology]
  (count (filter #(= :leaf (:kind %)) (:nodes topology))))

;; ============================================================================
;; :chain
;; ============================================================================

(deftest chain-basic
  (testing "three-step serial → top-level :chain"
    (let [sm (step/serial (step/step :a inc)
                          (step/step :b inc)
                          (step/step :c inc))
          t  (shape/decompose (step/topology sm))]
      (is (= :chain (-> t :shape :kind)))
      (is (= [[:a] [:b] [:c]] (-> t :shape :order))))))

(deftest chain-of-one
  (testing "single-step is a 1-node chain"
    (let [sm (step/step :only inc)
          t  (shape/decompose (step/topology sm))]
      (is (= :chain (-> t :shape :kind))))))

;; ============================================================================
;; :scatter-gather
;; ============================================================================

(deftest scatter-gather-basic
  (testing "c/parallel inside a serial yields scatter-gather inside the parallel scope"
    (let [sm (step/serial
              (cc/parallel :ann
                           {:a (step/step :sa inc)
                            :b (step/step :sb inc)
                            :c (step/step :sc inc)})
              (step/sink))
          t  (shape/decompose (step/topology sm))
          ;; descend into the :ann container's shape
          ann (->> (:children t)
                   (filter #(= [:ann] (:path %)))
                   first)]
      (is (= :scatter-gather (-> ann :shape :kind)))
      (is (= 3 (count (-> ann :shape :branches)))))))

;; ============================================================================
;; :cycle
;; ============================================================================

(deftest cycle-two-node
  (testing "controller↔pipeline-style 2-cycle is :cycle"
    (let [agent (step/step :agent
                           {:ins  {:in "" :loop ""}
                            :outs {:to "" :final ""}}
                           (fn [_ _ _] {}))
          worker (step/step :worker {:ins {:in ""} :outs {:out ""}}
                            (fn [_ _ _] {}))
          sm (-> (step/beside agent worker)
                 (step/connect [:agent :to]   [:worker :in])
                 (step/connect [:worker :out] [:agent :loop])
                 (step/input-at  [:agent :in])
                 (step/output-at [:agent :final]))
          t  (shape/decompose (step/topology sm))]
      (is (= :cycle (-> t :shape :kind)))
      (is (= #{[:agent] [:worker]} (set (-> t :shape :order))))
      (is (= 2 (count (-> t :shape :internal-edges)))))))

;; ============================================================================
;; :prime fallback
;; ============================================================================

(deftest prime-fallback
  (testing "two disjoint nodes at one level → :prime"
    (let [sm (step/beside (step/step :a inc) (step/step :b inc))
          t  (shape/decompose (step/topology sm))]
      (is (= :prime (-> t :shape :kind))))))

;; ============================================================================
;; Property: leaves preserved
;; ============================================================================

;; ============================================================================
;; :series-parallel recursion — non-trivial sub-shapes inside a chain
;; ============================================================================

(deftest y-shape-decomposes-as-chain-with-nested-parallel
  (testing "src → {A, B} → C → sink: chain[src, parallel(A, B), C, sink]"
    (let [src (step/step :src {:ins {:in ""} :outs {:to-a "" :to-b ""}} (fn [_ _ _] {}))
          a   (step/step :a   {:ins {:in ""} :outs {:out ""}}            (fn [_ _ _] {}))
          b   (step/step :b   {:ins {:in ""} :outs {:out ""}}            (fn [_ _ _] {}))
          c   (step/step :c   {:ins {:from-a "" :from-b ""} :outs {:out ""}} (fn [_ _ _] {}))
          snk (step/sink)
          sm (-> (step/beside src a b c snk)
                 (step/connect [:src :to-a] [:a :in])
                 (step/connect [:src :to-b] [:b :in])
                 (step/connect [:a :out]    [:c :from-a])
                 (step/connect [:b :out]    [:c :from-b])
                 (step/connect [:c :out]    [:sink :in]))
          t (shape/decompose (step/topology sm))
          shape (:shape t)]
      (is (= :chain (:kind shape))
          "top-level shape is a chain (not scatter-gather with one weird branch)")
      (is (= 4 (count (:order shape)))
          "chain has 4 elements: src, parallel-sub-shape, c, sink")
      (is (= [:src] (first (:order shape))))
      (is (= [:c]   (nth (:order shape) 2)))
      (is (= [:sink] (last (:order shape))))
      (let [sg (second (:order shape))]
        (is (and (map? sg) (= :scatter-gather (:kind sg)))
            "second order element is a nested scatter-gather record")
        (is (= 2 (count (:branches sg))))
        (is (= #{{:kind :chain :order [[:a]]}
                 {:kind :chain :order [[:b]]}}
               (set (:branches sg)))
            "branches are chain shape records, not bare path vecs")))))

(deftest series-cut-recurses-cleanly
  (testing "src → A → {B, C} → D → sink: chain[src, A, parallel(B, C), D, sink]"
    (let [src (step/step :src {:ins {:in ""} :outs {:out ""}}    (fn [_ _ _] {}))
          a   (step/step :a   {:ins {:in ""} :outs {:to-b "" :to-c ""}} (fn [_ _ _] {}))
          b   (step/step :b   {:ins {:in ""} :outs {:out ""}}    (fn [_ _ _] {}))
          c   (step/step :c   {:ins {:in ""} :outs {:out ""}}    (fn [_ _ _] {}))
          d   (step/step :d   {:ins {:from-b "" :from-c ""} :outs {:out ""}} (fn [_ _ _] {}))
          snk (step/sink)
          sm (-> (step/beside src a b c d snk)
                 (step/connect [:src :out]  [:a :in])
                 (step/connect [:a :to-b]   [:b :in])
                 (step/connect [:a :to-c]   [:c :in])
                 (step/connect [:b :out]    [:d :from-b])
                 (step/connect [:c :out]    [:d :from-c])
                 (step/connect [:d :out]    [:sink :in]))
          shape (:shape (shape/decompose (step/topology sm)))]
      (is (= :chain (:kind shape)))
      (is (= 5 (count (:order shape))))
      (is (= [:src] (first (:order shape))))
      (is (= [:a] (nth (:order shape) 1)))
      (let [sg (nth (:order shape) 2)]
        (is (and (map? sg) (= :scatter-gather (:kind sg))))
        (is (= 2 (count (:branches sg)))))
      (is (= [:d] (nth (:order shape) 3)))
      (is (= [:sink] (nth (:order shape) 4))))))

(deftest mixed-cycles-and-sp-recurse-via-unmark
  (testing "Two cycles in series at one level: src → (A↔B) → join → (C↔D) → sink"
    (let [src   (step/step :src  {:ins {:in ""} :outs {:out ""}}                       (fn [_ _ _] {}))
          a     (step/step :a    {:ins {:in "" :loop ""} :outs {:fwd "" :back ""}}     (fn [_ _ _] {}))
          b     (step/step :b    {:ins {:in ""} :outs {:back ""}}                      (fn [_ _ _] {}))
          join  (step/step :join {:ins {:in ""} :outs {:out ""}}                       (fn [_ _ _] {}))
          c     (step/step :c    {:ins {:in "" :loop ""} :outs {:fwd "" :back ""}}     (fn [_ _ _] {}))
          d     (step/step :d    {:ins {:in ""} :outs {:back ""}}                      (fn [_ _ _] {}))
          snk   (step/sink)
          sm (-> (step/beside src a b join c d snk)
                 (step/connect [:src :out]   [:a :in])
                 (step/connect [:a :back]    [:b :in])
                 (step/connect [:b :back]    [:a :loop])
                 (step/connect [:a :fwd]     [:join :in])
                 (step/connect [:join :out]  [:c :in])
                 (step/connect [:c :back]    [:d :in])
                 (step/connect [:d :back]    [:c :loop])
                 (step/connect [:c :fwd]     [:sink :in]))
          shape (:shape (shape/decompose (step/topology sm)))]
      (is (= :chain (:kind shape))
          "outer level decomposes as chain (cycles condense to opaque markers, then chain across them)")
      (is (= 5 (count (:order shape)))
          ":order = [src, cycle1, join, cycle2, sink]")
      (is (= [:src] (first (:order shape))))
      (is (= [:join] (nth (:order shape) 2)))
      (is (= [:sink] (last (:order shape))))
      (is (= :cycle (:kind (nth (:order shape) 1)))
          "first cycle properly unmarked")
      (is (= :cycle (:kind (nth (:order shape) 3)))
          "second cycle properly unmarked"))))

(deftest wheatstone-bridge-stays-prime
  (testing "src → {A, B}, A → {C, T}, B → C, C → T (T = sink): non-SP, falls to :prime"
    (let [src (step/step :src {:ins {:in ""} :outs {:to-a "" :to-b ""}} (fn [_ _ _] {}))
          a   (step/step :a   {:ins {:in ""} :outs {:to-c "" :to-t ""}} (fn [_ _ _] {}))
          b   (step/step :b   {:ins {:in ""} :outs {:to-c ""}}          (fn [_ _ _] {}))
          c   (step/step :c   {:ins {:from-a "" :from-b ""} :outs {:to-t ""}} (fn [_ _ _] {}))
          t   (step/step :t   {:ins {:from-a "" :from-c ""} :outs {}}   (fn [_ _ _] {}))
          sm (-> (step/beside src a b c t)
                 (step/connect [:src :to-a] [:a :in])
                 (step/connect [:src :to-b] [:b :in])
                 (step/connect [:a :to-c]   [:c :from-a])
                 (step/connect [:a :to-t]   [:t :from-a])
                 (step/connect [:b :to-c]   [:c :from-b])
                 (step/connect [:c :to-t]   [:t :from-c]))
          shape (:shape (shape/decompose (step/topology sm)))]
      (is (= :prime (:kind shape))
          "Wheatstone bridge is the canonical non-SP DAG; correctly classified as :prime"))))

(deftest leaves-preserved
  (testing "decompose preserves leaf count for various shapes"
    (doseq [[label sm] [[:chain     (step/serial (step/step :a inc)
                                                  (step/step :b inc)
                                                  (step/step :c inc))]
                        [:parallel  (cc/parallel :p
                                                 {:x (step/step :sx inc)
                                                  :y (step/step :sy inc)})]
                        [:nested    (step/serial
                                     (cc/parallel :p1
                                                  {:a (step/step :a1 inc)
                                                   :b (step/step :b1 inc)})
                                     (cc/parallel :p2
                                                  {:c (step/step :c1 inc)
                                                   :d (step/step :d1 inc)}))]]]
      (let [topo (step/topology sm)
            tree (shape/decompose topo)]
        (is (= (topology-leaves topo) (count-leaves tree))
            (str "leaves mismatch for " label))))))
