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
      (is (= #{[:agent] [:worker]} (set (-> t :shape :members))))
      (is (= 2 (count (-> t :shape :back-edges)))))))

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
