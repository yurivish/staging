(ns toolkit.pareto-property-test
  "Property-based tests for toolkit.pareto: a naive O(n²) frontier in
   `toolkit.pareto-test` is the oracle, and the M3 implementation is
   asserted equal on random k-D inputs across mixed `:min`/`:max`
   directions."
  (:require [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.pareto :as pareto]
            [toolkit.pareto-test :refer [naive-frontier]]))

;; --- generators -----------------------------------------------------------

(def gen-direction (gen/elements [:min :max]))

(defn gen-points [k]
  ;; Small integer coords to give plenty of duplicates and ties.
  (gen/vector (gen/vector (gen/choose 0 5) k k) 0 20))

(def gen-shape
  ;; (k, directions, points). k from 1 to 4.
  (gen/let [k    (gen/choose 1 4)
            dirs (gen/vector gen-direction k k)
            pts  (gen-points k)]
    {:k k :dirs dirs :pts pts}))

;; --- properties ----------------------------------------------------------

(defspec frontier-equals-oracle 500
  (prop/for-all [{:keys [pts dirs]} gen-shape]
                (= (set (naive-frontier pts dirs identity))
                   (set (pareto/frontier pts :directions dirs)))))

(defspec incremental-equals-batch 200
  (prop/for-all [{:keys [pts dirs]} gen-shape]
                (let [batch (pareto/frontier pts :directions dirs)
                      inc'  (reduce pareto/insert
                                    (pareto/empty-frontier :directions dirs)
                                    pts)]
                  (= (set batch) (set inc')))))

(defspec frontier-is-idempotent 200
  ;; Inserting any point already on the frontier leaves the frontier
  ;; unchanged. (Re-inserts are common in incremental usage.)
  (prop/for-all [{:keys [pts dirs]} gen-shape]
                (let [frt (pareto/frontier pts :directions dirs)]
                  (every? (fn [p]
                            (= (set frt)
                               (set (pareto/insert frt p :directions dirs))))
                          frt))))

(defspec no-frontier-point-dominates-another 200
  (prop/for-all [{:keys [pts dirs k]} gen-shape]
                (let [frt (pareto/frontier pts :directions dirs)]
                  (every? (fn [p]
                            (every? (fn [q]
                                      (or (= p q)
                                          (let [ge?     (fn [d a b] (case d :max (>= a b) :min (<= a b)))
                                                better? (fn [d a b] (case d :max (>  a b) :min (<  a b)))
                                                p-dom-q (and (every? (fn [i] (ge?     (nth dirs i) (nth p i) (nth q i))) (range k))
                                                             (some  (fn [i] (better? (nth dirs i) (nth p i) (nth q i))) (range k)))]
                                            (not p-dom-q))))
                                    frt))
                          frt))))
