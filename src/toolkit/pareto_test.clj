(ns toolkit.pareto-test
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.pareto :as pareto]))

;; --- naive O(n²) oracle ---------------------------------------------------

(defn- naive-dominates?
  "Does p1 dominate p2 under `directions`? p1 ≥ p2 on every dim and
   strictly better on at least one."
  [directions p1 p2]
  (let [k       (count directions)
        ge?     (fn [d a b] (case d :max (>= a b) :min (<= a b)))
        better? (fn [d a b] (case d :max (>  a b) :min (<  a b)))]
    (and (every? (fn [i] (ge?     (nth directions i) (nth p1 i) (nth p2 i))) (range k))
         (boolean
           (some (fn [i] (better? (nth directions i) (nth p1 i) (nth p2 i))) (range k))))))

(defn naive-frontier
  "O(n²) reference: a point is on the frontier iff no other point
   dominates it. Tie-break: for coord-equal points, first index wins."
  ([points]
   (naive-frontier points (vec (repeat (count (first points)) :max)) identity))
  ([points directions key-fn]
   (let [coords (mapv key-fn points)
         n      (count points)]
     (->> (range n)
          (filter (fn [i]
                    (let [ci (nth coords i)]
                      (and (not (some (fn [j] (and (not= i j)
                                                   (naive-dominates? directions (nth coords j) ci)))
                                      (range n)))
                           (not (some (fn [j] (and (< j i)
                                                   (= (nth coords j) ci)))
                                      (range n)))))))
          (mapv #(nth points %))))))

;; --- trivial / singleton --------------------------------------------------

(deftest empty-input
  (is (= [] (pareto/frontier [])))
  (is (= [] (pareto/empty-frontier))))

(deftest singleton
  (is (= [[1 2 3]] (pareto/frontier [[1 2 3]]))))

(deftest two-equal-points-deduped
  (testing "second equal point is dropped (matches JS reference)"
    (is (= [[1 2]] (pareto/frontier [[1 2] [1 2]])))
    (is (= [[7 7 7]] (pareto/frontier [[7 7 7] [7 7 7] [7 7 7]])))))

;; --- total-order / all-incomparable ---------------------------------------

(deftest total-order-keeps-best
  (testing "totally-ordered chain → only the max point"
    (is (= [[5 5]] (pareto/frontier [[1 1] [2 2] [3 3] [4 4] [5 5]])))
    (is (= [[5 5]] (pareto/frontier (shuffle [[1 1] [2 2] [3 3] [4 4] [5 5]]))))))

(deftest all-incomparable
  (testing "every point beats some other on exactly one dim → all on frontier"
    (let [pts [[1 5] [2 4] [3 3] [4 2] [5 1]]]
      (is (= (set pts) (set (pareto/frontier pts))))
      (is (= (set pts) (set (pareto/frontier (shuffle pts))))))))

;; --- mixed directions -----------------------------------------------------

(deftest mixed-directions
  (testing ":min :max — minimize first, maximize second"
    ;; (cost, value): want low cost, high value.
    ;; (1,1) on (lowest cost). (2,5) better value than (1,1) at slightly higher cost.
    ;; (3,4) dominated by (2,5) — same direction, worse on both.
    ;; (4,6) better value than (2,5). (5,5) dominated by (4,6).
    (is (= #{[1 1] [2 5] [4 6]}
           (set (pareto/frontier [[1 1] [2 5] [3 4] [4 6] [5 5]]
                                 :directions [:min :max]))))))

(deftest min-everywhere
  (testing ":min on every dim → keep the lowest-on-each chain"
    (is (= [[1 1]] (pareto/frontier [[1 1] [2 2] [3 3]] :directions [:min :min])))))

;; --- key-fn ---------------------------------------------------------------

(deftest key-fn-projects
  (testing "key-fn extracts the comparison vector from richer points"
    (let [pts [{:label "a" :score 0.7 :length 10}
               {:label "b" :score 0.9 :length 20}
               {:label "c" :score 0.6 :length 15}]
          ;; minimize length, maximize score
          frt (pareto/frontier pts
                               :directions [:min :max]
                               :key-fn     (juxt :length :score))]
      ;; "c" (15, 0.6) is dominated by "a" (10, 0.7) — shorter AND higher score.
      (is (= #{"a" "b"} (set (map :label frt)))))))

;; --- 3-D / 5-D against naive oracle ---------------------------------------

(deftest matches-naive-3d
  (testing "3D random fixtures against naive oracle"
    (dotimes [_ 50]
      (let [pts (vec (repeatedly 25 (fn [] (vec (repeatedly 3 #(rand-int 8))))))]
        (is (= (set (naive-frontier pts))
               (set (pareto/frontier pts))))))))

(deftest matches-naive-5d
  (testing "5D random fixtures against naive oracle"
    (dotimes [_ 30]
      (let [pts (vec (repeatedly 20 (fn [] (vec (repeatedly 5 #(rand-int 6))))))]
        (is (= (set (naive-frontier pts))
               (set (pareto/frontier pts))))))))

(deftest matches-naive-mixed-dirs
  (testing "mixed :min/:max in 4D against naive oracle"
    (dotimes [_ 30]
      (let [dirs (vec (repeatedly 4 #(rand-nth [:min :max])))
            pts  (vec (repeatedly 25 (fn [] (vec (repeatedly 4 #(rand-int 8))))))]
        (is (= (set (naive-frontier pts dirs identity))
               (set (pareto/frontier pts :directions dirs))))))))

;; --- incremental == batch -------------------------------------------------

(deftest incremental-equals-batch
  (testing "fold pareto/insert over points reaches the same set as batch"
    (dotimes [_ 30]
      (let [pts (vec (repeatedly 25 (fn [] (vec (repeatedly 3 #(rand-int 8))))))]
        (is (= (set (pareto/frontier pts))
               (set (reduce pareto/insert (pareto/empty-frontier) pts))))))))

(deftest insert-inherits-meta
  (testing "directions and key-fn carry through repeated inserts"
    (let [pts [{:l 10 :s 0.7} {:l 20 :s 0.9} {:l 15 :s 0.6}]
          frt (reduce pareto/insert
                      (pareto/empty-frontier :directions [:min :max]
                                             :key-fn     (juxt :l :s))
                      pts)]
      (is (= [:min :max] (:toolkit.pareto/directions (meta frt))))
      (is (= [{:l 10 :s 0.7} {:l 20 :s 0.9}]
             (sort-by :l frt))
          "(15, 0.6) is dominated by (10, 0.7); the others survive"))))

;; --- multiplicity ---------------------------------------------------------

(deftest multiplicity-via-tiebreaker
  (testing "duplicate-coord points are deduped without tie-breaker"
    (let [pts [{:id 1 :x 5 :y 5} {:id 2 :x 5 :y 5}]
          frt (pareto/frontier pts :key-fn (juxt :x :y))]
      ;; (memory note: assert on the full collection, not just count)
      (is (= [{:id 1 :x 5 :y 5}] (vec frt)))))
  (testing "an incomparable tie-breaker preserves both points"
    ;; A single ordered dim (e.g. :id with :min) lets one point dominate
    ;; the other on that dim. To keep both, the tie-breaker has to be
    ;; structurally incomparable — here, (a, b) where p1=(1,0), p2=(0,1).
    (let [pts [{:id 1 :x 5 :y 5 :a 1 :b 0}
               {:id 2 :x 5 :y 5 :a 0 :b 1}]
          frt (pareto/frontier pts :key-fn (juxt :x :y :a :b))]
      (is (= #{1 2} (set (map :id frt)))))))

;; --- adversarial M3 ejection chains --------------------------------------

(deftest adversarial-incomparable-chain
  (testing "incomparable chain of length 30 — the entire chain is the frontier"
    (let [pts (for [i (range 30)] [i (- 29 i)])]
      (is (= (set pts) (set (pareto/frontier pts)))))))

(deftest adversarial-late-ejector
  (testing "a single late point ejects everything else"
    (let [pts (concat (for [i (range 20)] [i (rand-int 10)])
                      [[100 100]])]
      (is (= [[100 100]] (pareto/frontier pts))))))

(deftest adversarial-many-ejections
  (testing "build a frontier then add one strict dominator — only the dominator survives"
    (let [pts  [[1 5] [2 4] [3 3] [4 2] [5 1]]
          frt0 (pareto/frontier pts)
          frt1 (pareto/insert frt0 [10 10])]
      (is (= [[10 10]] frt1)))))

;; --- output sorting -------------------------------------------------------

(deftest sort-by-dim-asc-and-desc
  (let [pts [[5 1] [2 4] [4 2] [1 5] [3 3]]
        frt (pareto/frontier pts)]
    (is (apply <= (map first (pareto/sort-by-dim frt 0)))
        "ascending on dim 0")
    (is (apply >= (map first (pareto/sort-by-dim frt 0 :order :desc)))
        "descending on dim 0")))

;; --- compare-points self-consistency via public API ----------------------

(deftest insert-then-insert-same-point
  (testing "inserting the same point twice leaves the frontier unchanged"
    (let [p   [3 3]
          frt (-> (pareto/empty-frontier) (pareto/insert p) (pareto/insert p))]
      (is (= [p] frt)))))
