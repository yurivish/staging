(ns toolkit.spline-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.spline :as spline]))

(defn- approx=
  ([a b] (approx= a b 1e-10))
  ([a b eps]
   (cond
     (and (number? a) (number? b)) (<= (Math/abs (- (double a) (double b))) eps)
     (and (vector? a) (vector? b) (= (count a) (count b)))
     (every? true? (map #(approx= %1 %2 eps) a b))
     :else false)))

(defn- bezier-at
  "Analytic evaluation of a quadratic Bézier at parameter t, for cross-checking
   the forward-difference sampler."
  [[p0x p0y] [p1x p1y] [p2x p2y] t]
  (let [u (- 1.0 t)]
    [(+ (* u u p0x) (* 2.0 u t p1x) (* t t p2x))
     (+ (* u u p0y) (* 2.0 u t p1y) (* t t p2y))]))

;; --- cubic->quads --------------------------------------------------------

(deftest cubic->quads-hand-worked-example
  (let [p0 [0.0 0.0], p1 [1.0 0.0], p2 [2.0 1.0], p3 [3.0 1.0]
        [[a0 a1 a2] [b0 b1 b2]] (spline/cubic->quads p0 p1 p2 p3)]
    ;; a = 3p1 + p0 = [3 0]; b = 3p2 + p3 = [9 4]
    ;; q01 = a/4 = [0.75 0]; q02 = (a+b)/8 = [1.5 0.5]; q11 = b/4 = [2.25 1]
    (is (approx= a0 p0))
    (is (approx= a1 [0.75 0.0]))
    (is (approx= a2 [1.5  0.5]))
    (is (approx= b0 [1.5  0.5]))
    (is (approx= b1 [2.25 1.0]))
    (is (approx= b2 p3))))

;; --- subdivisions --------------------------------------------------------

(deftest subdivisions-collinear-midpoint
  ;; p1 exactly on segment p0-p2: zero deviation, expect the minimum of 1.
  (is (= 1 (spline/subdivisions [0.0 0.0] [1.0 1.0] [2.0 2.0] 0.1))))

(deftest subdivisions-clamps
  ;; Extreme curvature vs. tiny tolerance: must saturate at 100.
  (is (= 100 (spline/subdivisions [0.0 0.0] [1e6 1e6] [0.0 0.0] 1e-6))))

(deftest subdivisions-monotone-in-curvature
  ;; Raising p1 off the line increases deviation, must not decrease n.
  (let [ns (for [y [0.0 1.0 5.0 25.0 100.0]]
             (spline/subdivisions [0.0 0.0] [1.0 y] [2.0 0.0] 0.5))]
    (is (apply <= ns))))

;; --- sample-quadratic ----------------------------------------------------

(deftest sample-quadratic-empty-when-n-zero
  (is (= [] (spline/sample-quadratic [0.0 0.0] [1.0 2.0] [3.0 4.0] 0))))

(deftest sample-quadratic-count-and-endpoint
  (let [p0 [0.0 0.0], p1 [1.0 2.0], p2 [3.0 4.0]
        pts (spline/sample-quadratic p0 p1 p2 3)]
    (is (= 3 (count pts)))
    (is (not= p0 (first pts)) "first sample is not t=0")
    (is (= p2 (last pts)) "final sample is p2 exactly")))

(deftest sample-quadratic-matches-analytic
  (let [p0 [0.0 0.0], p1 [2.5 7.5], p2 [10.0 0.0]
        n 12
        pts (spline/sample-quadratic p0 p1 p2 n)]
    (is (= n (count pts)))
    (doseq [i (range n)
            :let [t (/ (double (inc i)) n)]]
      (is (approx= (nth pts i) (bezier-at p0 p1 p2 t) 1e-10)))))

;; --- quad-beziers --------------------------------------------------------

(deftest quad-beziers-needs-at-least-four
  (is (empty? (spline/quad-beziers [])))
  (is (empty? (spline/quad-beziers [[0 0]])))
  (is (empty? (spline/quad-beziers [[0 0] [1 0]])))
  (is (empty? (spline/quad-beziers [[0 0] [1 0] [2 0]]))))

(deftest quad-beziers-count
  (doseq [n [4 5 6 10 25]
          :let [pts (vec (for [i (range n)] [(double i) (* 0.5 i i)]))]]
    (is (= (* 2 (- n 3)) (count (spline/quad-beziers pts)))
        (str "expected 2·(n-3) triples for n=" n))))

(deftest quad-beziers-continuity
  (let [pts [[0.0 0.0] [1.0 0.5] [2.0 2.0] [3.5 2.5] [5.0 1.0] [6.0 0.0]]
        tris (vec (spline/quad-beziers pts))]
    ;; Each triple's endpoint equals the next triple's start.
    (doseq [[a b] (partition 2 1 tris)]
      (is (approx= (last a) (first b))))))

(deftest quad-beziers-yuksel-midpoint
  ;; Within each emitted pair, the shared joint is the midpoint of the
  ;; outer control points of the two triples.
  (let [pts [[0.0 0.0] [1.0 0.5] [2.0 2.0] [3.5 2.5] [5.0 1.0] [6.0 0.0]]
        tris (vec (spline/quad-beziers pts))]
    (doseq [i (range 0 (count tris) 2)
            :let [[_ q01 q02a] (nth tris i)
                  [q02b q11 _] (nth tris (inc i))
                  [q01x q01y] q01, [q11x q11y] q11]]
      (is (approx= q02a q02b))
      (is (approx= q02a [(* 0.5 (+ q01x q11x))
                         (* 0.5 (+ q01y q11y))])))))

(deftest quad-beziers-interpolates-input-anchors
  (let [pts [[0.0 0.0] [1.0 2.0] [3.0 1.0] [4.0 3.0] [6.0 0.0]]
        tris (vec (spline/quad-beziers pts))]
    ;; First triple's start is pts[1]; last triple's end is pts[n-2].
    (is (approx= (ffirst tris) (nth pts 1)))
    (is (approx= (last (last tris)) (nth pts (- (count pts) 2))))))

(deftest quad-beziers-handles-coincident-points
  ;; All four points identical: denominators are zero on both sides; the
  ;; Rust implementation degrades q01/q11 to p1/p2 — we follow, and must
  ;; emit finite coordinates (no NaN).
  (let [p [7.0 -3.0]
        tris (spline/quad-beziers [p p p p])]
    (is (= 2 (count tris)))
    (doseq [t tris
            coord (apply concat t)]
      (is (Double/isFinite coord)))))

;; --- SVG lowering example -----------------------------------------------

(deftest svg-path-d-lowering-example
  ;; Pins the exact output of the rich-comment example in toolkit.spline.
  ;; If this drifts, update both sites in lockstep.
  (let [fmt  (fn [[x y]] (str x "," y))
        tris (spline/quad-beziers [[0 0] [0 0] [5 5] [10 0] [10 0]])
        d    (apply str "M" (fmt (ffirst tris))
                    (for [[_start control end] tris]
                      (str " Q" (fmt control) " " (fmt end))))]
    (is (= "M0.0,0.0 Q0.0,0.0 1.875,2.5 Q3.75,5.0 5.0,5.0 Q6.25,5.0 8.125,2.5 Q10.0,0.0 10.0,0.0"
           d))))

;; --- interpolation property ---------------------------------------------

(def ^:private gen-point
  (gen/let [x (gen/double* {:infinite? false :NaN? false :min -1e3 :max 1e3})
            y (gen/double* {:infinite? false :NaN? false :min -1e3 :max 1e3})]
    [x y]))

(defspec internal-points-appear-as-anchors 50
  (prop/for-all [pts (gen/vector gen-point 4 20)]
    (let [tris (vec (spline/quad-beziers pts))
          starts (set (map first tris))
          ends   (set (map last tris))]
      (every? (fn [i]
                (or (contains? starts (nth pts i))
                    (contains? ends   (nth pts i))))
              (range 1 (dec (count pts)))))))
