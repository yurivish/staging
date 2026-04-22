(ns toolkit.spline
  "Centripetal Catmull-Rom spline through a sequence of points, expressed as
   composite quadratic Bézier curves via Yuksel's cubic→quadratic approximation.
   See https://yuri.is/splines/.

   Uses α = 0.5 for centripetal Catmull-Rom
   (https://en.wikipedia.org/wiki/Centripetal_Catmull%E2%80%93Rom_spline) and
   γ = 0.5 as the cubic-to-quadratic splitting point — the two quadratic halves
   of each underlying cubic meet exactly at the cubic's midpoint, which
   minimises the degree-reduction error.

   The curve interpolates every input point except the first and last; those
   two act only as tangent hints. Repeat the first and last input points if
   you want the spline to start and end on them.

   Points are `[x y]` vectors; each emitted Bézier is a triple of such points
   `[p0 p1 p2]`. `quad-beziers` is the top-level entry; `sample-spline` flattens
   the spline to polyline vertices within a tolerance.")

(set! *warn-on-reflection* true)

;; --- pure Bézier helpers -------------------------------------------------

(defn cubic->quads
  "Approximate one cubic Bézier `p0 p1 p2 p3` as two quadratic Béziers that
   meet exactly at the cubic's midpoint (γ = 0.5), which minimises the
   degree-reduction error. See Yuksel, Quadratic Approximation of Cubic Curves."
  [[p0x p0y] [p1x p1y] [p2x p2y] [p3x p3y]]
  (let [p0x (double p0x) p0y (double p0y)
        p1x (double p1x) p1y (double p1y)
        p2x (double p2x) p2y (double p2y)
        p3x (double p3x) p3y (double p3y)
        ax (+ (* 3.0 p1x) p0x), ay (+ (* 3.0 p1y) p0y)
        bx (+ (* 3.0 p2x) p3x), by (+ (* 3.0 p2y) p3y)
        mx (* 0.125 (+ ax bx)), my (* 0.125 (+ ay by))]
    [[[p0x p0y] [(* 0.25 ax) (* 0.25 ay)] [mx my]]
     [[mx my] [(* 0.25 bx) (* 0.25 by)] [p3x p3y]]]))

(defn subdivisions
  "Equal-length segment count needed to flatten a quadratic Bézier `p0 p1 p2`
   to within `tol` of the true curve. See Sederberg §10.6, with the denominator
   corrected from 8 to 4 — Sederberg's value can yield points above `tol`;
   4 is the value that holds up under randomised testing. Discussion and
   verification:
   https://raphlinus.github.io/graphics/curves/2019/12/23/flatten-quadbez.html,
   https://github.com/raphlinus/raphlinus.github.io/issues/57,
   https://observablehq.com/d/fa7a7d536c24e4d4.

   Clamped to [1, 100]: always at least one segment, and never so many that a
   degenerate input could fan out unboundedly."
  ^long [[p0x p0y] [p1x p1y] [p2x p2y] tol]
  (let [p0x (double p0x) p0y (double p0y)
        p1x (double p1x) p1y (double p1y)
        p2x (double p2x) p2y (double p2y)
        tol (double tol)
        dx  (- (* 2.0 p1x) p0x p2x)
        dy  (- (* 2.0 p1y) p0y p2y)
        dd  (Math/sqrt (+ (* dx dx) (* dy dy)))
        n   (Math/ceil (Math/sqrt (/ dd (* 4.0 tol))))]
    (long (max 1 (min 100 n)))))

(defn sample-quadratic
  "Vector of `n` points along a quadratic Bézier at t = 1/n, 2/n, …, 1. The
   starting point at t = 0 is omitted — this makes the sampler trivially
   chainable across connected Béziers, since the endpoint of one curve is the
   starting point of the next and would otherwise be duplicated. The point at
   t = 1 is emitted exactly (as `p2`) so that accumulated forward-difference
   drift doesn't show up as a visible gap at the joint. Returns `[]` when
   `n = 0`. See Sederberg, Computer Aided Geometric Design, ch. 4, and Bartley,
   Forward Difference Calculation of Bezier Curves."
  [[p0x p0y] [p1x p1y] [p2x p2y] n]
  (let [n (long n)]
    (if (zero? n)
      []
      (let [p0x (double p0x) p0y (double p0y)
            p1x (double p1x) p1y (double p1y)
            p2x (double p2x) p2y (double p2y)
            ;; p(t) = a·t² + b·t + c, c = p0
            ax (+ (- p0x (* 2.0 p1x)) p2x)
            ay (+ (- p0y (* 2.0 p1y)) p2y)
            bx (* 2.0 (- p1x p0x))
            by (* 2.0 (- p1y p0y))
            h  (/ 1.0 n)
            hh (* h h)
            d1x0 (+ (* hh ax) (* h bx))
            d1y0 (+ (* hh ay) (* h by))
            d2x  (* 2.0 hh ax)
            d2y  (* 2.0 hh ay)]
        (loop [i 1
               cx p0x, cy p0y
               d1x d1x0, d1y d1y0
               acc (transient [])]
          (if (= i n)
            ;; emit p2 exactly to avoid accumulated drift
            (persistent! (conj! acc [p2x p2y]))
            (let [cx' (+ cx d1x), cy' (+ cy d1y)]
              (recur (inc i)
                     cx' cy'
                     (+ d1x d2x) (+ d1y d2y)
                     (conj! acc [cx' cy'])))))))))

;; --- spline --------------------------------------------------------------

(defn- dist
  "Euclidean distance between two points."
  ^double [[ax ay] [bx by]]
  (let [dx (- (double ax) (double bx))
        dy (- (double ay) (double by))]
    (Math/sqrt (+ (* dx dx) (* dy dy)))))

(defn- window->pair
  "Two quadratic Bézier triples interpolating the middle two points of a
   4-point window, using precomputed distances `[d01 d12 d23]` and their square
   roots `[s01 s12 s23]`."
  [[[p0x p0y] [p1x p1y] [p2x p2y] [p3x p3y]]
   [d01 d12 d23]
   [s01 s12 s23]]
  (let [p0x (double p0x) p0y (double p0y)
        p1x (double p1x) p1y (double p1y)
        p2x (double p2x) p2y (double p2y)
        p3x (double p3x) p3y (double p3y)
        d01 (double d01), d12 (double d12), d23 (double d23)
        s01 (double s01), s12 (double s12), s23 (double s23)
        dx01 (- p1x p0x), dy01 (- p1y p0y)
        dx12 (- p2x p1x), dy12 (- p2y p1y)
        dx23 (- p3x p2x), dy23 (- p3y p2y)
        ;; q01 degrades to p1 when p0 = p1 (denominator is zero)
        [q01x q01y] (if (zero? s01)
                      [p1x p1y]
                      (let [den (* 4.0 s01 (+ s01 s12))]
                        [(+ p1x (/ (+ (* dx12 d01) (* dx01 d12)) den))
                         (+ p1y (/ (+ (* dy12 d01) (* dy01 d12)) den))]))
        ;; q11 degrades to p2 when p2 = p3 (denominator is zero)
        [q11x q11y] (if (zero? s23)
                      [p2x p2y]
                      (let [den (* 4.0 s23 (+ s12 s23))]
                        [(- p2x (/ (+ (* dx23 d12) (* dx12 d23)) den))
                         (- p2y (/ (+ (* dy23 d12) (* dy12 d23)) den))]))
        qmx (* 0.5 (+ q01x q11x))
        qmy (* 0.5 (+ q01y q11y))]
    [[[p1x p1y] [q01x q01y] [qmx qmy]]
     [[qmx qmy] [q11x q11y] [p2x p2y]]]))

(defn quad-beziers
  "Lazy sequence of quadratic Bézier triples approximating a centripetal
   Catmull-Rom spline through `points`. Needs ≥ 4 points; emits 2·(n − 3)
   triples. The curve interpolates `points[1..n-2]` and uses `points[0]` and
   `points[n-1]` only as tangent hints — repeat the first and last if the
   spline should start/end on them.

   Within each emitted pair of triples, the two triples share their middle
   endpoint (the `q02 = q10` joint from the paper). Between consecutive pairs,
   the outer endpoint is an original input point — so triple endpoints chain
   exactly, with no joint-smoothing arithmetic required downstream.

   Each inter-point distance and its square root is computed once: the cached
   values ride alongside the point window through parallel lazy sequences."
  [points]
  (let [ds (map dist points (next points))
        ss (map #(Math/sqrt (double %)) ds)]
    (mapcat window->pair
            (partition 4 1 points)
            (partition 3 1 ds)
            (partition 3 1 ss))))

(defn sample-spline
  "Lazy sequence of polyline vertices flattening the spline through `points`
   to within `tol`. The spline's starting anchor is not included — prepend
   `(ffirst (quad-beziers points))` if you need it."
  [points tol]
  (mapcat (fn [[p0 p1 p2]]
            (sample-quadratic p0 p1 p2 (subdivisions p0 p1 p2 tol)))
          (quad-beziers points)))

(comment
  ;; SVG path `d` attribute through a sequence of points. `tris` is the lazy
  ;; sequence of quadratic Bézier triples `[start control end]` — each triple
  ;; lowers to one `Q<control> <end>` command, with the initial `M` set to the
  ;; first triple's `start`. The `start` of every subsequent triple is
  ;; identical to the previous triple's `end` (by construction), so we bind it
  ;; to `_`: the pen is already there. Repeat the first and last input points
  ;; to anchor the spline's endpoints — interior points are interpolated
  ;; exactly.
  (let [fmt  (fn [[x y]] (str x "," y))
        tris (quad-beziers [[0 0] [0 0] [5 5] [10 0] [10 0]])]
    (apply str "M" (fmt (ffirst tris))
           (for [[_start control end] tris]
             (str " Q" (fmt control) " " (fmt end)))))
  ;; => "M0.0,0.0 Q0.0,0.0 1.875,2.5 Q3.75,5.0 5.0,5.0 Q6.25,5.0 8.125,2.5 Q10.0,0.0 10.0,0.0"
  )
