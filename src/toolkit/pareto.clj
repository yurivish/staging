(ns toolkit.pareto
  "Pareto frontier (non-dominated set) over n-D points.

   Algorithm: M3 from Bentley/Clarkson/Levine, expected linear time. The
   frontier is scanned linearly per insert; on dominance, the dominator
   is moved to the front so the next insert can fail fast against a
   likely dominator. Domination check is a single-pass 3-way compare
   that subsumes the JS reference's `dominates(a,b) || dominates(b,a) ||
   equals(a,b)` triple traversal.

   The frontier is held internally in a `java.util.LinkedList` so move-
   to-front and ejection are O(1) (the JS reference uses array splice,
   which is O(n)). The public API returns plain Clojure vectors with
   `directions` / `key-fn` carried as metadata, so subsequent `insert`
   calls don't need to repeat them.

   Tie-breaking: points equal on every projected dimension are
   deduplicated — the first wins (matches the JS reference). To preserve
   distinct points that happen to share coords, add a tie-breaking
   dimension via `:key-fn`.

   Reference: https://yuri.is/writing/pareto-frontier/"
  (:import [java.util LinkedList ListIterator]))

(defn- compare-points
  "Returns :left if p1 dominates p2, :right if p2 dominates p1,
   :equal if they're identical on every dimension, :incomparable
   otherwise. Single pass. `directions` is a vector of `:max`/`:min`,
   one per dimension."
  [directions p1 p2]
  (let [k (count directions)]
    (loop [i 0, l? false, r? false]
      (if (>= i k)
        (cond l? :left, r? :right, :else :equal)
        (let [a   (nth p1 i)
              b   (nth p2 i)
              d   (nth directions i)
              la? (case d :max (> a b) :min (< a b))
              ra? (case d :max (< a b) :min (> a b))
              l?  (or l? la?)
              r?  (or r? ra?)]
          (if (and l? r?)
            :incomparable
            (recur (inc i) l? r?)))))))

(defn- insert!*
  "Internal: mutates the LinkedList by considering candidate `[coord-p p]`.
   Each list entry is `[coord original-point]` so we project once per
   point and reuse across compares."
  [^LinkedList ll directions coord-p p]
  (let [^ListIterator it (.listIterator ll)]
    (loop []
      (if (.hasNext it)
        (let [entry   (.next it)
              coord-q (nth entry 0)]
          (case (compare-points directions coord-q coord-p)
            :left          (do (.remove it) (.addFirst ll entry) nil)
            :equal         nil
            :right         (do (.remove it) (recur))
            :incomparable  (recur)))
        (.add ll [coord-p p])))))

(defn- default-directions [k] (vec (repeat k :max)))

(defn empty-frontier
  "Returns an empty frontier with the given directions / key-fn baked
   into metadata, so subsequent `insert` calls inherit them."
  [& {:keys [directions key-fn]}]
  (with-meta [] {::directions directions ::key-fn (or key-fn identity)}))

(defn frontier
  "Returns a vector of non-dominated points from `points`.

   Options:
     :directions  vector of :max/:min, one per dimension; default
                  :max on every dimension.
     :key-fn      fn from point → numeric vector for comparison;
                  default `identity` (points are themselves numeric
                  vectors).

   The returned vector carries `:directions` and `:key-fn` in metadata
   so `insert` can be called on it without repeating them."
  [points & {:keys [directions key-fn]}]
  (let [kf (or key-fn identity)]
    (if (empty? points)
      (with-meta [] {::directions directions ::key-fn kf})
      (let [d  (or directions (default-directions (count (kf (first points)))))
            ll (LinkedList.)]
        (doseq [p points]
          (insert!* ll d (kf p) p))
        (with-meta (mapv second ll) {::directions d ::key-fn kf})))))

(defn insert
  "Add `p` to `frontier`, removing any points it dominates. Returns a
   new frontier vector. `directions` / `key-fn` are inherited from
   metadata if not overridden.

   For points equal on every projected dimension, the existing one
   is kept and `p` is dropped."
  [frontier p & {:keys [directions key-fn]}]
  (let [m  (meta frontier)
        kf (or key-fn (::key-fn m) identity)
        d  (or directions (::directions m) (default-directions (count (kf p))))
        ll (LinkedList.)]
    (doseq [pt frontier]
      (.add ll [(kf pt) pt]))
    (insert!* ll d (kf p) p)
    (with-meta (mapv second ll) {::directions d ::key-fn kf})))

(defn sort-by-dim
  "Sort a frontier by its dimension `dim-idx`. Default direction is
   ascending; pass `:desc` for descending. Inherits `:key-fn` from
   the frontier's metadata."
  [frontier dim-idx & {:keys [order key-fn]
                       :or   {order :asc}}]
  (let [kf (or key-fn (::key-fn (meta frontier)) identity)
        cmp (case order
              :asc  compare
              :desc (fn [a b] (compare b a)))]
    (vec (sort-by #(nth (kf %) dim-idx) cmp frontier))))
