(ns toolkit.exhaustigen
  "Exhaustive-testing utility. Tracks variable-radix digit counters so a
   loop of the form `(while (not (done!? g)) ...)` eventually visits every
   combination of non-deterministic choices made inside the loop.

   Port of `toolkit/exhaustigen` (Go), itself a port of Graydon Hoare's Rust
   implementation (https://github.com/graydon/exhaustigen-rs/) of Alex
   Kladov's idea
   (https://matklad.github.io/2021/11/07/generate-all-the-things.html).

   Typical use:

     (let [g (ex/make)]
       (while (not (ex/done!? g))
         (let [n   (ex/gen g 3)
               xs  (ex/elts g 3 4)]
           (check-my-invariant n xs))))")

;; State: {:started? bool, :v [[cur bnd] ...], :p int}. `:v` is the
;; odometer — one digit per `gen` call made within a single pass. `:p`
;; is the read cursor reset to 0 at the top of each pass.

(defn make
  "Returns a fresh generator."
  []
  (atom {:started? false :v [] :p 0}))

(defn- step-odometer
  "On the first call, just mark started. Subsequently, walk the digits
   back-to-front and increment the rightmost that hasn't hit its bound;
   truncate digits to the right of it and reset the read cursor. If every
   digit is at its bound, mark ::exhausted."
  [{:keys [started? v] :as s}]
  (if-not started?
    (assoc s :started? true)
    (loop [i (dec (count v))]
      (if (neg? i)
        (assoc s ::exhausted? true)
        (let [[cur bnd] (nth v i)]
          (if (< cur bnd)
            {:started?     true
             :v            (conj (subvec v 0 i) [(inc cur) bnd])
             :p            0}
            (recur (dec i))))))))

(defn done!?
  "Advances the odometer and returns true once every combination has been
   visited. Mutates `g`; call once at the head of each loop iteration:
   `(while (not (done!? g)) ...)`."
  [g]
  (boolean (::exhausted? (swap! g step-odometer))))

(defn gen
  "Returns a long in [0, bound] inclusive. Eventually, across successive
   outer-loop iterations, returns every value in the range. Mutates `g`."
  ^long [g ^long bound]
  (let [{:keys [v p]} (swap! g
                       (fn [{:keys [v p] :as s}]
                         (let [v   (if (= p (count v)) (conj v [0 0]) v)
                               cur (get-in v [p 0])]
                           (assoc s :v (assoc v p [cur bound]) :p (inc p)))))]
    (long (get-in v [(dec p) 0]))))

(defn flip
  "Returns false then true across iterations. Mutates `g`."
  [g]
  (= 1 (gen g 1)))

(defn pick
  "Returns an index in [0, n). Mutates `g`."
  ^long [g ^long n]
  (gen g (dec n)))

(defn fixed-by
  "Returns a vector of `fixed` values, each produced by calling `(f g)`."
  [g ^long fixed f]
  (loop [i 0 out []]
    (if (= i fixed) out (recur (inc i) (conj out (f g))))))

(defn bound-by
  "Returns a variable-length vector (length in [0, bound]) of values
   produced by calling `(f g)`."
  [g ^long bound f]
  (fixed-by g (gen g bound) f))

(defn elts
  "Variable-length vector (length ≤ len-bound) of ints each in
   [0, elt-bound]."
  [g ^long len-bound ^long elt-bound]
  (bound-by g len-bound #(gen % elt-bound)))

(defn fixed-comb
  "Fixed-size combination with replacement: `fixed` indices into [0, n)."
  [g ^long fixed ^long n]
  (fixed-by g fixed #(pick % n)))

(defn bound-comb
  "Variable-size (≤ bound) combination with replacement from [0, n)."
  [g ^long bound ^long n]
  (fixed-comb g (gen g bound) n))

(defn comb
  "Variable-size combination with replacement from [0, n). Equivalent to
   `(bound-comb g n n)`."
  [g ^long n]
  (bound-comb g n n))

(defn perm
  "A permutation of [0, n) as a vector."
  [g ^long n]
  (loop [remaining (vec (range n)) out []]
    (if (empty? remaining)
      out
      (let [p   (gen g (dec (count remaining)))
            val (nth remaining p)]
        (recur (into (subvec remaining 0 p) (subvec remaining (inc p)))
               (conj out val))))))

(defn subset
  "Indices of a subset of [0, n), as a vector in increasing order."
  [g ^long n]
  (loop [i 0 out []]
    (if (= i n)
      out
      (recur (inc i) (if (flip g) (conj out i) out)))))
