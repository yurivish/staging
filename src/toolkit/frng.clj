(ns toolkit.frng
  "Minimal property-based testing built around a Finite RNG: a PRNG whose
   bytes are pre-generated and can run out. Entropy size is a direct proxy
   for test complexity, so minimization is just 'try a fresh smaller seed
   and see if it still breaks' — no shrinkers, no mutation. Port of the
   core trick from matklad's '256 Lines or Less' post.

   Useful for simulation testing where randomness is used throughout the
   test, rather than the test being a pure function of an input data structure.

   See: https://matklad.github.io/2026/04/20/test-case-minimization.html

   Usage:

     (require '[toolkit.frng :as frng])

     (defn my-test [f]
       (let [weights (frng/swarm-weights f [:request :crash])
             n       (frng/int-inclusive f 1000)]
         (dotimes [_ n]
           (case (frng/weighted f weights)
             :request (do ...)
             :crash   (do ...)))
         ;; raise on any invariant violation — anything other than
         ;; ::out-of-entropy counts as a failing case
         (assert (invariant-holds?) \"split brain!\")))

     (frng/search my-test {:attempts 100})
     ;; => :pass | {:fail {:size 48 :seed 12345 :ex #<AssertionError ...>}}

     (frng/run-once my-test {:size 48 :seed 12345})   ; replay"
  (:import [java.util Arrays Random]
           [java.util.concurrent ThreadLocalRandom]
           [java.nio ByteBuffer ByteOrder]))

(set! *warn-on-reflection* true)

;; --- FRNG: an atomic cursor over a fixed byte array ---

(defn make
  "Wrap a byte array as an FRNG cursor. Safe to share across threads."
  [^bytes entropy]
  {:entropy entropy :pos (atom 0)})

(defn remaining ^long [{:keys [^bytes entropy pos]}]
  (- (alength entropy) (long @pos)))

(defn- out-of-entropy! []
  (throw (ex-info "out of entropy" {::out-of-entropy true})))

(defn bytes*
  "Consume the next n bytes; return a fresh byte[]. Throws
   ::out-of-entropy if fewer than n remain."
  ^bytes [{:keys [^bytes entropy pos]} ^long n]
  (let [end   (long (swap! pos
                           (fn [^long p]
                             (let [e (+ p n)]
                               (when (> e (alength entropy)) (out-of-entropy!))
                               e))))
        start (- end n)]
    (Arrays/copyOfRange entropy (int start) (int end))))

;; --- scalar generators ---

(defn long*
  "Draw a signed 64-bit long (8 bytes, little-endian)."
  ^long [frng]
  (-> (ByteBuffer/wrap (bytes* frng 8))
      (.order ByteOrder/LITTLE_ENDIAN)
      (.getLong)))

(defn bool [frng]
  (let [^bytes bs (bytes* frng 1)]
    (odd? (aget bs 0))))

(defn- long-from-bytes
  "Big-endian assemble n bytes into a non-negative long. Caller chooses n
   so the result fits in 63 bits (i.e. n <= 7, or n == 8 with a mask that
   clears the top bit)."
  ^long [^bytes bs]
  (areduce bs i acc 0
           (bit-or (bit-shift-left acc 8)
                   (long (bit-and (aget bs i) 0xff)))))

(defn int-inclusive
  "Uniform integer in [0, max] inclusive, via rejection sampling. Each
   attempt draws ceil(log2(max+1)/8) bytes; rejection rate is < 50%.
   Requires 0 <= max < Long/MAX_VALUE."
  ^long [frng ^long max]
  (assert (and (>= max 0) (< max Long/MAX_VALUE))
          "max must be in [0, Long/MAX_VALUE)")
  (if (zero? max)
    0
    (let [bits    (- 64 (Long/numberOfLeadingZeros max))
          n-bytes (long (Math/ceil (/ bits 8.0)))
          mask    (dec (bit-shift-left 1 bits))
          bound   (inc max)]
      (loop []
        (let [v (bit-and (long-from-bytes (bytes* frng n-bytes)) mask)]
          (if (< v bound) v (recur)))))))

(defn range-inclusive
  "Uniform integer in [lo, hi]."
  ^long [frng ^long lo ^long hi]
  (assert (<= lo hi) "lo must be <= hi")
  (+ lo (int-inclusive frng (- hi lo))))

(defn index
  "Uniform index into a non-empty collection."
  ^long [frng coll]
  (let [n (count coll)]
    (assert (pos? n) "collection must be non-empty")
    (int-inclusive frng (dec n))))

(defn weighted
  "Pick a key from `weights` — a map of k -> non-negative int — with
   probability proportional to each value. Entries iterated in sorted-key
   order for reproducibility (so keys must be mutually comparable;
   keywords are fine). Total weight must be positive."
  [frng weights]
  (let [entries (sort-by key (seq weights))
        total   (reduce + 0 (map val entries))]
    (assert (pos? total) "weights must sum to a positive number")
    (loop [pick (long (int-inclusive frng (dec total)))
           es   entries]
      (let [[k w] (first es)
            w     (long w)]
        (if (< pick w)
          k
          (recur (- pick w) (rest es)))))))

(defn swarm-weights
  "Generate a random weight per key — {k -> int in [lo, hi]}. Drawn at
   the start of each test run so the search also covers different
   behaviour mixes (cf. 'Swarm Testing Data Structures')."
  ([frng ks] (swarm-weights frng ks 1 100))
  ([frng ks ^long lo ^long hi]
   (into {} (map (fn [k] [k (range-inclusive frng lo hi)])) ks)))

;; --- driver: search for the smallest entropy size that fails ---

(defn- entropy-from-seed
  "Expand a 64-bit seed into `size` bytes via java.util.Random — cheap,
   deterministic, and only needs two numbers (size + seed) to reproduce
   a run."
  ^bytes [^long seed ^long size]
  (let [r  (Random. seed)
        bs (byte-array size)]
    (.nextBytes r bs)
    bs))

(defn run-once
  "Run (test-fn frng) once on entropy derived from `size` and `seed`.
   Returns :pass on successful completion or ::out-of-entropy, or
   {:fail ex} on any other Throwable."
  [test-fn {:keys [^long size ^long seed]}]
  (let [frng (make (entropy-from-seed seed size))]
    (try
      (test-fn frng)
      :pass
      (catch Throwable ex
        (if (::out-of-entropy (ex-data ex)) :pass {:fail ex})))))

(defn- try-seeds
  "Try up to `attempts` fresh random seeds at the given size. Return
   [:pass] or [:fail seed ex] at the first failure."
  [test-fn ^long size ^long attempts]
  (let [rng (ThreadLocalRandom/current)]
    (loop [i 0]
      (if (< i attempts)
        (let [seed (.nextLong rng)
              r    (run-once test-fn {:size size :seed seed})]
          (if (= r :pass)
            (recur (inc i))
            [:fail seed (:fail r)]))
        [:pass]))))

(defn search
  "Find the smallest entropy size at which test-fn fails. Walks sizes via
   a doubling/halving step — grows while passing, shrinks after the first
   fail — and tries `attempts` seeds at each size to distinguish 'no
   failure here' from 'too small to fail.' Returns :pass, or
   {:fail {:size n :seed s :ex ex}} suitable to replay via `run-once`.

   Because the walk decrements size while in the failing state and halves
   step on every pass↔fail flip, the last recorded fail is the smallest
   seen, so we just overwrite on each failure.

   Options:
     :attempts  seeds tried at each size (default 100)
     :max-size  ceiling in bytes           (default 4 MiB)
     :verbose?  log per-step status to *err* (default false)"
  [test-fn {:keys [attempts max-size verbose?]
            :or   {attempts 100 max-size (* 4 1024 1024)}}]
  (let [max-size (long max-size)
        attempts (long attempts)
        log!     (fn [& xs] (when verbose? (binding [*out* *err*] (apply println xs))))]
    (loop [size 16, step 16, pass? true, found nil, iters 0]
      (let [size-next (if pass? (+ size step) (- size step))]
        (if (or (zero? step)
                (>= iters 1024)
                (> size-next max-size)
                (<= size-next 0))
          (if found {:fail found} :pass)
          (let [[outcome seed ex] (try-seeds test-fn size-next attempts)
                pass-next? (= outcome :pass)
                _          (if pass-next?
                             (log! "pass size=" size-next)
                             (log! "FAIL size=" size-next "seed=" seed))
                found'     (if pass-next? found {:size size-next :seed seed :ex ex})
                step'      (if (= pass? pass-next?) (* step 2) (quot step 2))
                ;; Don't commit when currently failing and the smaller
                ;; candidate passed — stay put, halve step, try again.
                advance?   (or pass? (not pass-next?))]
            (recur (if advance? size-next size)
                   step'
                   (if advance? pass-next? pass?)
                   found'
                   (inc iters))))))))
