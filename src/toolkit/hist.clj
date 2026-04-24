(ns toolkit.hist
  "H2-encoded histograms for latency/throughput measurement.

   Two variants:
     - Dense  — fixed-size AtomicLongArray keyed by bin index. One atomic
                increment per record. Out-of-range values clamp to top bin.
     - Sparse — ConcurrentHashMap<Long, LongAdder>. Bins allocated on first
                write. Use when most bins would stay empty (rare events,
                highly-clustered distributions).

   Hot path intentionally does not track count / sum / min / max as separate
   atomics — all of those are derivable from the bin array at snapshot time
   and tracking them separately would add atomic ops per record.

   A snapshot is an EDN-friendly map:
     {:kind :dense  :a a :b b :bins [c0 c1 ... cN]}
     {:kind :sparse :a a :b b :bins {idx count ...}}

   Snapshots are independent of the live recorder and can be merged,
   serialized, or fed to `summary` / `percentile`."
  (:require [toolkit.h2 :as h2])
  (:import [java.util.concurrent.atomic AtomicLongArray LongAdder]
           [java.util.concurrent ConcurrentHashMap]))

(set! *warn-on-reflection* true)

(deftype Dense [^long a ^long b ^int n-bins ^AtomicLongArray bins])
(deftype Sparse [^long a ^long b ^ConcurrentHashMap bins])

(defn- required-bins
  "How many bins are needed to cover values 0..max-v under params a, b."
  ^long [^long a ^long b ^long max-v]
  (unchecked-inc (h2/encode a b max-v)))

(defn dense
  "Create a dense histogram.
   Opts: {:a 2 :b 4 :max-v 100000000000}
   Default covers 0..10^11 (ns up to ~100s) with ~6% relative error."
  (^Dense [] (dense {}))
  (^Dense [{:keys [a b max-v] :or {a 2 b 4 max-v 100000000000}}]
   (let [n (required-bins a b max-v)]
     (Dense. a b (int n) (AtomicLongArray. (int n))))))

(defn sparse
  "Create a sparse histogram. No max-value: covers full non-negative long range."
  (^Sparse [] (sparse {}))
  (^Sparse [{:keys [a b] :or {a 2 b 4}}]
   (Sparse. a b (ConcurrentHashMap.))))

(defn record-dense!
  "Record v into a Dense histogram. Out-of-range values clamp to top bin.
   Negative values clamp to 0. Returns nothing meaningful."
  [^Dense h ^long v]
  (let [a    (.a h)
        b    (.b h)
        n    (.n-bins h)
        bins (.bins h)
        v'   (if (neg? v) 0 v)
        raw  (h2/encode a b v')
        idx  (int (if (< raw n) raw (unchecked-dec-int n)))]
    (.incrementAndGet ^AtomicLongArray bins idx)
    nil))

(defn record-sparse!
  "Record v into a Sparse histogram. Bins allocated lazily on first write."
  [^Sparse h ^long v]
  (let [a                  (.a h)
        b                  (.b h)
        ^ConcurrentHashMap bins (.bins h)
        v'                 (if (neg? v) 0 v)
        idx                (h2/encode a b v')
        k                  (Long/valueOf idx)
        ^LongAdder adder   (or (.get bins k)
                               (let [na (LongAdder.)]
                                 (or (.putIfAbsent bins k na) na)))]
    (.increment adder)
    nil))

(defn record!
  "Record v into a histogram (Dense or Sparse). Prefer the typed variants
   (record-dense! / record-sparse!) in tight loops."
  [h ^long v]
  (cond
    (instance? Dense  h) (record-dense!  h v)
    (instance? Sparse h) (record-sparse! h v)
    :else (throw (ex-info "unknown histogram" {:got (type h)}))))

;; ---- Snapshots -----------------------------------------------------

(defn snapshot
  "Capture a frozen view of `h`. Safe to call concurrently with record!;
   the view reflects a consistent-enough sample (per-bin atomics, but
   bins are read one at a time so two bins may be from slightly
   different moments). For precise snapshots, quiesce writers first."
  [h]
  (cond
    (instance? Dense h)
    (let [^Dense d h
          a (.a d) b (.b d) n (.n-bins d)
          ^AtomicLongArray bins (.bins d)
          v (transient [])]
      (dotimes [i n]
        (conj! v (.get bins i)))
      {:kind :dense :a a :b b :bins (persistent! v)})

    (instance? Sparse h)
    (let [^Sparse s h
          a (.a s) b (.b s)
          ^ConcurrentHashMap bins (.bins s)
          m (reduce (fn [acc ^java.util.Map$Entry e]
                      (assoc acc
                             (long (.getKey e))
                             (.sum ^LongAdder (.getValue e))))
                    {}
                    (.entrySet bins))]
      {:kind :sparse :a a :b b :bins m})))

(defn merge-snapshots
  "Merge two snapshots with matching params. Missing bins treated as 0."
  [s1 s2]
  (when (or (not= (:a s1) (:a s2)) (not= (:b s1) (:b s2)))
    (throw (ex-info "snapshot param mismatch" {:s1 (dissoc s1 :bins) :s2 (dissoc s2 :bins)})))
  (cond
    (and (= :dense (:kind s1)) (= :dense (:kind s2)))
    (let [b1 (:bins s1) b2 (:bins s2)
          n  (max (count b1) (count b2))]
      {:kind :dense :a (:a s1) :b (:b s1)
       :bins (mapv #(+ (long (nth b1 % 0)) (long (nth b2 % 0))) (range n))})

    :else
    (let [->map (fn [{:keys [kind bins]}]
                  (case kind
                    :dense  (into {} (keep-indexed (fn [i c] (when (pos? (long c)) [i c])) bins))
                    :sparse bins))
          m     (merge-with + (->map s1) (->map s2))]
      {:kind :sparse :a (:a s1) :b (:b s1) :bins m})))

;; ---- Analysis on snapshots -----------------------------------------

(defn- bins-seq
  "Ascending seq of [idx count] pairs, non-zero only."
  [snap]
  (case (:kind snap)
    :dense  (keep-indexed (fn [i c] (when (pos? (long c)) [i (long c)])) (:bins snap))
    :sparse (->> (:bins snap)
                 (filter (fn [[_ c]] (pos? (long c))))
                 (sort-by first))))

(defn total-count
  ^long [snap]
  (reduce (fn [^long s [_ ^long c]] (unchecked-add s c)) 0 (bins-seq snap)))

(defn- bin-mid
  "Midpoint of bin idx (for approximate moment calculations)."
  ^long [^long a ^long b ^long idx]
  (unchecked-add (h2/decode-lower a b idx)
                 (quot (h2/decode-width a b idx) 2)))

(defn percentile
  "Interpolated value at percentile p ∈ [0,1]. nil for empty histograms.
   Interpolation is linear within the bin that contains the target rank."
  [snap p]
  (let [n  (total-count snap)
        a  (long (:a snap))
        b  (long (:b snap))]
    (when (pos? n)
      (let [tgt (max 1 (long (Math/ceil (* (double p) (double n)))))]
        (loop [bs (bins-seq snap), cum 0]
          (if-let [[idx c] (first bs)]
            (let [cum' (unchecked-add cum (long c))]
              (if (>= cum' tgt)
                (let [lower (h2/decode-lower a b idx)
                      width (h2/decode-width a b idx)
                      within (- tgt cum)
                      frac   (/ (double within) (double c))]
                  (long (+ lower (* width frac))))
                (recur (rest bs) cum')))
            ;; shouldn't reach here given n > 0
            nil))))))

(defn summary
  "Stats over a snapshot. Returns nil for empty histograms.
   count, min, max, mean, stddev, p50..p9999. Mean/stddev computed
   from bin midpoints (approximate, precision-bounded by `b`)."
  [snap]
  (let [bs (bins-seq snap)
        n  (total-count snap)
        a  (long (:a snap))
        b  (long (:b snap))]
    (when (pos? n)
      (let [first-idx (first (first bs))
            last-idx  (first (last bs))
            min-v     (h2/decode-lower a b first-idx)
            max-v     (dec (+ (h2/decode-lower a b last-idx)
                              (h2/decode-width a b last-idx)))
            sum       (reduce (fn [^double s [idx c]]
                                (+ s (* (double c) (double (bin-mid a b idx)))))
                              0.0 bs)
            mean      (/ sum (double n))
            var-sum   (reduce (fn [^double s [idx c]]
                                (let [d (- (double (bin-mid a b idx)) mean)]
                                  (+ s (* (double c) d d))))
                              0.0 bs)
            stddev    (Math/sqrt (/ var-sum (double n)))]
        {:count  n
         :min    min-v
         :max    max-v
         :mean   mean
         :stddev stddev
         :p50    (percentile snap 0.50)
         :p90    (percentile snap 0.90)
         :p99    (percentile snap 0.99)
         :p999   (percentile snap 0.999)
         :p9999  (percentile snap 0.9999)}))))
