(ns toolkit.sublist-bench
  "sublist (subscription routing trie) benchmark scenarios.

   Dataset: ~75% literal patterns, ~20% single-`:*`, ~5% trailing-`:>`,
   with ~25% placed in queue groups (8 rotating queue names). The
   first-token pool (16 buckets) ensures `[<first> :>]` reverse-match
   queries hit a meaningful fraction at each scale."
  (:require [toolkit.bench :as b]
            [toolkit.sublist :as sl])
  (:import [java.util Random]))

(set! *warn-on-reflection* true)

(def ^:private sizes [1000 10000 100000])
(def ^:private ^:const max-size 100000)

(defn- rand-token ^String [^Random r ^long max-len]
  (let [len (inc (.nextInt r (int max-len)))
        sb  (StringBuilder. len)]
    (dotimes [_ len]
      (.append sb (char (+ (int \a) (.nextInt r 26)))))
    (.toString sb)))

(def ^:private first-tokens
  (into [] (for [i (range 16)]
             (apply str (repeat (inc (rem i 3))
                                (char (+ (int \a) i)))))))

(defn- rand-subject [^Random r]
  (let [first-tok (nth first-tokens (.nextInt r 16))
        n-more    (inc (.nextInt r 4))]
    (into [first-tok] (repeatedly n-more #(rand-token r 4)))))

(defn- rand-pattern [^Random r]
  (let [toks (rand-subject r)
        n    (count toks)
        roll (.nextInt r 20)]
    (cond
      (< roll 15) toks                           ; literal
      (< roll 19) (assoc toks (quot n 2) :*)     ; middle *
      :else       (conj (pop toks) :>))))        ; trailing >

(def ^:private all-patterns
  (delay
    (let [r (Random. 73)]
      (into [] (repeatedly (+ max-size 1000) #(rand-pattern r))))))

(defn- build-sublist [patterns]
  (let [sl-ref (sl/make)
        r      (Random. 11)]
    (dotimes [i (count patterns)]
      (let [p (nth patterns i)]
        (if (zero? (.nextInt r 4))
          (sl/insert! sl-ref p i {:queue (str "q" (rem i 8))})
          (sl/insert! sl-ref p i))))
    sl-ref))

(defn- literal-from-pattern [p]
  (mapv (fn [t] (if (or (identical? t :*) (identical? t :>)) "x" t)) p))

(defn- miss-subject [^Random r]
  (into ["ZZZZ"] (rand-subject r)))

;; Query inputs are pinned across sizes so only the tree varies. The
;; hit index is well below the smallest sweep size; miss/fanout use
;; fixed seeds. rev-pat is literal, no RNG needed.
(def ^:private ^:const hit-index 17)
(def ^:private fixed-miss (miss-subject (Random. 7)))
(def ^:private fixed-fanout
  (let [r (Random. 9)]
    [(nth first-tokens 0) (rand-token r 4) (rand-token r 4)]))
(def ^:private fixed-rev-pat [(nth first-tokens 0) :>])

(defn- fixture [n]
  (let [patterns (subvec @all-patterns 0 n)
        sl-ref   (build-sublist patterns)
        hit      (literal-from-pattern (nth patterns hit-index))]
    {:n n :sl sl-ref :hit hit :miss fixed-miss
     :fanout fixed-fanout :rev-pat fixed-rev-pat}))

(defn run []
  (b/banner "sublist benchmarks")
  (print "building fixtures") (flush)
  (let [fxs (mapv (fn [n] (print ".") (flush) [n (fixture n)]) sizes)]
    (println " done")

    (b/header "match (literal subject, one likely hit)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(sl/match (:sl fx) (:hit fx))))

    (b/header "match-miss (no matching subs)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(sl/match (:sl fx) (:miss fx))))

    (b/header "match-fanout (first-token bucket; exercises *, >)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(sl/match (:sl fx) (:fanout fx))))

    (b/header "has-interest? hit (short-circuit on first match)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(sl/has-interest? (:sl fx) (:hit fx))))

    (b/header "has-interest? miss (worst-case negative walk)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(sl/has-interest? (:sl fx) (:miss fx))))

    (b/header "reverse-match (wildcard query)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(sl/reverse-match (:sl fx) (:rev-pat fx))))

    (b/header "count-subs (O(n) full walk)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(sl/count-subs (:sl fx))))

    (b/header "bulk-insert (build sublist from empty; per-op time = total / n)")
    (doseq [n sizes]
      (let [patterns (subvec @all-patterns 0 n)]
        (b/bench-case (format "n=%d" n) #(build-sublist patterns))))))
