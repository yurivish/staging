(ns toolkit.stree-bench
  "stree (subject tree) benchmark scenarios.

   Dataset: deterministic (seeded RNG) subjects of 2-5 dot-separated
   tokens. The first token is drawn from a 16-bucket pool so that
   `X.>` patterns deterministically match ~n/16 entries and stress the
   fwc walk at scale. The same subject vector is sliced for each size."
  (:require [clojure.string :as str]
            [toolkit.bench :as b]
            [toolkit.stree :as st])
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

(defn- rand-subject ^String [^Random r]
  (let [first-tok (nth first-tokens (.nextInt r 16))
        n-more    (inc (.nextInt r 4))
        toks      (into [first-tok] (repeatedly n-more #(rand-token r 4)))]
    (str/join "." toks)))

(defn- gen-subjects
  "Vector of `n` distinct subjects from a seeded RNG."
  [seed n]
  (let [r (Random. (long seed))]
    (loop [acc (transient []) seen (transient #{})]
      (if (>= (count acc) n)
        (persistent! acc)
        (let [s (rand-subject r)]
          (if (contains? seen s)
            (recur acc seen)
            (recur (conj! acc s) (conj! seen s))))))))

(def ^:private all-subjects
  (delay (gen-subjects 42 (+ max-size 1000))))

(defn- build-tree [subjects]
  (let [t (st/make)]
    (doseq [s subjects] (st/insert! t s 1))
    t))

(defn- star-pattern ^String [^String subject]
  (let [toks (str/split subject #"\.")
        mid  (quot (count toks) 2)]
    (str/join "." (assoc (vec toks) mid "*"))))

(defn- fanout-pattern ^String [^String subject]
  (str (first (str/split subject #"\.")) ".>"))

(defn- miss-subject ^String [^Random r]
  (str "ZZZZ." (rand-subject r)))

(defn- fixture [n]
  (let [subs       (subvec @all-subjects 0 n)
        tree       (build-tree subs)
        hit        (nth subs (quot n 2))
        miss       (miss-subject (Random. (+ 99 n)))]
    {:n       n
     :tree    tree
     :subs    subs
     :hit     hit
     :miss    miss
     :star    (star-pattern hit)
     :fanout  (fanout-pattern hit)}))

(def ^:private noop-cb (fn [_ _] true))

(defn run []
  (b/banner "stree benchmarks")
  (print "building fixtures") (flush)
  (let [fxs (mapv (fn [n] (print ".") (flush) [n (fixture n)]) sizes)]
    (println " done")

    (b/header "lookup-hit (literal lookup, key present)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(st/lookup (:tree fx) (:hit fx))))

    (b/header "lookup-miss (literal lookup, key absent)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(st/lookup (:tree fx) (:miss fx))))

    (b/header "match-literal (filter is literal, 1 hit)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(st/match (:tree fx) (:hit fx) noop-cb)))

    (b/header "match-wildcard-* (one middle-token wildcard)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(st/match (:tree fx) (:star fx) noop-cb)))

    (b/header "match-wildcard-> (first-token prefix + fwc, ~n/16 hits)")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(st/match (:tree fx) (:fanout fx) noop-cb)))

    (b/header "iter-fast (full walk, O(n))")
    (doseq [[n fx] fxs]
      (b/bench-case (format "n=%d" n) #(st/iter-fast (:tree fx) noop-cb)))

    (b/header "bulk-insert (build tree from empty; per-op time = total / n)")
    (doseq [n sizes]
      (let [subs (subvec @all-subjects 0 n)]
        (b/bench-case (format "n=%d" n) #(build-tree subs))))))
