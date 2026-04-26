# NLP toolkit — draft sketches

Pure functions over abstract data. Token type is anything hashable
(strings, keywords, vectors of (POS, lemma), entity-IDs — whatever the
caller has). No tokenization, no IO, no HTML stripping; pipelines bring
those.

## Location

Single namespace `toolkit.nlp` at `/work/src/toolkit/nlp.clj`. Matches
the existing one-file-per-module pattern (`web.clj`, `lmdb.clj`,
`sqlite.clj`, `datapotamus.clj`). Split into a subdir later if it grows.

Sections within the namespace:

```
;; ── statistical primitives ────────────────────────────────────────
;;    g2, poisson-surprise
;;
;; ── bigram collocation ────────────────────────────────────────────
;;    bigram-counts, bigram-llr, bigram-poisson
;;
;; ── keyness ───────────────────────────────────────────────────────
;;    weighted-log-odds-dirichlet
;;
;; ── n-gram LM with Kneser-Ney smoothing ───────────────────────────
;;    fit-kn, kn-prob, kn-perplexity
```

## 1. Statistical primitives

```clojure
(defn- xlogx-y
  "x · log(x/y); 0 · log(0/_) ≜ 0."
  ^double [x y]
  (if (zero? x) 0.0 (* x (Math/log (double (/ x y))))))

(defn g2
  "Dunning's log-likelihood ratio (G²) for a 2×2 contingency table.
       | col-1 | col-2 |
   r-1 |   a   |   b   |
   r-2 |   c   |   d   |
   χ²(1)-distributed under independence."
  ^double [a b c d]
  (let [n  (+ a b c d)
        ea (/ (* (+ a b) (+ a c)) n)
        eb (/ (* (+ a b) (+ b d)) n)
        ec (/ (* (+ c d) (+ a c)) n)
        ed (/ (* (+ c d) (+ b d)) n)]
    (* 2.0 (+ (xlogx-y a ea) (xlogx-y b eb)
              (xlogx-y c ec) (xlogx-y d ed)))))

(defn- log-factorial
  "log(n!). Exact below 50; Stirling above."
  ^double [n]
  (cond
    (< n 2)  0.0
    (< n 50) (loop [acc 0.0 i 2]
               (if (> i n) acc
                   (recur (+ acc (Math/log (double i))) (inc i))))
    :else    (let [n (double n)]
               (+ (* n (Math/log n)) (- n)
                  (* 0.5 (Math/log (* 2.0 Math/PI n)))))))

(defn poisson-surprise
  "Quasthoff & Wolff Poisson collocation measure:
   −log P(X = k | X ~ Poisson(λ))  =  log(k!) − k·log(λ) + λ.
   Higher = more surprising under the independence model."
  ^double [k lambda]
  (+ (log-factorial k)
     (- (* k (Math/log (double lambda))))
     (double lambda)))
```

## 2. Bigram collocation

```clojure
(defn bigram-counts
  "{:n n-bigrams, :marginal {token count}, :joint {[a b] count}}."
  [tokens]
  (let [bigrams (partition 2 1 tokens)]
    {:n        (count bigrams)
     :marginal (frequencies tokens)
     :joint    (frequencies (map vec bigrams))}))

(defn- pair-2x2
  "Contingency cells (a,b,c,d) for the bigram (w1,w2) given marginals."
  [n marginal joint w1 w2]
  (let [j (get joint [w1 w2] 0)
        m1 (marginal w1)
        m2 (marginal w2)]
    [j (- m1 j) (- m2 j) (- n m1 m2 (- j))]))   ; a b c d

(defn bigram-llr
  "Returns sorted seq of {:pair :joint :llr} for bigrams with joint ≥ min."
  [tokens & {:keys [min-joint] :or {min-joint 5}}]
  (let [{:keys [n marginal joint]} (bigram-counts tokens)]
    (->> joint
         (keep (fn [[[a b] j]]
                 (when (>= j min-joint)
                   (let [[a' b' c' d'] (pair-2x2 n marginal joint a b)]
                     {:pair [a b] :joint j :llr (g2 a' b' c' d')}))))
         (sort-by :llr >))))

(defn bigram-poisson
  "Same shape as bigram-llr but scoring with Quasthoff-Wolff."
  [tokens & {:keys [min-joint] :or {min-joint 5}}]
  (let [{:keys [n marginal joint]} (bigram-counts tokens)]
    (->> joint
         (keep (fn [[[a b] j]]
                 (when (>= j min-joint)
                   (let [lambda (/ (* (marginal a) (marginal b)) n)]
                     {:pair [a b] :joint j
                      :poisson (poisson-surprise j lambda)}))))
         (sort-by :poisson >))))
```

## 3. Weighted log-odds with Dirichlet prior (Monroe 2008)

```clojure
(defn weighted-log-odds-dirichlet
  "Weighted log-odds with informative Dirichlet prior — Monroe, Colaresi
   & Quinn 'Fightin' Words' (2008).

   counts-a, counts-b : {token long}.
   prior              : {token long} pseudocounts; typically the union
                        background-corpus counts.

   Returns {token z}. Positive z → distinctive of A; negative → B.
   Tokens with no prior mass are skipped (no information)."
  [counts-a counts-b prior]
  (let [n-a (reduce + (vals counts-a))
        n-b (reduce + (vals counts-b))
        a0  (reduce + (vals prior))]
    (into {}
          (keep (fn [[w aw]]
                  (when (pos? aw)
                    (let [yA (get counts-a w 0)
                          yB (get counts-b w 0)
                          ωA (/ (+ yA aw) (- (+ n-a a0) yA aw))
                          ωB (/ (+ yB aw) (- (+ n-b a0) yB aw))
                          δ  (- (Math/log (double ωA)) (Math/log (double ωB)))
                          σ² (+ (/ 1.0 (+ yA aw)) (/ 1.0 (+ yB aw)))]
                      [w (/ δ (Math/sqrt σ²))]))))
          prior)))
```

## 4. Kneser-Ney n-gram language model

Interpolated Kneser-Ney (Ney/Essen/Kneser 1994, Kneser & Ney 1995). The
recursion:

```
P_KN(w | h) = max(c(h,w) − D, 0) / c(h)
            + γ(h) · P_cont(w | h′)

γ(h)        = D · N₊(h, •) / c(h)            ; N₊(h, •) = #distinct continuations
P_cont(w|h′) — same shape but using N₊(•, h′, w) / N₊(•, h′, •)
P_cont(w)   = N₊(•, w) / N₊(•, •)            ; unigram base case
```

`h′` is `h` with the leftmost token dropped.

```clojure
(defn- ngram-stats
  "Walks each sequence once. Returns:
     :counts        {n {ngram-vec count}}        for n in 1..order
     :continuations {n {prefix-vec   distinct}}  for n in 2..order
                                                  prefix has length n−1
     :predecessors  {n {suffix-vec   distinct}}  for n in 2..order
                                                  suffix has length n−1
     :n-bigram-types  N₊(•, •) — distinct bigrams"
  [sequences order]
  (let [counts (reduce
                (fn [acc s]
                  (reduce
                   (fn [acc n]
                     (reduce
                      (fn [acc ng] (update-in acc [n (vec ng)] (fnil inc 0)))
                      acc
                      (partition n 1 s)))
                   acc
                   (range 1 (inc order))))
                {} sequences)
        derive (fn [n keyfn]
                 (->> (keys (get counts n))
                      (group-by keyfn)
                      (into {} (map (fn [[k v]] [k (count v)])))))
        derived (reduce
                 (fn [acc n]
                   (-> acc
                       (assoc-in [:continuations n] (derive n #(vec (butlast %))))
                       (assoc-in [:predecessors  n] (derive n #(vec (rest %))))))
                 {} (range 2 (inc order)))]
    (assoc derived
           :counts          counts
           :order           order
           :n-bigram-types  (count (get counts 2)))))

(defn fit-kn
  "Fit an interpolated KN n-gram LM.
   sequences: seq of seqs of hashable tokens (typically sentences).
   :order     — n-gram order (default 3)
   :discount  — fixed D in (0,1); default estimated as n₁/(n₁+2n₂)."
  [sequences {:keys [order discount] :or {order 3}}]
  (let [stats (ngram-stats sequences order)
        D     (or discount
                  (let [c (vals (get-in stats [:counts order]))
                        n1 (count (filter #{1} c))
                        n2 (count (filter #{2} c))]
                    (if (zero? (+ n1 (* 2 n2)))
                      0.5
                      (/ (double n1) (+ n1 (* 2.0 n2))))))]
    (assoc stats :discount D)))

(defn- kn-prob*
  "Recursive helper. n is the LENGTH of the (h,w) tuple being scored."
  [model h w n]
  (if (= n 1)
    ;; Unigram base case: continuation distribution
    (let [n+w  (get-in model [:predecessors 2 [w]] 0)
          n++  (:n-bigram-types model)]
      (if (zero? n++) 0.0 (/ (double n+w) n++)))
    (let [counts (get-in model [:counts n])
          c-h    (get-in model [:counts (dec n)] 0)
          c-h-v  (get c-h h 0)
          c-hw   (get counts (conj h w) 0)
          n+h•   (get-in model [:continuations n h] 0)
          D      (:discount model)]
      (if (zero? c-h-v)
        (recur model (vec (rest h)) w (dec n))
        (+ (max (/ (- c-hw D) (double c-h-v)) 0.0)
           (* (/ (* D n+h•) (double c-h-v))
              (kn-prob* model (vec (rest h)) w (dec n))))))))

(defn kn-prob
  "P(w | context). context is up to (order−1) preceding tokens."
  [model context w]
  (let [order (:order model)
        h     (vec (take-last (dec order) context))]
    (kn-prob* model h w (inc (count h)))))

(defn kn-perplexity
  "Geometric-mean inverse-probability per token over eval sequences."
  [model sequences]
  (let [[ll n] (reduce
                (fn [[ll n] s]
                  (let [n+1grams (partition (:order model) 1 s)]
                    (reduce
                     (fn [[ll n] ng]
                       [(+ ll (Math/log (kn-prob model (butlast ng) (last ng))))
                        (inc n)])
                     [ll n] n+1grams)))
                [0.0 0] sequences)]
    (Math/exp (/ (- ll) n))))
```

## What's deferred

- **Modified Kneser-Ney** — separate D₁, D₂, D₃₊ (Chen & Goodman 1998).
  More accurate; same skeleton, three discounts instead of one.
- **Windowed (non-adjacent) collocation** — generalize bigram-counts to
  a window > 1; marginals become "occurrences within window" and the
  contingency table becomes an approximation rather than exact.
- **Iterative phrase merging** — fixpoint over `bigram-llr`: pick top-K
  pairs above a threshold, replace as merged tokens (e.g.
  `[:phrase :w1 :w2]`), re-tokenize, repeat. Composes over the toolkit
  rather than living inside it.
- **LLM pre-normalization stages** — entity canonicalization, coref,
  structure extraction. These compose at the pipeline layer (Datapotamus
  steps), feeding canonicalized token streams into the toolkit functions.

## Tests (sanity, not benchmarks)

```clojure
;; g2: independence → 0; perfect dependence → large
(t/is (< (Math/abs (g2 50 50 50 50)) 1e-9))
(t/is (> (g2 100 0 0 100) 100))

;; poisson-surprise: at-expectation small, way-above-expectation large
(t/is (< (poisson-surprise 5 5)  2.0))
(t/is (> (poisson-surprise 100 1) 100))

;; bigram-llr on `the cat the cat the cat` finds [the cat] over [cat the]
(let [r (bigram-llr [:the :cat :the :cat :the :cat] :min-joint 1)]
  (t/is (= [:the :cat] (:pair (first r)))))

;; weighted log-odds: identical corpora → all z = 0
(let [c {:a 10 :b 20} p {:a 100 :b 200}]
  (t/is (every? #(< (Math/abs %) 1e-9)
                (vals (weighted-log-odds-dirichlet c c p)))))

;; KN: on aabaab corpus, P(b | a) > P(a | a)
(let [m (fit-kn [[:a :a :b :a :a :b]] {:order 2})]
  (t/is (> (kn-prob m [:a] :b) (kn-prob m [:a] :a))))
```

## Verification

- Run the sanity tests; they should all pass.
- Manually run `bigram-llr` on a real HN comment-thread token stream
  and eyeball whether the top results are recognizable phrases.
- Manually fit a KN model on a few hundred sentences from a podcast
  transcript; compare perplexity to a uniform unigram baseline (KN
  should be much lower).
