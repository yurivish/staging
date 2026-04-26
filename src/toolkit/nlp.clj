(ns toolkit.nlp
  "Small NLP toolkit — pure functions over abstract token sequences and
   count maps. No tokenization, no IO. Token type is anything hashable.

   References

   - Dunning (1993) 'Accurate Methods for the Statistics of Surprise and
     Coincidence', Computational Linguistics 19(1):61-74.
   - Quasthoff & Wolff (2002) 'The Poisson Collocation Measure and its
     Applications', Computational Approaches to Collocations workshop.
   - Monroe, Colaresi & Quinn (2008) 'Fightin' Words: Lexical Feature
     Selection and Evaluation for Identifying the Content of Political
     Conflict', Political Analysis 16(4):372-403.
   - Kneser & Ney (1995) 'Improved backing-off for M-gram language
     modeling', ICASSP-95.
   - Chen & Goodman (1999) 'An Empirical Study of Smoothing Techniques
     for Language Modeling', Computer Speech & Language 13:359-394.
   - Manning & Schütze (1999) 'Foundations of Statistical NLP',
     ch. 5 (collocations) and ch. 6 (smoothing) — textbook treatment.")

;; ── statistical primitives ────────────────────────────────────────

(defn- xlogx-y
  ^double [x y]
  (if (zero? x) 0.0 (* x (Math/log (double (/ x y))))))

(defn g2
  "Dunning G² (LLR) for a 2×2 contingency table:
       | col-1 | col-2 |
   r-1 |   a   |   b   |
   r-2 |   c   |   d   |
   χ²(1) under the independence null. — Dunning 1993."
  ^double [a b c d]
  (let [n  (+ a b c d)
        ea (/ (* (+ a b) (+ a c)) n)
        eb (/ (* (+ a b) (+ b d)) n)
        ec (/ (* (+ c d) (+ a c)) n)
        ed (/ (* (+ c d) (+ b d)) n)]
    (* 2.0 (+ (xlogx-y a ea) (xlogx-y b eb)
              (xlogx-y c ec) (xlogx-y d ed)))))

(defn- log-factorial
  ^double [n]
  (cond
    (< n 2)  0.0
    (< n 50) (loop [acc 0.0 i 2]
               (if (> i n) acc (recur (+ acc (Math/log (double i))) (inc i))))
    :else    (let [n (double n)]
               (+ (* n (Math/log n)) (- n)
                  (* 0.5 (Math/log (* 2.0 Math/PI n)))
                  (/ 1.0 (* 12.0 n))))))                ; 1st Stirling correction

(defn poisson-surprise
  "Quasthoff-Wolff: −log P(X = k | X ~ Poisson(λ))
   = log(k!) − k·log(λ) + λ. Higher = more surprising under
   independence. — Quasthoff & Wolff 2002."
  ^double [k lambda]
  (+ (log-factorial k)
     (- (* k (Math/log (double lambda))))
     (double lambda)))

;; ── bigram collocation ────────────────────────────────────────────

(defn bigram-counts
  "{:n n-bigrams, :left {token count}, :right {token count}, :joint {[a b] count}}.
   :left[w]  = # bigrams with w in first position
   :right[w] = # bigrams with w in second position
   Marginals are derived from joint counts so the 2×2 cells sum exactly to n.
   Takes a flat seq of tokens; insert sentinels between sentences if you want
   to suppress cross-boundary bigrams."
  [tokens]
  (let [bigrams (map vec (partition 2 1 tokens))
        joint   (frequencies bigrams)]
    {:n     (count bigrams)
     :left  (reduce-kv (fn [m [a _] c] (update m a (fnil + 0) c)) {} joint)
     :right (reduce-kv (fn [m [_ b] c] (update m b (fnil + 0) c)) {} joint)
     :joint joint}))

(defn- pair-2x2 [n left right joint a b]
  (let [j  (get joint [a b] 0)
        m1 (left a)
        m2 (right b)]
    [j (- m1 j) (- m2 j) (+ (- n m1 m2) j)]))

(defn bigram-llr
  "Sorted seq of {:pair :joint :llr} for bigrams with joint ≥ min-joint.
   Score is Dunning G² over the 2×2 (w1 followed by w2) contingency table."
  [tokens & {:keys [min-joint] :or {min-joint 5}}]
  (let [{:keys [n left right joint]} (bigram-counts tokens)]
    (->> joint
         (keep (fn [[[a b] j]]
                 (when (>= j min-joint)
                   (let [[a' b' c' d'] (pair-2x2 n left right joint a b)]
                     {:pair [a b] :joint j :llr (g2 a' b' c' d')}))))
         (sort-by :llr >))))

(defn bigram-poisson
  "Sorted seq of {:pair :joint :poisson} for bigrams with joint ≥ min-joint.
   Score is −log P(X = j | Poisson(m1·m2/n)) — Quasthoff-Wolff."
  [tokens & {:keys [min-joint] :or {min-joint 5}}]
  (let [{:keys [n left right joint]} (bigram-counts tokens)]
    (->> joint
         (keep (fn [[[a b] j]]
                 (when (>= j min-joint)
                   (let [lambda (/ (* (left a) (right b)) (double n))]
                     {:pair [a b] :joint j
                      :poisson (poisson-surprise j lambda)}))))
         (sort-by :poisson >))))

;; ── keyness ───────────────────────────────────────────────────────

(defn weighted-log-odds-dirichlet
  "Weighted log-odds with informative Dirichlet prior — Monroe, Colaresi
   & Quinn (2008), 'Fightin' Words'.

   counts-a, counts-b : {token long}
   prior              : {token long} pseudocounts; typically the
                        union background-corpus counts.

   Returns {token z}. Positive z → distinctive of A; negative → B.
   Tokens absent from `prior` are skipped (no information)."
  [counts-a counts-b prior]
  (let [n-a (reduce + (vals counts-a))
        n-b (reduce + (vals counts-b))
        a0  (reduce + (vals prior))]
    (into {}
          (keep (fn [[w aw]]
                  (when (pos? aw)
                    (let [yA      (get counts-a w 0)
                          yB      (get counts-b w 0)
                          denom-A (- (+ n-a a0) yA aw)
                          denom-B (- (+ n-b a0) yB aw)]
                      ;; Skip degenerate tokens where the corpus + prior
                      ;; is entirely this token (no "everything else" to
                      ;; compare against). Their log-odds is undefined.
                      (when (and (pos? denom-A) (pos? denom-B))
                        (let [ωA (/ (+ yA aw) denom-A)
                              ωB (/ (+ yB aw) denom-B)
                              δ  (- (Math/log (double ωA)) (Math/log (double ωB)))
                              σ² (+ (/ 1.0 (+ yA aw)) (/ 1.0 (+ yB aw)))]
                          [w (/ δ (Math/sqrt σ²))]))))))
          prior)))

;; ── Kneser-Ney n-gram language model ──────────────────────────────
;; Interpolated KN — Kneser & Ney 1995.
;; Modified KN     — Chen & Goodman 1999 (three discounts D1, D2, D3+).

(defn- count-ngrams
  "{n {ngram count}} for n in 1..order across all sequences."
  [sequences order]
  (reduce (fn [acc s]
            (reduce (fn [acc n]
                      (reduce (fn [acc ng]
                                (update-in acc [n (vec ng)] (fnil inc 0)))
                              acc
                              (partition n 1 s)))
                    acc (range 1 (inc order))))
          {} sequences))

(defn- continuation-counts
  "cont[n][ng] = N₊(•, ng) — # distinct preceding tokens. n in 1..order-1."
  [raw order]
  (into {}
        (for [n (range 1 order)]
          [n (->> (keys (raw (inc n)))
                  (group-by #(vec (rest %)))
                  (into {} (map (fn [[suf preds]] [suf (count preds)]))))])))

(defn- joint-totals
  "{prefix sum} for a {ngram count} table — sums by prefix-of-length n-1.
   At level k=order this gives 'how many bigrams start with this prefix'
   (NOT the same as the (k-1)-gram unigram count, which over-counts by
   the number of times the prefix appears at the end of a sentence)."
  [table]
  (reduce (fn [acc [ng c]]
            (update acc (vec (butlast ng)) (fnil + 0) c))
          {} table))

(defn- bucket-counts-at
  "{prefix [n1 n2 n3+]} — at the given count table, for each prefix-of-length
   |ngram|-1, count how many completions fall in each count bucket."
  [table]
  (reduce (fn [acc [ng c]]
            (let [h (vec (butlast ng))
                  b (cond (= c 1) 0  (= c 2) 1  :else 2)]
              (update acc h #(update (or % [0 0 0]) b inc))))
          {} table))

(defn- estimate-discounts-mkn
  "Modified-KN [D1 D2 D3+] — Chen & Goodman 1999 eq. (26).
   Falls back to lower-order discount when count-of-counts is sparse."
  [table]
  (let [cs (vals table)
        n1 (count (filter #(= 1 %) cs))
        n2 (count (filter #(= 2 %) cs))
        n3 (count (filter #(= 3 %) cs))
        n4 (count (filter #(= 4 %) cs))]
    (if (and (pos? n1) (pos? n2))
      (let [Y  (/ (double n1) (+ n1 (* 2.0 n2)))
            D1 (max 0.0 (- 1.0 (* 2.0 Y (/ (double n2) n1))))
            D2 (if (pos? n3) (max 0.0 (- 2.0 (* 3.0 Y (/ (double n3) n2)))) D1)
            D3 (if (pos? n4) (max 0.0 (- 3.0 (* 4.0 Y (/ (double n4) n3)))) D2)]
        [D1 D2 D3])
      [0.5 0.5 0.5])))

(defn- estimate-discount-ikn
  "Single discount D = n₁/(n₁+2n₂) — Ney/Essen/Kneser 1994."
  [table]
  (let [cs (vals table)
        n1 (count (filter #(= 1 %) cs))
        n2 (count (filter #(= 2 %) cs))]
    (if (pos? (+ n1 (* 2 n2)))
      (/ (double n1) (+ n1 (* 2.0 n2)))
      0.5)))

(defn fit-kn
  "Fit a Kneser-Ney n-gram language model.
   sequences : seq of seqs of hashable tokens (sentences).
   :order    — n-gram order (default 3).
   :variant  — :interpolated (single D, default) or :modified (D1, D2, D3+)."
  [sequences {:keys [order variant] :or {order 3 variant :interpolated}}]
  (let [raw       (count-ngrams sequences order)
        cont      (continuation-counts raw order)
        table-at  (fn [k] (if (= k order) (raw k) (cont k)))
        totals    (into {} (for [k (range 1 (inc order))
                                 :let [t (table-at k)]
                                 :when t]
                             [k (joint-totals t)]))
        discounts (into {} (for [k (range 1 (inc order))
                                 :let [t (table-at k)]
                                 :when t]
                             [k (case variant
                                  :modified
                                  (estimate-discounts-mkn t)
                                  :interpolated
                                  (let [d (estimate-discount-ikn t)]
                                    [d d d]))]))
        buckets   (into {} (for [k (range 2 (inc order))
                                 :let [t (table-at k)]
                                 :when t]
                             [k (bucket-counts-at t)]))]
    {:order     order
     :variant   variant
     :raw       raw
     :cont      cont
     :totals    totals
     :discounts discounts
     :buckets   buckets}))

(defn- discount-for [Ds c]
  (cond (zero? c) 0.0
        (= c 1)   (Ds 0)
        (= c 2)   (Ds 1)
        :else     (Ds 2)))

(defn- kn-prob*
  "Recursive scorer. k = length of (h, w) at this level; |h| = k − 1.
   At level k = order: uses raw counts. At k < order: uses cont counts.
   At k = 1: continuation distribution, no discount."
  [model h w k]
  (let [order (:order model)]
    (if (= k 1)
      (let [n+w (get-in model [:cont 1 [w]] 0)
            n++ (get-in model [:totals 1 []] 0)]
        (if (zero? n++) 0.0 (/ (double n+w) n++)))
      (let [is-top (= k order)
            joint  (if is-top (get-in model [:raw k])
                              (get-in model [:cont k]))
            denom  (get-in model [:totals k])
            c-hw   (get joint (conj h w) 0)
            c-h    (get denom h 0)]
        (if (zero? c-h)
          (recur model (vec (rest h)) w (dec k))
          (let [Ds         (get-in model [:discounts k])
                D-hw       (discount-for Ds c-hw)
                [n1 n2 n3] (get-in model [:buckets k h] [0 0 0])
                gamma      (/ (+ (* (Ds 0) n1) (* (Ds 1) n2) (* (Ds 2) n3))
                              (double c-h))]
            (+ (max (/ (- c-hw D-hw) (double c-h)) 0.0)
               (* gamma (kn-prob* model (vec (rest h)) w (dec k))))))))))

(defn kn-prob
  "P(w | context). context is a seq of preceding tokens; uses up to (order-1)."
  [model context w]
  (let [order (:order model)
        h     (vec (take-last (dec order) context))]
    (kn-prob* model h w (inc (count h)))))

(defn kn-perplexity
  "Per-token perplexity over held-out sequences (using full-order n-grams)."
  [model sequences]
  (let [order (:order model)
        items (mapcat (fn [s] (partition order 1 s)) sequences)
        [ll n] (reduce (fn [[ll n] ng]
                         (let [p (kn-prob model (butlast ng) (last ng))]
                           [(+ ll (Math/log (max p 1e-300))) (inc n)]))
                       [0.0 0] items)]
    (if (zero? n) Double/POSITIVE_INFINITY
        (Math/exp (/ (- ll) n)))))

;; ── synthetic example ─────────────────────────────────────────────
;; Two small "domain" corpora to exercise everything end-to-end.
;; Sentences are token vectors so the "any-hashable-token" abstraction
;; is visible. Run forms in the comment block at the REPL.

(comment
  (def ml
    [["a" "neural" "network" "learns" "from" "training" "data"]
     ["the" "loss" "function" "guides" "gradient" "descent"]
     ["machine" "learning" "models" "need" "training" "data"]
     ["a" "deep" "neural" "network" "has" "many" "layers"]
     ["machine" "learning" "is" "the" "future"]
     ["the" "model" "minimizes" "the" "loss" "function"]
     ["gradient" "descent" "updates" "the" "weights"]
     ["training" "data" "shapes" "the" "model"]
     ["a" "neural" "network" "can" "approximate" "functions"]
     ["the" "team" "studies" "machine" "learning"]
     ["loss" "function" "values" "decrease" "during" "training"]
     ["the" "neural" "network" "predicts" "from" "the" "data"]])

  (def cooking
    [["heat" "olive" "oil" "in" "a" "frying" "pan"]
     ["preheat" "the" "oven" "to" "two" "hundred" "degrees"]
     ["add" "salt" "and" "pepper" "to" "taste"]
     ["stir" "gently" "for" "five" "minutes"]
     ["fry" "the" "onions" "in" "olive" "oil"]
     ["preheat" "the" "oven" "before" "you" "begin"]
     ["a" "frying" "pan" "needs" "olive" "oil"]
     ["salt" "and" "pepper" "are" "essential"]
     ["stir" "gently" "to" "avoid" "splashing"]
     ["the" "frying" "pan" "is" "hot" "now"]
     ["bake" "for" "ten" "minutes" "in" "the" "oven"]
     ["add" "olive" "oil" "to" "the" "frying" "pan"]])

  ;; (1) Bigram LLR on the combined corpus — domain phrases at the top.
  (def all-tokens (vec (mapcat identity (concat ml cooking))))

  (->> (bigram-llr all-tokens :min-joint 2) (take 10) (map #(select-keys % [:pair :joint :llr])))
  ;; ⇒ top results include [neural network] [olive oil] [frying pan]
  ;;   [machine learning] [training data] [loss function]
  ;;   [stir gently] [salt and] [and pepper] [gradient descent]

  ;; (2) Quasthoff-Wolff Poisson — same shape, similar ranking.
  (->> (bigram-poisson all-tokens :min-joint 2) (take 10) (map #(select-keys % [:pair :joint :poisson])))

  ;; (3) Weighted log-odds: ML vs cooking.
  (def ml-counts      (frequencies (mapcat identity ml)))
  (def cooking-counts (frequencies (mapcat identity cooking)))
  (def prior          (merge-with + ml-counts cooking-counts))

  (def z (weighted-log-odds-dirichlet ml-counts cooking-counts prior))
  ;; Most negative z → distinctive of cooking (oil, frying, pan, oven, salt, …).
  (->> z (sort-by val)         (take 8))
  ;; Most positive z → distinctive of ML (neural, network, training, …).
  (->> z (sort-by val >)       (take 8))

  ;; (4) Kneser-Ney — fit and query.
  (def m-mkn (fit-kn (concat ml cooking) {:order 3 :variant :modified}))
  (def m-ikn (fit-kn (concat ml cooking) {:order 3 :variant :interpolated}))

  (kn-prob m-mkn ["neural"] "network")     ; ≈ high — frequent collocate
  (kn-prob m-mkn ["olive"]  "oil")         ; ≈ high
  (kn-prob m-mkn ["neural"] "oil")         ; ≈ low — backs off
  (kn-prob m-mkn ["banana" "split"] "the") ; backs off all the way to unigram

  ;; In-corpus perplexity — both fit easily; modified is generally lower
  ;; on held-out data with richer count statistics.
  (kn-perplexity m-mkn (concat ml cooking))
  (kn-perplexity m-ikn (concat ml cooking))
  )
