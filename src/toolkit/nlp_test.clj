(ns toolkit.nlp-test
  "Tests for toolkit.nlp — math edge cases and property-based invariants."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.nlp :as nlp]))

;; ── Section 1: primitive math edge cases ──────────────────────────

(deftest g2-zero-cells
  (testing "any single zero cell — xlogx-y handles 0 · log 0 → 0"
    (is (>= (nlp/g2 0 5 5 5) 0))
    (is (>= (nlp/g2 5 0 5 5) 0))
    (is (>= (nlp/g2 5 5 0 5) 0))
    (is (>= (nlp/g2 5 5 5 0) 0)))
  (testing "equal cells = independence ⇒ exactly 0"
    (is (< (Math/abs (nlp/g2 50 50 50 50)) 1e-9)))
  (testing "perfect dependence ⇒ large G²"
    (is (> (nlp/g2 100 0 0 100) 100))))

(deftest log-factorial-boundary
  ;; Stirling kicks in at n ≥ 50; verify continuity across the cutover.
  (let [lf #'toolkit.nlp/log-factorial]
    (is (= 0.0 (lf 0)))
    (is (= 0.0 (lf 1)))
    ;; log(50!) = log(49!) + log(50). Stirling is approximate; allow 1e-3.
    (is (< (Math/abs (- (lf 50) (lf 49) (Math/log 50))) 1e-3))))

(deftest poisson-surprise-edge-cases
  (testing "k = 0 ⇒ surprise = λ"
    (is (< (Math/abs (- (nlp/poisson-surprise 0 5.0) 5.0)) 1e-9)))
  (testing "k near λ ⇒ low surprise (Poisson mode)"
    (is (< (nlp/poisson-surprise 5 5.0) 2.0)))
  (testing "k far above λ ⇒ huge surprise"
    (is (> (nlp/poisson-surprise 100 1.0) 100))))

;; ── Section 2: sequence / corpus edge cases ───────────────────────

(deftest bigram-counts-degenerate
  (testing "empty seq"
    (let [bc (nlp/bigram-counts [])]
      (is (= 0 (:n bc)))
      (is (= {} (:joint bc)))))
  (testing "one token has no bigrams"
    (is (= 0 (:n (nlp/bigram-counts ["a"])))))
  (testing "marginals sum to n"
    (let [{:keys [n left right]} (nlp/bigram-counts ["a" "b" "a" "b" "c"])]
      (is (= n (reduce + (vals left))))
      (is (= n (reduce + (vals right)))))))

(deftest bigram-llr-min-joint-filters
  (is (empty? (nlp/bigram-llr ["a" "b" "a" "b"] :min-joint 100))))

(deftest weighted-log-odds-skips-no-prior-tokens
  (let [z (nlp/weighted-log-odds-dirichlet {:a 5} {:b 5} {})]
    (is (= {} z))))

(deftest weighted-log-odds-identical-corpora
  (let [c {:a 10 :b 20 :c 5}
        p {:a 100 :b 200 :c 50}
        z (nlp/weighted-log-odds-dirichlet c c p)]
    (is (every? #(< (Math/abs %) 1e-9) (vals z)))))

(deftest kn-prob-unseen-token
  (let [m (nlp/fit-kn [["a" "b" "a" "b"]] {:order 2})]
    (is (= 0.0 (nlp/kn-prob m ["a"] "z")))))

(deftest kn-prob-unseen-context-backs-off
  (let [m (nlp/fit-kn [["a" "b" "c" "a" "b" "c"]] {:order 3})]
    (is (pos? (nlp/kn-prob m ["x" "y"] "a")))))

(deftest kn-perplexity-empty
  (let [m (nlp/fit-kn [["a" "b" "a" "b"]] {:order 2})]
    (is (= Double/POSITIVE_INFINITY (nlp/kn-perplexity m [])))))

;; ── Section 3: property tests ─────────────────────────────────────

(def gen-cell (gen/choose 1 200))

(defspec g2-non-negative 100
  (prop/for-all [a gen-cell b gen-cell c gen-cell d gen-cell]
    (>= (nlp/g2 a b c d) -1e-12)))

(defspec g2-scales-linearly 50
  ;; G² = 2 Σ O log(O/E). Doubling all cells doubles all O while keeping
  ;; O/E fixed (E = row·col/n scales the same way), so G² doubles.
  (prop/for-all [a gen-cell b gen-cell c gen-cell d gen-cell]
    (let [g  (nlp/g2 a b c d)
          g' (nlp/g2 (* 2 a) (* 2 b) (* 2 c) (* 2 d))]
      (or (< g 1e-9)
          (< (Math/abs (- g' (* 2 g))) 1e-6)))))

(defspec g2-symmetric-under-permutations 50
  (prop/for-all [a gen-cell b gen-cell c gen-cell d gen-cell]
    (let [g (nlp/g2 a b c d)]
      (and (< (Math/abs (- g (nlp/g2 b a d c))) 1e-9)
           (< (Math/abs (- g (nlp/g2 c d a b))) 1e-9)
           (< (Math/abs (- g (nlp/g2 a c b d))) 1e-9)))))

(defspec g2-zero-when-rows-proportional 30
  (prop/for-all [k1 (gen/choose 1 50) k2 (gen/choose 1 50) m (gen/choose 1 10)]
    (< (nlp/g2 k1 k2 (* k1 m) (* k2 m)) 1e-9)))

(def gen-pos-double
  (gen/double* {:min 0.01 :max 200.0 :NaN? false :infinite? false}))

(defspec poisson-surprise-non-negative 100
  (prop/for-all [k (gen/choose 0 200) lambda gen-pos-double]
    (>= (nlp/poisson-surprise k lambda) -1e-9)))

(defspec poisson-surprise-k-zero-equals-lambda 100
  (prop/for-all [lambda gen-pos-double]
    (< (Math/abs (- (nlp/poisson-surprise 0 lambda) lambda)) 1e-9)))

(def gen-token (gen/elements [:a :b :c :d :e]))
(def gen-tokens (gen/vector gen-token 0 30))

(defspec bigram-counts-marginals-sum-to-n 100
  (prop/for-all [tokens gen-tokens]
    (let [{:keys [n left right joint]} (nlp/bigram-counts tokens)]
      (and (= n (reduce + 0 (vals joint)))
           (= n (reduce + 0 (vals left)))
           (= n (reduce + 0 (vals right)))))))

(def gen-counts
  (gen/map gen-token (gen/choose 1 50) {:min-elements 1 :max-elements 5}))

(defspec wlo-antisymmetric 50
  (prop/for-all [a gen-counts b gen-counts]
    (let [prior (merge-with + a b)
          zAB   (nlp/weighted-log-odds-dirichlet a b prior)
          zBA   (nlp/weighted-log-odds-dirichlet b a prior)]
      (every? (fn [w]
                (< (Math/abs (+ (zAB w 0.0) (zBA w 0.0))) 1e-9))
              (concat (keys zAB) (keys zBA))))))

(def gen-corpus (gen/vector (gen/vector gen-token 2 8) 1 8))

(defn- kn-vocab [model]
  ;; raw[1] keys are length-1 vectors like [:a]; unwrap to the bare tokens.
  (map first (keys (get-in model [:raw 1]))))

(defn- kn-vocab-sum [model context]
  (reduce + (map #(nlp/kn-prob model context %) (kn-vocab model))))

(defspec kn-sums-to-one-modified 30
  (prop/for-all [corpus gen-corpus]
    (let [m     (nlp/fit-kn corpus {:order 2 :variant :modified})
          vocab (kn-vocab m)]
      (or (empty? vocab)
          (< (Math/abs (- 1.0 (kn-vocab-sum m [(first vocab)]))) 1e-6)))))

(defspec kn-sums-to-one-interpolated 30
  (prop/for-all [corpus gen-corpus]
    (let [m     (nlp/fit-kn corpus {:order 2 :variant :interpolated})
          vocab (kn-vocab m)]
      (or (empty? vocab)
          (< (Math/abs (- 1.0 (kn-vocab-sum m [(first vocab)]))) 1e-6)))))

(defspec kn-prob-bounded-0-to-1 50
  (prop/for-all [corpus gen-corpus w gen-token]
    (let [m     (nlp/fit-kn corpus {:order 2})
          vocab (kn-vocab m)
          p     (nlp/kn-prob m [(or (first vocab) w)] w)]
      (and (>= p -1e-12) (<= p (+ 1.0 1e-9))))))
