(ns toolkit.sublist-property-test
  "Property-based tests for toolkit.sublist: a naive linear matcher acts
   as an oracle, and the trie's match result is asserted equal on random
   (subs, subject) inputs. A round-trip property checks that insert-then-
   remove returns to the empty-node structure."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.exhaustigen :as ex]
            [toolkit.stree :as stree]
            [toolkit.sublist :as sl]))

;; --- generators ---

(def gen-literal
  ;; 1-4 ascii letters. Disallows the wildcard chars and `.`.
  (gen/fmap str/join (gen/vector gen/char-alpha 1 4)))

(def gen-pattern-token
  (gen/frequency [[4 gen-literal] [1 (gen/return "*")]]))

(def gen-pattern
  ;; 1-4 tokens; 1-in-4 chance of a trailing `>`.
  (gen/let [toks     (gen/vector gen-pattern-token 1 4)
            trailing (gen/frequency [[3 (gen/return nil)] [1 (gen/return ">")]])]
    (str/join "." (if trailing (conj toks trailing) toks))))

(def gen-subject
  (gen/fmap #(str/join "." %) (gen/vector gen-literal 1 5)))

(def gen-queue
  (gen/frequency [[3 (gen/return nil)] [1 gen-literal]]))

(def gen-sub
  (gen/tuple gen-pattern gen/small-integer gen-queue))

(def gen-subs (gen/vector gen-sub 0 20))

;; --- naive matcher (oracle) ---

(defn match-tokens? [pat subj]
  (cond
    (and (empty? pat) (empty? subj))  true
    (= ">" (first pat))               (boolean (seq subj))
    (or (empty? pat) (empty? subj))   false
    (= "*" (first pat))               (recur (rest pat) (rest subj))
    (= (first pat) (first subj))      (recur (rest pat) (rest subj))
    :else                             false))

(defn naive-match [subs subject]
  (let [subj-toks (str/split subject #"\.")]
    (reduce (fn [r [pat v q]]
              (if (match-tokens? (str/split pat #"\.") subj-toks)
                (if q
                  (update-in r [:groups q] (fnil conj #{}) v)
                  (update r :plain conj v))
                r))
            {:plain #{} :groups {}}
            (distinct subs))))

;; --- helpers ---

(defn apply-inserts [sl subs]
  (doseq [[pat v q] subs]
    (sl/insert! sl pat v (when q {:queue q}))))

(defn apply-removes [sl subs]
  (doseq [[pat v q] subs]
    (sl/remove! sl pat v (when q {:queue q}))))

(defn- groups->sets
  "Normalizes a match result's `:groups` values (vectors) to sets so the
   oracle (which uses sets) can compare by value equality."
  [r]
  (update r :groups update-vals set))

;; Literal-subject subs for `reverse-match`, whose primary use case (per
;; the Go docstring) is a sublist of literals queried wildcard-style.
;; We don't characterize the wildcard-sub case here.
(def gen-literal-subs
  (gen/vector (gen/tuple gen-subject gen/small-integer gen-queue) 0 20))

(defn naive-reverse-match [subs query]
  (reduce (fn [r [pat v q]]
            (if (sl/subject-matches-filter? pat query)
              (if q
                (update-in r [:groups q] (fnil conj #{}) v)
                (update r :plain conj v))
              r))
          {:plain #{} :groups {}}
          (distinct subs)))

;; --- properties ---

(defspec match-equals-oracle 200
  (prop/for-all [subs    gen-subs
                 subject gen-subject]
                (let [sl (sl/make)]
                  (apply-inserts sl subs)
                  (= (groups->sets (sl/match sl subject))
                     (naive-match subs subject)))))

(defspec round-trip-to-empty 100
  (prop/for-all [subs gen-subs]
                (let [sl    (sl/make)
                      fresh @(sl/make)]
                  (apply-inserts sl subs)
                  (apply-removes sl (shuffle subs))
                  (= fresh @sl))))

(defspec reverse-match-vs-oracle 200
  (prop/for-all [subs  gen-literal-subs
                 query gen-pattern]
                (let [sl (sl/make)]
                  (apply-inserts sl subs)
                  (= (groups->sets (sl/reverse-match sl query))
                     (naive-reverse-match subs query)))))

(defspec subjects-collide-symmetric 200
  (prop/for-all [a gen-pattern
                 b gen-pattern]
                (= (sl/subjects-collide? a b)
                   (sl/subjects-collide? b a))))

(defspec subject-matches-filter-vs-match-tokens 200
  (prop/for-all [lit gen-subject
                 pat gen-pattern]
                ;; When the first arg is a literal, `subject-matches-filter?`
                ;; reduces to "does `pat` (as a pattern) match `lit`".
                (= (sl/subject-matches-filter? lit pat)
                   (match-tokens? (str/split pat #"\.") (str/split lit #"\.")))))

(defspec subjects-collide-literal-case 200
  (prop/for-all [lit gen-subject
                 pat gen-pattern]
                ;; For a literal `lit` the only subject matched by `lit` is
                ;; itself, so `lit` collides with `pat` iff `pat` matches `lit`.
                (= (sl/subjects-collide? lit pat)
                   (sl/subject-matches-filter? lit pat))))

;; --- intersect-stree ---

(def gen-stree-entry
  (gen/tuple gen-subject gen/small-integer))

(def gen-stree-entries
  (gen/fmap distinct (gen/vector gen-stree-entry 0 15)))

(defspec intersect-stree-vs-oracle 200
  (prop/for-all [entries gen-stree-entries
                 subs    gen-subs]
                (let [st   (stree/make)
                      sl   (sl/make)
                      !got (atom #{})]
                  (doseq [[subj v] entries] (stree/insert! st subj v))
                  (apply-inserts sl subs)
                  (sl/intersect-stree st sl (fn [subj _] (swap! !got conj subj)))
                  (let [expected (into #{}
                                       (comp (map first)
                                             (filter #(sl/has-interest? sl %)))
                                       entries)]
                    (= expected @!got)))))

;; --- exhaustigen-driven tests ---
;;
;; Enumerate small combinatorial spaces deterministically. These cover
;; the narrow-alphabet shapes that the defspec generators can skip past
;; on any given run.

(defn- token-tuples
  "All length-n tuples of items from `xs` (cartesian product)."
  [xs n]
  (if (zero? n)
    [[]]
    (vec (for [t (token-tuples xs (dec n))
               x xs]
           (conj t x)))))

(defn- build-patterns
  "All pattern strings of lengths `lens` over `pat-alpha`, optionally
   with a trailing `>` token. `pat-alpha` should be literals plus \"*\"."
  [pat-alpha lens]
  (vec (for [n        lens
             toks     (token-tuples pat-alpha n)
             trailing [nil ">"]]
         (let [base (str/join "." toks)]
           (if trailing (str base "." trailing) base)))))

(defn- build-subjects
  [alpha lens]
  (vec (for [n    lens
             toks (token-tuples alpha n)]
         (str/join "." toks))))

;; Curated 8-pattern set spanning literal, pwc, fwc and mixed shapes —
;; small enough that 2^8 subsets × 6 subjects = 1536 cases is cheap.
(def ^:private tiny-sub-patterns
  ["a" "b" "*" ">" "a.b" "a.*" "*.b" "a.>"])

(def ^:private tiny-query-subjects
  (build-subjects ["a" "b"] [1 2]))

(deftest match-equals-oracle-exhaustive
  ;; 2^8 × 6 = 1536 deterministic cases. Covers single-sub trees,
  ;; two-token collisions, fwc-only interest, and wildcard-only trees.
  (let [g     (ex/make)
        cases (atom 0)
        fail  (atom nil)]
    (loop []
      (when (and (not @fail) (not (ex/done!? g)))
        (let [pat-idxs (ex/subset g (count tiny-sub-patterns))
              subj-idx (ex/pick g (count tiny-query-subjects))
              subs     (mapv (fn [i] [(nth tiny-sub-patterns i) (inc i) nil]) pat-idxs)
              subject  (nth tiny-query-subjects subj-idx)
              sl       (sl/make)]
          (apply-inserts sl subs)
          (let [expected (naive-match subs subject)
                got      (groups->sets (sl/match sl subject))]
            (when (not= expected got)
              (reset! fail {:subs subs :subject subject
                            :expected expected :got got})))
          (swap! cases inc))
        (recur)))
    (is (nil? @fail) (str "mismatch: " @fail))
    (when-not @fail
      (is (= (* (bit-shift-left 1 (count tiny-sub-patterns))
                (count tiny-query-subjects))
             @cases)))))

;; Pattern universe for pure-function property tests. With {a,b,*}
;; token-alphabet over 1–3 token lengths plus optional trailing ">",
;; there are 4 + 12 + 36 = 52 patterns and 52^2 = 2704 ordered pairs.
(def ^:private all-patterns
  (build-patterns ["a" "b" "*"] [1 2 3]))

(def ^:private all-subjects
  (build-subjects ["a" "b"] [1 2 3]))

(defn- pattern-pair-loop
  "Exhaustively runs `f` over every ordered pair of patterns. Returns
   nil on pass or a failing [a b extra] vector."
  [patterns f]
  (let [n    (count patterns)
        g    (ex/make)
        fail (atom nil)]
    (loop []
      (when (and (not @fail) (not (ex/done!? g)))
        (let [a (nth patterns (ex/pick g n))
              b (nth patterns (ex/pick g n))]
          (when-let [info (f a b)]
            (reset! fail info)))
        (recur)))
    @fail))

(deftest subjects-collide-symmetric-exhaustive
  ;; Exhaustive proof of commutativity over the 52-pattern universe.
  (let [fail (pattern-pair-loop all-patterns
                                (fn [a b]
                                  (when (not= (sl/subjects-collide? a b)
                                              (sl/subjects-collide? b a))
                                    {:a a :b b
                                     :ab (sl/subjects-collide? a b)
                                     :ba (sl/subjects-collide? b a)})))]
    (is (nil? fail) (str "asymmetric collision: " fail))))

(deftest subjects-collide-literal-case-exhaustive
  ;; For a literal subject `lit` and any pattern `pat`,
  ;; `subjects-collide?` reduces to `subject-matches-filter?`.
  (let [g    (ex/make)
        fail (atom nil)]
    (loop []
      (when (and (not @fail) (not (ex/done!? g)))
        (let [lit (nth all-subjects (ex/pick g (count all-subjects)))
              pat (nth all-patterns (ex/pick g (count all-patterns)))]
          (when (not= (sl/subjects-collide? lit pat)
                      (sl/subject-matches-filter? lit pat))
            (reset! fail {:lit lit :pat pat})))
        (recur)))
    (is (nil? @fail) (str "collide≠matches-filter for literal: " @fail))))

(deftest subject-matches-filter-vs-match-tokens-exhaustive
  ;; `subject-matches-filter?` on a literal subject must agree with the
  ;; naive oracle token matcher.
  (let [g    (ex/make)
        fail (atom nil)]
    (loop []
      (when (and (not @fail) (not (ex/done!? g)))
        (let [lit (nth all-subjects (ex/pick g (count all-subjects)))
              pat (nth all-patterns (ex/pick g (count all-patterns)))]
          (when (not= (sl/subject-matches-filter? lit pat)
                      (match-tokens? (str/split pat #"\.")
                                     (str/split lit #"\.")))
            (reset! fail {:lit lit :pat pat})))
        (recur)))
    (is (nil? @fail) (str "matches-filter≠oracle for literal: " @fail))))
