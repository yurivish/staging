(ns toolkit.stree-property-test
  "Property-based tests for toolkit.stree. A naive token-level matcher
   serves as an oracle against the trie's `match`. Round-trip and
   lookup-preservation properties cover insert/delete invariants."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.exhaustigen :as ex]
            [toolkit.stree :as st]))

;; --- generators ---

(def gen-literal
  ;; 1-4 ascii letters. No wildcards, no tseps, no DEL.
  (gen/fmap str/join (gen/vector gen/char-alpha 1 4)))

(def gen-subject
  (gen/fmap #(str/join "." %) (gen/vector gen-literal 1 5)))

(def gen-subjects
  (gen/fmap distinct (gen/vector gen-subject 0 30)))

(def gen-pattern-token
  (gen/frequency [[4 gen-literal] [1 (gen/return "*")]]))

(def gen-pattern
  (gen/let [toks     (gen/vector gen-pattern-token 1 4)
            trailing (gen/frequency [[3 (gen/return nil)] [1 (gen/return ">")]])]
    (str/join "." (if trailing (conj (vec toks) trailing) toks))))

;; --- oracle: naive token-level matcher ---

(defn- match-tokens? [pat-toks subj-toks]
  (cond
    (and (empty? pat-toks) (empty? subj-toks))   true
    (= ">" (first pat-toks))                     (boolean (seq subj-toks))
    (or (empty? pat-toks) (empty? subj-toks))    false
    (= "*" (first pat-toks))                     (recur (rest pat-toks) (rest subj-toks))
    (= (first pat-toks) (first subj-toks))       (recur (rest pat-toks) (rest subj-toks))
    :else                                        false))

(defn- oracle-matches [subjects filt]
  (let [pat-toks (str/split filt #"\.")]
    (set (filter (fn [s] (match-tokens? pat-toks (str/split s #"\."))) subjects))))

;; --- helpers ---

(defn- match-set [tree filt]
  (let [acc (atom #{})]
    (st/match tree filt (fn [s _v] (swap! acc conj s)))
    @acc))

(defn- iter-set [tree]
  (let [acc (atom [])]
    (st/iter-ordered tree (fn [s _v] (swap! acc conj s) true))
    @acc))

;; --- properties ---

(defspec lookup-matches-inserted 100
  (prop/for-all [subjects gen-subjects]
                (let [t (st/make)]
                  (doseq [s subjects] (st/insert! t s s))
                  (every? (fn [s] (= [s true] (st/lookup t s))) subjects))))

(defspec iter-ordered-equals-inserted-and-sorted 100
  (prop/for-all [subjects gen-subjects]
                (let [t (st/make)]
                  (doseq [s subjects] (st/insert! t s s))
                  (let [got (iter-set t)]
                    (and (= (set subjects) (set got))
                         (= got (sort got)))))))

(defspec match-equals-oracle 200
  (prop/for-all [subjects gen-subjects
                 filt     gen-pattern]
                (let [t (st/make)]
                  (doseq [s subjects] (st/insert! t s s))
                  (= (oracle-matches subjects filt)
                     (match-set t filt)))))

(defspec round-trip-insert-delete-to-empty 100
  (prop/for-all [subjects gen-subjects]
                (let [t (st/make)]
                  (doseq [s subjects] (st/insert! t s s))
                  (doseq [s (shuffle subjects)] (st/delete! t s))
                  (and (zero? (st/size t))
                       (nil? (:root @t))))))

(defspec delete-preserves-remaining-lookups 100
  (prop/for-all [subjects gen-subjects]
                (let [t       (st/make)
                      _       (doseq [s subjects] (st/insert! t s s))
                      to-del  (set (take (quot (count subjects) 2)
                                         (shuffle subjects)))
                      remain  (remove to-del subjects)]
                  (doseq [s to-del] (st/delete! t s))
                  (and
                    (= (count remain) (st/size t))
                    (every? (fn [s] (= [s true] (st/lookup t s))) remain)
                    (every? (fn [s] (= [nil false] (st/lookup t s))) to-del)))))

(defspec insert-order-does-not-affect-lookups 50
  (prop/for-all [subjects gen-subjects]
                (let [t1 (st/make)
                      t2 (st/make)]
                  (doseq [s subjects]           (st/insert! t1 s s))
                  (doseq [s (shuffle subjects)] (st/insert! t2 s s))
                  (every? (fn [s] (= (st/lookup t1 s) (st/lookup t2 s))) subjects))))

;; --- exhaustigen-driven tests ---
;;
;; These enumerate small combinatorial state spaces deterministically.
;; They complement the sampling defspecs above, which cover larger
;; spaces probabilistically; exhaustigen covers the *small* spaces
;; QuickCheck can skip past.

;; Subject set chosen to exercise the hairy stree shapes: a shared
;; "foo.ba" prefix, two internal subtrees ("bar"/"baz"), three leaves
;; under each, and a "foo.bar" leaf that hangs off the noPivot slot.
;; Mirrors stree-test/nodes-and-paths-prefix-absorption.
(def ^:private ordering-fixture-subjects
  ["foo.bar.A" "foo.bar.B" "foo.bar.C"
   "foo.baz.A" "foo.baz.B" "foo.baz.C"
   "foo.bar"])

(defn- factorial [n] (reduce * 1 (range 1 (inc n))))

(deftest insert-order-invariance-exhaustive
  ;; 7! = 5040 insertion orderings. Every order must produce a tree
  ;; where each subject looks up to its index value.
  (let [subjects ordering-fixture-subjects
        n        (count subjects)
        g        (ex/make)
        cases    (atom 0)
        fail     (atom nil)]
    (loop []
      (when (and (not @fail) (not (ex/done!? g)))
        (let [order (ex/perm g n)
              t     (st/make)]
          (doseq [i order]
            (st/insert! t (nth subjects i) i))
          (doseq [[i s] (map-indexed vector subjects)
                  :while (nil? @fail)]
            (when (not= [i true] (st/lookup t s))
              (reset! fail {:order order :subject s
                            :got   (st/lookup t s)
                            :expected [i true]})))
          (swap! cases inc))
        (recur)))
    (is (nil? @fail) (str "mismatch: " @fail))
    (when-not @fail
      (is (= (factorial n) @cases)))))

(deftest delete-order-collapse-exhaustive
  ;; 7! = 5040 deletion orderings. After inserting the full set in a
  ;; fixed canonical order, every permutation of deletions must leave
  ;; the tree empty with a nil root.
  (let [subjects ordering-fixture-subjects
        n        (count subjects)
        g        (ex/make)
        cases    (atom 0)
        fail     (atom nil)]
    (loop []
      (when (and (not @fail) (not (ex/done!? g)))
        (let [order (ex/perm g n)
              t     (st/make)]
          (doseq [s subjects] (st/insert! t s s))
          (doseq [i order]    (st/delete! t (nth subjects i)))
          (when-not (and (zero? (st/size t)) (nil? (:root @t)))
            (reset! fail {:order order :size (st/size t) :root (:root @t)}))
          (swap! cases inc))
        (recur)))
    (is (nil? @fail) (str "non-empty after delete: " @fail))
    (when-not @fail
      (is (= (factorial n) @cases)))))

;; --- tiny-alphabet exhaustive match-vs-oracle ---
;;
;; Enumerate every (subject-set × pattern) pair over a 2-letter
;; alphabet. The universe is small enough to check in a few thousand
;; cases — deterministic coverage of corner shapes like single-subject
;; trees, two-token collisions, and patterns longer than any subject.

(defn- token-tuples
  "All length-n tuples of items from `xs` (cartesian product). Returns
   a vector of vectors so iteration order is stable."
  [xs n]
  (if (zero? n)
    [[]]
    (vec (for [t  (token-tuples xs (dec n))
               x  xs]
           (conj t x)))))

(defn- build-subject-universe
  "All subjects of lengths `lens` over `alpha`."
  [alpha lens]
  (vec (for [n    lens
             toks (token-tuples alpha n)]
         (str/join "." toks))))

(defn- build-pattern-universe
  "All pattern strings of lengths `lens` over pattern-alphabet
   (typically literals + \"*\"), optionally with a trailing \">\" token."
  [pat-alpha lens]
  (vec (for [n        lens
             toks     (token-tuples pat-alpha n)
             trailing [nil ">"]]
         (let [base (str/join "." toks)]
           (if trailing (str base "." trailing) base)))))

(def ^:private tiny-subjects
  ;; 2 + 4 = 6 subjects over {a,b} with 1 or 2 tokens.
  (build-subject-universe ["a" "b"] [1 2]))

(def ^:private tiny-patterns
  ;; (3 + 9 + 27) * 2 = 78 patterns over {a,b,*} with 1–3 tokens,
  ;; optionally trailing ">". Some won't match any 1–2 token subject,
  ;; which exercises the empty-match path.
  (build-pattern-universe ["a" "b" "*"] [1 2 3]))

(deftest match-equals-oracle-exhaustive
  ;; 2^6 × 78 = 4992 cases. `subset` picks which subjects from the
  ;; universe to insert; `pick` picks one of the patterns.
  (let [g        (ex/make)
        cases    (atom 0)
        fail     (atom nil)]
    (loop []
      (when (and (not @fail) (not (ex/done!? g)))
        (let [subj-idxs (ex/subset g (count tiny-subjects))
              pat-idx   (ex/pick g (count tiny-patterns))
              subjects  (mapv tiny-subjects subj-idxs)
              pat       (nth tiny-patterns pat-idx)
              t         (st/make)]
          (doseq [s subjects] (st/insert! t s s))
          (let [expected (oracle-matches subjects pat)
                got      (match-set t pat)]
            (when (not= expected got)
              (reset! fail {:subjects subjects :pattern pat
                            :expected expected :got got})))
          (swap! cases inc))
        (recur)))
    (is (nil? @fail) (str "mismatch: " @fail))
    (when-not @fail
      (is (= (* (bit-shift-left 1 (count tiny-subjects))
                (count tiny-patterns))
             @cases)))))
