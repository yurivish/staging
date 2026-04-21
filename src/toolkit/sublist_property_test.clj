(ns toolkit.sublist-property-test
  "Property-based tests for toolkit.sublist: a naive linear matcher acts
   as an oracle, and the trie's match result is asserted equal on random
   (subs, subject) inputs. A round-trip property checks that insert-then-
   remove returns to the empty-node structure."
  (:require [clojure.string :as str]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
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
