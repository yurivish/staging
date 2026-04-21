(ns toolkit.stree-property-test
  "Property-based tests for toolkit.stree. A naive token-level matcher
   serves as an oracle against the trie's `match`. Round-trip and
   lookup-preservation properties cover insert/delete invariants."
  (:require [clojure.string :as str]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
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
