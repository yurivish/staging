(ns hn-drift.core-property-test
  (:require [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hn-drift.core :as core]))

(defspec clip-identity-when-short-enough 100
  (prop/for-all [s gen/string-alphanumeric
                 n (gen/choose 1 200)]
    (if (<= (count s) n)
      (= s (#'core/clip s n))
      true)))

(defspec clip-bounded-length 100
  (prop/for-all [s gen/string-alphanumeric
                 n (gen/choose 1 50)]
    (let [c (#'core/clip s n)]
      ;; ellipsis adds one Unicode codepoint, so worst case is n+1 chars
      (<= (count c) (inc n)))))

(defn- gen-node [depth-budget]
  (if (zero? depth-budget)
    (gen/let [id  gen/large-integer
              txt gen/string-alphanumeric]
      {:id id :time 1100 :by "u" :text txt :kid-trees []})
    (gen/let [n-kids (gen/choose 0 3)
              kids   (gen/vector (gen-node (dec depth-budget)) n-kids)
              id     gen/large-integer
              txt    gen/string-alphanumeric]
      {:id id :time 1100 :by "u" :text txt :kid-trees (vec kids)})))

(def ^:private gen-tree
  (gen/let [n-top (gen/choose 0 5)
            kids  (gen/vector (gen-node 3) n-top)]
    {:id 1 :type "story" :title "T" :url "U" :time 1000
     :kid-trees (vec kids)}))

(defspec top-comments-bounded-by-k 50
  (prop/for-all [t gen-tree
                 k (gen/choose 0 10)]
    (<= (count (#'core/top-comments t k)) k)))

(defspec subtree-size-equals-recursive-count 50
  (prop/for-all [t gen-tree]
    (= (#'core/subtree-size t)
       (+ 1 (apply + 0 (map #'core/subtree-size (:kid-trees t)))))))
