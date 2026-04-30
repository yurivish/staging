(ns hn-shape.core-property-test
  "Property tests for hn-shape pure-data functions over generated trees."
  (:require [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hn-shape.core :as core]))

;; A node generator with bounded depth and width. We enforce monotonic
;; timestamps in the comment ancestry so first/percentile/time-span aren't
;; nonsensical, but kept simple — each node gets a random delta from its
;; parent.
(defn- node-with-children [depth-budget]
  (if (zero? depth-budget)
    (gen/fmap (fn [[id by]]
                {:id id :by by :time 0 :kid-trees []})
              (gen/tuple gen/large-integer gen/string-alphanumeric))
    (gen/let [n-kids (gen/choose 0 4)
              kids   (gen/vector (node-with-children (dec depth-budget)) n-kids)
              id     gen/large-integer
              by     gen/string-alphanumeric]
      {:id id :by by :time 0 :kid-trees (vec kids)})))

(def ^:private gen-tree
  (gen/fmap (fn [t]
              (-> t
                  (assoc :title "x" :url "u" :type "story" :time 1000)))
            (node-with-children 4)))

(defspec nodes-count-matches-recursive 50
  (prop/for-all [t gen-tree]
    (= (count (#'core/nodes t))
       (+ 1 (apply + (map (comp count #'core/nodes) (:kid-trees t)))))))

(defspec max-depth-non-negative 50
  (prop/for-all [t gen-tree]
    (>= (#'core/max-depth t) 0)))

(defspec n-comments-equals-non-root-nodes 50
  (prop/for-all [t gen-tree]
    (= (:n_comments (#'core/shape-row t))
       (count (rest (#'core/nodes t))))))

(defspec widest-branch-bounded-by-n-comments 50
  (prop/for-all [t gen-tree]
    (let [r (#'core/shape-row t)
          n (:n_comments r)
          w (:widest_branch_size r)]
      (or (nil? w) (<= w n)))))

(defspec branching-factor-defined-iff-some-non-leaf 50
  (prop/for-all [t gen-tree]
    (let [r        (#'core/shape-row t)
          any-kid? (some #(seq (:kid-trees %)) (#'core/nodes t))]
      (= (some? (:branching_factor_mean r))
         (boolean any-kid?)))))

(defspec percentile-is-an-element-of-input 50
  (prop/for-all [xs (gen/such-that seq (gen/vector gen/large-integer) 100)
                 p  (gen/choose 0 100)]
    (let [pp (/ p 100.0)]
      (some #{(#'core/percentile xs pp)} xs))))
