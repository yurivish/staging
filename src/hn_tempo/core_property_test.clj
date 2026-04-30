(ns hn-tempo.core-property-test
  "Property tests for hn-tempo's three analyzers."
  (:require [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hn-tempo.core :as core]))

(defn- gen-comment [depth-budget]
  (if (zero? depth-budget)
    (gen/let [id   gen/large-integer
              by   gen/string-alphanumeric
              t    (gen/choose 1700000000 1700100000)]
      {:id id :by by :time t :kid-trees []})
    (gen/let [n-kids (gen/choose 0 3)
              kids   (gen/vector (gen-comment (dec depth-budget)) n-kids)
              id     gen/large-integer
              by     gen/string-alphanumeric
              t      (gen/choose 1700000000 1700100000)]
      {:id id :by by :time t :kid-trees (vec kids)})))

(def ^:private gen-tree
  (gen/let [n-top (gen/choose 0 5)
            kids  (gen/vector (gen-comment 3) n-top)]
    {:id 1 :type "story" :time 1700000000 :title "x" :url "u"
     :kid-trees (vec kids)}))

(defspec flow-counts-add-up 50
  (prop/for-all [t gen-tree]
    (let [r (#'core/flow-analysis t)]
      (= (count (rest (#'core/nodes t)))
         (+ (:n_top_level r) (:n_deeper r))))))

(defspec peak-6h-fraction-bounded 50
  (prop/for-all [t gen-tree]
    (let [r (#'core/tempo-analysis t)]
      (or (nil? (:peak_6h_fraction r))
          (<= 0.0 (:peak_6h_fraction r) 1.0)))))

(defspec peak-5min-no-larger-than-n 50
  (prop/for-all [t gen-tree]
    (let [r (#'core/burst-analysis t)
          n (:n_comments r)
          p (:peak_5min_count r)]
      (or (nil? p) (<= p n)))))

(defspec cv-non-negative-when-defined 50
  (prop/for-all [t gen-tree]
    (let [r (#'core/burst-analysis t)
          c (:coefficient_of_variation r)]
      (or (nil? c) (>= c 0)))))

(defspec max-thread-depth-positive-when-any-kids 50
  (prop/for-all [t gen-tree]
    (let [r (#'core/flow-analysis t)]
      (if (zero? (:n_top_level r))
        (= 0 (:max_thread_depth r))
        (>= (:max_thread_depth r) 1)))))
