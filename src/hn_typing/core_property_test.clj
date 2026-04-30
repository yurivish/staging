(ns hn-typing.core-property-test
  (:require [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hn-typing.core :as core]))

(def ^:private gen-label
  (gen/one-of
   [(gen/elements core/edge-types)        ; valid
    (gen/return nil)                      ; nil → unclassifiable
    (gen/elements ["bogus" "??" ""])]))   ; invalid → unclassifiable

(def ^:private gen-rows
  (gen/let [labels (gen/vector gen-label 0 30)]
    (mapv (fn [l] {:data {:story-id 1 :title "T" :url "U" :edge_type l}})
          labels)))

(def ^:private edge-keys (set (map keyword core/edge-types)))

(defspec totals-add-up 100
  (prop/for-all [par-msgs gen-rows]
    (let [s (#'core/summarize-story 1 par-msgs)]
      (= (:n_edges_classified s)
         (+ (:n_unclassifiable s) (apply + 0 (vals (:by_type s))))))))

(defspec by-type-keys-are-the-seven-edge-types 100
  (prop/for-all [par-msgs gen-rows]
    (= edge-keys (set (keys (:by_type (#'core/summarize-story 1 par-msgs)))))))

(defspec invalid-and-nil-labels-go-to-unclassifiable 100
  (prop/for-all [par-msgs gen-rows]
    (let [s     (#'core/summarize-story 1 par-msgs)
          rows  (mapv :data par-msgs)
          unbad (count (filter #(not (contains? (set core/edge-types) (:edge_type %)))
                               rows))]
      (= unbad (:n_unclassifiable s)))))

(defspec empty-input-zero-classified 50
  (prop/for-all [_ (gen/return nil)]
    (let [s (#'core/summarize-story 1 [])]
      (and (zero? (:n_edges_classified s))
           (zero? (:n_unclassifiable s))
           (every? zero? (vals (:by_type s)))))))
