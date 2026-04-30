(ns hn-density.core-property-test
  (:require [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hn-density.core :as core]))

(def ^:private gen-score
  (gen/choose 0 10))

(def ^:private gen-row
  (gen/let [d (gen/one-of [gen-score (gen/return nil)])
            e (gen/one-of [gen-score (gen/return nil)])
            cid gen/large-integer
            txt gen/string-alphanumeric]
    {:density d :emotion e :comment-id cid :text txt}))

(def ^:private gen-rows (gen/vector gen-row 0 20))

(defspec n-scored-equals-count-with-both-scores 100
  (prop/for-all [rows gen-rows]
    (= (:n_scored_comments (#'core/summarize-user "u" rows))
       (count (filter #(and (:density %) (:emotion %)) rows)))))

(defspec means-bounded-when-defined 100
  (prop/for-all [rows gen-rows]
    (let [s (#'core/summarize-user "u" rows)
          d (:info_density_mean s)
          e (:emotional_intensity_mean s)]
      (and (or (nil? d) (<= 0 d 10))
           (or (nil? e) (<= 0 e 10))))))

(def ^:private valid-quadrants
  #{"high-density-high-emotion" "high-density-low-emotion"
    "low-density-high-emotion"  "low-density-low-emotion"
    nil})

(defspec quadrant-always-valid 100
  (prop/for-all [rows gen-rows]
    (contains? valid-quadrants (:quadrant (#'core/summarize-user "u" rows)))))

(defspec sample-comments-bounded 100
  (prop/for-all [rows gen-rows]
    (<= (count (:sample_comments (#'core/summarize-user "u" rows))) 5)))

(defspec stdev-non-negative 50
  (prop/for-all [xs (gen/vector gen-score 0 30)]
    (let [s (#'core/stdev xs)]
      (or (nil? s) (>= s 0)))))
