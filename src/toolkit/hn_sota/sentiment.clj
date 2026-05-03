(ns toolkit.hn-sota.sentiment
  "Tiny windowed-keyword sentiment scorer. Pure: no LLM, no ML.

   Strategy: count positive- and negative-word matches in a slice of
   text around a position (typically the location of a model mention).
   Sign of (pos - neg) → +1 / 0 / -1. Bag-of-words; no negation
   handling. Good enough for a demo where lineage to the source comment
   makes any scoring quirk inspectable."
  (:require [clojure.string :as str]))

(def positive-words
  #{"great" "love" "loved" "amazing" "impressive" "best" "prefer"
    "preferred" "excellent" "fantastic" "awesome" "fast" "smart"
    "powerful" "useful" "reliable" "accurate" "incredible" "solid"
    "magic" "magical" "favorite" "favourite" "winning" "wins"
    "outperforms" "better" "improved" "improvement"})

(def negative-words
  #{"broken" "sucks" "worse" "worst" "hallucinates" "hallucinating"
    "hallucination" "useless" "slop" "dumb" "stupid" "refuses"
    "refused" "slow" "lazy" "buggy" "garbage" "trash" "annoying"
    "frustrating" "wrong" "fails" "failing" "failed" "terrible"
    "awful" "disappointing" "regression" "regressed" "overrated"})

(def ^:private word-re #"[A-Za-z]+")

(defn- words-in [text]
  (->> (re-seq word-re text)
       (map str/lower-case)))

(defn score
  "Score a string as -1 / 0 / +1 by counting positive vs negative
   keyword matches. Ties → 0."
  [text]
  (when (string? text)
    (let [ws  (words-in text)
          pos (count (filter positive-words ws))
          neg (count (filter negative-words ws))
          d   (- pos neg)]
      (cond (pos? d) 1
            (neg? d) -1
            :else    0))))

(defn score-around
  "Score the slice of `text` within `radius` chars of `idx`. The mention
   itself sits at `idx`; we look at the surrounding sentence-ish context
   for sentiment cues."
  ([text idx] (score-around text idx 120))
  ([text idx radius]
   (when (and (string? text) (integer? idx))
     (let [n  (count text)
           lo (max 0 (- idx radius))
           hi (min n (+ idx radius))]
       (score (subs text lo hi))))))
