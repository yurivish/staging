(ns hn-self-correct.core-test
  "Unit + end-to-end tests for hn-self-correct. No live LLM or HTTP calls."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hn-self-correct.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- summarize-loop unit tests ---------------------------------------------

(defn- step-loop
  "Build a summarize-loop step driven by stub LLM functions. Each
   stub records its call count for assertion."
  [{:keys [k threshold max-rounds score-fn]
    :or   {k 1 threshold 0.8 max-rounds 5}}]
  (core/summarize-loop
   {:k          k
    :threshold  threshold
    :max-rounds max-rounds
    :draft!     (fn [_story] {:text "draft-0"})
    :revise!    (fn [_story prev _crit] {:text (str (:text prev) "+r")})
    :critique!  (fn [_story draft]
                  {:score    (score-fn draft)
                   :critique (str "critique-of:" (:text draft))})}))

(defn- mk-input [id]
  {:story {:id id :title (str "T" id)} :round 0
   :prev-draft nil :prev-critique nil})

(deftest terminates-immediately-on-threshold-hit
  ;; score-fn always returns 1.0 → first critique meets threshold → 1 round
  (let [wf  (step-loop {:score-fn (constantly 1.0)})
        res (flow/run-seq wf [(mk-input 1)])
        out (-> res :outputs first first)]
    (is (= :completed (:state res)))
    (is (= 1 (count (first (:outputs res)))) "exactly one final emission")
    (is (= 1 (:story-id out)))
    (is (= 1 (:rounds out)) "rounds counts the single iteration")
    (is (= 1.0 (:final-score out)))
    (is (= false (:max-rounds-hit? out)))))

(deftest terminates-on-max-rounds-when-threshold-never-hit
  ;; score-fn always returns 0.0 → critique never meets threshold → exactly max-rounds
  (let [wf  (step-loop {:score-fn (constantly 0.0) :max-rounds 4})
        res (flow/run-seq wf [(mk-input 1)])
        out (-> res :outputs first first)]
    (is (= :completed (:state res)))
    (is (= 1 (count (first (:outputs res)))))
    (is (= 4 (:rounds out)) "rounds = max-rounds when threshold never hit")
    (is (= true (:max-rounds-hit? out)))))

(deftest each-input-terminates-independently
  ;; Three stories with deterministic stop-rounds: 1, 3, 5.
  ;; Score function is keyed on the story id, so concurrent loops don't
  ;; cross-contaminate. The whole point of this test is multiplicity:
  ;; assert on the round-count VECTOR keyed by story-id, not just "all
  ;; terminated" — per the assertion-multiplicity rule.
  (let [stop-at  {1 1, 2 3, 3 5}
        score-fn (fn [draft]
                   ;; draft text: "draft-0" then "draft-0+r" then "draft-0+r+r"…
                   ;; Number of "+r"s = round number on entry to critique.
                   ;; We want score >= threshold on round = stop-at.
                   (let [pluses (count (re-seq #"\+r" (:text draft)))
                         entry-round (inc pluses)
                         id (some (fn [[id r]] (when (= r entry-round) id)) stop-at)]
                     ;; Map the draft length to "which round are we on" — but
                     ;; we don't know which story this draft belongs to from
                     ;; text alone. Simpler: use a round counter via a stub
                     ;; that knows the story.
                     (if id 1.0 0.0)))]
    ;; The simpler approach: stub critique to read story-id directly.
    (let [step (core/summarize-loop
                {:k 4 :threshold 0.8 :max-rounds 10
                 :draft!    (fn [_story] {:text "d"})
                 :revise!   (fn [_story prev _crit] {:text (str (:text prev) "+r")})
                 :critique! (fn [story draft]
                              (let [pluses (count (re-seq #"\+r" (:text draft)))
                                    entry-round (inc pluses)]
                                (if (>= entry-round (get stop-at (:id story)))
                                  {:score 1.0 :critique "good"}
                                  {:score 0.0 :critique "bad"})))})
          res  (flow/run-seq step (mapv mk-input [1 2 3]))
          outs (vec (mapcat identity (:outputs res)))
          rounds-by-id (into {} (map (juxt :story-id :rounds)) outs)]
      (is (= :completed (:state res)))
      (is (= 3 (count outs)) "exactly three final emissions, one per story")
      (is (= {1 1, 2 3, 3 5} rounds-by-id)
          "each story stops at its own configured round, independently"))))

;; --- prepare-step --------------------------------------------------------

(deftest prepare-extracts-loop-input-from-tree
  (let [tree {:id 7 :type "story" :title "T" :url "U"
              :kid-trees [{:id 8 :type "comment" :text "first" :kid-trees []}
                          {:id 9 :type "comment" :text "second" :kid-trees []}]}
        out  (#'core/tree->loop-input tree)]
    (is (= 7 (-> out :story :id)))
    (is (= "T" (-> out :story :title)))
    (is (= 0 (:round out)))
    (is (nil? (:prev-draft out)))
    (is (nil? (:prev-critique out)))
    (testing "comments-text concatenates all comment bodies"
      (is (re-find #"first" (-> out :story :comments-text)))
      (is (re-find #"second" (-> out :story :comments-text))))))

;; --- E2E pipeline --------------------------------------------------------

(deftest end-to-end-with-stubs
  (let [hn  {"https://hacker-news.firebaseio.com/v0/topstories.json" [10 20]
             "https://hacker-news.firebaseio.com/v0/item/10.json"
             {:id 10 :type "story" :title "Story A" :url "ua" :kids [11]}
             "https://hacker-news.firebaseio.com/v0/item/11.json"
             {:id 11 :type "comment" :text "reply A1" :kids []}
             "https://hacker-news.firebaseio.com/v0/item/20.json"
             {:id 20 :type "story" :title "Story B" :url "ub" :kids [21]}
             "https://hacker-news.firebaseio.com/v0/item/21.json"
             {:id 21 :type "comment" :text "reply B1" :kids []}}
        ;; Story 10 hits threshold on round 1 (good draft).
        ;; Story 20 needs revisions; threshold met on round 3.
        score-of (fn [story-id round]
                   (cond
                     (= story-id 10) 1.0
                     (and (= story-id 20) (>= round 3)) 1.0
                     :else 0.0))]
    (with-redefs [core/get-json  (fn [url]
                                   (or (get hn url)
                                       (throw (ex-info "fixture missing" {:url url}))))
                  core/draft!    (fn [story]
                                   {:text (str "draft for " (:id story))
                                    :round 1})
                  core/revise!   (fn [story prev _crit]
                                   {:text  (str (:text prev) " revised")
                                    :round (inc (:round prev))})
                  core/critique! (fn [story draft]
                                   {:score    (score-of (:id story) (:round draft))
                                    :critique "stub"})]
      (let [res (flow/run-seq (core/build-flow {:n-stories 2 :tree-workers 2
                                                :loop-workers 2
                                                :threshold 0.8 :max-rounds 5})
                              [:tick])
            outs (vec (mapcat identity (:outputs res)))
            by-id (into {} (map (juxt :story-id identity)) outs)]
        (is (= :completed (:state res)))
        (is (= 2 (count outs)) "one final per story")
        (is (= 1 (get-in by-id [10 :rounds])))
        (is (= 3 (get-in by-id [20 :rounds])))
        (is (= 1.0 (get-in by-id [10 :final-score])))
        (is (= 1.0 (get-in by-id [20 :final-score])))
        (is (false? (get-in by-id [10 :max-rounds-hit?])))
        (is (false? (get-in by-id [20 :max-rounds-hit?])))))))

;; --- Property: round count == configured stop-round ------------------------

(defspec property-rounds-match-stop-round 25
  (prop/for-all [stops (gen/vector (gen/choose 1 6) 1 5)]
    (let [stops-vec stops
          stop-at   (zipmap (range) stops-vec)
          step (core/summarize-loop
                {:k 4 :threshold 0.8 :max-rounds 10
                 :draft!    (fn [_story] {:text "d"})
                 :revise!   (fn [_story prev _crit] {:text (str (:text prev) "+r")})
                 :critique! (fn [story draft]
                              (let [pluses      (count (re-seq #"\+r" (:text draft)))
                                    entry-round (inc pluses)]
                                (if (>= entry-round (get stop-at (:id story)))
                                  {:score 1.0 :critique "good"}
                                  {:score 0.0 :critique "bad"})))})
          inputs (mapv mk-input (range (count stops-vec)))
          res    (flow/run-seq step inputs)
          outs   (vec (mapcat identity (:outputs res)))
          rounds-by-id (into {} (map (juxt :story-id :rounds)) outs)]
      (and (= :completed (:state res))
           (= (count stops-vec) (count outs))
           (= stop-at rounds-by-id)))))
