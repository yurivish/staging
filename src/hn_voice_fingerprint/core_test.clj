(ns hn-voice-fingerprint.core-test
  "Unit + e2e tests for hn-voice-fingerprint."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-voice-fingerprint.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Tokenization ----------------------------------------------------------

(deftest tokenize-basic
  (is (= ["the" "quick" "brown" "fox"]
         (core/tokenize "The quick brown fox")))
  (is (= [] (core/tokenize "")))
  (is (= [] (core/tokenize nil)))
  (testing "punctuation stripped, contractions kept"
    (is (= ["don't" "stop"] (core/tokenize "Don't stop!")))
    (is (= ["a" "b" "c"]    (core/tokenize "a, b. c?")))))

;; --- Period bucket ---------------------------------------------------------

(deftest period-of-quarter
  (is (= "2024-Q1" (core/period-of 1704067200 :quarter)))    ; 2024-01-01
  (is (= "2024-Q2" (core/period-of 1712016000 :quarter)))    ; 2024-04-02
  (is (= "2024-Q4" (core/period-of 1735603200 :quarter))))   ; 2024-12-30

(deftest period-of-month
  (is (= "2024-01" (core/period-of 1704067200 :month)))
  (is (= "2024-04" (core/period-of 1712016000 :month))))

;; --- Features ---------------------------------------------------------------

(deftest features-empty-text
  (let [f (core/features "")]
    (is (zero? (:length-tokens f)))
    (is (zero? (:length-chars f)))
    (is (zero? (:hedge-rate f)))
    (is (every? number? (vals f))
        "every feature is a number, no nils, even on empty text")))

(deftest features-length-and-words
  (let [f (core/features "The quick brown fox jumps over the lazy dog.")]
    (is (= 9 (:length-tokens f)))
    (is (= 44 (:length-chars f)))
    (testing "mean word length"
      ;; (3+5+5+3+5+4+3+4+3) / 9 = 35/9 ≈ 3.89
      (is (< 3.85 (:mean-word-length f) 3.95)))))

(deftest features-sentence-stats
  (let [f (core/features "First sentence. Second sentence here. Third.")]
    (testing "split on terminator yields three sentences"
      (is (= 3 (:n-sentences f))))
    (testing "mean sentence length in tokens"
      ;; sentences: [First sentence] [Second sentence here] [Third]
      ;; tokens: 2, 3, 1 → mean 2.0
      (is (< 1.95 (:mean-sentence-length f) 2.05)))))

(deftest features-hedges-and-assertions
  (testing "hedge rate counts hedge phrases"
    (let [f (core/features "Maybe this works. Perhaps not. I think it might.")]
      ;; tokens: 10; hedges: maybe, perhaps, "I think", might → 4 / 10 * 100 = 40
      (is (< 30 (:hedge-rate f) 50))))
  (testing "assertion rate counts assertive phrases"
    (let [f (core/features "Obviously this is correct. Definitely.")]
      ;; tokens: 5; assertions: obviously, definitely → 2 / 5 * 100 = 40
      (is (< 35 (:assertion-rate f) 45))))
  (testing "no false positives on uncharged text"
    (let [f (core/features "The cat sat on the mat.")]
      (is (zero? (:hedge-rate f)))
      (is (zero? (:assertion-rate f))))))

(deftest features-pronouns
  (let [f (core/features "I think you should know my opinion is yours now.")]
    ;; tokens: 11 ("I","think","you","should","know","my","opinion","is","yours","now") -> 10
    ;; first-person: I, my → 2 ; second-person: you, yours → 2
    (is (pos? (:first-person-rate f)))
    (is (pos? (:second-person-rate f)))))

(deftest features-negation
  (let [f (core/features "I don't think that is not correct, never.")]
    (is (pos? (:negation-rate f)))))

(deftest features-links
  (let [f (core/features "See https://example.com for details.")]
    (is (pos? (:link-rate f))))
  (testing "no link → zero"
    (is (zero? (:link-rate (core/features "Plain text only."))))))

(deftest features-code-fraction
  (testing "fraction of chars inside backticks"
    (let [f (core/features "Use `foo` and `bar` here")]
      (is (< 0.2 (:code-fraction f) 0.5))))
  (testing "no backticks → zero"
    (is (zero? (:code-fraction (core/features "no code here"))))))

(deftest features-numeric-rate
  (let [f (core/features "We had 42 users and 3.14 pies in 2024.")]
    (is (pos? (:numeric-rate f))))
  (is (zero? (:numeric-rate (core/features "no numbers anywhere")))))

(deftest features-capitalized-bigrams
  (testing "adjacent capitalized words count"
    (let [f (core/features "Worked at Google Cloud last year.")]
      ;; "Google Cloud" is one capitalized bigram
      (is (pos? (:capitalized-bigram-rate f)))))
  (testing "single caps don't count"
    (is (zero? (:capitalized-bigram-rate
                (core/features "Worked at Google last year."))))))

(deftest features-quote-rate
  (testing "fraction of lines starting with >"
    (let [f (core/features "> quoted line\nmy reply\n> another quote")]
      ;; 2 of 3 lines are quotes
      (is (< 0.6 (:quote-rate f) 0.7))))
  (is (zero? (:quote-rate (core/features "no quotes at all")))))

(deftest features-paragraphs
  (let [f (core/features "First para line one.\nFirst para line two.\n\nSecond para.")]
    (is (= 2 (:n-paragraphs f)))))

(deftest features-question-and-exclam
  (let [f (core/features "Why? How? Wow!")]
    (is (pos? (:question-mark-rate f)))
    (is (pos? (:exclamation-rate f)))))

(deftest features-vocab-richness-short
  (testing "short text falls back to TTR"
    (let [f (core/features "the cat sat on the mat")]
      ;; 6 tokens, 5 unique → TTR = 5/6 ≈ 0.833
      (is (< 0.8 (:vocab-richness f) 0.9)))))

(deftest features-vocab-richness-mattr
  (testing "longer text uses MATTR (window 50)"
    (let [text (clojure.string/join " " (repeat 10 "the quick brown fox jumps over the lazy dog and"))
          f    (core/features text)]
      ;; ~100 tokens, every 10-token chunk is the same 10 unique types
      ;; window 50 has 10 unique → 10/50 = 0.2
      (is (< 0.15 (:vocab-richness f) 0.25)))))

;; --- Aggregator (per-period stats) -----------------------------------------

(deftest summarize-period-shape
  (let [feats [{:length-tokens 10 :hedge-rate 5.0}
               {:length-tokens 20 :hedge-rate 0.0}
               {:length-tokens 30 :hedge-rate 10.0}]
        s     (core/summarize-period feats)]
    (is (= 3 (:n-comments s)))
    (let [length (-> s :features :length-tokens)
          hedge  (-> s :features :hedge-rate)]
      (is (< 19.9 (:mean length) 20.1))
      (is (< 4.99 (:mean hedge)  5.01))
      (is (pos? (:stdev length)))
      (is (pos? (:stdev hedge))))))

(deftest summarize-period-order-invariance
  (let [feats (mapv (fn [i] {:length-tokens i :hedge-rate (mod i 7)}) (range 30))
        a     (core/summarize-period feats)
        b     (core/summarize-period (shuffle feats))]
    (is (= (:n-comments a) (:n-comments b)))
    (is (< (Math/abs (- (-> a :features :length-tokens :mean)
                        (-> b :features :length-tokens :mean)))
           1e-9))
    (is (< (Math/abs (- (-> a :features :length-tokens :stdev)
                        (-> b :features :length-tokens :stdev)))
           1e-9))))

;; --- Drift series ----------------------------------------------------------

(deftest drift-series-length
  (let [periods [{:year-period "2024-Q1" :n-comments 10
                  :features {:length-tokens {:mean 50.0 :stdev 10.0}
                             :hedge-rate    {:mean 5.0  :stdev 1.0}}}
                 {:year-period "2024-Q2" :n-comments 8
                  :features {:length-tokens {:mean 60.0 :stdev 12.0}
                             :hedge-rate    {:mean 6.0  :stdev 1.5}}}
                 {:year-period "2024-Q3" :n-comments 12
                  :features {:length-tokens {:mean 55.0 :stdev 8.0}
                             :hedge-rate    {:mean 4.5  :stdev 0.8}}}]
        d       (core/drift-series periods)]
    (testing "n-1 distances for n periods"
      (is (= 2 (count d))))
    (is (= "2024-Q1" (-> d first :from)))
    (is (= "2024-Q2" (-> d first :to)))
    (is (number? (-> d first :distance)))))

(deftest drift-identical-periods-zero
  (testing "identical mean vectors → zero distance"
    (let [periods [{:year-period "Q1" :n-comments 10
                    :features {:length-tokens {:mean 50.0 :stdev 10.0}
                               :hedge-rate    {:mean 5.0  :stdev 1.0}}}
                   {:year-period "Q2" :n-comments 10
                    :features {:length-tokens {:mean 50.0 :stdev 10.0}
                               :hedge-rate    {:mean 5.0  :stdev 1.0}}}]
          d       (core/drift-series periods)]
      (is (< (Math/abs (-> d first :distance)) 1e-9)))))

(deftest drift-single-period-empty
  (let [periods [{:year-period "Q1" :n-comments 10 :features {}}]]
    (is (= [] (core/drift-series periods)))))

;; --- E2E with stubbed Algolia ----------------------------------------------

(defn- stub-page [pages-by-user]
  (fn [user page]
    (or (get-in pages-by-user [user page])
        {:hits [] :nb-pages 0})))

(deftest end-to-end-with-stubs
  (testing "two users → two rows, each with periods and drift"
    (let [pages
          {"alice"
           {0 {:hits [{:author "alice" :objectID "1"
                       :comment_text "Maybe this works. Perhaps."
                       :created_at_i 1704067200}      ; 2024-01-01
                      {:author "alice" :objectID "2"
                       :comment_text "Probably true."
                       :created_at_i 1706745600}]     ; 2024-02-01
               :nb-pages 2}
            1 {:hits [{:author "alice" :objectID "3"
                       :comment_text "I think this is fine."
                       :created_at_i 1712016000}]     ; 2024-04-02 (Q2)
               :nb-pages 2}}
           "bob"
           {0 {:hits [{:author "bob" :objectID "10"
                       :comment_text "Obviously correct, definitely."
                       :created_at_i 1704067200}      ; 2024-01-01
                      {:author "bob" :objectID "11"
                       :comment_text "Clearly wrong."
                       :created_at_i 1712016000}]     ; 2024-Q2
               :nb-pages 1}}}]
      (with-redefs [core/algolia-author-page (stub-page pages)]
        (let [res  (flow/run-seq
                     (core/build-flow {:user-ids ["alice" "bob"]
                                       :bucket :quarter
                                       :workers 2})
                     [:tick])
              rows (->> (first (:outputs res))
                        (group-by :user-id) vals (mapv last))
              by   (into {} (map (juxt :user-id identity)) rows)]
          (is (= :completed (:state res)))
          (is (= 2 (count rows)))
          (testing "alice has two periods (Q1, Q2)"
            (is (= 2 (count (-> by (get "alice") :periods)))))
          (testing "bob has two periods"
            (is (= 2 (count (-> by (get "bob") :periods)))))
          (testing "drift series length = periods - 1"
            (is (= 1 (count (-> by (get "alice") :drift))))
            (is (= 1 (count (-> by (get "bob") :drift)))))
          (testing "every period has all features present"
            (let [ps (-> by (get "alice") :periods)]
              (doseq [p ps]
                (is (every? #(contains? (:features p) %)
                            [:length-tokens :hedge-rate :assertion-rate]))))))))))

(deftest end-to-end-handles-empty-user
  (testing "user with no comments returns empty periods"
    (with-redefs [core/algolia-author-page (constantly {:hits [] :nb-pages 0})]
      (let [res  (flow/run-seq
                   (core/build-flow {:user-ids ["ghost"] :bucket :month :workers 2})
                   [:tick])
            rows (->> (first (:outputs res))
                      (group-by :user-id) vals (mapv last))]
        (is (= :completed (:state res)))
        (is (= 1 (count rows)))
        (is (= "ghost" (-> rows first :user-id)))
        (is (= [] (-> rows first :periods)))
        (is (= [] (-> rows first :drift)))))))
