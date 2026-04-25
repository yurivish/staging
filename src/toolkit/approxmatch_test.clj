(ns toolkit.approxmatch-test
  "Offline tests for toolkit.approxmatch — port of the Go test suite plus
   a regression bound against the existing podcast extraction output."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [toolkit.approxmatch :as am]))

;; ============================================================================
;; Core scenarios — direct port of the Go tests
;; ============================================================================

(deftest exact-match
  (let [m (am/find-all "the quick brown fox jumps over the lazy dog" "brown fox" 0)]
    (is (= 1 (count m)))
    (is (= "brown fox" (:text (first m))))
    (is (= 0 (:distance (first m))))))

(deftest substitution
  ;; LLM wrote "brawn fox" — one substitution.
  (let [m (am/find-all "the quick brown fox jumps over the lazy dog" "brawn fox" 1)]
    (is (= 1 (count m)))
    (is (= "brown fox" (:text (first m))))
    (is (= 1 (:distance (first m))))))

(deftest llm-inserted-words
  ;; LLM hallucinated "brown cute fox".
  (let [m (am/find-all "the quick brown fox jumps over the lazy dog" "brown cute fox" 5)]
    (is (= 1 (count m)))
    (is (= "brown fox" (:text (first m))))))

(deftest llm-dropped-words
  ;; "quick fox" — algorithm finds "quick " at distance 3, which is the
  ;; minimum-distance match (shorter and closer than "quick brown fox" at d=6).
  (let [m (am/find-all "the quick brown fox jumps over the lazy dog" "quick fox" 6)]
    (is (= 1 (count m)))
    (is (= 3 (:distance (first m))))))

(deftest multiple-matches
  (let [t "the cat sat on the mat while another cat sat on a hat"
        m (am/find-all t "cat sat" 1)]
    (is (= 2 (count m)))
    (is (every? #(<= (:distance %) 1) m))))

(deftest no-match
  (let [m (am/find-all "the quick brown fox jumps over the lazy dog" "elephant" 2)]
    (is (= 0 (count m)))))

;; ============================================================================
;; Unicode
;; ============================================================================

(deftest unicode-exact
  (let [m (am/find-all "日本語のテキストで検索するテスト" "テキスト" 0)]
    (is (= 1 (count m)))
    (is (= "テキスト" (:text (first m))))
    (is (= 4 (:start (first m))))
    (is (= 8 (:end (first m))))))

(deftest unicode-fuzzy
  ;; "テスト" (one deletion from "テキスト") also matches at maxDist=1.
  (let [m (am/find-all "日本語のテキストで検索するテスト" "テキスト" 1)]
    (is (= 2 (count m)))))

;; ============================================================================
;; Empty + degenerate inputs
;; ============================================================================

(deftest empty-pattern
  (is (= [] (am/find-all "some text" "" 5)))
  (is (= [] (am/find-all "some text" "   " 5))))

(deftest nil-text
  (is (= [] (am/find-all nil "x" 0))))

(deftest empty-text-no-match
  (is (= [] (am/find-all "" "anything" 1))))

;; ============================================================================
;; Positions
;; ============================================================================

(deftest positions
  (let [t "abcdefghij"
        m (first (am/find-all t "def" 0))]
    (is (= 3 (:start m)))
    (is (= 6 (:end m)))
    (is (= "def" (subs t (:start m) (:end m))))))

(deftest positions-with-collapsed-whitespace
  ;; "a   b   c" — match on "b" reports positions in the original 9-char string.
  (let [t "a   b   c"
        m (first (am/find-all t "b" 0))]
    (is (= "b" (subs t (:start m) (:end m))))))

(deftest positions-with-case-difference
  (let [t "Hello World"
        m (first (am/find-all t "world" 0))]
    (is (= 6 (:start m)))
    (is (= 11 (:end m)))
    (is (= "World" (:text m)))))

;; ============================================================================
;; Case insensitivity
;; ============================================================================

(deftest case-insensitive-match
  (let [m (am/find-all "the quick brown fox" "BROWN FOX" 0)]
    (is (= 1 (count m)))
    (is (= 0 (:distance (first m))))))

(deftest case-preserved-in-output
  (let [m (am/find-all "The Quick Brown Fox" "the quick brown fox" 0)]
    (is (= 1 (count m)))
    (is (= "The Quick Brown Fox" (:text (first m))))))

(deftest mixed-case-fuzzy
  ;; Case is free; only the typo "brawn" costs.
  (let [m (am/find-all "the quick brown fox" "BRAWN FOX" 1)]
    (is (= 1 (count m)))
    (is (= 1 (:distance (first m))))))

;; ============================================================================
;; Whitespace collapsing
;; ============================================================================

(deftest whitespace-collapse-match
  (let [m (am/find-all "the quick brown fox" "quick    brown" 0)]
    (is (= 1 (count m)))
    (is (= 0 (:distance (first m))))))

(deftest whitespace-preserved-in-output
  (let [m (am/find-all "quick   brown   fox" "quick brown fox" 0)]
    (is (= 1 (count m)))
    (is (= "quick   brown   fox" (:text (first m))))))

(deftest tabs-and-newlines-collapse
  (let [m (am/find-all "hello\t\tworld" "hello world" 0)]
    (is (= 1 (count m)))
    (is (= 0 (:distance (first m))))
    (is (= "hello\t\tworld" (:text (first m))))))

;; ============================================================================
;; NFC normalization
;; ============================================================================

(deftest nfc-vs-nfd
  ;; "café" decomposed (e + combining acute) vs NFC composed (é).
  (let [nfd "café"
        nfc "café"
        m (am/find-all nfd nfc 0)]
    (is (= 1 (count m)))
    (is (= 0 (:distance (first m))))))

(deftest nfc-in-longer-text
  (let [text    "the café is open"   ; NFC
        pattern "café"          ; NFD
        m       (am/find-all text pattern 0)]
    (is (= 1 (count m)))
    (is (= "café" (:text (first m))))))

;; ============================================================================
;; Combined normalization
;; ============================================================================

(deftest case-and-whitespace
  (let [m (am/find-all "The   Quick   Brown" "the quick brown" 0)]
    (is (= 1 (count m)))
    (is (= 0 (:distance (first m))))
    (is (= "The   Quick   Brown" (:text (first m))))))

(deftest all-normalizations-plus-fuzzy
  (let [text    "The   café   is   open"
        pattern "THE CAFé IS OPAN"   ; "open" → "opan"
        m       (am/find-all text pattern 1)]
    (is (= 1 (count m)))
    (is (= 1 (:distance (first m))))))

;; ============================================================================
;; Trailing whitespace boundary
;; ============================================================================

(deftest trailing-whitespace-included
  ;; When the match's normalized end falls at a collapsed-whitespace
  ;; boundary, the literal range covers the full run in the original.
  (let [t "hello     world"
        m (am/find-all t "hello " 0)]
    (is (= 1 (count m)))
    (is (= "hello     " (:text (first m))))))

;; ============================================================================
;; LLM quote scenarios — declaration of the problem this library solves
;; ============================================================================

(deftest llm-quote-scenarios
  (let [source (str "We hold these truths to be self-evident, that all men are "
                    "created equal, that they are endowed by their Creator with "
                    "certain unalienable Rights, that among these are Life, "
                    "Liberty and the pursuit of Happiness.")]
    (testing "exact quote"
      (is (= 1 (count (am/find-all source "certain unalienable Rights" 0)))))
    (testing "LLM paraphrased slightly (unalienable → inalienable)"
      (is (= 1 (count (am/find-all source "certain inalienable Rights" 2)))))
    (testing "LLM garbled the middle (extra comma + dropped 'the')"
      (is (= 1 (count (am/find-all source "Life, Liberty, and pursuit of Happiness" 5)))))
    (testing "totally wrong quote"
      (is (= 0 (count (am/find-all source "Congress shall make no law" 5)))))))

;; ============================================================================
;; matches? / best convenience
;; ============================================================================

(deftest best-picks-lowest-distance
  (let [t "the cat sat on the mat. the dog sat on the log."
        b (am/best t "the cat sat" 5)]
    (is (= 0 (:distance b)))
    (is (= "the cat sat" (:text b)))))

(deftest matches?-bool
  (is (am/matches? "the grass is green" "Grass IS green" 0))
  (is (am/matches? "the grass is green" "grass is blue" 3))
  (is (not (am/matches? "the grass is green" "totally unrelated phrase" 1))))

;; ============================================================================
;; Regression: every cached sentiment-full2 quote still grounds at a
;; quote-length-proportional budget. This protects against regressions
;; when `quote-matches?` swaps the LCS-fraction validator for approxmatch.
;; ============================================================================

(defn- quote-budget
  "Default ~20% character budget — same heuristic the Go Example uses.
   At least 4 to allow for trivial typos in short quotes."
  [^String quote]
  (max 4 (quot (count quote) 5)))

(deftest sentiment-full2-quotes-ground
  (let [out (io/file "out/sentiment-full2.json")
        src (io/file "stumpf.json")]
    (when (and (.exists out) (.exists src))
      (let [d (json/read-str (slurp out) :key-fn keyword)
            paras (into {} (map (juxt :id :text))
                        (:paragraphs (json/read-str (slurp src) :key-fn keyword)))
            results
            (for [[_ e] (:entities d)
                  o (:occurrences e)
                  :let [ptext (paras (:paragraph_id o))
                        q (:quote o)
                        ok? (and ptext (am/matches? ptext q (quote-budget q)))]]
              {:ok? ok? :record o})
            failures (filter (complement :ok?) results)]
        ;; The strict quote-length/5 budget is more rigorous than the
        ;; old LCS-50% validator; a small number of pre-existing records
        ;; whose quotes were genuinely loose will fail. We just want to
        ;; bound that to a small fraction.
        (is (< (count failures) (* 0.02 (count results)))
            (format "expected <2%% of %d records to fail at quote-length/5 budget; got %d"
                    (count results) (count failures)))))))
