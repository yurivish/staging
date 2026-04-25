(ns distill.core-test
  "Smoke tests for the distill flow. The three LLM fns are stubbed via
   `with-redefs`; each test exercises one stop-reason."
  (:refer-clojure :exclude [run!])
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [distill.core :as distill]
            [distill.llm :as llm]))

;; --- helper to discard the runner's stdout summary -----------------------

(defmacro ^:private silent
  "Run `body`, returning its result while discarding any stdout the body
   prints. Keeps the runner's println summary out of the test log."
  [body]
  `(let [r# (atom nil)]
     (with-out-str (reset! r# ~body))
     @r#))

;; --- fakes ----------------------------------------------------------------

(defn- fake-encode
  "Truncate the original to `target-len` words. Deterministic — same
   target-len always produces the same short, regardless of tips."
  [_role-cfg {:keys [original target-len] :as data}]
  (let [words (str/split (str original) #"\s+")
        s     (str/join " " (take target-len words))]
    (assoc data :short s :encode-tokens 50)))

(defn- fake-decode
  "Expansion = short, repeated twice. Length doesn't matter for the
   judge stub."
  [_role-cfg {:keys [short] :as data}]
  (assoc data :reconstructed (str short " " short) :decode-tokens 100))

(defn- make-fake-judge
  "Build a judge stub that returns the score chosen by `score-fn` (a fn
   of the augmented data map — `:length` is set before score-fn runs,
   so score-fn can depend on it). `:length` matches the real judge —
   from `:short` via word-count."
  [score-fn]
  (fn [_role-cfg {:keys [short] :as data}]
    (let [d (assoc data :length (llm/word-count short))]
      (assoc d
             :score        (double (score-fn d))
             :judge-tips   "stub tip"
             :judge-tokens 25))))

(def ^:private long-input
  ;; 100 words, deterministic — long enough for target-lengths [5 10 20] to
  ;; produce three distinct shorts.
  (str/join " " (repeat 100 "word")))

(def ^:private base-config
  {:role-models       {:encoder {} :decoder {} :judge {}}
   :k                 3
   :max-rounds        10
   :token-budget      1000000
   :target-length     5
   :quality-threshold 0.85
   :plateau-rounds    2})

(defn- run! [config]
  (silent (distill/run-distill! long-input :config config
                                :quiet? true :return? true)))

;; --- one test per stop-reason --------------------------------------------

(deftest stops-on-target-hit
  (testing "score above threshold at the soft target → :target-hit, 1 round"
    (with-redefs [llm/encode! fake-encode
                  llm/decode! fake-decode
                  llm/judge!  (make-fake-judge (constantly 0.95))]
      (let [result (run! base-config)]
        (is (= :target-hit (:stop-reason result)))
        (is (= 1 (:rounds result)))
        (is (some (fn [{:keys [length score]}]
                    (and (<= length 5) (>= score 0.85)))
                  (:frontier result))
            "frontier contains a point that hit the target")))))

(deftest stops-on-max-turns
  (testing "low score forever → :max-turns when round count caps"
    (with-redefs [llm/encode! fake-encode
                  llm/decode! fake-decode
                  llm/judge!  (make-fake-judge (constantly 0.4))]
      (let [result (run! (assoc base-config :max-rounds 2 :plateau-rounds 99))]
        (is (= :max-turns (:stop-reason result)))
        (is (= 2 (:rounds result)))))))

(deftest stops-on-budget
  (testing "tiny token budget → :budget after the first round"
    (with-redefs [llm/encode! fake-encode
                  llm/decode! fake-decode
                  llm/judge!  (make-fake-judge (constantly 0.4))]
      (let [result (run! (assoc base-config
                                 :token-budget   100
                                 :max-rounds     99
                                 :plateau-rounds 99))]
        (is (= :budget (:stop-reason result)))
        (is (= 1 (:rounds result)))))))

(deftest stops-on-plateau
  (testing "deterministic encoder + constant score → frontier unchanged → :plateau"
    (with-redefs [llm/encode! fake-encode
                  llm/decode! fake-decode
                  llm/judge!  (make-fake-judge (constantly 0.4))]
      (let [result (run! (assoc base-config :plateau-rounds 2 :max-rounds 99))]
        (is (= :plateau (:stop-reason result)))
        ;; Round 0 establishes frontier; rounds 1 and 2 are unchanged → plateau-count 2.
        (is (= 3 (:rounds result)))))))

;; --- output shape --------------------------------------------------------

(deftest frontier-sorted-by-length-asc
  (with-redefs [llm/encode! fake-encode
                llm/decode! fake-decode
                ;; Score increases with length, so we get a real Pareto curve.
                llm/judge!  (make-fake-judge (fn [{:keys [length]}]
                                               (min 0.99 (* 0.05 length))))]
    (let [result (run! (assoc base-config :quality-threshold 99.0 :max-rounds 1))]
      (is (apply <= (map :length (:frontier result)))
          "frontier output is sorted by length ascending"))))

(deftest accumulates-tokens
  (with-redefs [llm/encode! fake-encode
                llm/decode! fake-decode
                llm/judge!  (make-fake-judge (constantly 0.4))]
    (let [result (run! (assoc base-config :max-rounds 2 :plateau-rounds 99))]
      ;; 2 rounds × 3 ports × (50 + 100 + 25) tokens = 1050.
      (is (= (* 2 3 (+ 50 100 25)) (:tokens-used result))))))

;; --- input validation ---------------------------------------------------

(deftest empty-input-rejected
  (testing "missing or blank input throws — no silent run on a default"
    (is (thrown? clojure.lang.ExceptionInfo
                 (distill/run-distill! nil :config base-config :quiet? true)))
    (is (thrown? clojure.lang.ExceptionInfo
                 (distill/run-distill! "" :config base-config :quiet? true)))
    (is (thrown? clojure.lang.ExceptionInfo
                 (distill/run-distill! "   " :config base-config :quiet? true)))))

