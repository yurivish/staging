(ns toolkit.llm.cli-test
  "Tests for the `claude -p` shell-out adapter. The process invocation
   is hidden behind a dynamic var so tests run without ever spawning a
   real subprocess."
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [toolkit.llm.cli :as cli]))

(defn- wrap-success [m]
  {:exit 0
   :err  ""
   :out  (json/write-str
           {:type "result" :subtype "success" :is_error false
            :result "Done!"
            :structured_output m})})

(defn- wrap-failure []
  {:exit 0 :err ""
   :out  (json/write-str {:type "result" :is_error true
                          :result "Not logged in · Please run /login"})})

;; --- happy path -----------------------------------------------------------

(deftest call-json-happy-path
  (let [captured (atom nil)]
    (with-redefs [cli/*invoke-claude*
                  (fn [argv stdin]
                    (reset! captured {:argv argv :stdin stdin})
                    (wrap-success {:topic "rust" :intensity 7}))]
      (let [r (cli/call-json! {:system "be terse"
                               :user "classify this comment"
                               :schema {:type "object"
                                        :properties {:topic {:type "string"}
                                                     :intensity {:type "integer"}}
                                        :required [:topic :intensity]}
                               :model "claude-haiku-4-5"})]
        (testing "result is parsed JSON with kebab-case keys"
          (is (= "rust" (:topic r)))
          (is (= 7 (:intensity r))))
        (testing "user message goes via stdin"
          (is (= "classify this comment" (:stdin @captured))))
        (testing "argv carries model + format + schema + system"
          (let [argv (:argv @captured)]
            (is (= "claude" (first argv)))
            (is (some #{"-p"} argv))
            (is (= "claude-haiku-4-5" (nth argv (inc (.indexOf argv "--model")))))
            (is (= "json" (nth argv (inc (.indexOf argv "--output-format")))))
            (is (some #{"--json-schema"} argv))
            (is (= "be terse" (nth argv (inc (.indexOf argv "--append-system-prompt")))))))))))

(deftest underscore-keys-converted-to-dashes
  (with-redefs [cli/*invoke-claude*
                (fn [_ _] (wrap-success {:is_mind_change true :confidence 0.9}))]
    (let [r (cli/call-json! {:system "" :user ""
                             :schema {:type "object"}
                             :model "claude-haiku-4-5"})]
      (is (= true (:is-mind-change r)))
      (is (= 0.9  (:confidence r))))))

(deftest keys-snake-leaves-underscores-alone
  (with-redefs [cli/*invoke-claude*
                (fn [_ _] (wrap-success {:drift_score 8 :drift_target "tangent"}))]
    (let [r (cli/call-json! {:system "" :user ""
                             :schema {:type "object"}
                             :model "claude-haiku-4-5"
                             :keys :snake})]
      (is (= 8 (:drift_score r)))
      (is (= "tangent" (:drift_target r))))))

(deftest result-can-be-prefixed-with-prose
  (testing "fallback path — no structured_output, but JSON in result prose"
    (with-redefs [cli/*invoke-claude*
                  (fn [_ _]
                    {:exit 0 :err ""
                     :out (json/write-str
                            {:type "result" :is_error false
                             :result (str "Sure, here you go:\n"
                                          (json/write-str {:answer 42})
                                          "\nLet me know if you need more.")})})]
      (let [r (cli/call-json! {:system "" :user ""
                               :schema {:type "object"}
                               :model "claude-haiku-4-5"})]
        (is (= 42 (:answer r)))))))

(deftest structured-output-wins-over-result-prose
  (testing "with both fields set, structured_output is preferred"
    (with-redefs [cli/*invoke-claude*
                  (fn [_ _]
                    {:exit 0 :err ""
                     :out (json/write-str
                            {:type "result" :is_error false
                             :result "Done!"
                             :structured_output {:answer 7}})})]
      (let [r (cli/call-json! {:system "" :user ""
                               :schema {:type "object"}
                               :model "claude-haiku-4-5"})]
        (is (= 7 (:answer r)))))))

;; --- error paths ----------------------------------------------------------

(deftest call-json-not-logged-in-returns-nil
  (with-redefs [cli/*invoke-claude* (fn [_ _] (wrap-failure))]
    (is (nil? (cli/call-json! {:system "" :user ""
                               :schema {:type "object"}
                               :model "claude-haiku-4-5"})))))

(deftest call-json-non-zero-exit-returns-nil
  (with-redefs [cli/*invoke-claude* (fn [_ _]
                                      {:exit 1 :err "boom" :out ""})]
    (is (nil? (cli/call-json! {:system "" :user ""
                               :schema {:type "object"}
                               :model "claude-haiku-4-5"})))))

(deftest call-json-malformed-result-returns-nil
  (testing "wrapper parses but inner result isn't valid JSON"
    (with-redefs [cli/*invoke-claude*
                  (fn [_ _]
                    {:exit 0 :err ""
                     :out (json/write-str {:type "result" :is_error false
                                           :result "not json at all"})})]
      (is (nil? (cli/call-json! {:system "" :user ""
                                 :schema {:type "object"}
                                 :model "claude-haiku-4-5"}))))))

(deftest call-json-malformed-wrapper-returns-nil
  (with-redefs [cli/*invoke-claude* (fn [_ _]
                                      {:exit 0 :err ""
                                       :out "totally garbage"})]
    (is (nil? (cli/call-json! {:system "" :user ""
                               :schema {:type "object"}
                               :model "claude-haiku-4-5"})))))

;; --- defaults -------------------------------------------------------------

(deftest model-defaults-to-haiku
  (let [captured (atom nil)]
    (with-redefs [cli/*invoke-claude*
                  (fn [argv _]
                    (reset! captured argv)
                    (wrap-success {}))]
      (cli/call-json! {:system "" :user ""
                       :schema {:type "object"}})
      (is (= "claude-haiku-4-5"
             (nth @captured (inc (.indexOf @captured "--model"))))))))
