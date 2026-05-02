(ns toolkit.datapotamus.steps.claude-code-test
  "Sanity checks for the Claude Code step. Real-binary calls; cost-minimized
   with `:bare? true` (skip project-context loading) and `:model \"haiku\"`.
   Auth comes from `claude.key` in the JVM cwd, passed to the subprocess
   via `:env` so we don't pollute the global environment.

   Tests skip gracefully when `claude.key` is missing — keeps CI green
   without auth setup."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus.steps.claude-code :as cc])
  (:import [java.io BufferedReader StringReader]))

(defn- claude-key []
  (let [f (java.io.File. "claude.key")]
    (when (.exists f) (str/trim (slurp f)))))

(defn- base-opts [k]
  {:model     "haiku"
   :max-turns 1
   :bare?     true
   :env       {"ANTHROPIC_API_KEY" k}})

#_ ; disabled: redundant with run!-json-schema, which exercises the same path.
(deftest run!-roundtrip
  (if-let [k (claude-key)]
    (let [r (cc/run! (assoc (base-opts k)
                            :prompt "Reply with exactly the word: hi"))]
      (is (= "hi" (-> r :result str/trim str/lower-case)))
      (is (string? (:session-id r)))
      (is (false? (:is-error r)))
      (is (number? (:total-cost-usd r))))
    (println "skipping run!-roundtrip: claude.key not present")))

;; ============================================================================
;; Stream-mode unit tests — no subprocess, fixture NDJSON over StringReader.
;; ============================================================================

(def ^:private fixture-ndjson
  ;; Real shapes captured from claude 2.1.119 against gemma via llama.cpp,
  ;; trimmed to the fields our projection actually reads.
  (str/join "\n"
    [(str "{\"type\":\"system\",\"subtype\":\"status\",\"status\":null,"
          "\"session_id\":\"sess-1\"}")
     (str "{\"type\":\"system\",\"subtype\":\"init\",\"cwd\":\"/work\","
          "\"session_id\":\"sess-1\",\"tools\":[\"Read\",\"Grep\"],"
          "\"mcp_servers\":[],\"model\":\"gemma\"}")
     (str "{\"type\":\"assistant\",\"message\":{\"content\":["
          "{\"type\":\"thinking\",\"thinking\":\"let me check\"},"
          "{\"type\":\"tool_use\",\"id\":\"tu-1\",\"name\":\"Read\","
          "\"input\":{\"file_path\":\"paragraphs.txt\"}}]},"
          "\"session_id\":\"sess-1\"}")
     (str "{\"type\":\"user\",\"message\":{\"content\":["
          "{\"type\":\"tool_result\",\"tool_use_id\":\"tu-1\","
          "\"content\":\"file contents here\",\"is_error\":false}]},"
          "\"session_id\":\"sess-1\"}")
     (str "{\"type\":\"assistant\",\"message\":{\"content\":["
          "{\"type\":\"text\",\"text\":\"Hello there\"}]},"
          "\"session_id\":\"sess-1\"}")
     (str "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,"
          "\"num_turns\":2,\"result\":\"Hello there\","
          "\"stop_reason\":\"end_turn\",\"session_id\":\"sess-1\","
          "\"total_cost_usd\":0.001,\"usage\":{\"input_tokens\":100,"
          "\"output_tokens\":20}}")]))

(defn- mock-ctx []
  (let [emitted (atom [])
        ctx     (reify clojure.lang.ILookup
                  (valAt [_ k] (when (= k :pubsub) :mock-pubsub))
                  (valAt [_ k _] (when (= k :pubsub) :mock-pubsub)))]
    {:emitted emitted
     :ctx ctx}))

(deftest parse-stream-line-handles-good-and-bad
  (testing "valid JSON returns the parsed map (snake_case keywords)"
    (let [r (@#'cc/parse-stream-line "{\"type\":\"system\",\"subtype\":\"init\"}")]
      (is (= "system" (:type r)))
      (is (= "init" (:subtype r)))
      (is (not (:parse-error r)))))
  (testing "invalid JSON returns a parse-error sentinel"
    (let [r (@#'cc/parse-stream-line "this is not json")]
      (is (true? (:parse-error r)))
      (is (= "this is not json" (:line r)))
      (is (some? (:ex r))))))

(deftest project-event-system-init-and-status
  (let [init   {:type "system" :subtype "init" :session_id "s1"
                :model "gemma" :cwd "/work" :tools ["Read"] :mcp_servers []}
        status {:type "system" :subtype "status" :session_id "s1"}]
    (testing "init produces one :session-init event with the right fields"
      (let [[e :as evs] (@#'cc/project-event init 0)]
        (is (= 1 (count evs)))
        (is (= :session-init (:phase e)))
        (is (= "s1" (:session-id e)))
        (is (= "gemma" (:model e)))
        (is (= 1 (:tools-count e)))
        (is (= 0 (:mcp-servers-count e)))))
    (testing "status (or other subtype) projects to nothing"
      (is (empty? (@#'cc/project-event status 0))))))

(deftest project-event-assistant-content-blocks
  (let [text   {:type "assistant"
                :message {:content [{:type "text" :text "hello world"}]}}
        thnk   {:type "assistant"
                :message {:content [{:type "thinking" :thinking "thoughts"}]}}
        tool   {:type "assistant"
                :message {:content [{:type "tool_use" :id "tu-1" :name "Read"
                                     :input {:file_path "p.txt"}}]}}
        multi  {:type "assistant"
                :message {:content [{:type "text" :text "one"}
                                    {:type "tool_use" :id "tu-2" :name "Grep"
                                     :input {:pattern "foo"}}]}}]
    (testing "text block → :assistant-text with length and preview"
      (let [[e] (@#'cc/project-event text 3)]
        (is (= :assistant-text (:phase e)))
        (is (= 3 (:turn-index e)))
        (is (= 11 (:length e)))
        (is (= "hello world" (:preview e)))))
    (testing "thinking block → :assistant-thinking with length, no content"
      (let [[e] (@#'cc/project-event thnk 5)]
        (is (= :assistant-thinking (:phase e)))
        (is (= 8 (:length e)))
        (is (not (contains? e :preview)))))
    (testing "tool_use block → :tool-use with name + id + input preview"
      (let [[e] (@#'cc/project-event tool 1)]
        (is (= :tool-use (:phase e)))
        (is (= "Read" (:tool e)))
        (is (= "tu-1" (:tool-use-id e)))
        (is (str/includes? (:input-preview e) "p.txt"))))
    (testing "multiple content blocks → multiple events in order"
      (let [evs (@#'cc/project-event multi 7)]
        (is (= 2 (count evs)))
        (is (= [:assistant-text :tool-use] (mapv :phase evs)))
        (is (every? #(= 7 (:turn-index %)) evs))))))

(deftest project-event-tool-result
  (let [ok-result   {:type "user"
                     :message {:content [{:type "tool_result" :tool_use_id "tu-1"
                                          :content "ok!" :is_error false}]}}
        err-result  {:type "user"
                     :message {:content [{:type "tool_result" :tool_use_id "tu-2"
                                          :content "boom" :is_error true}]}}
        no-error    {:type "user"
                     :message {:content [{:type "tool_result" :tool_use_id "tu-3"
                                          :content "ok"}]}}]
    (testing "successful tool_result → :tool-result with is-error false"
      (let [[e] (@#'cc/project-event ok-result 0)]
        (is (= :tool-result (:phase e)))
        (is (= "tu-1" (:tool-use-id e)))
        (is (false? (:is-error e)))
        (is (= 3 (:content-length e)))
        (is (= "ok!" (:content-preview e)))))
    (testing "failing tool_result → :tool-result with is-error true"
      (let [[e] (@#'cc/project-event err-result 0)]
        (is (true? (:is-error e)))))
    (testing "missing is_error defaults to false"
      (let [[e] (@#'cc/project-event no-error 0)]
        (is (false? (:is-error e)))))))

(deftest consume-stream-end-to-end
  (testing "fixture stream → emits all expected events and returns kebab-keyed result"
    (let [emitted (atom [])
          mock-emit (fn [_ctx ev] (swap! emitted conj ev))
          ;; Stub trace/emit for the duration of the test so we can capture
          ;; events without standing up a real pubsub.
          rdr     (BufferedReader. (StringReader. fixture-ndjson))
          result  (with-redefs [toolkit.datapotamus.trace/emit mock-emit]
                    (@#'cc/consume-stream! rdr {:pubsub :mock :step-id :test}))]
      (is (= "end_turn" (:stop-reason result)))
      (is (= 2 (:num-turns result)))
      (is (= 0.001 (:total-cost-usd result)))
      (testing "phases emitted in stream order"
        (is (= [:session-init        ; system init (status was skipped)
                :assistant-thinking  ; first assistant block (turn 0)
                :tool-use            ; second block of same assistant turn
                :tool-result         ; user echo
                :assistant-text]     ; final assistant turn (turn 1)
               (mapv :phase @emitted))))
      (testing "turn-index advances on each assistant event"
        (let [by-phase (group-by :phase @emitted)]
          (is (= [0 0]   (mapv :turn-index (concat (by-phase :assistant-thinking)
                                                   (by-phase :tool-use))))
              "first assistant turn's content blocks share turn-index 0")
          (is (= [1] (mapv :turn-index (by-phase :assistant-text)))
              "second assistant turn's content has turn-index 1"))))))

(deftest consume-stream-throws-without-result-event
  (let [stream (str/join "\n"
                 [(str "{\"type\":\"system\",\"subtype\":\"init\","
                       "\"session_id\":\"s1\",\"tools\":[],\"mcp_servers\":[]}")
                  (str "{\"type\":\"assistant\",\"message\":{\"content\":["
                       "{\"type\":\"text\",\"text\":\"hi\"}]}}")])
        rdr    (BufferedReader. (StringReader. stream))]
    (with-redefs [toolkit.datapotamus.trace/emit (fn [_ _])]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"stream ended without :result"
           (@#'cc/consume-stream! rdr {:pubsub :mock :step-id :test}))))))

(deftest consume-stream-tolerates-bad-line
  (let [stream (str/join "\n"
                 [(str "{\"type\":\"system\",\"subtype\":\"init\","
                       "\"session_id\":\"s1\",\"tools\":[],\"mcp_servers\":[]}")
                  "this line is not json at all"
                  (str "{\"type\":\"result\",\"subtype\":\"success\","
                       "\"is_error\":false,\"num_turns\":1,"
                       "\"result\":\"hi\",\"stop_reason\":\"end_turn\","
                       "\"session_id\":\"s1\",\"total_cost_usd\":0.0,"
                       "\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}")])
        emitted (atom [])
        rdr    (BufferedReader. (StringReader. stream))
        result (with-redefs [toolkit.datapotamus.trace/emit
                             (fn [_ ev] (swap! emitted conj ev))]
                 (@#'cc/consume-stream! rdr {:pubsub :mock :step-id :test}))]
    (is (= "end_turn" (:stop-reason result)))
    (is (= [:session-init :stream-parse-error] (mapv :phase @emitted)))
    (is (str/includes? (:line-preview (second @emitted)) "not json"))))

(deftest consume-stream-with-nil-ctx
  (testing "no events emitted but final result returned"
    (let [emitted (atom [])
          rdr     (BufferedReader. (StringReader. fixture-ndjson))
          result  (with-redefs [toolkit.datapotamus.trace/emit
                                (fn [_ ev] (swap! emitted conj ev))]
                    (@#'cc/consume-stream! rdr nil))]
      (is (= "end_turn" (:stop-reason result)))
      (is (empty? @emitted)))))

;; ============================================================================
;; Real-binary integration tests
;; ============================================================================

(deftest ^:costly run!-json-schema
  (cond
    (not (System/getProperty "test.costly"))
    (println "skipping run!-json-schema: requires -Dtest.costly (see :test-costly alias in deps.edn)")

    (not (claude-key))
    (println "skipping run!-json-schema: claude.key not present")

    :else
    (let [r (cc/run! (assoc (base-opts (claude-key))
                            :max-turns 3   ; schema-conforming output needs ≥2 turns
                            :prompt "Set greeting to \"hi\" and answer to 42."
                            :json-schema {:type       "object"
                                          :properties {:greeting {:type "string"}
                                                       :answer   {:type "integer"}}
                                          :required   ["greeting" "answer"]}))
          out (:structured-output r)]
      (is (false? (:is-error r)))
      (is (map? out))
      (is (= "hi" (str/lower-case (str (:greeting out)))))
      (is (= 42 (:answer out))))))
