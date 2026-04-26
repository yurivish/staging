(ns toolkit.llm.openai-test
  "Offline tests for toolkit.llm.openai. No network — verifies the
   unified-request → wire-body translation and the response parse
   against fixture data. Covers OpenAI proper plus the alternate-
   base-url use cases (Groq, Ollama, etc.)."
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [toolkit.llm.openai :as oai]))

(def ^:private c (oai/client "sk-test"))

(defn- wire [request] ((:->request c) request))
(defn- body [request] (:body (wire request)))

(defn- text-msg [role s]
  {:role role :content [{:type :text :text s}]})

;; --- URL + auth ------------------------------------------------------------

(deftest default-base-url-is-openai
  (let [{:keys [url headers]} (wire {:model "gpt-5"
                                     :max-tokens 1
                                     :messages [(text-msg :user "hi")]})]
    (is (= "https://api.openai.com/v1/chat/completions" url))
    (is (= "Bearer sk-test" (get headers "Authorization")))))

(deftest base-url-honored
  (let [c2 (oai/client "key" "http://localhost:11434/v1")
        {:keys [url]} ((:->request c2) {:model "llama3.3" :max-tokens 1
                                        :messages [(text-msg :user "x")]})]
    (is (= "http://localhost:11434/v1/chat/completions" url)))
  (let [c3 (oai/client "gk" "https://api.groq.com/openai/v1")
        {:keys [url]} ((:->request c3) {:model "llama-3.3-70b" :max-tokens 1
                                        :messages [(text-msg :user "x")]})]
    (is (= "https://api.groq.com/openai/v1/chat/completions" url))))

;; --- system + messages -----------------------------------------------------

(deftest system-becomes-first-system-message
  (let [b (body {:model "m" :max-tokens 1 :system "be brief"
                 :messages [(text-msg :user "hi")]})]
    (is (= [{:role "system" :content "be brief"}
            {:role "user"   :content "hi"}]
           (:messages b)))))

(deftest single-text-collapses-to-string
  (let [b (body {:model "m" :max-tokens 1
                 :messages [(text-msg :user "just text")]})]
    (is (= "just text" (-> b :messages first :content))
        "single-text user content collapses to a plain string for max compat")))

(deftest multipart-content-stays-array
  (let [b (body {:model "m" :max-tokens 1
                 :messages [{:role :user
                             :content [{:type :text :text "look at this"}
                                       {:type :image
                                        :media-type "image/png"
                                        :data "ZGF0YQ=="}]}]})
        msg (-> b :messages first)]
    (is (= "user" (:role msg)))
    (is (= [{:type "text" :text "look at this"}
            {:type "image_url" :image_url {:url "data:image/png;base64,ZGF0YQ=="}}]
           (:content msg)))))

;; --- top-level fields ------------------------------------------------------

(deftest max-tokens-and-temperature
  (let [b (body {:model "m" :max-tokens 256 :temperature 0.3
                 :stop-sequences ["\n\n"]
                 :messages [(text-msg :user "x")]})]
    (is (= 256 (:max_tokens b)))
    (is (= 0.3 (:temperature b)))
    (is (= ["\n\n"] (:stop b)))))

;; --- :response-schema → response_format with json_schema ------------------

(deftest response-schema-translates
  (let [schema {:type "object"
                :properties {:answer {:type "string"}}
                :required ["answer"]}
        b (body {:model "m" :max-tokens 1
                 :messages [(text-msg :user "x")]
                 :response-schema schema})]
    (is (= {:type "json_schema"
            :json_schema {:name "Response" :schema schema :strict true}}
           (:response_format b)))))

(deftest response-schema-omitted-when-absent
  (let [b (body {:model "m" :max-tokens 1 :messages [(text-msg :user "x")]})]
    (is (not (contains? b :response_format)))))

;; --- tools / function calling ---------------------------------------------

(deftest tools-become-function-array
  (let [schema {:type "object" :properties {:q {:type "string"}} :required ["q"]}
        b (body {:model "m" :max-tokens 1
                 :messages [(text-msg :user "x")]
                 :tools [{:name "lookup" :description "search" :input-schema schema}]})]
    (is (= [{:type "function"
             :function {:name "lookup" :description "search" :parameters schema}}]
           (:tools b))
        ":input-schema becomes :parameters under {type: function, function: {...}}")))

;; --- tool-call round trip --------------------------------------------------

(deftest assistant-tool-call-message
  (let [b (body {:model "m" :max-tokens 1
                 :messages [{:role :assistant
                             :content [{:type :tool-call
                                        :id "c1"
                                        :name "lookup"
                                        :arguments {:q "x"}}]}]})
        msg (-> b :messages first)]
    (is (= "assistant" (:role msg)))
    (is (= [{:id "c1"
             :type "function"
             :function {:name "lookup" :arguments (json/write-str {:q "x"})}}]
           (:tool_calls msg))
        "arguments are JSON-stringified per the OpenAI wire format")
    ;; content key may or may not be present; the spec accepts either.
    ))

(deftest tool-result-becomes-tool-role-message
  (let [b (body {:model "m" :max-tokens 1
                 :messages [(text-msg :user "q")
                            {:role :assistant
                             :content [{:type :tool-call
                                        :id "c1"
                                        :name "lookup"
                                        :arguments {:q "x"}}]}
                            {:role :tool
                             :content [{:type :tool-result
                                        :tool-call-id "c1"
                                        :content "result"}]}]})
        last-msg (-> b :messages last)]
    (is (= {:role "tool" :tool_call_id "c1" :content "result"} last-msg))))

;; --- :document content parts (no first-class doc on chat-completions) -----

(deftest document-text-becomes-text-part
  (let [b (body {:model "m" :max-tokens 1
                 :messages [{:role :user
                             :content [{:type :document
                                        :source-kind :text
                                        :data "The grass is green."}]}]})
        msg (-> b :messages first)]
    (is (= "The grass is green." (:content msg)))))

(deftest document-blocks-joined-as-text
  (let [b (body {:model "m" :max-tokens 1
                 :messages [{:role :user
                             :content [{:type :document
                                        :source-kind :blocks
                                        :blocks ["chunk-a" "chunk-b"]}]}]})
        msg (-> b :messages first)]
    (is (= "chunk-a\n\nchunk-b" (:content msg)))))

(deftest document-with-title-and-context
  (let [b (body {:model "m" :max-tokens 1
                 :messages [{:role :user
                             :content [{:type :document
                                        :source-kind :text
                                        :data "Body text."
                                        :title "T"
                                        :context "ctx"}]}]})
        content (-> b :messages first :content)]
    (is (re-find #"^\[T\]\nctx\n\n=====\n\nBody text\.$" content))))

;; --- response parsing ------------------------------------------------------

(deftest parse-text-from-choice
  (let [r (oai/parse-response
           {:choices [{:message {:role "assistant" :content "hello"}
                       :finish_reason "stop"}]
            :usage {:prompt_tokens 1 :completion_tokens 2 :total_tokens 3}})]
    (is (= "hello" (:text r)))
    (is (= "stop" (:stop-reason r)))
    (is (= {:prompt_tokens 1 :completion_tokens 2 :total_tokens 3} (:usage r)))))

(deftest parse-structured-when-text-is-json
  (let [r (oai/parse-response
           {:choices [{:message {:role "assistant" :content "{\"x\":42}"}
                       :finish_reason "stop"}]})]
    (is (= {:x 42} (:structured r)))))

(deftest parse-structured-nil-when-text-isnt-json
  (let [r (oai/parse-response
           {:choices [{:message {:role "assistant" :content "Hello, world!"}
                       :finish_reason "stop"}]})]
    (is (nil? (:structured r)))))

(deftest parse-tool-calls-with-string-args
  (let [r (oai/parse-response
           {:choices [{:message {:role "assistant"
                                  :content nil
                                  :tool_calls
                                  [{:id "c1"
                                    :type "function"
                                    :function {:name "lookup"
                                               :arguments "{\"q\":\"x\"}"}}]}
                       :finish_reason "tool_calls"}]})]
    (is (= [{:id "c1" :name "lookup" :arguments {:q "x"}}]
           (:tool-calls r))
        "OpenAI returns arguments as a JSON string; we parse it back")))

(deftest parse-multipart-content
  ;; Some providers return assistant content as an array of parts.
  (let [r (oai/parse-response
           {:choices [{:message {:role "assistant"
                                  :content [{:type "text" :text "first"}
                                            {:type "text" :text "second"}]}
                       :finish_reason "stop"}]})]
    (is (= "first\nsecond" (:text r)))))

(deftest parse-reasoning-field
  ;; Ollama serving reasoning models (e.g. Gemma) puts the model's
  ;; chain-of-thought on a sibling `reasoning` key. Surface it.
  (let [r (oai/parse-response
           {:choices [{:message {:role      "assistant"
                                 :content   "{\"answer\":42}"
                                 :reasoning "step 1: think... step 2: 42"}
                       :finish_reason "stop"}]})]
    (is (= "{\"answer\":42}" (:text r)))
    (is (= {:answer 42} (:structured r)))
    (is (= "step 1: think... step 2: 42" (:reasoning r)))))

(deftest parse-reasoning-nil-when-absent
  (let [r (oai/parse-response
           {:choices [{:message {:role "assistant" :content "plain"}
                       :finish_reason "stop"}]})]
    (is (nil? (:reasoning r)))))

;; --- :provider-extra deep-merge --------------------------------------------

(deftest provider-extra-deep-merges
  (let [body* (oai/build-body
               {:model "m" :max-tokens 1 :messages [(text-msg :user "x")]})
        merged ((requiring-resolve 'toolkit.llm/deep-merge) body* {:logprobs true})]
    (is (true? (:logprobs merged)))
    (is (contains? merged :messages) "existing keys preserved")))
