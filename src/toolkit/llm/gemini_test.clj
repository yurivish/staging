(ns toolkit.llm.gemini-test
  "Offline tests for the Gemini adapter. No network — verifies the
   unified-request → wire-body translation and the wire-response →
   unified-response parse against fixture data."
  (:require [clojure.test :refer [deftest is]]
            [toolkit.llm.gemini :as gemini]))

(def ^:private g (gemini/client "test-key"))

(defn- wire [request] ((:->request g) request))
(defn- body [request] (:body (wire request)))

(defn- text-msg [role s]
  {:role role :content [{:type :text :text s}]})

;; --- URL + headers ---------------------------------------------------------

(deftest url-includes-model
  (let [{:keys [url headers]} (wire {:model "gemini-2.5-flash"
                                     :max-tokens 64
                                     :messages [(text-msg :user "hi")]})]
    (is (= (str "https://generativelanguage.googleapis.com"
                "/v1beta/models/gemini-2.5-flash:generateContent")
           url))
    (is (= "test-key" (get headers "x-goog-api-key")))))

(deftest base-url-honored
  (let [c (gemini/client "k" "https://proxy.test")
        {:keys [url]} ((:->request c) {:model "gemini-2.5-flash" :max-tokens 1
                                       :messages [(text-msg :user "x")]})]
    (is (= "https://proxy.test/v1beta/models/gemini-2.5-flash:generateContent" url))))

;; --- contents + roles ------------------------------------------------------

(deftest user-message-becomes-contents-with-parts
  (let [b (body {:model "g" :max-tokens 1 :messages [(text-msg :user "hi")]})]
    (is (= [{:role "user" :parts [{:text "hi"}]}]
           (:contents b)))))

(deftest assistant-role-becomes-model
  (let [b (body {:model "g" :max-tokens 1
                 :messages [(text-msg :user "q") (text-msg :assistant "a")]})]
    (is (= ["user" "model"] (mapv :role (:contents b))))))

(deftest tool-role-becomes-user
  (let [b (body {:model "g" :max-tokens 1
                 :messages [{:role :tool
                             :content [{:type :tool-result
                                        :tool-call-id "fn"
                                        :content "ok"}]}]})]
    (is (= "user" (-> b :contents first :role)))))

;; --- system prompt ---------------------------------------------------------

(deftest system-becomes-systemInstruction
  (let [b (body {:model "g" :max-tokens 1 :system "be brief"
                 :messages [(text-msg :user "x")]})]
    (is (= {:parts [{:text "be brief"}]} (:systemInstruction b)))))

(deftest no-systemInstruction-when-absent
  (let [b (body {:model "g" :max-tokens 1 :messages [(text-msg :user "x")]})]
    (is (not (contains? b :systemInstruction)))))

;; --- generationConfig ------------------------------------------------------

(deftest max-tokens-becomes-maxOutputTokens
  (let [b (body {:model "g" :max-tokens 256 :messages [(text-msg :user "x")]})]
    (is (= 256 (get-in b [:generationConfig :maxOutputTokens])))))

(deftest temperature-and-stop-sequences
  (let [b (body {:model "g" :max-tokens 1 :temperature 0.3
                 :stop-sequences ["\n\n"]
                 :messages [(text-msg :user "x")]})]
    (is (= 0.3 (get-in b [:generationConfig :temperature])))
    (is (= ["\n\n"] (get-in b [:generationConfig :stopSequences])))))

;; --- :response-schema → responseSchema ------------------------------------

(deftest response-schema-translates
  (let [schema {:type "object"
                :properties {:answer {:type "string"}}
                :required ["answer"]}
        b (body {:model "g" :max-tokens 1
                 :messages [(text-msg :user "x")]
                 :response-schema schema})]
    (is (= "application/json" (get-in b [:generationConfig :responseMimeType])))
    (is (= schema (get-in b [:generationConfig :responseSchema])))))

(deftest response-schema-omitted-when-absent
  (let [b (body {:model "g" :max-tokens 1 :messages [(text-msg :user "x")]})]
    (is (not (contains? (get b :generationConfig {}) :responseMimeType)))
    (is (not (contains? (get b :generationConfig {}) :responseSchema)))))

;; --- tools / function declarations ----------------------------------------

(deftest tools-become-function-declarations
  (let [schema {:type "object" :properties {:q {:type "string"}} :required ["q"]}
        b (body {:model "g" :max-tokens 1
                 :messages [(text-msg :user "x")]
                 :tools [{:name "lookup" :description "search" :input-schema schema}]})]
    (is (= [{:function_declarations
             [{:name "lookup" :description "search" :parameters schema}]}]
           (:tools b))
        ":input-schema becomes :parameters; tools wrapped in function_declarations")))

;; --- content parts ---------------------------------------------------------

(deftest part-image
  (let [b (body {:model "g" :max-tokens 1
                 :messages [{:role :user
                             :content [{:type :image
                                        :media-type "image/png"
                                        :data "ZGF0YQ=="}]}]})]
    (is (= [{:inline_data {:mime_type "image/png" :data "ZGF0YQ=="}}]
           (-> b :contents first :parts)))))

(deftest part-document-base64
  (let [b (body {:model "g" :max-tokens 1
                 :messages [{:role :user
                             :content [{:type :document
                                        :media-type "application/pdf"
                                        :data "ZGF0YQ=="}]}]})]
    (is (= [{:inline_data {:mime_type "application/pdf" :data "ZGF0YQ=="}}]
           (-> b :contents first :parts)))))

(deftest part-document-text-source
  (let [b (body {:model "g" :max-tokens 1
                 :messages [{:role :user
                             :content [{:type :document
                                        :source-kind :text
                                        :data "The grass is green."}]}]})]
    (is (= [{:text "The grass is green."}]
           (-> b :contents first :parts)))))

(deftest part-document-blocks-source-joined
  (let [b (body {:model "g" :max-tokens 1
                 :messages [{:role :user
                             :content [{:type :document
                                        :source-kind :blocks
                                        :blocks ["chunk-0" "chunk-1" "chunk-2"]}]}]})]
    (is (= [{:text "chunk-0\n\nchunk-1\n\nchunk-2"}]
           (-> b :contents first :parts))
        "Gemini has no content-blocks document type; collapse to a single text part")))

;; --- tool-call round trip --------------------------------------------------

(deftest part-tool-call
  (let [b (body {:model "g" :max-tokens 1
                 :messages [{:role :assistant
                             :content [{:type :tool-call
                                        :id "c1"
                                        :name "lookup"
                                        :arguments {:q "x"}}]}]})]
    (is (= [{:functionCall {:name "lookup" :args {:q "x"}}}]
           (-> b :contents first :parts)))))

;; --- :provider-extra deep-merges into the body -----------------------------

(deftest provider-extra-deep-merges
  (let [body* (gemini/build-body
               {:model "g" :max-tokens 1 :messages [(text-msg :user "x")]})
        ;; Use the same deep-merge helper the unified driver applies.
        merged ((requiring-resolve 'toolkit.llm/deep-merge) body* {:safetySettings [:foo]})]
    (is (= [:foo] (:safetySettings merged)))
    (is (contains? merged :contents) "existing keys preserved")))

;; --- response parsing ------------------------------------------------------

(deftest parse-text-from-candidates
  (let [r (gemini/parse-response
           {:candidates [{:content {:parts [{:text "hello"} {:text "world"}]
                                    :role "model"}
                          :finishReason "STOP"}]
            :usageMetadata {:promptTokenCount 1 :candidatesTokenCount 2}})]
    (is (= "hello\nworld" (:text r)))
    (is (= "STOP" (:stop-reason r)))
    (is (= {:promptTokenCount 1 :candidatesTokenCount 2} (:usage r)))))

(deftest parse-structured-when-text-is-json
  (let [r (gemini/parse-response
           {:candidates [{:content {:parts [{:text "{\"x\":42}"}]
                                    :role "model"}
                          :finishReason "STOP"}]})]
    (is (= {:x 42} (:structured r)))))

(deftest parse-extracts-tool-calls
  (let [r (gemini/parse-response
           {:candidates [{:content {:parts [{:text "looking up..."}
                                             {:functionCall {:name "lookup"
                                                             :args {:q "x"}}}]
                                    :role "model"}
                          :finishReason "TOOL_CALL"}]})]
    (is (= [{:id "lookup" :name "lookup" :arguments {:q "x"}}]
           (:tool-calls r)))))

(deftest parse-empty-tool-calls-when-absent
  (let [r (gemini/parse-response
           {:candidates [{:content {:parts [{:text "no tools here"}]
                                    :role "model"}
                          :finishReason "STOP"}]})]
    (is (= [] (:tool-calls r)))))
