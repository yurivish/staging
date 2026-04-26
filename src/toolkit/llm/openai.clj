;; ============================================================
;; toolkit.llm.openai — OpenAI-API-compatible adapter.
;;
;; Covers any provider that speaks the OpenAI Chat Completions wire
;; format at `<base>/chat/completions`:
;;   • OpenAI itself             https://api.openai.com/v1
;;   • Groq                      https://api.groq.com/openai/v1
;;   • Together AI               https://api.together.xyz/v1
;;   • OpenRouter                https://openrouter.ai/api/v1
;;   • Cerebras                  https://api.cerebras.ai/v1
;;   • Ollama (`/v1` endpoint)   http://localhost:11434/v1
;;   • vLLM, llama.cpp, etc.     wherever you host them
;;
;; Construct a client by passing the API key plus an optional base URL:
;;   (openai/client api-key)                       ; OpenAI
;;   (openai/client gk "https://api.groq.com/openai/v1")
;;   (openai/client "ollama" "http://localhost:11434/v1")
;;
;; Capability differences across providers:
;;   - response_format with json_schema: OpenAI, Together, Fireworks,
;;     newer Ollama all support it. The adapter passes the schema
;;     through; if a provider ignores it, the model still emits text
;;     (which `:structured` will try-parse as JSON).
;;   - tools: most providers support function calling. Tool-call
;;     translation matches OpenAI's `tool_calls` shape on assistant
;;     messages and `role: tool` for results.
;;   - documents: there's no first-class PDF type on /chat/completions.
;;     :text and :blocks document parts collapse to text content;
;;     :base64 falls through as a single image_url-style URI when the
;;     media type is image-* (and is otherwise dropped with a warning,
;;     since putting binary base64 PDFs into Chat Completions is a
;;     dead end without the Files API).
;; ============================================================

(ns toolkit.llm.openai
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))

;; --- content parts ---------------------------------------------------------

(defn- image-data-uri [media-type data]
  (str "data:" media-type ";base64," data))

(defn- document-text
  "Compose a single text string for a :document part. Used as a
   fallback because /chat/completions has no first-class document type."
  [{:keys [source-kind data blocks title context]
    :or   {source-kind :base64}}]
  (let [body (case source-kind
               :text   data
               :blocks (str/join "\n\n" blocks)
               :base64 nil)] ; binary base64 isn't useful as text
    (when body
      (cond->> body
        context (str context "\n\n=====\n\n")
        title   (str "[" title "]\n")))))

(def ^:private part-builders
  {:text        (fn [{:keys [text]}]
                  {:type "text" :text text})
   :image       (fn [{:keys [media-type data]}]
                  {:type "image_url"
                   :image_url {:url (image-data-uri media-type data)}})
   :document    (fn [{:keys [media-type] :as part}]
                  (cond
                    (some? (document-text part))
                    {:type "text" :text (document-text part)}
                    (and media-type (str/starts-with? media-type "image/"))
                    {:type "image_url"
                     :image_url {:url (image-data-uri media-type (:data part))}}
                    :else
                    ;; Drop binary base64 PDFs etc. — chat-completions
                    ;; can't render them. Caller should use the Files
                    ;; API or a different content kind.
                    {:type "text" :text "[unsupported document]"}))
   :tool-call   (fn [{:keys [id name arguments]}]
                  ;; Outbound assistant message: tool_calls live as a
                  ;; sibling key on the message, not in :content. We
                  ;; tag the call here and let `build-message` lift it
                  ;; back to message level.
                  {::tool-call {:id   id
                                :type "function"
                                :function {:name      name
                                           :arguments (json/write-str arguments)}}})
   :tool-result (fn [{:keys [tool-call-id content]}]
                  ;; Outbound tool-role message: needs tool_call_id.
                  {::tool-result {:tool-call-id tool-call-id
                                  :content      (if (string? content)
                                                  content
                                                  (json/write-str content))}})})

(defn- build-part [{:keys [type] :as part}]
  (if-let [f (part-builders type)]
    (f part)
    (throw (ex-info "unknown content part" {:type type}))))

;; --- messages --------------------------------------------------------------

(defn- build-message
  "Translate one unified message into one or more OpenAI messages.
   Most messages map 1:1; tool-call and tool-result parts get
   special-cased because OpenAI puts them at message level rather
   than inside content."
  [{:keys [role content]}]
  (let [parts (mapv build-part content)
        tool-calls (keep ::tool-call parts)
        tool-results (keep ::tool-result parts)
        plain (remove (some-fn ::tool-call ::tool-result) parts)]
    (cond
      ;; tool-result(s) — emit one message per result, role=tool
      (seq tool-results)
      (mapv (fn [{:keys [tool-call-id content]}]
              {:role "tool" :tool_call_id tool-call-id :content content})
            tool-results)

      ;; assistant with tool calls — content can be string or omitted
      (and (seq tool-calls) (= role :assistant))
      [(cond-> {:role "assistant" :tool_calls (vec tool-calls)}
         (seq plain) (assoc :content (if (and (= 1 (count plain))
                                              (= "text" (:type (first plain))))
                                       (:text (first plain))
                                       (vec plain))))]

      ;; multi-part user/assistant — array content
      (or (> (count plain) 1)
          (some #(not= "text" (:type %)) plain))
      [{:role (case role :assistant "assistant" :system "system" "user")
        :content (vec plain)}]

      ;; single text part — collapse to a string for max compatibility
      (= 1 (count plain))
      [{:role (case role :assistant "assistant" :system "system" "user")
        :content (:text (first plain))}]

      :else
      [{:role (case role :assistant "assistant" :system "system" "user")
        :content ""}])))

(defn- build-messages
  "Translate a unified messages vec into the OpenAI shape, with the
   system prompt prepended as a first system-role message if given."
  [system messages]
  (let [translated (mapcat build-message messages)]
    (if system
      (cons {:role "system" :content system} translated)
      (vec translated))))

;; --- request body ----------------------------------------------------------

(defn- response-format [response-schema]
  ;; OpenAI's structured-outputs shape: a json_schema with a name,
  ;; the schema itself, and `strict: true`. Most OpenAI-compat
  ;; providers ignore strictness silently.
  {:type "json_schema"
   :json_schema {:name   "Response"
                 :schema response-schema
                 :strict true}})

(defn- tool-spec [{:keys [name description input-schema]}]
  {:type "function"
   :function {:name        name
              :description description
              :parameters  input-schema}})

(defn build-body
  [{:keys [model system messages max-tokens temperature stop-sequences
           tools response-schema]}]
  (cond-> {:model    model
           :messages (vec (build-messages system messages))}
    max-tokens      (assoc :max_tokens max-tokens)
    temperature     (assoc :temperature temperature)
    stop-sequences  (assoc :stop stop-sequences)
    response-schema (assoc :response_format (response-format response-schema))
    (seq tools)     (assoc :tools (mapv tool-spec tools))))

;; --- response parsing ------------------------------------------------------

(defn- maybe-parse-json [^String s]
  (when (and s (not (str/blank? s)))
    (try (json/read-str s :key-fn keyword) (catch Throwable _ nil))))

(defn- parse-tool-calls
  "Wire `tool_calls` shape on an assistant message → unified
   `[{:id :name :arguments}]`. Arguments come back as a JSON string;
   we parse it eagerly."
  [tool-calls]
  (vec
   (for [{:keys [id function]} tool-calls]
     {:id        id
      :name      (:name function)
      :arguments (or (maybe-parse-json (:arguments function)) {})})))

(defn parse-response
  "Translate a parsed OpenAI Chat Completions response into the unified
   shape: `{:raw :stop-reason :text :structured :reasoning :tool-calls :usage}`.

   `:reasoning` is non-nil when the provider exposes the model's
   chain-of-thought as a separate `reasoning` field on the assistant
   message (e.g. Ollama serving a reasoning-capable model like Gemma).
   These tokens count toward `max_tokens` during generation but aren't
   billed under `completion_tokens` in usage — bump `:max-tokens`
   generously when calling reasoning models or the visible answer can
   get truncated before it's emitted."
  [resp]
  (let [choice  (-> resp :choices first)
        msg     (:message choice)
        content (:content msg)
        text    (cond
                  (string? content) content
                  ;; multi-part assistant content — concatenate text parts
                  (sequential? content)
                  (->> content (keep #(when (= "text" (:type %)) (:text %)))
                       (str/join "\n"))
                  :else "")]
    {:raw         resp
     :stop-reason (:finish_reason choice)
     :text        text
     :structured  (maybe-parse-json text)
     :reasoning   (:reasoning msg)
     :tool-calls  (parse-tool-calls (:tool_calls msg))
     :usage       (:usage resp)}))

;; --- client construction ---------------------------------------------------

(defn- ->request-fn [api-key base-url]
  (fn [request]
    {:url     (str (or base-url "https://api.openai.com/v1") "/chat/completions")
     :headers (cond-> {"Authorization" (str "Bearer " api-key)}
                ;; Some providers (Ollama, vLLM) accept any value here;
                ;; an empty string is fine. We always send the header
                ;; so the request is well-formed under OpenAI's spec.
                false (identity))
     :body    (build-body request)}))

(defn client
  "Returns a provider value usable with `llm/query`. Pass `base-url` to
   target an OpenAI-compatible provider other than openai.com — e.g.
   `\"https://api.groq.com/openai/v1\"`,
   `\"http://localhost:11434/v1\"` (Ollama),
   `\"https://api.together.xyz/v1\"`."
  ([api-key] (client api-key nil))
  ([api-key base-url]
   {:->request      (->request-fn api-key base-url)
    :parse-response parse-response}))
