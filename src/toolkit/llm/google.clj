(ns toolkit.llm.google
  "Google Gemini provider for `toolkit.llm`.

   Endpoint template:
     https://generativelanguage.googleapis.com/v1beta/models/{model}:{method}?key={api-key}
     where {method} is `generateContent` (one-shot) or
     `streamGenerateContent?alt=sse` (streaming).
   Auth: API key in the query string (no header).

   Translation notes:
     - `\"assistant\"` role maps to `\"model\"` on the wire.
     - `:system` → `systemInstruction.parts[0].text`.
     - `:content` blocks are flattened into `parts`:
         {:type \"text\"}                              → {:text ...}
         {:type \"document\" :source {:type \"base64\"}} → {:inlineData {:mimeType :data}}
         {:type \"document\" :source {:type \"text\"}}   → {:text <source.data>}
         {:type \"document\" :source {:type \"content\"}} → nested text parts, other types dropped.
     - `:max-tokens` → `generationConfig.maxOutputTokens`.
     - `:output-schema` → `generationConfig.responseMimeType=\"application/json\"` +
       `generationConfig.responseJsonSchema=<schema>`."
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.llm :as llm])
  (:import [java.io InputStream]
           [java.net URLEncoder]
           [java.nio.charset StandardCharsets]))

(def ^:private base
  "https://generativelanguage.googleapis.com/v1beta/models/")

(defn- endpoint-url [model method api-key]
  (str base model ":" method "?key="
       (URLEncoder/encode ^String api-key StandardCharsets/UTF_8)))

;; --- request translation ---------------------------------------------------

(defn- flatten-content [cb]
  (case (:type cb)
    "text"
    [{:text (:text cb)}]

    "document"
    (let [src (:source cb)]
      (case (:type src)
        "base64"  [{:inlineData {:mimeType (:media-type src)
                                 :data     (:data src)}}]
        "text"    [{:text (:data src)}]
        "content" (into []
                        (comp (filter #(= "text" (:type %)))
                              (map #(hash-map :text (:text %))))
                        (:content src))
        []))

    (llm/fail! :invalid-request (str "unknown content-block type: " (:type cb))
               :block cb)))

(defn- ->role [r]
  (if (= r "assistant") "model" r))

(defn- ->content [{:keys [role content]}]
  {:role  (->role role)
   :parts (into [] (mapcat flatten-content) content)})

(defn- build-body
  [{:keys [system messages max-tokens output-schema]}]
  (cond-> {:contents (mapv ->content messages)}
    (not (str/blank? system))
    (assoc :systemInstruction
           {:parts [{:text system}]})

    (or (pos-int? max-tokens) (some? output-schema))
    (assoc :generationConfig
           (cond-> {}
             (pos-int? max-tokens)
             (assoc :maxOutputTokens max-tokens)

             (some? output-schema)
             (assoc :responseMimeType   "application/json"
                    :responseJsonSchema output-schema)))))

;; --- response parsing ------------------------------------------------------

(defn- extract-text
  "Concat every `candidates[].content.parts[].text`."
  [resp]
  (str/join
    (for [c   (:candidates resp)
          p   (get-in c [:content :parts])
          :let [t (:text p)]
          :when (seq t)]
      t)))

(defn- parse-sse-event
  "Parse one SSE `data:` payload. Each Gemini event is a full response
   chunk with one or more candidates. Returns a seqable of events in the
   shape expected by `toolkit.llm/start-sse-stream!` (or `nil` for
   payloads with no interesting state)."
  [payload]
  (let [event (try (llm/read-json payload)
                   (catch Throwable _ nil))]
    (when event
      (let [candidates (:candidates event)
            text       (str/join
                         (for [c candidates
                               p (get-in c [:content :parts])
                               :let [t (:text p)]
                               :when (seq t)]
                           t))
            finish     (some :finishReason candidates)]
        (cond
          (= "MAX_TOKENS" finish)
          [{:type  :error
            :value (ex-info "max_tokens" {:reason :max-tokens})}]

          (seq text)
          [{:type :chunk :value text}]

          (and finish (not= "STOP" finish))
          [{:type  :error
            :value (ex-info (str "gemini finish: " finish)
                            {:reason :stream-error :finish finish})}]

          finish
          [{:type :done}])))))

;; --- HTTP helpers ----------------------------------------------------------

(def ^:private json-headers {"Content-Type" "application/json"})

(defn- post-blocking [model api-key body]
  (let [url  (endpoint-url model "generateContent" api-key)
        resp @(http/post url
                         {:headers json-headers
                          :body    (llm/write-json body)})
        {:keys [status error body]} resp]
    (cond
      error
      (llm/fail! :http-error "gemini: http error" :cause error)

      (not= 200 status)
      (llm/fail! :http-status "gemini: non-200"
                 :status status :body body)

      :else
      (try (llm/read-json body)
           (catch Throwable t
             (llm/fail! :parse-error "gemini: bad JSON in response"
                        :raw body :cause t))))))

(defn- post-streaming ^InputStream [model api-key body]
  (let [url  (str (endpoint-url model "streamGenerateContent" api-key)
                  "&alt=sse")
        resp @(http/post url
                         {:headers json-headers
                          :body    (llm/write-json body)
                          :as      :stream})
        {:keys [status error body]} resp]
    (cond
      error
      (llm/fail! :http-error "gemini: http error" :cause error)

      (not= 200 status)
      (let [body-str (when (instance? InputStream body)
                       (slurp body))]
        (llm/fail! :http-status "gemini: non-200"
                   :status status :body body-str))

      :else body)))

;; --- provider --------------------------------------------------------------

(defrecord GoogleProvider [api-key]
  llm/Provider
  (-query [_ req]
    (llm/check-required! req)
    (let [raw  (post-blocking (:model req) api-key (build-body req))
          text (extract-text raw)]
      {:text text :raw raw}))

  (-query-stream [_ req]
    (llm/check-required! req)
    (let [is (post-streaming (:model req) api-key (build-body req))]
      (llm/start-sse-stream! is parse-sse-event))))

(defn make
  "Construct a `GoogleProvider`. Required: `:api-key`."
  [{:keys [api-key] :as opts}]
  (when (str/blank? api-key)
    (llm/fail! :invalid-config "gemini: missing :api-key" :opts opts))
  (->GoogleProvider (str/trim api-key)))
