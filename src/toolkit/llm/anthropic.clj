(ns toolkit.llm.anthropic
  "Anthropic Claude provider for `toolkit.llm`.

   Endpoint:  https://api.anthropic.com/v1/messages
   Auth:      x-api-key header + anthropic-version: 2023-06-01
   Streaming: same endpoint with `\"stream\": true` in the body.

   Translation notes:
     - `:max-tokens`, `:output-schema`, `:media-type` in the public API
       become `max_tokens`, `output_config`, `media_type` on the wire.
     - `:output-schema` is wrapped as
       `output_config: {format: {type: \"json_schema\", schema: <schema>}}`.
     - The public `{:type \"document\" :source {...}}` block is copied
       straight through with the single `:media-type`→`media_type` rename."
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.llm :as llm])
  (:import [java.io InputStream]))

(def ^:private endpoint "https://api.anthropic.com/v1/messages")
(def ^:private api-version "2023-06-01")

;; --- request translation ---------------------------------------------------

(declare ^:private ->source)

(defn- ->content-block [cb]
  (case (:type cb)
    "text"
    {:type "text" :text (:text cb)}

    "document"
    (cond-> {:type "document"}
      (:title cb)   (assoc :title (:title cb))
      (:context cb) (assoc :context (:context cb))
      (:source cb)  (assoc :source (->source (:source cb))))

    (llm/fail! :invalid-request (str "unknown content-block type: " (:type cb))
               :block cb)))

(defn- ->source [src]
  (case (:type src)
    ("text" "base64")
    (cond-> {:type (:type src)}
      (:media-type src) (assoc :media_type (:media-type src))
      (:data src)       (assoc :data (:data src)))

    "content"
    {:type    "content"
     :content (mapv ->content-block (:content src))}

    (llm/fail! :invalid-request (str "unknown source type: " (:type src))
               :source src)))

(defn- ->message [{:keys [role content]}]
  {:role    role
   :content (mapv ->content-block content)})

(defn- build-body
  "Build the Anthropic request body as a Clojure map. `stream?` toggles
   `stream: true`."
  [{:keys [model max-tokens system messages output-schema]} stream?]
  (cond-> {:model      model
           :max_tokens max-tokens
           :messages   (mapv ->message messages)}
    (not (str/blank? system))
    (assoc :system system)

    (some? output-schema)
    (assoc :output_config
           {:format {:type   "json_schema"
                     :schema output-schema}})

    stream?
    (assoc :stream true)))

(defn- headers [api-key]
  {"Content-Type"      "application/json"
   "x-api-key"         api-key
   "anthropic-version" api-version})

;; --- response parsing ------------------------------------------------------

(defn- extract-text
  "Join every `content[i].text` where `content[i].type = \"text\"`."
  [resp]
  (->> (:content resp)
       (filter #(= "text" (:type %)))
       (map :text)
       (str/join "\n")))

;; --- SSE event parser ------------------------------------------------------

(defn- parse-sse-event
  "Parse one `data:` payload from the Claude SSE stream. Returns a seqable
   of events in the shape expected by `toolkit.llm/start-sse-stream!`
   (or `nil` for payloads we intentionally ignore)."
  [payload]
  (let [event (try (llm/read-json payload)
                   (catch Throwable _ nil))]
    (when event
      (let [t  (:type event)
            d  (:delta event)
            dt (:type d)]
        (cond
          (= "error" t)
          [{:type  :error
            :value (ex-info "claude stream error"
                            {:reason :stream-error :raw payload})}]

          (and (= "content_block_delta" t)
               (= "text_delta" dt)
               (not (str/blank? (:text d))))
          [{:type :chunk :value (:text d)}]

          (and (= "message_delta" t)
               (= "max_tokens" (:stop_reason d)))
          [{:type  :error
            :value (ex-info "max_tokens" {:reason :max-tokens})}]

          (= "message_stop" t)
          [{:type :done}])))))

;; --- HTTP helpers ----------------------------------------------------------

(defn- post-blocking
  "POST a JSON body and return the parsed response map. Throws ex-info on
   non-200 or transport errors."
  [api-key body]
  (let [resp @(http/post endpoint
                         {:headers (headers api-key)
                          :body    (llm/write-json body)})
        {:keys [status error body]} resp]
    (cond
      error
      (llm/fail! :http-error "claude: http error" :cause error)

      (not= 200 status)
      (llm/fail! :http-status "claude: non-200"
                 :status status :body body)

      :else
      (try (llm/read-json body)
           (catch Throwable t
             (llm/fail! :parse-error "claude: bad JSON in response"
                        :raw body :cause t))))))

(defn- post-streaming
  "POST a JSON body with `:as :stream`. Returns a ready-to-read
   `InputStream` of the SSE body, or throws ex-info."
  ^InputStream [api-key body]
  (let [resp @(http/post endpoint
                         {:headers (headers api-key)
                          :body    (llm/write-json body)
                          :as      :stream})
        {:keys [status error body]} resp]
    (cond
      error
      (llm/fail! :http-error "claude: http error" :cause error)

      (not= 200 status)
      (let [body-str (when (instance? InputStream body)
                       (slurp body))]
        (llm/fail! :http-status "claude: non-200"
                   :status status :body body-str))

      :else body)))

;; --- provider --------------------------------------------------------------

(defrecord AnthropicProvider [api-key]
  llm/Provider
  (-query [_ req]
    (llm/check-required! req)
    (let [raw  (post-blocking api-key (build-body req false))
          text (extract-text raw)]
      {:text text :raw raw}))

  (-query-stream [_ req]
    (llm/check-required! req)
    (let [is (post-streaming api-key (build-body req true))]
      (llm/start-sse-stream! is parse-sse-event))))

(defn make
  "Construct an `AnthropicProvider`. Required: `:api-key`."
  [{:keys [api-key] :as opts}]
  (when (str/blank? api-key)
    (llm/fail! :invalid-config "anthropic: missing :api-key" :opts opts))
  (->AnthropicProvider (str/trim api-key)))
