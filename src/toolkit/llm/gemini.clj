;; ============================================================
;; toolkit.llm.gemini — Google AI Studio (Gemini) adapter.
;;
;; Parallel structure to toolkit.llm.anthropic. The unified-request
;; shape is identical; this namespace just translates to/from
;; Gemini's wire format.
;;
;; Differences worth knowing:
;;   • Model name lives in the URL (`/models/{model}:generateContent`),
;;     not the body. Headers carry `x-goog-api-key`.
;;   • Roles are `user` / `model` (no separate assistant). `tool`
;;     messages become `user` messages, matching Anthropic's choice.
;;   • Documents: Gemini supports inline_data (base64 PDFs/images) and
;;     plain text parts. There's no native equivalent of Anthropic's
;;     content-blocks document with citation indices, so :blocks-source
;;     documents collapse to a single text part with blocks joined by
;;     blank lines. Citations are not a Gemini feature.
;;   • Structured output: `generationConfig.responseMimeType` +
;;     `responseSchema`. Composes with tools (function calling); no
;;     mutual-exclusion warning like Anthropic citations.
;;   • Tools: `tools: [{function_declarations: [{name description parameters}]}]`.
;;     Function-call results come back as `parts[].functionCall`.
;;   • Token usage: `usageMetadata.promptTokenCount` / `candidatesTokenCount`.
;; ============================================================

(ns toolkit.llm.gemini
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))

;; --- request: parts + messages ---------------------------------------------

(defn- document-part
  "Translate a unified :document content part into Gemini's shape.
   Base64 → inline_data; text → text; blocks → joined text.

   Note that Gemini has no native citations feature — the :citations?
   flag on the unified part is ignored here; if you need provenance
   under Gemini, ground via paragraph_id + quote in the schema itself."
  [{:keys [source-kind media-type data blocks title context]
    :or   {source-kind :base64}}]
  (case source-kind
    :base64 {:inline_data {:mime_type (or media-type "application/pdf")
                           :data data}}
    :text   {:text (cond->> data
                     context (str context "\n\n=====\n\n")
                     title   (str "[" title "]\n"))}
    ;; Concatenate blocks with double newlines. Block-level granularity
    ;; would need a separate part per block; that's possible but adds
    ;; little since Gemini won't emit block-indexed citations anyway.
    :blocks {:text (cond->> (str/join "\n\n" blocks)
                     context (str context "\n\n=====\n\n")
                     title   (str "[" title "]\n"))}))

(def ^:private part-builders
  {:text        (fn [{:keys [text]}]
                  {:text text})
   :image       (fn [{:keys [media-type data]}]
                  {:inline_data {:mime_type media-type :data data}})
   :document    document-part
   :tool-call   (fn [{:keys [name arguments]}]
                  {:functionCall {:name name :args arguments}})
   :tool-result (fn [{:keys [tool-call-id content]}]
                  ;; Gemini expects functionResponse with name + response.
                  ;; We don't have the function name here (the unified
                  ;; shape uses tool-call-id); callers that round-trip
                  ;; tool calls under Gemini should pass the name back
                  ;; via tool-call-id by convention.
                  {:functionResponse {:name tool-call-id
                                      :response {:content content}}})})

(defn- build-part [{:keys [type] :as part}]
  (if-let [f (part-builders type)]
    (f part)
    (throw (ex-info "unknown content part" {:type type}))))

(defn- build-message [{:keys [role content]}]
  {:role  (case role
            :assistant "model"
            :tool      "user"
            (name role))
   :parts (mapv build-part content)})

;; --- request: top-level body -----------------------------------------------

(defn- generation-config
  [{:keys [max-tokens temperature stop-sequences response-schema]}]
  (cond-> {}
    max-tokens      (assoc :maxOutputTokens max-tokens)
    temperature     (assoc :temperature temperature)
    stop-sequences  (assoc :stopSequences stop-sequences)
    response-schema (assoc :responseMimeType "application/json"
                           :responseSchema   response-schema)))

(defn build-body
  "Translate a unified Request into a Gemini request body. Note that
   :model lives in the URL, not the body — see `->request-fn`."
  [{:keys [system messages tools] :as request}]
  (let [gc (generation-config request)]
    (cond-> {:contents (mapv build-message messages)}
      system       (assoc :systemInstruction {:parts [{:text system}]})
      (seq gc)     (assoc :generationConfig gc)
      (seq tools)  (assoc :tools
                          [{:function_declarations
                            (mapv (fn [{:keys [name description input-schema]}]
                                    {:name        name
                                     :description description
                                     :parameters  input-schema})
                                  tools)}]))))

;; --- response parsing ------------------------------------------------------

(defn- maybe-parse-json [^String s]
  (when (and s (not (str/blank? s)))
    (try (json/read-str s :key-fn keyword) (catch Throwable _ nil))))

(defn parse-response
  "Translate a parsed Gemini response into the unified shape:
     {:raw :stop-reason :text :structured :tool-calls :usage}."
  [resp]
  (let [parts (-> resp :candidates first :content :parts)
        text (->> parts (keep :text) (str/join "\n"))
        tool-calls (vec
                    (keep (fn [{:keys [functionCall]}]
                            (when functionCall
                              {:id        (:name functionCall)
                               :name      (:name functionCall)
                               :arguments (:args functionCall)}))
                          parts))
        usage (:usageMetadata resp)]
    {:raw         resp
     :stop-reason (-> resp :candidates first :finishReason)
     :text        text
     :structured  (maybe-parse-json text)
     :tool-calls  tool-calls
     :usage       usage}))

;; --- client construction ---------------------------------------------------

(defn- ->request-fn [api-key base-url]
  (fn [{:keys [model] :as request}]
    {:url     (str (or base-url "https://generativelanguage.googleapis.com")
                   "/v1beta/models/" model ":generateContent")
     :headers {"x-goog-api-key" api-key}
     :body    (build-body request)}))

(defn client
  "Returns a provider value usable with llm/query."
  ([api-key] (client api-key nil))
  ([api-key base-url]
   {:->request      (->request-fn api-key base-url)
    :parse-response parse-response}))
