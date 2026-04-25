;; ============================================================
;; toolkit.llm.anthropic — Anthropic adapter.
;; ============================================================

(ns toolkit.llm.anthropic
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))

(defn- document-source [{:keys [source-kind media-type data blocks]
                         :or   {source-kind :base64}}]
  (case source-kind
    :base64 {:type "base64" :media_type media-type :data data}
    :text   {:type "text"   :media_type (or media-type "text/plain") :data data}
    ;; "content" source: a vector of strings becomes one text content
    ;; block per element; citations come back as start_block_index /
    ;; end_block_index referring to this list.
    :blocks {:type "content"
             :content (mapv (fn [t] {:type "text" :text t}) blocks)}))

(def ^:private part-builders
  {:text        (fn [{:keys [text]}]
                  {:type "text" :text text})
   :image       (fn [{:keys [media-type data]}]
                  {:type "image"
                   :source {:type "base64" :media_type media-type :data data}})
   :document    (fn [{:keys [citations? title context] :as part}]
                  (cond-> {:type "document"
                           :source (document-source part)}
                    citations? (assoc :citations {:enabled true})
                    title      (assoc :title title)
                    context    (assoc :context context)))
   :tool-call   (fn [{:keys [id name arguments]}]
                  {:type "tool_use" :id id :name name :input arguments})
   :tool-result (fn [{:keys [tool-call-id content error?]}]
                  (cond-> {:type        "tool_result"
                           :tool_use_id tool-call-id
                           :content     content}
                    error? (assoc :is_error true)))})

(defn- build-part [{:keys [type] :as part}]
  (if-let [f (part-builders type)]
    (f part)
    (throw (ex-info "unknown content part" {:type type}))))

(defn- build-message [{:keys [role content]}]
  ;; tool results live in user-role messages on anthropic
  {:role    (name (if (= role :tool) :user role))
   :content (mapv build-part content)})

(defn build-body
  [{:keys [model system messages max-tokens temperature stop-sequences
           tools response-schema]}]
  (cond-> {:model      model
           :max_tokens max-tokens
           :messages   (mapv build-message messages)}
    system         (assoc :system system)
    temperature    (assoc :temperature temperature)
    stop-sequences (assoc :stop_sequences stop-sequences)
    (seq tools)    (assoc :tools
                          (mapv (fn [{:keys [name description input-schema]}]
                                  {:name         name
                                   :description  description
                                   :input_schema input-schema})
                                tools))
    ;; Anthropic structured outputs: output_config.format with a
    ;; json_schema spec. Mutually exclusive with citations on document
    ;; blocks (the API returns 400 if both are present), but otherwise
    ;; composes with tools, system, etc.
    response-schema (assoc :output_config
                           {:format {:type   "json_schema"
                                     :schema response-schema}})))

(defn- normalize-citation
  "Wire citation → unified shape. Three location flavors map to one
   schema with `:type` carrying the source-kind kind and the
   appropriate range fields:
     :char-location          → :start :end                (plain text)
     :page-location          → :start-page :end-page      (PDFs; 1-indexed)
     :content-block-location → :start-block :end-block    (custom content)"
  [{:keys [type cited_text document_index document_title
           start_char_index end_char_index
           start_page_number end_page_number
           start_block_index end_block_index]}]
  (let [base {:cited-text cited_text
              :document   document_index
              :title      document_title}]
    (case type
      "char_location"
      (assoc base :type :char-location :start start_char_index :end end_char_index)
      "page_location"
      (assoc base :type :page-location :start-page start_page_number :end-page end_page_number)
      "content_block_location"
      (assoc base :type :content-block-location
             :start-block start_block_index :end-block end_block_index))))

(defn- maybe-parse-json
  "Cheap try-parse: nil on failure. Used to expose `:structured` when
   the response is JSON (e.g. under `:response-schema`)."
  [^String s]
  (when (and s (not (str/blank? s)))
    (try (json/read-str s :key-fn keyword) (catch Throwable _ nil))))

(defn parse-response [resp]
  (let [text (->> (:content resp)
                  (filter #(= "text" (:type %)))
                  (map :text)
                  (str/join "\n"))]
    {:raw         resp
     :stop-reason (:stop_reason resp)
     :text        text
     :structured  (maybe-parse-json text)
     :tool-calls  (->> (:content resp)
                       (filter #(= "tool_use" (:type %)))
                       (mapv (fn [{:keys [id name input]}]
                               {:id id :name name :arguments input})))
   ;; Citations attach to text blocks. Each text block in the response
   ;; carries an optional :citations array; we flatten and pair each
   ;; with the literal text it justifies for downstream consumption.
     :citations   (->> (:content resp)
                       (filter #(= "text" (:type %)))
                       (mapcat (fn [{:keys [text citations]}]
                                 (for [c (or citations [])]
                                   (assoc (normalize-citation c)
                                          :supports text))))
                       vec)}))

(defn- ->request-fn [api-key base-url]
  (fn [request]
    {:url     (str (or base-url "https://api.anthropic.com") "/v1/messages")
     :headers {"x-api-key"         api-key
               "anthropic-version" "2023-06-01"}
     :body    (build-body request)}))

(defn client
  "Returns a provider value usable with llm/query."
  ([api-key] (client api-key nil))
  ([api-key base-url]
   {:->request      (->request-fn api-key base-url)
    :parse-response parse-response}))
