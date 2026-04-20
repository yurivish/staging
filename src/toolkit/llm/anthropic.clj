;; ============================================================
;; toolkit.llm.anthropic — Anthropic adapter.
;; ============================================================

(ns toolkit.llm.anthropic
  (:require [clojure.string :as str]))

(def ^:private part-builders
  {:text        (fn [{:keys [text]}]
                  {:type "text" :text text})
   :image       (fn [{:keys [media-type data]}]
                  {:type "image"
                   :source {:type "base64" :media_type media-type :data data}})
   :document    (fn [{:keys [media-type data]}]
                  {:type "document"
                   :source {:type "base64" :media_type media-type :data data}})
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
  [{:keys [model system messages max-tokens temperature stop-sequences tools]}]
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
                                tools))))

(defn parse-response [resp]
  {:raw         resp
   :stop-reason (:stop_reason resp)
   :text        (->> (:content resp)
                     (filter #(= "text" (:type %)))
                     (map :text)
                     (str/join "\n"))
   :tool-calls  (->> (:content resp)
                     (filter #(= "tool_use" (:type %)))
                     (mapv (fn [{:keys [id name input]}]
                             {:id id :name name :arguments input})))})

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
