;; ============================================================
;; toolkit.llm — generic driver. Knows nothing about providers.
;; ============================================================

(ns toolkit.llm
  (:require [clojure.data.json :as json]
            [malli.core :as m]
            [malli.error :as me]
            [org.httpkit.client :as http]))

(defn deep-merge [& maps]
  (apply merge-with
         (fn [a b] (if (and (map? a) (map? b)) (deep-merge a b) b))
         maps))

(def Request
  [:map
   [:model :string]
   [:max-tokens pos-int?]
   [:system {:optional true} :string]
   [:temperature {:optional true} number?]
   [:stop-sequences {:optional true} [:vector :string]]
   [:provider-extra {:optional true} :map]
   ;; Constrain the response to a JSON-Schema map. Adapters translate
   ;; this into provider-native structured-output controls — Anthropic's
   ;; output_config.format, OpenAI's response_format with json_schema,
   ;; Gemini's responseMimeType + responseSchema. The schema goes
   ;; through verbatim; no sugar, no per-provider rewriting.
   [:response-schema {:optional true} :map]
   [:tools {:optional true}
    [:vector
     [:map
      [:name :string]
      [:description :string]
      [:input-schema :map]]]]
   [:messages
    [:vector
     [:map
      [:role [:enum :user :assistant :tool]]
      [:content
       [:vector
        [:multi {:dispatch :type}
         [:text        [:map [:type [:= :text]] [:text :string]]]
         [:image       [:map [:type [:= :image]]    [:media-type :string] [:data :string]]]
         [:document    [:map
                        [:type [:= :document]]
                        ;; Source kind. Defaults to :base64 (existing PDF
                        ;; pattern). :text inlines plain text;
                        ;; :blocks supplies a vec of content chunks
                        ;; (one citation = one block index range).
                        [:source-kind {:optional true}
                         [:enum :base64 :text :blocks]]
                        [:media-type {:optional true} :string]
                        [:data       {:optional true} :string]
                        [:blocks     {:optional true} [:vector :string]]
                        [:citations? {:optional true} :boolean]
                        [:title      {:optional true} :string]
                        [:context    {:optional true} :string]]]
         [:tool-call   [:map
                        [:type [:= :tool-call]]
                        [:id :string]
                        [:name :string]
                        [:arguments :map]]]
         [:tool-result [:map
                        [:type [:= :tool-result]]
                        [:tool-call-id :string]
                        [:content any?]
                        [:error? {:optional true} :boolean]]]]]]]]]])

(defn- validate-request! [request]
  (when-let [explanation (m/explain Request request)]
    (throw (ex-info "llm: invalid request"
                    {:errors (me/humanize explanation)}))))

(def ^:private default-timeout-ms
  "10-minute idle timeout. Reasoning models (Gemma 4, o1, claude-opus
   with thinking) routinely take minutes on a single response;
   http-kit's default 60s is too tight. Override per-call with
   `:timeout-ms` on the request."
  600000)

(defn query
  "Performs the HTTP call. `provider` is a map of two pure fns:
     {:->request      (fn [request] {:url :headers :body})
      :parse-response (fn [response-map] unified-result)}
   produced by e.g. (anthropic/client api-key).

   `request` may carry `:timeout-ms` to override the default 10-minute
   idle timeout for slow / reasoning-heavy models."
  [{:keys [->request parse-response]} {:keys [timeout-ms] :as request}]
  (validate-request! (dissoc request :timeout-ms))
  (let [{:keys [url headers body]} (->request request)
        body (deep-merge body (:provider-extra request))
        {:keys [status body error]}
        @(http/post url {:headers (merge {"Content-Type" "application/json"} headers)
                         :body    (json/write-str body)
                         :timeout (or timeout-ms default-timeout-ms)})]
    (when error
      (throw (ex-info "llm: http error" {} error)))
    (when-not (= 200 status)
      (throw (ex-info (str "llm: " status) {:status status :body body})))
    (parse-response (json/read-str body :key-fn keyword))))

;; ------------------------------------------------------------
;; Unified request shape: see `Request` schema above.
;;
;; Provider value (returned by adapter `client` fns):
;;   {:->request      (fn [request] {:url :headers :body})
;;    :parse-response (fn [response-map] unified-response)}
;;
;; Both fns are pure. Inspect a wire request without calling out:
;;   ((:->request provider) request)
;;
;; Unified response:
;;   {:raw original-parsed-response
;;    :stop-reason ...
;;    :text "..."
;;    :tool-calls [{:id :name :arguments} ...]}
;; ------------------------------------------------------------

(comment
  (require '[toolkit.llm :as llm]
           '[toolkit.llm.anthropic :as anthropic]
           '[clojure.string :as string])

  (def claude (anthropic/client (string/trim (slurp "claude.key"))))

  (llm/query claude
             {:model      "claude-sonnet-4-6"
              :max-tokens 1024
              :messages   [{:role :user :content [{:type :text :text "hi"}]}]}))
