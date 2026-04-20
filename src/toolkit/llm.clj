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
         [:document    [:map [:type [:= :document]] [:media-type :string] [:data :string]]]
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

(defn query
  "Performs the HTTP call. `provider` is a map of two pure fns:
     {:->request      (fn [request] {:url :headers :body})
      :parse-response (fn [response-map] unified-result)}
   produced by e.g. (anthropic/client api-key)."
  [{:keys [->request parse-response]} request]
  (validate-request! request)
  (let [{:keys [url headers body]} (->request request)
        body (deep-merge body (:provider-extra request))
        {:keys [status body error]}
        @(http/post url {:headers (merge {"Content-Type" "application/json"} headers)
                         :body    (json/write-str body)})]
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
