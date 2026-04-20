;; ============================================================
;; llm/core.clj — generic driver. Knows nothing about providers.
;; ============================================================

(ns toolkit.llm.core
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]))

(defn deep-merge [& maps]
  (apply merge-with
         (fn [a b] (if (and (map? a) (map? b)) (deep-merge a b) b))
         maps))

(defn build-payload
  "Pure: unified request → final body map ready to be JSON-encoded.
   Separated from query so it can be tested and inspected in isolation."
  [{:keys [build-body]} request]
  (deep-merge (build-body request) (:provider-extra request)))

(defn query
  "Shell: performs the HTTP call. `provider` is a map of fns produced by
   e.g. (anthropic/client api-key)."
  [{:keys [http-spec parse-response] :as provider} request]
  (let [{:keys [url headers]} (http-spec request)
        body (build-payload provider request)
        {:keys [status body error]}
        @(http/post url {:headers (merge {"Content-Type" "application/json"} headers)
                         :body    (json/write-str body)})]
    (when error
      (throw (ex-info "llm: http error" {} error)))
    (when-not (= 200 status)
      (throw (ex-info (str "llm: " status) {:status status :body body})))
    (parse-response (json/read-str body :key-fn keyword))))

;; ------------------------------------------------------------
;; Unified request shape (documented; no spec yet):
;;
;; {:model    "..."
;;  :system   "..."                                ; optional
;;  :messages [{:role :user|:assistant|:tool
;;              :content [part ...]}]
;;  :tools    [{:name :description :input-schema}] ; optional
;;  :output-schema {...}                           ; optional, opaque here
;;  :max-tokens N
;;  :temperature 0.7                               ; optional
;;  :stop-sequences [...]                          ; optional
;;  :provider-extra {...}}                         ; deep-merged into body
;;
;; Content parts:
;;   {:type :text        :text "..."}
;;   {:type :image       :media-type "image/png"      :data "<base64>"}
;;   {:type :document    :media-type "application/pdf" :data "<base64>"}
;;   {:type :tool-call   :id :name :arguments}
;;   {:type :tool-result :tool-call-id :content :error?}
;;
;; Provider value (returned by adapter `client` fns):
;;   {:build-body     (fn [request] body-map)
;;    :parse-response (fn [response-map] unified-response)
;;    :http-spec      (fn [request] {:url :headers})}
;;
;; Unified response:
;;   {:raw original-parsed-response
;;    :stop-reason ...
;;    :text "..."
;;    :tool-calls [{:id :name :arguments} ...]}
;; ------------------------------------------------------------

;; ============================================================
;; Example usage
;; ============================================================

(comment
  (require '[llm.core :as llm]
           '[llm.anthropic :as anthropic])

  (def claude (anthropic/client (System/getenv "ANTHROPIC_API_KEY")))

  ;; full call
  (llm/query claude
             {:model      "claude-opus-4-6"
              :max-tokens 1024
              :system     "be terse"
              :messages   [{:role :user :content [{:type :text :text "hi"}]}]})

  ;; inspect the exact JSON body without making a request
  (llm/build-payload claude
                     {:model      "claude-opus-4-6"
                      :max-tokens 1024
                      :messages   [{:role :user :content [{:type :text :text "hi"}]}]})

  ;; test parse-response in isolation
  ((:parse-response claude)
   {:stop_reason "end_turn"
    :content [{:type "text" :text "hello"}]}))
