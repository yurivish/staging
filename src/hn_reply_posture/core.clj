(ns hn-reply-posture.core
  "Per-user reply-posture distribution. Take a user's comment history,
   classify each reply edge (comment → its parent comment) into one of
   the seven `hn_typing` edge types, and report the user's posture
   distribution.

   Reuses `hn_typing`'s edge classifier verbatim; the new bit is the
   per-user paginated source feeding it.

   Data path:
     emit-users  →  fetch-history (paginated Algolia)
     filter-edges (drop top-level unless :include-top-level)
     fetch-parent (Firebase get-item per parent_id)
     classify-edge (Haiku — same prompt + tool spec as hn_typing)
     aggregate-by-user (counts + proportions + dominant-class +
                         per-class samples)

   One-shot:
     clojure -M -e \"(require 'hn-reply-posture.core) (hn-reply-posture.core/run-once-for-user! \\\"tptacek\\\" \\\"posture.json\\\" {:trace? true})\""
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.pubsub :as pubsub])
  (:import [dev.langchain4j.model.anthropic AnthropicChatModel]
           [dev.langchain4j.model.chat.request ChatRequest]
           [dev.langchain4j.model.chat.request.json JsonObjectSchema]
           [dev.langchain4j.agent.tool ToolSpecification]
           [dev.langchain4j.data.message UserMessage SystemMessage]))

(def algolia-base "https://hn.algolia.com/api/v1/search_by_date")
(def fb-base      "https://hacker-news.firebaseio.com/v0")
(def haiku "claude-haiku-4-5")

(def edge-types
  ["agree" "disagree" "correct" "extend" "tangent" "attack" "clarify"])

;; --- Algolia paginated source by author -----------------------------------

(defn algolia-author-page
  "One page of an author's comments. Stub-friendly."
  [user page]
  (try
    (let [params [(str "tags=" (java.net.URLEncoder/encode
                                 (str "author_" user ",comment") "UTF-8"))
                  "hitsPerPage=100"
                  (str "page=" page)]
          url    (str algolia-base "?" (str/join "&" params))
          {:keys [status body error]}
          @(http/get url {:timeout 15000 :as :text :follow-redirects true})]
      (if (and (nil? error) (= 200 status) (string? body))
        (let [resp (json/read-str body :key-fn keyword)]
          {:hits (or (:hits resp) []) :nb-pages (or (:nbPages resp) 0)})
        {:hits [] :nb-pages 0}))
    (catch Throwable _ {:hits [] :nb-pages 0})))

(defn- fetch-author-history [user max-comments]
  (loop [page 0 acc []]
    (let [{ph :hits np :nb-pages} (algolia-author-page user page)
          all (into acc ph)]
      (if (or (>= (count all) max-comments)
              (>= (inc page) (or np 0))
              (empty? ph))
        (vec (take max-comments all))
        (recur (inc page) all)))))

;; --- Firebase item fetch --------------------------------------------------

(defn get-item
  "Fetch one Firebase item; nil on error."
  [id]
  (try
    (let [{:keys [status body error]}
          @(http/get (str fb-base "/item/" id ".json")
                     {:timeout 10000 :as :text :follow-redirects true})]
      (when (and (nil? error) (= 200 status) (string? body))
        (json/read-str body :key-fn keyword)))
    (catch Throwable _ nil)))

;; --- Haiku edge classifier ------------------------------------------------

(defonce ^:private classifier
  (delay (-> (AnthropicChatModel/builder)
             (.apiKey (str/trim (slurp "claude.key")))
             (.modelName haiku)
             (.maxTokens (int 64))
             .build)))

(def ^:private edge-schema
  (-> (JsonObjectSchema/builder)
      (.addStringProperty
       "edge_type"
       (str "EXACTLY one of: agree, disagree, correct, extend, tangent, attack, clarify. "
            "Pick the dominant relationship the reply has to the parent."))
      (.required ["edge_type"])
      .build))

(def ^:private edge-tool
  (-> (ToolSpecification/builder)
      (.name "submit_edge_type")
      (.description "Submit your classification of the reply.")
      (.parameters edge-schema)
      .build))

(defn llm-classify-edge!
  "Classify one parent→reply edge. Returns one of the edge-type
   strings, or nil. Stub-friendly."
  [{:keys [parent-text kid-text]}]
  (try
    (let [req (-> (ChatRequest/builder)
                  (.messages
                   [(SystemMessage/from
                     "Classify the reply's relationship to the parent comment. You MUST respond by calling submit_edge_type with one of the seven labels.")
                    (UserMessage/from
                     (str "PARENT:\n" (str/trim (or parent-text ""))
                          "\n\nREPLY:\n" (str/trim (or kid-text ""))))])
                  (.toolSpecifications [edge-tool])
                  .build)
          tcs (-> @classifier (.chat req) .aiMessage .toolExecutionRequests)]
      (when (seq tcs)
        (let [t (-> (json/read-str (.arguments ^Object (first tcs)) :key-fn keyword)
                    :edge_type str/lower-case str/trim)]
          (when ((set edge-types) t) t))))
    (catch Throwable _ nil)))

;; --- Per-user summary -----------------------------------------------------

(defn- clip [s n]
  (if (and s (> (count s) n)) (str (subs s 0 n) "…") s))

(defn summarize-user
  "Build a posture report for one user's edges."
  [user-id edges]
  (let [valid    (set edge-types)
        typed    (filter (comp valid :edge_type) edges)
        n        (count typed)
        zero-map (zipmap (map keyword edge-types) (repeat 0))
        counts   (merge zero-map
                        (frequencies (map (comp keyword :edge_type) typed)))
        proport  (if (zero? n)
                   (zipmap (keys counts) (repeat 0.0))
                   (into {} (map (fn [[k v]] [k (double (/ v n))])) counts))
        dominant (when (pos? n)
                   (key (apply max-key val counts)))
        samples-per-class
        (into {}
              (map (fn [[cls rs]]
                     [(keyword cls)
                      (->> rs
                           (take 3)
                           (mapv (fn [e]
                                   {:kid-id  (:kid-id e)
                                    :preview (clip (:kid-text e) 200)})))]))
              (group-by :edge_type typed))]
    {:user-id           user-id
     :n-edges           n
     :counts            counts
     :proportions       proport
     :dominant-class    dominant
     :samples-per-class samples-per-class}))

;; --- Steps ----------------------------------------------------------------

(defn- mk-emit-users [{:keys [user-ids]}]
  (step/step :emit-users nil
             (fn [ctx _s _tick]
               {:out (msg/children ctx (mapv (fn [u] {:user-id u}) user-ids))})))

(defn- reply-edge?
  "Algolia hit qualifies as a reply edge: parent_id is a comment, not a story."
  [{:keys [parent_id story_id]} include-top-level?]
  (and parent_id
       (or include-top-level?
           (not= parent_id story_id))))

(defn- mk-fetch-history [{:keys [max-comments include-top-level]}]
  (step/step :fetch-history nil
             (fn [ctx _s {:keys [user-id] :as row}]
               (let [t0 (System/nanoTime)
                     hs (->> (fetch-author-history user-id max-comments)
                             (filterv #(reply-edge? % include-top-level)))
                     ms (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :history-fetched
                                  :user-id user-id :n-replies (count hs) :ms ms})
                 {:out [(msg/child ctx (assoc row :replies hs))]}))))

(def split-edges
  (step/step :split-edges nil
             (fn [ctx _s {:keys [user-id replies]}]
               (if (empty? replies)
                 {:out (msg/children ctx [{:user-id user-id :empty? true}])}
                 {:out (msg/children
                         ctx
                         (mapv (fn [h]
                                 {:user-id    user-id
                                  :kid-id     (:objectID h)
                                  :kid-text   (or (:comment_text h) "")
                                  :parent-id  (:parent_id h)})
                               replies))}))))

(def fetch-parent-step
  (step/step :fetch-parent nil
             (fn [ctx _s edge]
               (if (:empty? edge)
                 {:out [(msg/child ctx edge)]}
                 (let [t0     (System/nanoTime)
                       parent (get-item (:parent-id edge))
                       ms     (long (/ (- (System/nanoTime) t0) 1e6))]
                   (trace/emit ctx {:event :parent-fetched
                                    :kid-id (:kid-id edge) :ms ms})
                   {:out [(msg/child ctx
                                     (assoc edge :parent-text
                                            (or (:text parent) "")))]})))))

(def classify-edge-step
  (step/step :classify nil
             (fn [ctx _s edge]
               (if (:empty? edge)
                 {:out [(msg/child ctx edge)]}
                 (let [t0  (System/nanoTime)
                       et  (llm-classify-edge! edge)
                       ms  (long (/ (- (System/nanoTime) t0) 1e6))]
                   (trace/emit ctx {:event :classified
                                    :kid-id (:kid-id edge) :edge_type et :ms ms})
                   {:out [(msg/child ctx (assoc edge :edge_type et))]})))))

(def aggregate-by-user
  {:procs
   {:agg
    (step/handler-map
     {:ports {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:rows []})
      :on-data (fn [ctx s row]
                 [(update s :rows conj {:msg (:msg ctx) :row row}) {}])
      :on-all-closed
      (fn [ctx s]
        (let [grouped (group-by (comp :user-id :row) (:rows s))
              out-msgs
              (mapv (fn [[user-id entries]]
                      (let [parents (mapv :msg entries)
                            rows    (mapv :row entries)
                            real    (remove :empty? rows)]
                        (msg/merge ctx parents
                                   (summarize-user user-id real))))
                    grouped)]
          (trace/emit ctx {:event :aggregated :n-users (count out-msgs)})
          {:out out-msgs}))})}
   :conns [] :in :agg :out :agg})

;; --- Flow -----------------------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [user-ids workers max-comments include-top-level]
     :or   {workers 8 max-comments 5000 include-top-level false}
     :as opts}]
   (let [opts' (assoc opts :workers workers :max-comments max-comments
                           :include-top-level include-top-level
                           :user-ids (or user-ids []))]
     (step/serial :hn-reply-posture
                  (mk-emit-users opts')
                  (c/stealing-workers :fetchers workers (mk-fetch-history opts'))
                  split-edges
                  (c/stealing-workers :parents workers fetch-parent-step)
                  (c/stealing-workers :classifiers workers classify-edge-step)
                  aggregate-by-user))))

;; --- Trace pretty-printer -------------------------------------------------

(defn- preview [v]
  (let [s (pr-str v)] (if (> (count s) 80) (str (subs s 0 77) "...") s)))

(defn- print-event [_subj ev _match]
  (locking *out*
    (println (format "[%-8s %-6s] %-32s %s"
                     (name (:kind ev))
                     (or (some-> (:msg-kind ev) name) "")
                     (str (:step-id ev) (when-let [p (:port ev)] (str " → " p)))
                     (cond-> ""
                       (:event ev)            (str "event=" (name (:event ev)) " ")
                       (:user-id ev)          (str "user=" (:user-id ev) " ")
                       (:kid-id ev)           (str "kid=" (:kid-id ev) " ")
                       (:edge_type ev)        (str "type=" (:edge_type ev) " ")
                       (:n-replies ev)        (str "nrep=" (:n-replies ev) " ")
                       (:n-users ev)          (str "users=" (:n-users ev) " ")
                       (:ms ev)               (str "ms=" (:ms ev) " ")
                       (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./posture.json" {}))
  ([out-path] (run-once! out-path {}))
  ([out-path {:keys [trace? pubsub] :as opts}]
   (let [ps    (or pubsub (when trace? (pubsub/make)))
         unsub (when trace? (pubsub/sub ps [:>] print-event))
         res   (flow/run-seq (build-flow opts) [:tick]
                             (cond-> {} ps (assoc :pubsub ps)))
         rows  (first (:outputs res))]
     (when unsub (unsub))
     (when (= :completed (:state res))
       (spit out-path (with-out-str (json/pprint rows))))
     {:state (:state res)
      :n-users (count (or rows []))
      :error (:error res)})))

(defn run-once-for-user!
  ([user out-path] (run-once-for-user! user out-path {}))
  ([user out-path opts]
   (run-once! out-path (assoc opts :user-ids [user]))))
