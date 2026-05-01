(ns hn-typing.core
  "HN top stories → fetch tree → top-K parent→reply edges per story →
   LLM classify each edge into {agree, disagree, correct, extend, tangent,
   attack, clarify} → per-story histogram of edge types.

   Demonstrates Datapotamus edge-level fan-out (deeper than comment-level),
   structured multi-class LLM output, and a count-driven per-story
   aggregator keyed by an upstream-supplied per-story count.

   One-shot:
     clojure -M -e \"(require 'hn-typing.core) (hn-typing.core/run-once! \\\"typing.json\\\" {:trace? true})\""
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
           [dev.langchain4j.data.message UserMessage SystemMessage]
           [java.util.concurrent Executors ExecutorService]))

(def base "https://hacker-news.firebaseio.com/v0")
(def haiku "claude-haiku-4-5")
(def edge-types ["agree" "disagree" "correct" "extend" "tangent" "attack" "clarify"])

(defonce ^:private vt-exec
  (delay (Executors/newVirtualThreadPerTaskExecutor)))

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

(defn- fetch-tree [emit-node! counter id]
  (let [t0   (System/nanoTime)
        item (get-json (str base "/item/" id ".json"))
        ms   (long (/ (- (System/nanoTime) t0) 1e6))
        _    (swap! counter inc)
        kids (or (:kids item) [])
        _    (emit-node! id (count kids) ms)
        futs (mapv #(.submit ^ExecutorService @vt-exec
                             ^Callable (fn [] (fetch-tree emit-node! counter %)))
                   kids)]
    (assoc item :kid-trees (mapv #(.get %) futs))))

(defn- subtree-size [n]
  (inc (reduce + 0 (map subtree-size (:kid-trees n)))))

(defn- comment-edges
  "All parent→child edges where parent is a comment (not the story root).
   Returns seq of {:parent-text :kid-text :kid-subtree-size}."
  [tree]
  (mapcat (fn walk [n]
            (concat
             (when (not= "story" (:type n))
               (for [k (:kid-trees n)]
                 {:parent-text (or (:text n) "")
                  :kid-text    (or (:text k) "")
                  :kid-subtree-size (subtree-size k)
                  :parent-id   (:id n)
                  :kid-id      (:id k)}))
             (mapcat walk (:kid-trees n))))
          (:kid-trees tree)))

;; --- LLM classifier ---------------------------------------------------------

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
            "Pick the dominant relationship the reply has to the parent. "
            "agree: confirms or amplifies the parent's view. "
            "disagree: argues against the parent's view. "
            "correct: points out a factual error in the parent. "
            "extend: builds on the parent with additional info or examples. "
            "tangent: changes the topic. "
            "attack: ad hominem or hostile rather than substantive. "
            "clarify: asks the parent to clarify or restates the parent."))
      (.required ["edge_type"])
      .build))

(def ^:private edge-tool
  (-> (ToolSpecification/builder)
      (.name "submit_edge_type")
      (.description "Submit your classification of the reply.")
      (.parameters edge-schema)
      .build))

(defn- classify-edge! [{:keys [parent-text kid-text]}]
  (let [req (-> (ChatRequest/builder)
                (.messages
                 [(SystemMessage/from
                   "Classify the reply's relationship to the parent comment. You MUST respond by calling submit_edge_type with one of the seven labels.")
                  (UserMessage/from
                   (str "PARENT:\n" (str/trim parent-text)
                        "\n\nREPLY:\n" (str/trim kid-text)))])
                (.toolSpecifications [edge-tool])
                .build)
        tcs (-> @classifier (.chat req) .aiMessage .toolExecutionRequests)]
    (if (seq tcs)
      (let [t (-> (json/read-str (.arguments ^Object (first tcs)) :key-fn keyword)
                  :edge_type str/lower-case str/trim)]
        (when ((set edge-types) t) t))
      nil)))

;; --- Steps ------------------------------------------------------------------

(defn- mk-fetch-top-ids [n]
  (step/step :fetch-top-ids
             (fn [_tick]
               (vec (take n (get-json (str base "/topstories.json")))))))

(def split-ids
  (step/step :split-ids nil
             (fn [ctx _s ids] {:out (msg/children ctx ids)})))

(def fetch-tree-step
  (step/step :fetch-tree nil
             (fn [ctx _s story-id]
               (trace/emit ctx {:event :fetch-start :story-id story-id})
               (let [t0         (System/nanoTime)
                     counter    (atom 0)
                     emit-node! (fn [id n-kids ms]
                                  (trace/emit ctx
                                              {:event    :fetch-node
                                               :story-id story-id
                                               :id       id
                                               :n-kids   n-kids
                                               :ms       ms}))
                     tree       (fetch-tree emit-node! counter story-id)
                     ms         (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :fetch-done
                                  :story-id story-id
                                  :n-nodes  @counter :ms ms})
                 {:out [tree]}))))

(defn- mk-pick-edges [k]
  (step/step :pick-edges nil
             (fn [ctx _s tree]
               (let [edges (->> (comment-edges tree)
                                (filter #(seq (:parent-text %)))
                                (filter #(seq (:kid-text %)))
                                (sort-by :kid-subtree-size >)
                                (take k)
                                (mapv #(assoc %
                                              :story-id  (:id tree)
                                              :title     (:title tree)
                                              :url       (:url tree))))]
                 (trace/emit ctx {:event :picked
                                  :story-id (:id tree) :n-edges (count edges)})
                 (if (empty? edges)
                   {}
                   {:out (msg/children ctx edges)})))))

(def classify-step
  (step/step :classify nil
             (fn [ctx _s edge]
               (let [t0    (System/nanoTime)
                     label (try (classify-edge! edge)
                                (catch Throwable _ nil))
                     ms    (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :classified
                                  :story-id (:story-id edge)
                                  :edge_type label :ms ms})
                 {:out [(assoc edge :edge_type label)]}))))

(defn- summarize-story [story-id par-msgs]
  (let [rows   (mapv :data par-msgs)
        d      (first rows)
        valid  (set edge-types)
        byt    (frequencies (filter valid (keep :edge_type rows)))
        total  (count rows)]
    {:story_id story-id
     :title    (:title d)
     :url      (:url d)
     :n_edges_classified total
     :n_unclassifiable (- total (apply + 0 (vals byt)))
     :by_type  (merge (zipmap (map keyword edge-types) (repeat 0))
                      (into {} (map (fn [[k v]] [(keyword k) v]) byt)))}))

(def aggregate-by-story
  "Aggregator: stash every classified edge under msg/drain. On close,
   group by story-id and emit one summary row per story via msg/merge."
  {:procs
   {:agg
    (step/handler-map
     {:ports         {:ins {:in ""} :outs {:out ""}}
      :on-init       (fn [] {:rows []})
      :on-data       (fn [ctx s _d]
                       [(update s :rows conj (:msg ctx)) msg/drain])
      :on-all-closed (fn [ctx s]
                       (let [grouped (group-by (comp :story-id :data) (:rows s))
                             out-msgs
                             (mapv (fn [[story-id par-msgs]]
                                     (msg/merge ctx par-msgs
                                                (summarize-story story-id par-msgs)))
                                   grouped)]
                         (trace/emit ctx {:event :aggregated
                                          :n-stories (count out-msgs)})
                         {:out out-msgs}))})}
   :conns [] :in :agg :out :agg})

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories tree-workers max-edges llm-workers]
     :or   {n-stories 30 tree-workers 8 max-edges 8 llm-workers 8}}]
   (step/serial :hn-typing
                (mk-fetch-top-ids n-stories)
                split-ids
                (c/stealing-workers :tree-fetchers tree-workers fetch-tree-step)
                (mk-pick-edges max-edges)
                (c/stealing-workers :classifiers llm-workers classify-step)
                aggregate-by-story)))

;; --- run-once! --------------------------------------------------------------

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
                       (:story-id ev)         (str "story=" (:story-id ev) " ")
                       (:edge_type ev)        (str "type="  (:edge_type ev) " ")
                       (:n-edges ev)          (str "edges=" (:n-edges ev) " ")
                       (:ms ev)               (str "ms=" (:ms ev) " ")
                       (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./typing.json" {}))
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
     {:state (:state res) :count (count (or rows [])) :error (:error res)})))
