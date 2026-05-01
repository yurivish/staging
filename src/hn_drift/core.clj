(ns hn-drift.core
  "HN top stories → fetch tree → sample top comments → one LLM call per
   story to detect title-vs-discussion drift → per-story drift report.

   Demonstrates a different LLM-call shape than the per-comment pipelines:
   one heavy summary call per story rather than many small classifications.
   No fan-out within a story; no aggregator; the 1:1 per-story flow rides
   straight through stealing-workers and into the collector.

   One-shot:
     clojure -M -e \"(require 'hn-drift.core) (hn-drift.core/run-once! \\\"drift.json\\\" {:trace? true})\""
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

(defn- subtree-size [n] (inc (reduce + 0 (map subtree-size (:kid-trees n)))))

(defn- top-comments
  "K depth-1 comments ranked by subtree size (the most-engaged-with branches)."
  [tree k]
  (->> (:kid-trees tree)
       (sort-by subtree-size >)
       (take k)
       (mapv #(or (:text %) ""))
       (filter seq)))

(defn- clip [s n]
  (if (> (count s) n) (str (subs s 0 n) "…") s))

;; --- LLM ---------------------------------------------------------------------

(defonce ^:private analyzer
  (delay (-> (AnthropicChatModel/builder)
             (.apiKey (str/trim (slurp "claude.key")))
             (.modelName haiku)
             (.maxTokens (int 512))
             .build)))

(def ^:private drift-schema
  (-> (JsonObjectSchema/builder)
      (.addIntegerProperty
       "drift_score"
       "0–10. 0 = the discussion is squarely about the submitted title. 10 = the discussion has nothing to do with the title and is debating something completely else.")
      (.addStringProperty
       "discussion_summary"
       "ONE sentence describing what the comments are actually arguing about, regardless of the title.")
      (.addStringProperty
       "drift_target"
       "If drift_score > 3, ONE phrase naming the topic the discussion drifted to. Otherwise the empty string.")
      (.required ["drift_score" "discussion_summary" "drift_target"])
      .build))

(def ^:private drift-tool
  (-> (ToolSpecification/builder)
      (.name "submit_drift")
      (.description "Submit your assessment of how far the comments drifted from the title.")
      (.parameters drift-schema)
      .build))

(defn- score-drift! [title comments]
  (let [user (str "TITLE:\n" title "\n\nTOP COMMENTS (one per blank line):\n\n"
                  (str/join "\n\n" (map #(clip (str/trim %) 800) comments)))
        req (-> (ChatRequest/builder)
                (.messages
                 [(SystemMessage/from
                   "Compare the submitted title to the actual discussion in the top comments. Score how far the comments drifted from the title's subject. You MUST respond by calling submit_drift.")
                  (UserMessage/from user)])
                (.toolSpecifications [drift-tool])
                .build)
        tcs (-> @analyzer (.chat req) .aiMessage .toolExecutionRequests)]
    (if (seq tcs)
      (json/read-str (.arguments ^Object (first tcs)) :key-fn keyword)
      {:drift_score nil :discussion_summary nil :drift_target nil})))

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

(defn- mk-drift-step [k]
  (step/step :drift nil
             (fn [ctx _s tree]
               (let [comments (top-comments tree k)
                     t0       (System/nanoTime)
                     scored   (try (score-drift! (or (:title tree) "") comments)
                                   (catch Throwable _
                                     {:drift_score nil :discussion_summary nil :drift_target nil}))
                     ms       (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :drift-scored
                                  :story-id   (:id tree)
                                  :drift_score (:drift_score scored)
                                  :ms         ms})
                 {:out [(merge {:story_id (:id tree)
                                :title    (:title tree)
                                :url      (:url tree)
                                :n_top_comments_used (count comments)}
                               scored)]}))))

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories tree-workers k-comments llm-workers]
     :or   {n-stories 30 tree-workers 8 k-comments 8 llm-workers 6}}]
   (step/serial :hn-drift
                (mk-fetch-top-ids n-stories)
                split-ids
                (c/stealing-workers :tree-fetchers tree-workers fetch-tree-step)
                (c/stealing-workers :drifters llm-workers (mk-drift-step k-comments)))))

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
                       (:drift_score ev)      (str "drift=" (:drift_score ev) " ")
                       (:n-nodes ev)          (str "n=" (:n-nodes ev) " ")
                       (:ms ev)               (str "ms=" (:ms ev) " ")
                       (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./drift.json" {}))
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
