(ns hn-density.core
  "Discover top HN commenters from the front-page comment trees, score
   each one's recent comments on info-density and emotional-intensity,
   and aggregate per user.

   Demonstrates Datapotamus aggregator nodes (handler-map with
   :on-all-closed driving a re-fan-out via msg/merge), two fan-out
   generations in one flow, and LLM scoring under stealing-workers
   with structured output via langchain4j tool spec.

   One-shot:
     clojure -M -e \"(require 'hn-density.core) (hn-density.core/run-once! \\\"density.json\\\" {:trace? true})\""
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

(defn- comment-nodes
  "All non-root comments in a fetched tree."
  [tree]
  (mapcat (fn walk [n] (cons n (mapcat walk (:kid-trees n))))
          (:kid-trees tree)))

;; --- LLM client -------------------------------------------------------------

(defonce ^:private scorer
  (delay (-> (AnthropicChatModel/builder)
             (.apiKey (str/trim (slurp "claude.key")))
             (.modelName haiku)
             (.maxTokens (int 128))
             .build)))

(def ^:private score-schema
  (-> (JsonObjectSchema/builder)
      (.addIntegerProperty "density"
                           "Information density 0–10. How many specific facts, numbers, named entities, technical claims, or links the comment contains. Independent of emotion.")
      (.addIntegerProperty "emotion"
                           "Emotional intensity 0–10. How charged, opinionated, or affective the language is. Independent of density.")
      (.required ["density" "emotion"])
      .build))

(def ^:private score-tool
  (-> (ToolSpecification/builder)
      (.name "submit_scores")
      (.description "Submit information density and emotional intensity for the comment.")
      (.parameters score-schema)
      .build))

(def ^:private score-system
  "Score the HN comment on two independent 0–10 axes — INFO_DENSITY (specific facts, numbers, named entities, technical claims, links) and EMOTIONAL_INTENSITY (how charged or affective the language is). They are independent: a comment can score high on both, low on both, or any mix. You MUST respond by calling the submit_scores tool.")

(defn- score-comment! [text]
  (let [req (-> (ChatRequest/builder)
                (.messages [(SystemMessage/from score-system)
                            (UserMessage/from (or text ""))])
                (.toolSpecifications [score-tool])
                .build)
        tcs (-> @scorer (.chat req) .aiMessage .toolExecutionRequests)]
    (if (seq tcs)
      (json/read-str (.arguments ^Object (first tcs)) :key-fn keyword)
      {:density nil :emotion nil})))

;; --- Steps: ingestion + tree fetch (same shape as hn-shape) -----------------

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
                                  :n-nodes  @counter
                                  :ms       ms})
                 {:out [tree]}))))

;; --- Aggregator 1: rank top commenters from front-page trees ----------------

(defn- top-commenters [m parent-msgs]
  (->> parent-msgs
       (mapcat #(comment-nodes (:data %)))
       (keep :by)
       frequencies
       (sort-by val >)
       (take m)
       (mapv (fn [[user n]] {:user-id user :n-in-top-stories n}))))

(defn- rank-step
  "Aggregator: stash every tree msg via msg/drain; on the close cascade
   from the boundary input, emit M user-id msgs each merging from all N
   tree parents."
  [m]
  {:procs
   {:rank-commenters
    (step/handler-map
     {:ports         {:ins {:in ""} :outs {:out ""}}
      :on-init       (fn [] {:trees []})
      :on-data       (fn [ctx s _d]
                       [(update s :trees conj (:msg ctx)) msg/drain])
      :on-all-closed (fn [ctx s]
                       (let [parents (:trees s)
                             ranked  (top-commenters m parents)]
                         (trace/emit ctx {:event :ranked
                                          :n-trees (count parents)
                                          :n-users (count ranked)})
                         {:out (mapv #(msg/merge ctx parents %) ranked)}))})}
   :conns [] :in :rank-commenters :out :rank-commenters})

;; --- Per-user history fetch -------------------------------------------------

(defn- fetch-user-history [emit-item! {:keys [user-id] :as row} k]
  (let [submitted (or (:submitted (get-json (str base "/user/" user-id ".json"))) [])
        items     (->> submitted
                       (take (* 4 k))            ; over-sample to filter out non-comments
                       (mapv (fn [iid]
                               (.submit ^ExecutorService @vt-exec
                                        ^Callable
                                        (fn []
                                          (let [t0   (System/nanoTime)
                                                item (get-json (str base "/item/" iid ".json"))
                                                ms   (long (/ (- (System/nanoTime) t0) 1e6))]
                                            (emit-item! iid (:type item) ms)
                                            item)))))
                       (mapv #(.get %))
                       (filter #(= "comment" (:type %)))
                       (take k)
                       vec)]
    (assoc row :comments items)))

(defn- mk-fetch-user-step [k]
  (step/step :fetch-user nil
             (fn [ctx _s row]
               (let [t0         (System/nanoTime)
                     emit-item! (fn [iid item-type ms]
                                  (trace/emit ctx
                                              {:event     :fetch-user-item
                                               :user-id   (:user-id row)
                                               :id        iid
                                               :item-type item-type
                                               :ms        ms}))
                     out        (fetch-user-history emit-item! row k)
                     ms         (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :user-fetched
                                  :user-id (:user-id row)
                                  :n-comments (count (:comments out))
                                  :ms ms})
                 {:out [out]}))))

(def split-comments
  (step/step :split-comments nil
             (fn [ctx _s {:keys [user-id comments]}]
               {:out (msg/children ctx
                                   (mapv #(hash-map :user-id    user-id
                                                    :comment-id (:id %)
                                                    :time       (:time %)
                                                    :text       (or (:text %) ""))
                                         comments))})))

;; --- LLM scorer step --------------------------------------------------------

(defn- clip [s n]
  (if (and s (> (count s) n)) (str (subs s 0 n) "…") s))

(def score-step
  (step/step :score nil
             (fn [ctx _s {:keys [text] :as row}]
               (let [t0     (System/nanoTime)
                     scores (try (score-comment! text)
                                 (catch Throwable _ {:density nil :emotion nil}))
                     ms     (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event   :scored
                                  :user-id (:user-id row)
                                  :density (:density scores)
                                  :emotion (:emotion scores)
                                  :ms      ms})
                 {:out [(merge row scores)]}))))

;; --- Aggregator 2: per-user means + sample -----------------------------------

(defn- mean [xs] (when (seq xs) (double (/ (reduce + xs) (count xs)))))

(defn- stdev [xs]
  (when (seq xs)
    (let [m  (mean xs)
          v  (mean (map (fn [x] (let [d (- x m)] (* d d))) xs))]
      (Math/sqrt v))))

(defn- quadrant [d e]
  (when (and d e)
    (let [hi-d (>= d 5)
          hi-e (>= e 5)]
      (cond (and hi-d hi-e)             "high-density-high-emotion"
            (and hi-d (not hi-e))       "high-density-low-emotion"
            (and (not hi-d) hi-e)       "low-density-high-emotion"
            :else                       "low-density-low-emotion"))))

(defn- summarize-user [user-id rows]
  (let [scored      (filter #(and (:density %) (:emotion %)) rows)
        densities   (map :density scored)
        emotions    (map :emotion scored)
        d-mean      (mean densities)
        e-mean      (mean emotions)
        sample      (->> scored
                         (sort-by (fn [r] (- (Math/abs (- (or (:density r) 0) (or d-mean 0)))
                                             (Math/abs (- (or (:emotion r) 0) (or e-mean 0))))))
                         (take 5)
                         (mapv (fn [r] {:comment_id (:comment-id r)
                                        :density    (:density r)
                                        :emotion    (:emotion r)
                                        :preview    (clip (:text r) 200)})))]
    {:user_id                  user-id
     :n_scored_comments        (count scored)
     :info_density_mean        d-mean
     :info_density_std         (stdev densities)
     :emotional_intensity_mean e-mean
     :emotional_intensity_std  (stdev emotions)
     :quadrant                 (quadrant d-mean e-mean)
     :sample_comments          sample}))

(def aggregate-by-user
  "Aggregator: stash every scored comment under msg/drain. On the close
   cascade, group by user-id and emit one summary row per user via
   msg/merge over that user's parent envelopes."
  {:procs
   {:agg
    (step/handler-map
     {:ports         {:ins {:in ""} :outs {:out ""}}
      :on-init       (fn [] {:rows []})
      :on-data       (fn [ctx s _d]
                       [(update s :rows conj (:msg ctx)) msg/drain])
      :on-all-closed (fn [ctx s]
                       (let [grouped (group-by (comp :user-id :data) (:rows s))
                             out-msgs
                             (mapv (fn [[user-id par-msgs]]
                                     (msg/merge ctx par-msgs
                                                (summarize-user user-id
                                                                (map :data par-msgs))))
                                   grouped)]
                         (trace/emit ctx {:event :aggregated
                                          :n-users (count out-msgs)})
                         {:out out-msgs}))})}
   :conns [] :in :agg :out :agg})

;; --- Flow -------------------------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories m-commenters k-comments
            tree-workers user-workers llm-workers]
     :or   {n-stories     30
            m-commenters  50
            k-comments    30
            tree-workers  8
            user-workers  16
            llm-workers   8}}]
   (step/serial :hn-density
                (mk-fetch-top-ids n-stories)
                split-ids
                (c/stealing-workers :tree-fetchers tree-workers fetch-tree-step)
                (rank-step m-commenters)
                (c/workers :user-fetchers user-workers (mk-fetch-user-step k-comments))
                split-comments
                (c/stealing-workers :scorers llm-workers score-step)
                aggregate-by-user)))

;; --- Trace pretty-printer ---------------------------------------------------

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
                       (:user-id ev)          (str "user="  (:user-id ev) " ")
                       (:density ev)          (str "d="     (:density ev) " ")
                       (:emotion ev)          (str "e="     (:emotion ev) " ")
                       (:n-users ev)          (str "users=" (:n-users ev) " ")
                       (:n-comments ev)       (str "ncom="  (:n-comments ev) " ")
                       (:ms ev)               (str "ms="    (:ms ev) " ")
                       (contains? ev :data)   (str "data="  (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens="(preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./density.json" {}))
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
