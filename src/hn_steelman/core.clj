(ns hn-steelman.core
  "Per reply edge in front-page threads, classify whether the reply
   engages with the strongest reasonable reading of the parent
   (steelman), a charitable but weaker reading (charitable / neutral),
   or a weaker version (strawman). Two-stage pipeline: hn_typing's
   Haiku edge classifier prunes to disagree/correct/attack edges, then
   Sonnet judges the steelmanning quality of each survivor.

   Data path:
     fetch-top-ids → tree-fetch → pick-edges (msg/children per edge)
     edge-classify (Haiku — same prompt as hn_typing)
     filter on :include-classes
     steelman-judge (Sonnet)
     final-collector (per-story, sort by engagement + confidence)

   One-shot:
     clojure -M -e \"(require 'hn-steelman.core) (hn-steelman.core/run-once! \\\"steelman.json\\\" {:trace? true})\""
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.hn.tree-fetch :as tree-fetch]
            [toolkit.pubsub :as pubsub])
  (:import [dev.langchain4j.model.anthropic AnthropicChatModel]
           [dev.langchain4j.model.chat.request ChatRequest]
           [dev.langchain4j.model.chat.request.json JsonObjectSchema JsonEnumSchema]
           [dev.langchain4j.agent.tool ToolSpecification]
           [dev.langchain4j.data.message UserMessage SystemMessage]))

(def base "https://hacker-news.firebaseio.com/v0")
(def haiku  "claude-haiku-4-5")
(def sonnet "claude-sonnet-4-6")

(def edge-types ["agree" "disagree" "correct" "extend" "tangent" "attack" "clarify"])

(def default-include-classes #{"disagree" "correct" "attack"})

(def engagement-rank
  "Lower rank = displayed first. Steelman and strawman are the
   interesting extremes; charitable/neutral/not-applicable trail."
  {"steelman"       0
   "strawman"       1
   "charitable"     2
   "neutral"        3
   "not-applicable" 4})

(defn get-json
  "URL → JSON map. Stub-friendly."
  [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

;; --- Edge selection -------------------------------------------------------

(defn comment-edges
  "All parent→child edges where parent is a comment (not the story root)."
  [tree]
  (mapcat (fn walk [n]
            (concat
              (when (not= "story" (:type n))
                (for [k (:kid-trees n)]
                  {:parent-text (or (:text n) "")
                   :kid-text    (or (:text k) "")
                   :parent-id   (:id n)
                   :parent-author (:by n)
                   :kid-id      (:id k)
                   :kid-author  (:by k)}))
              (mapcat walk (:kid-trees n))))
          (:kid-trees tree)))

;; --- LLM clients ----------------------------------------------------------

(defonce ^:private edge-model
  (delay (-> (AnthropicChatModel/builder)
             (.apiKey (str/trim (slurp "claude.key")))
             (.modelName haiku)
             (.maxTokens (int 64))
             .build)))

(defonce ^:private judge-model
  (delay (-> (AnthropicChatModel/builder)
             (.apiKey (str/trim (slurp "claude.key")))
             (.modelName sonnet)
             (.maxTokens (int 384))
             .build)))

;; --- Edge classifier (same as hn_typing) ----------------------------------

(def ^:private edge-schema
  (-> (JsonObjectSchema/builder)
      (.addStringProperty
       "edge_type"
       (str "EXACTLY one of: agree, disagree, correct, extend, tangent, attack, clarify."))
      (.required ["edge_type"])
      .build))

(def ^:private edge-tool
  (-> (ToolSpecification/builder)
      (.name "submit_edge_type")
      (.description "Submit your classification of the reply.")
      (.parameters edge-schema)
      .build))

(defn llm-edge-classify!
  "Classify reply→parent relationship. Returns one of the edge-type
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
          tcs (-> @edge-model (.chat req) .aiMessage .toolExecutionRequests)]
      (when (seq tcs)
        (let [t (-> (json/read-str (.arguments ^Object (first tcs)) :key-fn keyword)
                    :edge_type str/lower-case str/trim)]
          (when ((set edge-types) t) t))))
    (catch Throwable _ nil)))

;; --- Steelman judge (Sonnet) ----------------------------------------------

(def ^:private judge-schema
  (-> (JsonObjectSchema/builder)
      (.addProperty "engagement"
                    (-> (JsonEnumSchema/builder)
                        (.enumValues ["steelman" "charitable" "neutral"
                                      "strawman" "not-applicable"])
                        .build))
      (.addNumberProperty "confidence" "[0,1].")
      (.addStringProperty "parent_strongest_reading" "≤ 25 words.")
      (.addStringProperty "what_reply_addressed" "≤ 25 words.")
      (.addStringProperty "gap_summary" "≤ 30 words.")
      (.required ["engagement" "confidence" "parent_strongest_reading"
                  "what_reply_addressed" "gap_summary"])
      .build))

(def ^:private judge-tool
  (-> (ToolSpecification/builder)
      (.name "submit_engagement")
      (.description "Submit your engagement assessment.")
      (.parameters judge-schema)
      .build))

(def ^:private judge-system
  "You are evaluating one HN comment exchange: a parent and a reply that pushes back. Decide whether the reply engages with the STRONGEST reasonable reading of the parent (steelman / charitable), neutrally engages, or attacks a WEAKER reading (strawman). State explicitly the strongest reading of the parent and what specifically the reply addressed; the gap between those is the answer. You MUST respond by calling submit_engagement.")

(defn- ->kw-map [m]
  (reduce-kv (fn [a k v] (assoc a (keyword (str/replace (name k) #"_" "-")) v))
             {} m))

(defn- clip [s n]
  (if (and s (> (count s) n)) (str (subs s 0 n) "…") s))

(defn llm-steelman-judge!
  "Sonnet steelman judge. Returns judgment map. Stub-friendly."
  [{:keys [parent-text kid-text story-title]}]
  (try
    (let [user-msg (str (when story-title (str "Story: " story-title "\n\n"))
                        "PARENT:\n" (clip (or parent-text "") 600)
                        "\n\nREPLY:\n" (clip (or kid-text "") 600))
          req (-> (ChatRequest/builder)
                  (.messages [(SystemMessage/from judge-system)
                              (UserMessage/from user-msg)])
                  (.toolSpecifications [judge-tool])
                  .build)
          tcs (-> @judge-model (.chat req) .aiMessage .toolExecutionRequests)]
      (if (seq tcs)
        (->kw-map (json/read-str (.arguments ^Object (first tcs)) :key-fn keyword))
        {:engagement "not-applicable" :confidence 0.0
         :parent-strongest-reading "" :what-reply-addressed ""
         :gap-summary ""}))
    (catch Throwable _
      {:engagement "not-applicable" :confidence 0.0
       :parent-strongest-reading "" :what-reply-addressed ""
       :gap-summary ""})))

;; --- Sorting --------------------------------------------------------------

(defn sort-rows
  "Sort by engagement priority asc (lower rank = first), then by
   confidence desc within the same engagement."
  [rows]
  (->> rows
       (sort-by (fn [r] [(get engagement-rank (:engagement r) 99)
                         (- (or (:confidence r) 0))]))
       vec))

;; --- Steps ----------------------------------------------------------------

(defn- mk-fetch-top-ids [n]
  (step/step :fetch-top-ids
             (fn [_tick]
               (vec (take n (get-json (str base "/topstories.json")))))))

(def pick-edges-step
  (step/step :pick-edges nil
             (fn [ctx _s tree]
               (let [edges (->> (comment-edges tree)
                                (filter #(seq (:parent-text %)))
                                (filter #(seq (:kid-text %)))
                                (mapv #(assoc %
                                              :story-id    (:id tree)
                                              :story-title (:title tree)
                                              :story-url   (:url tree))))]
                 (trace/emit ctx {:event :picked
                                  :story-id (:id tree)
                                  :n-edges (count edges)})
                 (if (empty? edges)
                   {:out []}
                   {:out (msg/children ctx edges)})))))

(def edge-classify-step
  (step/step :edge-classify nil
             (fn [ctx _s edge]
               (let [t0 (System/nanoTime)
                     c  (try (llm-edge-classify! edge) (catch Throwable _ nil))
                     ms (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :edge-classified
                                  :kid-id (:kid-id edge) :class c :ms ms})
                 {:out [(msg/child ctx (assoc edge :class c))]}))))

(defn- mk-include-filter [include-classes]
  (let [include? (set (map str include-classes))]
    (step/step :include-filter nil
               (fn [ctx _s edge]
                 (if (include? (:class edge))
                   {:out [(msg/child ctx edge)]}
                   {:out []})))))

(def steelman-judge-step
  (step/step :steelman-judge nil
             (fn [ctx _s edge]
               (let [t0 (System/nanoTime)
                     j  (llm-steelman-judge! edge)
                     ms (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :judged
                                  :kid-id (:kid-id edge)
                                  :engagement (:engagement j) :ms ms})
                 {:out [(msg/child ctx (merge edge j))]}))))

(def final-collector
  "Buffer all judged rows; on close emit them sorted by engagement
   priority + confidence."
  {:procs
   {:final
    (step/handler-map
     {:ports {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:rows []})
      :on-data (fn [ctx s row]
                 [(update s :rows conj {:msg (:msg ctx) :row row}) {}])
      :on-all-closed
      (fn [ctx s]
        (let [sorted (sort-rows (mapv :row (:rows s)))
              by-row (zipmap (mapv :row (:rows s)) (mapv :msg (:rows s)))
              clean  (fn [r]
                       {:story-id      (:story-id r)
                        :story-title   (:story-title r)
                        :story-url     (:story-url r)
                        :parent-id     (:parent-id r)
                        :reply-id      (:kid-id r)
                        :parent-author (:parent-author r)
                        :reply-author  (:kid-author r)
                        :class         (:class r)
                        :engagement    (:engagement r)
                        :confidence    (:confidence r)
                        :parent-strongest-reading (:parent-strongest-reading r)
                        :what-reply-addressed (:what-reply-addressed r)
                        :gap-summary   (:gap-summary r)
                        :parent-preview (clip (:parent-text r) 200)
                        :reply-preview  (clip (:kid-text r) 200)})
              out    (mapv (fn [r]
                             (msg/merge ctx [(get by-row r)] (clean r)))
                           sorted)]
          (trace/emit ctx {:event :finalized :n-rows (count out)})
          {:out out}))})}
   :conns [] :in :final :out :final})

;; --- Flow ----------------------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories tree-workers workers include-classes]
     :or   {n-stories 30 tree-workers 8 workers 8
            include-classes default-include-classes}
     :as opts}]
   (step/serial :hn-steelman
                (mk-fetch-top-ids n-stories)
                (tree-fetch/step {:k tree-workers :get-json get-json})
                pick-edges-step
                (c/stealing-workers :edge-classifiers workers edge-classify-step)
                (mk-include-filter include-classes)
                (c/stealing-workers :judges workers steelman-judge-step)
                final-collector)))

;; --- Trace pretty-printer ------------------------------------------------

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
                       (:kid-id ev)           (str "kid=" (:kid-id ev) " ")
                       (:class ev)            (str "class=" (:class ev) " ")
                       (:engagement ev)       (str "eng=" (:engagement ev) " ")
                       (:n-edges ev)          (str "edges=" (:n-edges ev) " ")
                       (:n-rows ev)           (str "rows=" (:n-rows ev) " ")
                       (:ms ev)               (str "ms=" (:ms ev) " ")
                       (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./steelman.json" {}))
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
      :n-rows (count (or rows []))
      :error (:error res)})))
