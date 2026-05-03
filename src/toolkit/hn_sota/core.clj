(ns toolkit.hn-sota.core
  "AI-coding-model popularity from HN comments.

   Pipeline shape:

     algolia-fetch        (one-shot tick → coll of ~50 story stubs)
       ↓ split-stories    (msg/children: one msg per story)
       ↓ filter-relevant  (pure: regex on title+url; emit {} otherwise)
       ↓ tree-fetch/step  (recursive HN comment tree, only stage with parallelism)
       ↓ flatten-comments (msg/children: one per comment, with story metadata stamped)
       ↓ scan-mentions    (pure: regex models + windowed sentiment;
                            msg/children: one msg per (model, comment) pair)
       ↓ aggregate-by-model (cumulative; last emission per model_id is final)

   Wrapped with `recorder` + `obs.store` so every event is persisted to a
   DuckDB file. The lineage helper at `toolkit.hn-sota.lineage` walks the
   trace from a final ranking row back to the contributing comments.

   One-shot:
     (require 'toolkit.hn-sota.core)
     (toolkit.hn-sota.core/run-once!
       {:out-path \"sota.json\" :db-path \"sota.duckdb\" :n-stories 50 :trace? true})"
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.obs.store :as store]
            [toolkit.datapotamus.recorder :as recorder]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.hn-sota.algolia :as algolia]
            [toolkit.hn-sota.models :as models]
            [toolkit.hn-sota.sentiment :as sentiment]
            [toolkit.hn.tree-fetch :as tree-fetch]
            [toolkit.pubsub :as pubsub]))

(def ^:private hn-base "https://hacker-news.firebaseio.com/v0")

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

;; ---- Steps -----------------------------------------------------------------

(defn- mk-algolia-fetch [n]
  (step/step :algolia-fetch
             (fn [_tick] (algolia/top-stories-24h n))))

(def split-stories
  (step/step :split-stories nil
             (fn [ctx _s stories]
               (if (empty? stories)
                 {}
                 {:out (msg/children ctx (vec stories))}))))

(def filter-relevant
  (step/step :filter-relevant nil
             (fn [ctx _s story]
               (let [keep? (models/relevant? story)]
                 (trace/emit ctx {:event :filter
                                  :story-id (:id story)
                                  :title (:title story)
                                  :keep? keep?})
                 (if keep?
                   ;; tree-fetch's :in is a coll of ids
                   {:out [[(:id story)]]}
                   {})))))

(defn- walk-comments
  "Walk a fetched HN tree and yield one map per comment node carrying
   the originating story's metadata. Drops dead/empty/missing comments."
  [tree]
  (let [story-id    (:id tree)
        story-title (:title tree)
        story-url   (:url tree)]
    (letfn [(walk [n]
              (lazy-seq
               (let [self (when (and (= "comment" (:type n))
                                     (not (:deleted n))
                                     (not (:dead n))
                                     (string? (:text n))
                                     (not (str/blank? (:text n))))
                            [{:story-id     story-id
                              :story-title  story-title
                              :story-url    story-url
                              :comment-id   (:id n)
                              :comment-by   (:by n)
                              :comment-text (:text n)}])]
                 (concat self (mapcat walk (:kid-trees n))))))]
      (mapcat walk (:kid-trees tree)))))

(def flatten-comments
  (step/step :flatten-comments nil
             (fn [ctx _s tree]
               (let [comments (vec (walk-comments tree))]
                 (trace/emit ctx {:event :flatten
                                  :story-id (:id tree)
                                  :n-comments (count comments)})
                 (if (empty? comments)
                   {}
                   {:out (msg/children ctx comments)})))))

(defn- mentions-with-sentiment
  "For one comment, return a vector of `{:model_id :raw :sentiment}` —
   one entry per alias hit found in the comment text."
  [{:keys [comment-text]}]
  (->> (models/scan comment-text)
       (mapv (fn [{:keys [model_id raw start]}]
               {:model_id  model_id
                :raw       raw
                :sentiment (or (sentiment/score-around comment-text start) 0)}))))

(def scan-mentions
  "Scan the comment for model aliases; for each hit emit one downstream
   msg carrying the comment's metadata + the (model_id, sentiment, raw)
   triple. Combining scan + explode in one step makes each (model,
   comment) pair a distinct trace span — that's the lineage primitive
   the demo surfaces."
  (step/step :scan-mentions nil
             (fn [ctx _s comment]
               (let [hits (mentions-with-sentiment comment)
                     payloads (mapv (fn [m]
                                      (merge (select-keys comment
                                                          [:story-id :story-title :story-url
                                                           :comment-id :comment-by :comment-text])
                                             m))
                                    hits)]
                 (trace/emit ctx {:event :scan
                                  :story-id (:story-id comment)
                                  :comment-id (:comment-id comment)
                                  :n-mentions (count hits)})
                 (if (empty? payloads)
                   {}
                   {:out (msg/children ctx payloads)})))))

(defn- summarize-model [model-id par-msgs]
  (let [rows  (mapv :data par-msgs)
        sents (mapv :sentiment rows)
        n     (count rows)
        n-pos (count (filter pos? sents))
        n-neg (count (filter neg? sents))
        n-neu (- n n-pos n-neg)
        sum   (reduce + 0 sents)]
    {:model_id        model-id
     :mentions        n
     :mean_sentiment  (if (zero? n) 0.0 (double (/ sum n)))
     :n_pos           n-pos
     :n_neg           n-neg
     :n_neu           n-neu
     :sample_comments (->> rows
                           (take-last 5)
                           (mapv (fn [r]
                                   {:story_id   (:story-id r)
                                    :comment_id (:comment-id r)
                                    :sentiment  (:sentiment r)
                                    :raw        (:raw r)})))}))

(def aggregate-by-model
  "Cumulative aggregator: on each mention msg, accumulate it and emit the
   current cumulative summary for that mention's model. Lineage is
   preserved via msg/merge of all mention msgs seen so far for the
   model. The last emission per model is the final ranking row;
   quiescence (counter balance) is what tells the caller no more
   emissions are coming. Mirrors `hn-typing.core/aggregate-by-story`."
  {:procs
   {:aggregate
    (step/handler-map
     {:ports   {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:rows []})
      :on-data (fn [ctx s _d]
                 (let [s'        (update s :rows conj (:msg ctx))
                       model-id  (-> ctx :msg :data :model_id)
                       par-msgs  (filterv #(= model-id (-> % :data :model_id))
                                          (:rows s'))
                       summary   (summarize-model model-id par-msgs)]
                   [s' {:out [(msg/merge ctx par-msgs summary)]}]))})}
   :conns [] :in :aggregate :out :aggregate})

;; ---- Flow construction -----------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories tree-workers tree-get-json]
     :or   {n-stories 50 tree-workers 8 tree-get-json get-json}}]
   (step/serial :hn-sota
                (mk-algolia-fetch n-stories)
                split-stories
                filter-relevant
                (tree-fetch/step {:k tree-workers :get-json tree-get-json})
                flatten-comments
                scan-mentions
                aggregate-by-model)))

;; ---- Final-ranking extraction ----------------------------------------------

(defn- last-per-model
  "Given the seq of cumulative rows emitted by `aggregate-by-model`,
   keep only the last (and therefore final) emission per model_id."
  [rows]
  (->> rows
       (reduce (fn [acc r] (assoc acc (:model_id r) r)) {})
       vals
       (sort-by :mentions >)
       vec))

;; ---- Trace fixture ---------------------------------------------------------

(defn- preview [v]
  (let [s (pr-str v)] (if (> (count s) 80) (str (subs s 0 77) "...") s)))

(defn- print-event [_subj ev _match]
  (locking *out*
    (println (format "[%-9s %-6s] %-40s %s"
                     (name (:kind ev))
                     (or (some-> (:msg-kind ev) name) "")
                     (str (:step-id ev) (when-let [p (:port ev)] (str " → " p)))
                     (cond-> ""
                       (:event ev)            (str "event=" (name (:event ev)) " ")
                       (:story-id ev)         (str "story=" (:story-id ev) " ")
                       (:comment-id ev)       (str "comment=" (:comment-id ev) " ")
                       (:n-comments ev)       (str "comments=" (:n-comments ev) " ")
                       (:n-mentions ev)       (str "mentions=" (:n-mentions ev) " ")
                       (contains? ev :keep?)  (str "keep=" (:keep? ev) " ")
                       (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

;; ---- run-once! -------------------------------------------------------------

(defn run-once!
  "Run the pipeline once. Optionally:
     :out-path   spit a JSON ranking array to this path
     :db-path    persist the full trace to a DuckDB file at this path
     :n-stories  number of Algolia stories to fetch (default 50)
     :tree-workers  parallel HN tree-fetch workers (default 8)
     :trace?     stream events to stdout
     :pubsub     supply your own pubsub (else one is made if :trace? on)

   Returns `{:state :count :rows :error}`."
  ([] (run-once! {}))
  ([{:keys [out-path db-path trace? pubsub] :as opts}]
   (let [flow      (build-flow opts)
         ps        (or pubsub (when (or trace? db-path) (pubsub/make)))
         unsub     (when trace? (pubsub/sub ps [:>] print-event))
         run-meta  (when db-path (store/make-run-meta flow {:workflow-id "hn-sota"}))
         rec       (when db-path (recorder/start-recorder! ps run-meta))
         result    (flow/run-seq flow [:tick]
                                 (cond-> {} ps (assoc :pubsub ps)))
         _         (when unsub (unsub))
         rows      (last-per-model (or (first (:outputs result)) []))]
     (when rec
       (let [trace0 ((:stop rec))
             trace  (-> trace0
                        (update :run store/finalize-run-meta)
                        (assoc-in [:run :status]
                                  (if (= :completed (:state result)) "ok" "error")))]
         (store/flush-run! db-path trace)))
     (when (and (= :completed (:state result)) out-path)
       (spit out-path (with-out-str (json/pprint rows))))
     {:state (:state result) :count (count rows) :rows rows :error (:error result)})))
