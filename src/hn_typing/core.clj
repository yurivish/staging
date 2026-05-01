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
            [toolkit.hn.tree-fetch :as tree-fetch]
            [toolkit.llm.cli :as llm]
            [toolkit.pubsub :as pubsub]))

(def base "https://hacker-news.firebaseio.com/v0")
(def haiku "claude-haiku-4-5")
(def edge-types ["agree" "disagree" "correct" "extend" "tangent" "attack" "clarify"])

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

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

(def ^:private edge-schema
  {:type "object"
   :properties {:edge_type {:type "string"
                            :enum edge-types
                            :description (str "Pick the dominant relationship the reply has to the parent. "
                                              "agree: confirms or amplifies the parent's view. "
                                              "disagree: argues against the parent's view. "
                                              "correct: points out a factual error. "
                                              "extend: builds on the parent with more info. "
                                              "tangent: changes the topic. "
                                              "attack: ad hominem or hostile. "
                                              "clarify: asks for or offers clarification.")}}
   :required ["edge_type"]})

(def ^:private edge-system
  "Classify the reply's relationship to the parent comment. Pick exactly one of: agree, disagree, correct, extend, tangent, attack, clarify.")

(defn- classify-edge! [{:keys [parent-text kid-text]}]
  (let [user-msg (str "PARENT:\n" (str/trim parent-text)
                      "\n\nREPLY:\n" (str/trim kid-text))
        r (llm/call-json! {:system edge-system
                           :user   user-msg
                           :schema edge-schema
                           :model  haiku
                           :keys   :snake})]
    (when r
      (let [t (some-> r :edge_type str/lower-case str/trim)]
        (when ((set edge-types) t) t)))))

;; --- Steps ------------------------------------------------------------------

(defn- mk-fetch-top-ids [n]
  (step/step :fetch-top-ids
             (fn [_tick]
               (vec (take n (get-json (str base "/topstories.json")))))))

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
  "Aggregator: on each classified edge, accumulate it and emit the
   current cumulative summary for that edge's story. Lineage is
   preserved via msg/merge of all edges seen so far for the story.
   The last emission per story is the final summary — quiescence
   (counter balance) is what tells the caller that no more emissions
   are coming."
  {:procs
   {:agg
    (step/handler-map
     {:ports   {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:rows []})
      :on-data (fn [ctx s _d]
                 (let [s'        (update s :rows conj (:msg ctx))
                       story-id  (-> ctx :msg :data :story-id)
                       par-msgs  (filterv #(= story-id (-> % :data :story-id))
                                          (:rows s'))
                       summary   (summarize-story story-id par-msgs)]
                   [s' {:out [(msg/merge ctx par-msgs summary)]}]))})}
   :conns [] :in :agg :out :agg})

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories tree-workers max-edges llm-workers]
     :or   {n-stories 30 tree-workers 8 max-edges 8 llm-workers 8}}]
   (step/serial :hn-typing
                (mk-fetch-top-ids n-stories)
                (tree-fetch/step {:k tree-workers :get-json get-json})
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
