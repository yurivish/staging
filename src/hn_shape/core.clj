(ns hn-shape.core
  "HN top stories → full reply tree → per-story shape and timing metrics.
   No LLM. Pure I/O + tree math.

   Demonstrates Datapotamus c/recursive-pool driving a recursive HN
   tree fetch via the inner step's :work output port. Each node fetch
   is a real per-step trace event, so the visualizer sees per-node
   fan-out and per-node failures.

   One-shot:
     clojure -M -e \"(require 'hn-shape.core) (hn-shape.core/run-once! \\\"shape.json\\\" {:trace? true})\""
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.pubsub :as pubsub]))

(def base "https://hacker-news.firebaseio.com/v0")

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

;; --- Tree reassembly --------------------------------------------------------

(defn- reassemble-tree
  "Given a flat collection of HN node maps (each with :id and :kids
   from upstream) keyed by id, walk from `root-id` down through :kids
   and return a nested map with `:kid-trees` populated. Nodes missing
   from the map (e.g., a fetch failure) become an absent slot —
   reassemble-tree returns nil for that branch, which the caller then
   filters out at the parent level."
  [root-id nodes]
  (let [by-id (into {} (map (juxt :id identity)) nodes)]
    ((fn rec [id]
       (when-let [n (by-id id)]
         (assoc n :kid-trees (vec (keep rec (or (:kids n) []))))))
     root-id)))

;; --- Shape metrics ----------------------------------------------------------

(defn- nodes [tree]
  (cons tree (mapcat nodes (:kid-trees tree))))

(defn- max-depth [tree]
  (if (seq (:kid-trees tree))
    (inc (apply max (map max-depth (:kid-trees tree))))
    0))

(defn- mean [xs]
  (when (seq xs) (double (/ (reduce + xs) (count xs)))))

(defn- percentile [xs p]
  (when (seq xs)
    (let [sorted (vec (sort xs))]
      (nth sorted (min (dec (count sorted))
                       (long (* p (count sorted))))))))

(defn- shape-row [tree]
  (let [t0           (:time tree)
        all          (nodes tree)
        comments     (rest all)
        with-kids    (filter #(seq (:kid-trees %)) all)
        edge-latencies (for [p all, k (:kid-trees p)] (- (:time k) (:time p)))
        first-replies (map #(- (:time %) t0) (:kid-trees tree))
        branches     (for [c (:kid-trees tree)]
                       {:size            (count (nodes c))
                        :first_commenter (:by c)
                        :time_to_burst_s (- (:time c) t0)})]
    {:story_id              (:id tree)
     :title                 (:title tree)
     :url                   (:url tree)
     :submitted_at_unix     t0
     :n_comments            (count comments)
     :max_depth             (max-depth tree)
     :branching_factor_mean (when (seq with-kids)
                              (mean (map #(count (:kid-trees %)) with-kids)))
     :widest_branch_size    (when (seq branches) (apply max (map :size branches)))
     :first_reply_latency_s (when (seq first-replies) (apply min first-replies))
     :p50_reply_latency_s   (percentile edge-latencies 0.5)
     :p95_reply_latency_s   (percentile edge-latencies 0.95)
     :time_span_hours       (when (seq comments)
                              (/ (- (apply max (map :time comments)) t0) 3600.0))
     :top_branches          (->> branches (sort-by :size >) (take 3)
                                 (map-indexed #(assoc %2 :rank %1)) vec)}))

;; --- Steps ------------------------------------------------------------------

(defn- mk-fetch-top-ids [n]
  (step/step :fetch-top-ids
             (fn [_tick]
               (vec (take n (get-json (str base "/topstories.json")))))))

;; Per-story split: each top-id becomes a `{:root id :id id}` entry —
;; the seed for that story's recursive fetch. The :root tag stays with
;; every recursively-emitted child so the aggregator can group nodes
;; by their originating story.
(def split-ids
  (step/step :split-ids nil
             (fn [ctx _s ids]
               {:out (msg/children ctx (mapv (fn [id] {:root id :id id}) ids))})))

;; The recursive worker. Receives `{:root story-id :id node-id}`,
;; fetches the HN item, emits one node-data envelope on :out and one
;; recursive request per kid on :work. c/recursive-pool routes :work
;; back into its own queue for any free worker to pick up.
(def fetch-node
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out "" :work ""}}
    :on-data
    (fn [ctx _s {:keys [root id]}]
      (let [item (get-json (str base "/item/" id ".json"))
            kids (or (:kids item) [])]
        {:out  [(msg/child ctx {:root root :node item})]
         :work (mapv (fn [kid-id]
                       (msg/child ctx {:root root :id kid-id}))
                     kids)}))}))

;; Buffer-until-close aggregator. Stash every node by its :root in
;; state; on `:on-all-closed`, reconstruct each root's tree from the
;; flat node set and emit one merged envelope per root.
(def aggregate-trees
  {:procs
   {:aggregate-trees
    (step/handler-map
     {:ports         {:ins {:in ""} :outs {:out ""}}
      :on-init       (fn [] {})
      :on-data       (fn [ctx s {:keys [root node]}]
                       [(update s root (fnil conj [])
                                {:msg (:msg ctx) :node node})
                        msg/drain])
      :on-all-closed (fn [ctx s]
                       {:out (vec (for [[root entries] s
                                        :let [parents (mapv :msg entries)
                                              nodes   (mapv :node entries)
                                              tree    (reassemble-tree root nodes)]
                                        :when tree]
                                    (msg/merge ctx parents tree)))})})}
   :conns [] :in :aggregate-trees :out :aggregate-trees})

(def compute-metrics (step/step :compute-metrics shape-row))

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories tree-workers]
     :or   {n-stories 30 tree-workers 8}}]
   (step/serial :hn-shape
                (mk-fetch-top-ids n-stories)
                split-ids
                (c/recursive-pool :tree-fetchers tree-workers fetch-node)
                aggregate-trees
                compute-metrics)))

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
                       (:event ev)            (str "event="   (name (:event ev)) " ")
                       (contains? ev :data)   (str "data="    (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens="  (preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./shape.json" {}))
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
     {:state (:state res) :count (count rows) :error (:error res)})))
