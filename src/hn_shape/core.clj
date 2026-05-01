(ns hn-shape.core
  "HN top stories → full reply tree → per-story shape and timing metrics.
   No LLM. Pure I/O + tree math.

   Demonstrates Datapotamus stealing-workers (story-tree fetch cost varies
   100x), trace/emit progress events, and named-subflow scoping.

   One-shot:
     clojure -M -e \"(require 'hn-shape.core) (hn-shape.core/run-once! \\\"shape.json\\\" {:trace? true})\""
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.pubsub :as pubsub])
  (:import [java.util.concurrent Executors ExecutorService]))

(def base "https://hacker-news.firebaseio.com/v0")

(defonce ^:private vt-exec
  (delay (Executors/newVirtualThreadPerTaskExecutor)))

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

(defn- fetch-tree
  "Fetch HN item `id` and recursively all kids in parallel via virtual
   threads. Each node returned with its `:kid-trees` populated. `counter`
   is bumped per fetched node, surfacing live progress to the caller.
   `emit-node!` is invoked per node with [id n-kids fetch-ms] so the
   caller can publish per-node status events into the scoped pubsub."
  [emit-node! counter id]
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

(def compute-metrics (step/step :compute-metrics shape-row))

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories tree-workers]
     :or   {n-stories 30 tree-workers 8}}]
   (step/serial :hn-shape
                (mk-fetch-top-ids n-stories)
                split-ids
                (c/stealing-workers :tree-fetchers tree-workers fetch-tree-step)
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
                       (:story-id ev)         (str "story="   (:story-id ev) " ")
                       (:n-nodes ev)          (str "n-nodes=" (:n-nodes ev) " ")
                       (:ms ev)               (str "ms="      (:ms ev) " ")
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
