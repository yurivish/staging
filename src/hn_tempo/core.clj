(ns hn-tempo.core
  "HN top stories → full reply tree → three parallel timing analyses
   (burstiness, time-of-day tempo, conversation flow) → one combined row
   per story.

   Demonstrates Datapotamus c/parallel scatter-gather: one input fans
   out to a map of named analyzer steps and is gathered back into a
   {port data} map automatically. No LLM.

   One-shot:
     clojure -M -e \"(require 'hn-tempo.core) (hn-tempo.core/run-once! \\\"tempo.json\\\" {:trace? true})\""
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators.core :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]
            [toolkit.hn.tree-fetch :as tree-fetch]
            [toolkit.pubsub :as pubsub])
  (:import [java.time Instant ZoneOffset]))

(def base "https://hacker-news.firebaseio.com/v0")

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

(defn- nodes [tree]
  (cons tree (mapcat nodes (:kid-trees tree))))

(defn- mean [xs] (when (seq xs) (double (/ (reduce + xs) (count xs)))))

;; --- Three analyzers --------------------------------------------------------
;; c/parallel feeds the same tree to each. The gathered output is keyed
;; by the analyzer's port. One analyzer (burst) echoes the story header
;; so the assemble step can pull title/url out of the gathered map.

(defn- burst-analysis [tree]
  (let [comments (rest (nodes tree))
        times    (sort (map :time comments))
        header   {:story_id (:id tree) :title (:title tree)
                  :url (:url tree) :submitted_at_unix (:time tree)
                  :n_comments (count times)}]
    (if (< (count times) 3)
      header
      (let [gaps    (mapv #(- %2 %1) times (rest times))
            avg     (mean gaps)
            sd      (Math/sqrt (mean (map (fn [g] (let [d (- g avg)] (* d d))) gaps)))
            peak-5m (loop [ts times peak 0]
                      (if (empty? ts) peak
                          (let [t0 (first ts)
                                in-window (count (take-while #(<= (- % t0) 300) ts))]
                            (recur (rest ts) (max peak in-window)))))
            span     (- (last times) (first times))
            expected (when (pos? span) (* 300 (/ (count times) span)))]
        (assoc header
               :inter_arrival_p50 (nth (vec gaps) (quot (count gaps) 2))
               :inter_arrival_mean avg
               :coefficient_of_variation (when (pos? avg) (/ sd avg))
               :peak_5min_count peak-5m
               :peak_vs_expected (when expected (double (/ peak-5m expected))))))))

(defn- tempo-analysis [tree]
  (let [comments (rest (nodes tree))
        hours    (mapv (fn [c]
                         (-> ^long (:time c)
                             Instant/ofEpochSecond
                             (.atZone ZoneOffset/UTC)
                             .getHour))
                       comments)
        by-hour  (frequencies hours)
        peak-h   (when (seq by-hour) (key (apply max-key val by-hour)))
        windows  (for [h (range 24)]
                   (apply + (map #(get by-hour (mod (+ h %) 24) 0) (range 6))))
        peak-6h-frac (when (seq comments)
                       (double (/ (apply max windows) (count comments))))]
    {:peak_hour_utc    peak-h
     :peak_6h_fraction peak-6h-frac
     :hour_histogram   (into (sorted-map) by-hour)}))

(defn- flow-analysis [tree]
  (let [d1   (count (:kid-trees tree))
        all  (count (rest (nodes tree)))
        deep (- all d1)
        max-thread-depth
        (letfn [(depth [n]
                  (if (seq (:kid-trees n))
                    (inc (apply max (map depth (:kid-trees n)))) 0))]
          (depth tree))]
    {:n_top_level      d1
     :n_deeper         deep
     :depth_to_breadth (when (pos? d1) (double (/ deep d1)))
     :max_thread_depth max-thread-depth}))

;; --- Steps ------------------------------------------------------------------

(defn- mk-fetch-top-ids [n]
  (step/step :fetch-top-ids
             (fn [_tick]
               (vec (take n (get-json (str base "/topstories.json")))))))

(def burst-step (step/step :burst burst-analysis))
(def tempo-step (step/step :tempo tempo-analysis))
(def flow-step  (step/step :flow  flow-analysis))

(def assemble
  (step/step :assemble nil
             (fn [_ctx _s {:keys [burst tempo flow]}]
               {:out [(-> burst
                          (assoc :tempo tempo)
                          (assoc :flow  flow))]})))

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories tree-workers]
     :or   {n-stories 30 tree-workers 8}}]
   (step/serial :hn-tempo
                (mk-fetch-top-ids n-stories)
                (tree-fetch/step {:k tree-workers :get-json get-json})
                (c/parallel :analyzers
                            {:burst burst-step
                             :tempo tempo-step
                             :flow  flow-step})
                assemble)))

;; --- Trace pretty-printer + run-once! ---------------------------------------

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
                       (:n-nodes ev)          (str "n=" (:n-nodes ev) " ")
                       (:ms ev)               (str "ms=" (:ms ev) " ")
                       (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./tempo.json" {}))
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
