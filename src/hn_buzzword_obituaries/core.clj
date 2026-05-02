(ns hn-buzzword-obituaries.core
  "Time-series term frequency on Hacker News, via Algolia's nbHits.

   For each (term × month-bucket) the pipeline issues one Algolia
   search with hitsPerPage=0 — Algolia returns the count without
   any results, so each cell is one cheap HTTP roundtrip. Fan-out
   is the cross-product; the work units are tiny.

   Trajectory classification (`:buried`, `:fading`, `:sustained`,
   `:rising`) is a pure post-step over each term's series.

   No LLM. Mac-mini-trivial.

   One-shot:
     clojure -M -e \"(require 'hn-buzzword-obituaries.core) (hn-buzzword-obituaries.core/run-once! \\\"buzzwords.json\\\" {:trace? true})\""
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators.aggregate :as ca]
            [toolkit.datapotamus.combinators.workers :as cw]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.pubsub :as pubsub])
  (:import [java.time LocalDate ZoneOffset]
           [java.time.temporal ChronoUnit]))

(def algolia-base "https://hn.algolia.com/api/v1/search")

(def default-terms
  ["web3" "blockchain" "metaverse"
   "GPT-3" "GPT-4" "LLM" "agentic" "AGI" "RAG"
   "kubernetes" "k8s" "docker" "serverless" "microservices" "monorepo"
   "rust" "go" "elm" "clojure" "wasm" "WebAssembly"
   "edge computing" "5G" "AR/VR"
   "deep learning" "machine learning" "big data"
   "NoSQL" "GraphQL" "TypeScript" "Svelte" "React" "Vue"])

;; --- Date / bucket helpers --------------------------------------------------

(defn- ^:private parse-date
  "ISO yyyy-MM-dd → LocalDate."
  [s] (LocalDate/parse s))

(defn- ^:private epoch
  "LocalDate (UTC midnight) → epoch seconds."
  [^LocalDate d]
  (.toEpochSecond (.atStartOfDay d ZoneOffset/UTC)))

(defn- ^:private bucket-label
  "Format a bucket-start LocalDate as `YYYY-MM` for :month, `YYYY-Qq`
   for :quarter."
  [^LocalDate d bucket]
  (case bucket
    :month   (format "%04d-%02d" (.getYear d) (.getMonthValue d))
    :quarter (format "%04d-Q%d"  (.getYear d) (inc (quot (dec (.getMonthValue d)) 3)))))

(defn- ^:private next-bucket
  [^LocalDate d bucket]
  (case bucket
    :month   (.plusMonths d 1)
    :quarter (.plusMonths d 3)))

(defn- ^:private bucket-start
  "Snap to the start of the bucket containing `d`."
  [^LocalDate d bucket]
  (case bucket
    :month   (.withDayOfMonth d 1)
    :quarter (let [m  (.getMonthValue d)
                   q1 (- m (mod (dec m) 3))]
               (.withMonth (.withDayOfMonth d 1) q1))))

(defn buckets
  "Lazy seq of `{:label \"...\" :lo <epoch> :hi <epoch>}` over the half-
   open `[since, until)` window, snapped to bucket boundaries. `until`
   defaults to today."
  ([since bucket]
   (buckets since (LocalDate/now ZoneOffset/UTC) bucket))
  ([since until bucket]
   (let [since (cond-> since (string? since) parse-date)
         until (cond-> until (string? until) parse-date)]
     (loop [acc (transient []) cur (bucket-start since bucket)]
       (if (.isBefore cur until)
         (let [nxt (next-bucket cur bucket)
               hi  (.minusDays nxt 1)]
           (recur (conj! acc {:label (bucket-label cur bucket)
                              :lo    (epoch cur)
                              :hi    (epoch hi)})
                  nxt))
         (persistent! acc))))))

;; --- Algolia count ----------------------------------------------------------

(def ^:private scope->tag
  {:comments "comment"
   :stories  "story"
   :both     nil})

(defn- ^:private algolia-url
  [term scope lo hi]
  (let [tag (scope->tag scope)
        params (cond-> [(str "query=" (java.net.URLEncoder/encode term "UTF-8"))
                        (str "numericFilters="
                             (java.net.URLEncoder/encode
                              (str "created_at_i>=" lo ",created_at_i<=" hi)
                              "UTF-8"))
                        "hitsPerPage=0"]
                 tag (conj (str "tags=" tag)))]
    (str algolia-base "?" (str/join "&" params))))

(defn- algolia-count
  "Issue one Algolia search for `term` over the [lo,hi] epoch window
   under `scope` (`:comments`, `:stories`, `:both`); return the
   `nbHits` count, or `nil` on transport error. Never throws."
  [term scope lo hi]
  (try
    (let [{:keys [status body error]}
          @(http/get (algolia-url term scope lo hi)
                     {:timeout 10000 :as :text :follow-redirects true})]
      (when (and (nil? error) (= 200 status) (string? body))
        (:nbHits (json/read-str body :key-fn keyword))))
    (catch Throwable _ nil)))

;; --- Trajectory classification ----------------------------------------------

(defn classify-trajectory
  "Given a sorted-by-bucket series `[{:label ... :n N} ...]`, return
   one of `:buried`, `:fading`, `:sustained`, `:rising`, or `:flat`
   (the latter for series that never had a peak — typical for terms
   that simply don't trend on HN).

   Adaptation from the plan: `:rising` is interpreted as 'recent
   activity dwarfs older activity' (the peak is in the recent
   window, and recent-mean is meaningfully larger than older-mean).
   The plan's literal `current > 1.5 × peak` is unsatisfiable —
   `current ≤ peak` by definition — so we read it as the operative
   intent."
  [series]
  (let [series  (vec series)
        n-buck  (count series)
        peak    (when (seq series) (apply max-key :n series))
        peak-n  (or (:n peak) 0)
        peak-ix (when peak
                  (some (fn [[i b]] (when (= (:label b) (:label peak)) i))
                        (map-indexed vector series)))
        cur-n   (or (:n (last series)) 0)
        old     (when (>= n-buck 6) (drop-last 6 series))
        old-min (when (seq old) (apply min (map :n old)))
        gap     (when peak-ix (- (dec n-buck) peak-ix))]
    (cond
      (zero? peak-n) :flat
      ;; Rising: peak is in the most-recent 6 buckets AND the older
      ;; window had at least one bucket far below peak (i.e. the
      ;; series grew into its current scale rather than holding it).
      (and peak-ix gap (<= gap 6)
           old-min (< old-min (* 0.2 peak-n)))
      :rising
      (and gap (>= gap 12) (<= cur-n (* 0.1 peak-n))) :buried
      (and gap (>= gap 6)
           (< (* 0.1 peak-n) cur-n)
           (<= cur-n (* 0.5 peak-n)))
      :fading
      (and gap (<= gap 12)
           (< (* 0.5 peak-n) cur-n)
           (<= cur-n (* 1.5 peak-n)))
      :sustained
      :else :fading)))

(defn summarize-term
  "Sort a term's bucket counts by label and produce the output row."
  [term scope cells]
  (let [series   (->> cells
                      (sort-by :label)
                      (mapv (fn [c] {:label (:label c) :n (or (:n c) 0)})))
        peak     (when (seq series) (apply max-key :n series))
        current  (last series)
        traj     (classify-trajectory series)]
    {:term       term
     :scope      scope
     :n-total    (reduce + 0 (map :n series))
     :peak       peak
     :current    current
     :trajectory traj
     :series     series}))

;; --- Steps ------------------------------------------------------------------

(defn- mk-emit-cells
  "First step: takes :tick, emits one envelope per (term × bucket)
   cell. The whole cross-product is the unit of work for the
   downstream pool."
  [{:keys [terms since until bucket scope]}]
  (let [bs (buckets since until bucket)]
    (step/step :emit-cells nil
               (fn [ctx _s _tick]
                 {:out (msg/children ctx
                                     (vec (for [t  terms
                                                b  bs]
                                            {:term  t :scope scope
                                             :label (:label b)
                                             :lo    (:lo b) :hi (:hi b)})))}))))

(def count-cell-step
  (step/step :count-cell nil
             (fn [ctx _s {:keys [term scope label lo hi] :as cell}]
               (let [t0 (System/nanoTime)
                     n  (algolia-count term scope lo hi)
                     ms (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :cell-counted
                                  :term term :label label
                                  :scope scope :n n :ms ms})
                 {:out [(msg/child ctx (assoc cell :n (or n 0)))]}))))

(def aggregate-by-term
  (ca/batch-by-group
   :term
   (fn [term cells]
     (summarize-term term (:scope (first cells)) cells))))

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [terms since until bucket scope workers]
     :or   {terms   default-terms
            since   "2010-01-01"
            until   nil
            bucket  :month
            scope   :both
            workers 16}
     :as opts}]
   (let [opts' (assoc opts :terms terms :since since :until until
                          :bucket bucket :scope scope)]
     (step/serial :hn-buzzword-obituaries
                  (mk-emit-cells opts')
                  (cw/stealing-workers :counters workers count-cell-step)
                  aggregate-by-term))))

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
  ([] (run-once! "./buzzwords.json" {}))
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
      :n-terms (count rows)
      :error (:error res)})))
