(ns hn-firehose.core
  "Live HN firehose: poll /v0/maxitem.json, fetch new items, route by
   type, emit classifications. Optional cycle: a hostile-comment
   classification re-fetches the comment's parent for context.

   Stress-tests Datapotamus streaming (`flow/run-polling!`) layered on
   top of a recursive `:work`-port worker pool — the firehose runs
   indefinitely, items flow through continuously, and the cycle
   exercises the recently-fixed work-only-no-deadlock guarantee.

   One-shot:
     clojure -M -e \"(require 'hn-firehose.core) (let [h (hn-firehose.core/run-firehose! {:interval-ms 10000})] (Thread/sleep 60000) ((:stop! h)))\""
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators.workers :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.llm.cli :as llm]))

(def ^:private base "https://hacker-news.firebaseio.com/v0")
(def ^:private haiku "claude-haiku-4-5")

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

(defn- get-max-item! [] (get-json (str base "/maxitem.json")))
(defn- get-item!    [id] (get-json (str base "/item/" id ".json")))

;; --- LLM helpers (stubbed in tests via with-redefs) ----------------------

(def ^:private story-schema
  {:type "object"
   :properties {:tldr {:type "string"
                       :description "One-sentence TLDR of the story title."}}
   :required ["tldr"]})

(def ^:private comment-schema
  {:type "object"
   :properties {:label {:type "string"
                        :enum ["civil" "hostile" "off-topic" "informative"]
                        :description "Pick the dominant tone."}}
   :required ["label"]})

(defn- summarize-story! [item]
  (when-let [r (llm/call-json! {:system "Summarize an HN story title."
                                :user   (str (:title item))
                                :schema story-schema
                                :model  haiku
                                :keys   :snake})]
    (str "STORY:" (:id item) " " (:tldr r))))

(defn- classify-comment! [item]
  (when-let [r (llm/call-json! {:system "Classify the tone of this HN comment."
                                :user   (or (:text item) "")
                                :schema comment-schema
                                :model  haiku
                                :keys   :snake})]
    (str (str/upper-case (:label r)) ":" (:id item)
         (when-let [p (:parent item)] (str " parent=" p)))))

(defn- default-hostile? [s]
  (and s (str/starts-with? s "HOSTILE")))

;; --- Worker handler with :work cycle for parent context ------------------

(defn- mk-process-handler [{:keys [hostile?]}]
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out "" :work ""}}
    :on-data
    (fn [ctx _ id]
      (let [item (get-item! id)
            type (:type item)]
        (cond
          (= type "story")
          (if-let [r (summarize-story! item)]
            {:out [(msg/child ctx r)]}
            {})

          (= type "comment")
          (let [r (classify-comment! item)]
            (cond
              (and r (hostile? r) (:parent item))
              ;; Cycle: re-fetch parent to give downstream context.
              ;; The :work emission goes back through the pool; the
              ;; engine wrapper flags it as the invocation terminator
              ;; (engine fix for the work-only-no-deadlock case).
              {:out  [(msg/child ctx r)]
               :work [(msg/child ctx (:parent item))]}

              r {:out [(msg/child ctx r)]}
              :else {}))

          ;; jobs/polls/asks: drop silently
          :else {})))}))

;; --- Pipeline + run! -----------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [pool-k hostile?]
     :or   {pool-k 4 hostile? default-hostile?}}]
   (c/stealing-workers :firehose pool-k
                       (mk-process-handler {:hostile? hostile?}))))

(defn run-firehose!
  "Start the firehose. Polls maxitem at `:interval-ms` and injects
   every new ID. Returns the polling handle (`:handle :collected
   :stop!`) — call `(:stop! h)` to halt.

   Options:
     :interval-ms — poll cadence (default 10000)
     :start-id    — initial last-seen ID (default: current maxitem)
     :max-ticks   — optional cap on poll iterations (test-only)
     :pool-k      — worker pool size (default 4)
     :hostile?    — predicate over a classification string; true ⇒
                    cycle to fetch parent (default: starts-with HOSTILE)"
  [{:keys [interval-ms start-id max-ticks pool-k hostile?]
    :or   {interval-ms 10000 pool-k 4}
    :as   opts}]
  (let [start    (or start-id (get-max-item!))
        pipe     (build-flow (cond-> {:pool-k pool-k}
                               hostile? (assoc :hostile? hostile?)))]
    (flow/run-polling!
     pipe
     {:interval-ms interval-ms
      :init-state  {:last-id start}
      :max-ticks   max-ticks
      :poll-fn     (fn [{:keys [last-id]}]
                     (let [m       (get-max-item!)
                           new-ids (range (inc last-id) (inc m))]
                       [{:last-id m} (vec new-ids)]))})))
