(ns hn-mind-changed.core
  "Mind-changed traces on Hacker News: curated dataset of comments where
   someone publicly conceded a substantive point, annotated with the
   argumentative move that triggered the shift.

   Two-stage filter→judge: a cheap Haiku classifier prunes the
   phrase-matched candidate set, then Sonnet judges each survivor
   on the kind of move that did the work.

   Data path:
     emit-phrases  →  fetch-phrase-pages  (paginated Algolia, multi-query)
     dedup-by-id (aggregator → fan-out unique candidates)
     hydrate-context (parent + grandparent via Firebase)
     filter-llm  (Haiku — drop low-confidence / sarcasm)
     judge-llm   (Sonnet — direction, trigger-type, excerpt)
     final-collector (sort by created-at desc, emit one msg per row)

   One-shot:
     clojure -M -e \"(require 'hn-mind-changed.core) (hn-mind-changed.core/run-once! \\\"mind-changed.json\\\" {:trace? true :since-days 30})\""
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.llm.cli :as llm]
            [toolkit.pubsub :as pubsub])
  (:import [java.time LocalDate ZoneOffset]))

(def algolia-base "https://hn.algolia.com/api/v1/search_by_date")
(def fb-base      "https://hacker-news.firebaseio.com/v0")
(def haiku  "claude-haiku-4-5")
(def sonnet "claude-sonnet-4-6")

(def default-phrases
  ["you're right" "you are right" "fair point" "good point"
   "i stand corrected" "i was wrong" "i take that back"
   "you've convinced me" "you have convinced me"
   "changed my mind" "changes my mind" "i hadn't considered"
   "i had not considered" "fair enough" "i concede"])

;; --- Date helpers ----------------------------------------------------------

(defn- ^:private parse-date [s] (LocalDate/parse s))
(defn- ^:private epoch [^LocalDate d]
  (.toEpochSecond (.atStartOfDay d ZoneOffset/UTC)))

;; --- Algolia paginated source ---------------------------------------------

(defn algolia-search-page
  "One page of Algolia phrase-search hits. Returns
   {:hits [...] :nb-pages N}."
  [phrase since until page]
  (try
    (let [lo     (epoch (cond-> since (string? since) parse-date))
          hi     (epoch (cond-> until (string? until) parse-date))
          params [(str "tags=comment")
                  (str "query=" (java.net.URLEncoder/encode phrase "UTF-8"))
                  (str "numericFilters="
                       (java.net.URLEncoder/encode
                        (str "created_at_i>=" lo ",created_at_i<=" hi)
                        "UTF-8"))
                  "hitsPerPage=200"
                  (str "page=" page)]
          url    (str algolia-base "?" (str/join "&" params))
          {:keys [status body error]}
          @(http/get url {:timeout 15000 :as :text :follow-redirects true})]
      (if (and (nil? error) (= 200 status) (string? body))
        (let [resp (json/read-str body :key-fn keyword)]
          {:hits     (or (:hits resp) [])
           :nb-pages (or (:nbPages resp) 0)})
        {:hits [] :nb-pages 0}))
    (catch Throwable _ {:hits [] :nb-pages 0})))

(defn- fetch-all-pages [phrase since until]
  (loop [page 0
         acc  []]
    (let [{ph :hits np :nb-pages} (algolia-search-page phrase since until page)
          all (into acc ph)]
      (if (or (empty? ph) (>= (inc page) (or np 0)))
        all
        (recur (inc page) all)))))

;; --- Firebase item fetch & parent walk -------------------------------------

(defn get-item
  "Fetch one Firebase item; nil on error."
  [id]
  (try
    (let [{:keys [status body error]}
          @(http/get (str fb-base "/item/" id ".json")
                     {:timeout 10000 :as :text :follow-redirects true})]
      (when (and (nil? error) (= 200 status) (string? body))
        (json/read-str body :key-fn keyword)))
    (catch Throwable _ nil)))

(defn hydrate-context
  "Walk up from a candidate id, fetching the chain. Returns
   {:candidate-id ... :candidate-text ... :parent-text ...
    :grandparent-text ... :story-id ... :story-title ...
    :candidate-author ... :candidate-time ...}.

   `get-item-fn` is parameterized so tests can stub it."
  [get-item-fn candidate-id]
  (let [c   (get-item-fn candidate-id)
        p   (when (:parent c) (get-item-fn (:parent c)))
        gp  (when (and p (:parent p)) (get-item-fn (:parent p)))
        ;; story is the topmost ancestor of type "story"; walk further if needed
        chain (->> [c p gp]
                   (take-while some?))
        story (or (last (filter #(= "story" (:type %)) chain))
                  (when-let [top (last chain)]
                    (when (:parent top)
                      (loop [it (get-item-fn (:parent top)) hops 0]
                        (cond
                          (or (nil? it) (>= hops 4)) nil
                          (= "story" (:type it))    it
                          (:parent it)              (recur (get-item-fn (:parent it))
                                                           (inc hops))
                          :else                     nil)))))
        ;; If the topmost in [c p gp] was a story, demote to be the parent/gp.
        [parent grandparent]
        (cond
          (= "story" (:type p))  [nil nil]
          (= "story" (:type gp)) [p   nil]
          :else                  [p   gp])]
    {:candidate-id     (str candidate-id)
     :candidate-text   (:text c)
     :candidate-author (:by c)
     :candidate-time   (:time c)
     :parent-text      (:text parent)
     :grandparent-text (:text grandparent)
     :story-id         (:id story)
     :story-title      (:title story)
     :story-url        (:url story)}))

;; --- Dedup -----------------------------------------------------------------

(defn dedup-by-id
  "Keep the first occurrence per :objectID."
  [hits]
  (->> hits
       (reduce (fn [[seen acc] h]
                 (if (contains? seen (:objectID h))
                   [seen acc]
                   [(conj seen (:objectID h)) (conj acc h)]))
               [#{} []])
       second))

;; --- LLM clients -----------------------------------------------------------

(def ^:private filter-schema
  {:type "object"
   :properties {:is_mind_change {:type "boolean"
                                  :description "True only if the candidate clearly concedes a substantive point in response to the parent."}
                :confidence     {:type "number" :minimum 0 :maximum 1}
                :is_sarcasm     {:type "boolean"
                                  :description "True if the apparent concession is sarcastic or rhetorical."}}
   :required ["is_mind_change" "confidence" "is_sarcasm"]})

(def ^:private filter-system
  "Decide whether the candidate comment is a real intellectual concession in response to its parent — i.e. the author publicly updates a substantive position. Mark sarcasm and politeness filler as not-a-mind-change. Use the parent text for context.")

(def ^:private judge-schema
  {:type "object"
   :properties {:direction         {:type "string"
                                    :enum ["full-reversal" "partial-concession"
                                           "scope-narrowing" "unclear"]}
                :what_was_conceded {:type "string"
                                    :description "≤ 25 words."}
                :trigger_type      {:type "string"
                                    :enum ["data-or-citation" "lived-experience"
                                           "reframing" "edge-case" "authority"
                                           "clarifying-question" "emotional-appeal"
                                           "scope-narrowing" "unclear"]}
                :trigger_excerpt   {:type "string"
                                    :description "≤ 40 words from the parent."}
                :original_position {:type "string"
                                    :description "≤ 25 words."}}
   :required ["direction" "what_was_conceded" "trigger_type"
              "trigger_excerpt" "original_position"]})

(def ^:private judge-system
  "You are analyzing a 3-message HN exchange (grandparent → parent → candidate). The candidate concedes a point made in the parent. Identify (a) what was conceded, (b) the kind of argumentative move in the parent that triggered the concession (data, lived-experience, reframing, edge-case, authority, etc.), and (c) the original position.")

(defn- format-context [{:keys [grandparent-text parent-text candidate-text]}]
  (str "GRANDPARENT:\n" (or grandparent-text "(none)")
       "\n\nPARENT:\n" (or parent-text "(none)")
       "\n\nCANDIDATE:\n" (or candidate-text "(none)")))

(defn llm-filter!
  "Haiku filter call. Returns
   {:is-mind-change boolean :confidence number :is-sarcasm boolean}.
   Stub-friendly: tests redef this var."
  [row]
  (or (llm/call-json! {:system filter-system
                       :user   (format-context row)
                       :schema filter-schema
                       :model  haiku})
      {:is-mind-change false :confidence 0.0 :is-sarcasm false}))

(defn llm-judge!
  "Sonnet judge call. Returns judgment map. Stub-friendly."
  [row]
  (or (llm/call-json! {:system judge-system
                       :user   (format-context row)
                       :schema judge-schema
                       :model  sonnet})
      {:direction "unclear" :what-was-conceded "" :trigger-type "unclear"
       :trigger-excerpt "" :original-position ""}))

;; --- Steps -----------------------------------------------------------------

(defn- mk-emit-phrases [{:keys [phrases]}]
  (step/step :emit-phrases nil
             (fn [ctx _s _tick]
               {:out (msg/children ctx
                                   (mapv (fn [p] {:phrase p}) phrases))})))

(defn- mk-fetch-phrase-pages [{:keys [since until]}]
  (step/step :fetch-phrase-pages nil
             (fn [ctx _s {:keys [phrase]}]
               (let [t0 (System/nanoTime)
                     hs (fetch-all-pages phrase since until)
                     ms (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :search-done
                                  :phrase phrase
                                  :n-hits (count hs)
                                  :ms ms})
                 {:out (msg/children ctx hs)}))))

(def dedup-step
  (c/batch-by-group
   :objectID
   (fn [oid hits]
     (let [hit (first hits)]
       {:candidate-id (str oid)
        :created_at_i (:created_at_i hit)
        :seed-author  (:author hit)}))))

(def hydrate-step
  (step/step :hydrate nil
             (fn [ctx _s {:keys [candidate-id created_at_i] :as cell}]
               (let [t0 (System/nanoTime)
                     ;; candidate-id might come back as string from Algolia objectID
                     id (try (Long/parseLong candidate-id)
                             (catch Throwable _ candidate-id))
                     ctx-data (hydrate-context get-item id)
                     ms (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :hydrated
                                  :candidate-id candidate-id
                                  :ms ms})
                 {:out [(msg/child ctx (merge cell ctx-data
                                              {:created_at_i created_at_i}))]}))))

(defn- mk-filter-step [threshold]
  (step/step :filter nil
             (fn [ctx _s row]
               (let [t0 (System/nanoTime)
                     d  (llm-filter! row)
                     ms (long (/ (- (System/nanoTime) t0) 1e6))
                     keep? (and (:is-mind-change d)
                                (not (:is-sarcasm d))
                                (>= (or (:confidence d) 0.0) threshold))]
                 (trace/emit ctx {:event :filtered
                                  :candidate-id (:candidate-id row)
                                  :keep? keep?
                                  :confidence (:confidence d)
                                  :ms ms})
                 (if keep?
                   {:out [(msg/child ctx (merge row d))]}
                   {:out []})))))

(def judge-step
  (step/step :judge nil
             (fn [ctx _s row]
               (let [t0 (System/nanoTime)
                     j  (llm-judge! row)
                     ms (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :judged
                                  :candidate-id (:candidate-id row)
                                  :ms ms})
                 {:out [(msg/child ctx (merge row j))]}))))

(def final-collector
  "Per-row emit: each judged row passes through as its own emission.
   Ordering and any final dedup/sort happen downstream of the flow
   (callers can sort `(first (:outputs res))` by `:created_at_i`)."
  (step/step :final nil
             (fn [ctx _s _row] {:out [(msg/pass ctx)]})))

;; --- Flow ------------------------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [phrases since until filter-threshold
            phrase-workers filter-workers judge-workers]
     :or   {phrases          default-phrases
            since            "2024-01-01"
            until            nil
            filter-threshold 0.7
            phrase-workers   4
            filter-workers   8
            judge-workers    4}
     :as opts}]
   (let [opts' (assoc opts
                      :phrases phrases :since since :until until)]
     (step/serial :hn-mind-changed
                  (mk-emit-phrases opts')
                  (c/stealing-workers :phrase-pages
                                      phrase-workers
                                      (mk-fetch-phrase-pages opts'))
                  dedup-step
                  (c/stealing-workers :hydrators 8 hydrate-step)
                  (c/stealing-workers :filters
                                      filter-workers
                                      (mk-filter-step filter-threshold))
                  (c/stealing-workers :judges judge-workers judge-step)
                  final-collector))))

;; --- Trace pretty-printer --------------------------------------------------

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
                       (:phrase ev)           (str "phrase=" (preview (:phrase ev)) " ")
                       (:candidate-id ev)     (str "cid=" (:candidate-id ev) " ")
                       (:n-hits ev)           (str "hits=" (:n-hits ev) " ")
                       (:n-unique ev)         (str "uniq=" (:n-unique ev) " ")
                       (:keep? ev)            (str "keep=" (:keep? ev) " ")
                       (:confidence ev)       (str "conf=" (:confidence ev) " ")
                       (:ms ev)               (str "ms=" (:ms ev) " ")
                       (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./mind-changed.json" {}))
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
