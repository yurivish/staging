(ns hn-self-contradiction.core
  "Per-user self-contradiction finder. Pair the user's comments by
   topic, filter cheaply with Haiku, judge survivors with Sonnet.

   New shapes for the codebase:
     * Pairwise-within-group fan-out — group rows by topic, then
       emit C(n,2) pair msgs per group.
     * Filter → expensive two-stage — Haiku score prunes, Sonnet
       judges.

   Data path:
     emit-users → fetch-history (paginated Algolia) → split-comments
     tag-llm (Haiku — topic + stance-summary + is-substantive)
     topic-group-aggregator (group by [user-id topic]; drop singletons)
     pair-fanout (emit C(n,2) pair msgs per group, time-gap filter)
     pair-score-llm (Haiku — opposed-score; drop low scores)
     pair-judge-llm (Sonnet — verdict / confidence / summaries)
     final-collector (per-user, sort by verdict priority + confidence)

   One-shot:
     clojure -M -e \"(require 'hn-self-contradiction.core) (hn-self-contradiction.core/run-once-for-user! \\\"tptacek\\\" \\\"contradiction.json\\\" {:trace? true})\""
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators.aggregate :as ca]
            [toolkit.datapotamus.combinators.workers :as cw]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.llm.cli :as llm]
            [toolkit.pubsub :as pubsub]))

(def algolia-base "https://hn.algolia.com/api/v1/search_by_date")
(def haiku  "claude-haiku-4-5")
(def sonnet "claude-sonnet-4-6")

(def verdict-rank
  {"real-contradiction"   0
   "genuine-update"       1
   "world-changed"        2
   "scope-shift"          3
   "not-actually-opposed" 4})

;; --- Algolia paginated source by author ----------------------------------

(defn algolia-author-page
  "One page of an author's comments. Stub-friendly."
  [user page]
  (try
    (let [params [(str "tags=" (java.net.URLEncoder/encode
                                 (str "author_" user ",comment") "UTF-8"))
                  "hitsPerPage=100"
                  (str "page=" page)]
          url    (str algolia-base "?" (str/join "&" params))
          {:keys [status body error]}
          @(http/get url {:timeout 15000 :as :text :follow-redirects true})]
      (if (and (nil? error) (= 200 status) (string? body))
        (let [resp (json/read-str body :key-fn keyword)]
          {:hits (or (:hits resp) []) :nb-pages (or (:nbPages resp) 0)})
        {:hits [] :nb-pages 0}))
    (catch Throwable _ {:hits [] :nb-pages 0})))

(defn- fetch-author-history [user max-comments]
  (loop [page 0 acc []]
    (let [{ph :hits np :nb-pages} (algolia-author-page user page)
          all (into acc ph)]
      (if (or (>= (count all) max-comments)
              (>= (inc page) (or np 0))
              (empty? ph))
        (vec (take max-comments all))
        (recur (inc page) all)))))

;; --- Tag step (topic + stance + is-substantive) --------------------------

(def ^:private tag-schema
  {:type "object"
   :properties {:topic           {:type "string"
                                   :description "2-5 words, lowercase, canonical phrase."}
                :stance_summary  {:type "string"
                                   :description "≤ 20 words, the user's claim/attitude."}
                :is_substantive  {:type "boolean"
                                   :description "True if the comment makes a substantive claim about the topic."}}
   :required ["topic" "stance_summary" "is_substantive"]})

(def ^:private tag-system
  "Classify a Hacker News comment. Return TOPIC (2-5 words, lowercase, canonical), STANCE_SUMMARY (≤20 words, the author's claim or attitude on that topic), and IS_SUBSTANTIVE (true if the comment makes a substantive claim, false for jokes/one-liners). Use the story title as context only — classify the COMMENT.")

(defn llm-tag!
  "Tag a comment. Returns {:topic :stance-summary :is-substantive}.
   Stub-friendly."
  [{:keys [text story-title]}]
  (let [user-msg (str (when story-title (str "Story title: " story-title "\n\n"))
                      "Comment:\n" (or text ""))]
    (or (llm/call-json! {:system tag-system
                         :user   user-msg
                         :schema tag-schema
                         :model  haiku})
        {:topic "" :stance-summary "" :is-substantive false})))

;; --- Pair score (cheap Haiku) --------------------------------------------

(def ^:private score-schema
  {:type "object"
   :properties {:opposed_score {:type "integer" :minimum 0 :maximum 10
                                 :description "How strongly the two stances oppose."}
                :note          {:type "string" :description "≤ 12 words."}}
   :required ["opposed_score" "note"]})

(def ^:private score-system
  "You judge whether two stance summaries by the same author on the same topic OPPOSE each other. Score 0 (fully aligned) to 10 (clearly opposite). Same-author + same-topic + different-time, but you only see the topic + two short stance summaries.")

(defn llm-pair-score!
  "Cheap pair scorer. Returns {:opposed-score n :note s}. Stub-friendly."
  [{:keys [topic a b]}]
  (let [user-msg (str "TOPIC: " topic
                      "\n\nA stance: " (:stance-summary a)
                      "\n\nB stance: " (:stance-summary b))]
    (or (llm/call-json! {:system score-system
                         :user   user-msg
                         :schema score-schema
                         :model  haiku})
        {:opposed-score 0 :note ""})))

;; --- Pair judge (Sonnet) -------------------------------------------------

(def ^:private judge-schema
  {:type "object"
   :properties {:verdict        {:type "string"
                                 :enum ["real-contradiction" "scope-shift"
                                        "world-changed" "genuine-update"
                                        "not-actually-opposed"]}
                :confidence     {:type "number" :minimum 0 :maximum 1}
                :summary_a      {:type "string" :description "≤ 25 words."}
                :summary_b      {:type "string" :description "≤ 25 words."}
                :reconciliation {:type "string"
                                 :description "≤ 50 words: what would explain both."}}
   :required ["verdict" "confidence" "summary_a" "summary_b" "reconciliation"]})

(def ^:private judge-system
  "Two comments by the same author at different times on the same topic. Decide whether they actually contradict. Be specific: real-contradiction (clear stance flip), genuine-update (the author updated their view in good faith), world-changed (the underlying facts changed — policy reversed, technology improved), scope-shift (the apparent contradiction is actually about different sub-cases), or not-actually-opposed (false positive).")

(defn- clip [s n]
  (if (and s (> (count s) n)) (str (subs s 0 n) "…") s))

(defn llm-pair-judge!
  "Sonnet pair judge. Returns judgment map. Stub-friendly."
  [{:keys [topic a b]}]
  (let [user-msg (str "TOPIC: " topic
                      "\n\nA (" (or (:created_at a) "earlier") "):\n"
                      (clip (:text a) 600)
                      "\n\nB (" (or (:created_at b) "later") "):\n"
                      (clip (:text b) 600))]
    (or (llm/call-json! {:system judge-system
                         :user   user-msg
                         :schema judge-schema
                         :model  sonnet})
        {:verdict "not-actually-opposed" :confidence 0.0
         :summary-a "" :summary-b "" :reconciliation ""})))

;; --- Pure helpers ---------------------------------------------------------

(defn pairs-within-group
  "Generate C(n,2) pairs from a group of rows. With opts containing
   `:min-time-gap-days`, drop pairs whose `:time` epoch-seconds gap
   is below threshold."
  ([rows] (pairs-within-group rows {}))
  ([rows {:keys [min-time-gap-days]}]
   (let [v       (vec rows)
         n       (count v)
         min-sec (when min-time-gap-days (* 86400 min-time-gap-days))]
     (vec
       (for [i (range n)
             j (range (inc i) n)
             :let [a (nth v i) b (nth v j)
                   gap (when (and (:time a) (:time b))
                         (Math/abs (- (:time a) (:time b))))]
             :when (or (nil? min-sec) (nil? gap) (>= gap min-sec))]
         {:a a :b b})))))

(defn sort-pairs
  "Sort by verdict priority, then by confidence descending."
  [rows]
  (->> rows
       (sort-by (fn [r] [(get verdict-rank (:verdict r) 99)
                         (- (or (:confidence r) 0))]))
       vec))

;; --- Steps ---------------------------------------------------------------

(defn- mk-emit-users [{:keys [user-ids]}]
  (step/step :emit-users nil
             (fn [ctx _s _tick]
               {:out (msg/children ctx (mapv (fn [u] {:user-id u}) user-ids))})))

(defn- mk-fetch-history [{:keys [max-comments]}]
  (step/step :fetch-history nil
             (fn [ctx _s {:keys [user-id] :as row}]
               (let [t0 (System/nanoTime)
                     hs (fetch-author-history user-id max-comments)
                     ms (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :history-fetched
                                  :user-id user-id :n-comments (count hs) :ms ms})
                 {:out [(msg/child ctx (assoc row :comments hs))]}))))

(def split-comments
  (step/step :split-comments nil
             (fn [ctx _s {:keys [user-id comments]}]
               (if (empty? comments)
                 {:out (msg/children ctx [{:user-id user-id :empty? true}])}
                 {:out (msg/children
                         ctx
                         (mapv (fn [c]
                                 {:user-id    user-id
                                  :comment-id (str (:objectID c))
                                  :time       (:created_at_i c)
                                  :text       (or (:comment_text c) "")
                                  :story-title (:story_title c)})
                               comments))}))))

(def tag-step
  (step/step :tag nil
             (fn [ctx _s row]
               (if (:empty? row)
                 {:out [(msg/child ctx row)]}
                 (let [t0 (System/nanoTime)
                       t  (llm-tag! row)
                       ms (long (/ (- (System/nanoTime) t0) 1e6))]
                   (trace/emit ctx {:event :tagged
                                    :comment-id (:comment-id row) :ms ms})
                   {:out [(msg/child ctx (merge row t))]})))))

(def topic-group-step
  "Cumulative per-row emit: classifies each row into an outgoing msg
   shape that downstream pair-fanout already understands. On every
   row, the step emits exactly one msg:
     - `:empty? true` if the row is the per-user empty-marker, a
       non-substantive comment, or a substantive comment in a topic
       group that hasn't yet reached two entries (a 'user seen, no
       qualifying pair yet' placeholder).
     - `{:user-id :topic :rows [...]}` cumulative qualifying-group
       msg when the row pushes a `(user, topic)` group to two-or-more
       substantive entries (and on each subsequent row in that
       group). final-collector dedupes by user."
  {:procs
   {:tg
    (step/handler-map
     {:ports   {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:rows []})
      :on-data
      (fn [ctx s row]
        (let [s'      (update s :rows conj {:msg (:msg ctx) :row row})
              user-id (:user-id row)
              empty-marker
              (msg/merge ctx [(:msg ctx)] {:user-id user-id :empty? true})
              out
              (cond
                (:empty? row)
                empty-marker

                (and (:is-substantive row)
                     (not (str/blank? (:topic row))))
                (let [topic-key (-> row :topic str/lower-case str/trim)
                      group-key [user-id topic-key]
                      entries
                      (filterv (fn [{r :row}]
                                 (and (:is-substantive r)
                                      (not (str/blank? (:topic r)))
                                      (= [(:user-id r)
                                          (-> r :topic str/lower-case str/trim)]
                                         group-key)))
                               (:rows s'))]
                  (if (>= (count entries) 2)
                    (msg/merge ctx (mapv :msg entries)
                               {:user-id user-id
                                :topic   topic-key
                                :rows    (mapv :row entries)})
                    empty-marker))

                :else
                empty-marker)]
          [s' {:out [out]}]))})}
   :conns [] :in :tg :out :tg})

(defn- mk-pair-fanout [{:keys [min-time-gap-days]}]
  (step/step :pair-fanout nil
             (fn [ctx _s {:keys [user-id topic rows] :as in}]
               (if (or (:empty? in) (nil? rows))
                 {:out [(msg/child ctx in)]}
                 (let [pairs (pairs-within-group
                               rows {:min-time-gap-days min-time-gap-days})
                       msgs  (mapv (fn [{:keys [a b]}]
                                     {:user-id user-id
                                      :topic topic
                                      :a a :b b})
                                   pairs)]
                   (trace/emit ctx {:event :paired
                                    :user-id user-id
                                    :topic topic
                                    :n-pairs (count msgs)})
                   (if (empty? msgs)
                     {:out []}
                     {:out (msg/children ctx msgs)}))))))

(defn- mk-pair-score [threshold]
  (step/step :pair-score nil
             (fn [ctx _s pair]
               (if (:empty? pair)
                 {:out [(msg/child ctx pair)]}
                 (let [t0 (System/nanoTime)
                       s  (llm-pair-score! pair)
                       ms (long (/ (- (System/nanoTime) t0) 1e6))
                       keep? (>= (or (:opposed-score s) 0) threshold)]
                   (trace/emit ctx {:event :pair-scored
                                    :keep? keep?
                                    :score (:opposed-score s)
                                    :ms ms})
                   (if keep?
                     {:out [(msg/child ctx (merge pair s))]}
                     {:out []}))))))

(def pair-judge-step
  (step/step :pair-judge nil
             (fn [ctx _s pair]
               (if (:empty? pair)
                 {:out [(msg/child ctx pair)]}
                 (let [t0 (System/nanoTime)
                       j  (llm-pair-judge! pair)
                       ms (long (/ (- (System/nanoTime) t0) 1e6))]
                   (trace/emit ctx {:event :pair-judged
                                    :verdict (:verdict j) :ms ms})
                   {:out [(msg/child ctx (merge pair j))]})))))

(def final-collector
  (ca/batch-by-group
   :user-id
   (fn [user-id rows]
     (let [real   (remove :empty? rows)
           sorted (sort-pairs real)]
       {:user-id user-id
        :n-pairs (count sorted)
        :pairs   (mapv (fn [r]
                         (-> r
                             (dissoc :user-id)
                             (update :a select-keys
                                     [:comment-id :time :stance-summary :text])
                             (update :b select-keys
                                     [:comment-id :time :stance-summary :text])))
                       sorted)}))))

;; --- Flow ----------------------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [user-ids workers max-comments min-time-gap-days filter-threshold]
     :or   {workers 8 max-comments 5000
            min-time-gap-days 90 filter-threshold 6}
     :as opts}]
   (let [opts' (assoc opts :workers workers :max-comments max-comments
                           :min-time-gap-days min-time-gap-days
                           :filter-threshold filter-threshold
                           :user-ids (or user-ids []))]
     (step/serial :hn-self-contradiction
                  (mk-emit-users opts')
                  (cw/stealing-workers :fetchers workers (mk-fetch-history opts'))
                  split-comments
                  (cw/stealing-workers :taggers workers tag-step)
                  topic-group-step
                  (mk-pair-fanout opts')
                  (cw/stealing-workers :scorers workers (mk-pair-score filter-threshold))
                  (cw/stealing-workers :judges workers pair-judge-step)
                  final-collector))))

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
                       (:user-id ev)          (str "user=" (:user-id ev) " ")
                       (:n-pairs ev)          (str "npairs=" (:n-pairs ev) " ")
                       (:keep? ev)            (str "keep=" (:keep? ev) " ")
                       (:score ev)            (str "score=" (:score ev) " ")
                       (:verdict ev)          (str "verdict=" (:verdict ev) " ")
                       (:n-groups ev)         (str "groups=" (:n-groups ev) " ")
                       (:ms ev)               (str "ms=" (:ms ev) " ")
                       (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./contradiction.json" {}))
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
      :n-users (count (or rows []))
      :error (:error res)})))

(defn run-once-for-user!
  ([user out-path] (run-once-for-user! user out-path {}))
  ([user out-path opts]
   (run-once! out-path (assoc opts :user-ids [user]))))
