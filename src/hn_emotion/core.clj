(ns hn-emotion.core
  "Per-user emotional landscape over time on Hacker News. Paginated
   Algolia source by author, Haiku classifies each comment on a fixed
   emotion vocabulary plus valence + grounding + freeform topic, and a
   windowed-reduce aggregator emits one row per (user × month).

   Data path:
     emit-users   →  fetch-history (paginated Algolia)
     split-with-bucket (msg/children — each comment is a unit, year-month stamped)
     classify-llm (c/stealing-workers — Haiku tool call)
     aggregate-by-user (group-by user-id; secondary group-by year-month;
                        emit one msg per user with per-month rows nested)

   One-shot:
     clojure -M -e \"(require 'hn-emotion.core) (hn-emotion.core/run-once-for-user! \\\"tptacek\\\" \\\"emotion.json\\\" {:trace? true})\""
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
  (:import [java.time LocalDate Instant ZoneOffset]))

(def algolia-base "https://hn.algolia.com/api/v1/search_by_date")
(def haiku "claude-haiku-4-5")

(def emotion-vocab
  ["joy" "anger" "fear" "sadness" "disgust" "surprise" "anticipation"
   "trust" "admiration" "contempt" "hope" "frustration" "pride"
   "amusement" "curiosity" "neutral"])

;; --- Date helpers ---------------------------------------------------------

(defn year-month-of
  "epoch seconds → \"YYYY-MM\"."
  [^long epoch-s]
  (let [d (LocalDate/ofInstant (Instant/ofEpochSecond epoch-s) ZoneOffset/UTC)]
    (format "%04d-%02d" (.getYear d) (.getMonthValue d))))

;; --- Algolia paginated source by author ----------------------------------

(defn algolia-author-page
  "One page of an author's comments. Returns
   {:hits [...] :nb-pages N}. Empty page on transport error."
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
          {:hits     (or (:hits resp) [])
           :nb-pages (or (:nbPages resp) 0)})
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

;; --- Haiku classifier -----------------------------------------------------

(def ^:private classify-schema
  {:type "object"
   :properties {:emotions   {:type "array"
                             :items {:type "string" :enum emotion-vocab}
                             :maxItems 3
                             :description "Up to 3 emotions from the fixed vocabulary."}
                :valence    {:type "number" :minimum -1 :maximum 1
                             :description "[-1, 1] overall emotional polarity."}
                :grounding  {:type "number" :minimum 0 :maximum 1
                             :description "[0, 1] claim-support density."}
                :topic      {:type "string"
                             :description "2-5 words about the subject matter."}}
   :required ["emotions" "valence" "grounding" "topic"]})

(def ^:private classify-system
  "You classify a single Hacker News comment along four axes — EMOTIONS (multi-label, up to 3, from a fixed vocabulary), VALENCE (-1 negative … +1 positive), GROUNDING (0 = pure opinion, 1 = nearly every claim has support), TOPIC (a short phrase, 2-5 words). The story title is provided as context only — classify the COMMENT, not the story.")

(defn llm-classify!
  "Classify a comment. Returns
   {:emotions [...] :valence n :grounding n :topic s}.
   Stub-friendly: tests redef this var."
  [{:keys [text story-title]}]
  (let [user-msg (str (when story-title (str "Story title: " story-title "\n\n"))
                      "Comment:\n" (or text ""))]
    (or (llm/call-json! {:system classify-system
                         :user   user-msg
                         :schema classify-schema
                         :model  haiku})
        {:emotions ["neutral"] :valence 0.0 :grounding 0.5 :topic ""})))

;; --- Per-bucket summarization --------------------------------------------

(defn- mean ^double [xs]
  (if (empty? xs) 0.0 (double (/ (reduce + 0.0 xs) (count xs)))))

(defn- stdev ^double [xs]
  (if (< (count xs) 2)
    0.0
    (let [m (mean xs)]
      (Math/sqrt (mean (mapv (fn [x] (let [d (- x m)] (* d d))) xs))))))

(defn- clip [s n]
  (if (and s (> (count s) n)) (str (subs s 0 n) "…") s))

(defn summarize-bucket
  "Take a seq of classified comments for one (user × month) and return
   the per-bucket summary row."
  [rows]
  (let [n        (count rows)
        emotions (->> rows
                      (mapcat :emotions)
                      (map (fn [e] (-> e str/lower-case keyword)))
                      frequencies)
        valences (mapv :valence rows)
        ground   (mapv :grounding rows)
        topics   (->> rows
                      (map :topic)
                      (remove str/blank?)
                      (map (fn [t] (-> t str/lower-case str/trim)))
                      frequencies
                      (sort-by val >)
                      (take 5)
                      vec)
        sample   (->> rows
                      (sort-by :valence)
                      ((fn [s] (if (>= (count s) 3)
                                 (let [c (count s)]
                                   [(first s) (nth s (quot c 2)) (last s)])
                                 s)))
                      (mapv (fn [r]
                              {:comment-id (:comment-id r)
                               :preview    (clip (:text r) 200)
                               :valence    (:valence r)})))]
    {:n-comments      n
     :emotions        emotions
     :valence-mean    (mean valences)
     :valence-std     (stdev valences)
     :grounding-mean  (mean ground)
     :top-topics      topics
     :sample-comments sample}))

;; --- Steps ----------------------------------------------------------------

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
                                  :user-id user-id
                                  :n-comments (count hs)
                                  :ms ms})
                 {:out [(msg/child ctx (assoc row :comments hs))]}))))

(def split-comments
  (step/step :split-comments nil
             (fn [ctx _s {:keys [user-id comments]}]
               (if (empty? comments)
                 ;; Always emit at least one msg per user so empty users
                 ;; don't disappear from the aggregator's output.
                 {:out (msg/children ctx [{:user-id user-id :empty? true}])}
                 {:out (msg/children
                         ctx
                         (mapv (fn [c]
                                 {:user-id     user-id
                                  :comment-id  (str (:objectID c))
                                  :year-month  (year-month-of (:created_at_i c))
                                  :text        (or (:comment_text c) "")
                                  :story-title (:story_title c)})
                               comments))}))))

(def classify-step
  (step/step :classify nil
             (fn [ctx _s row]
               (if (:empty? row)
                 {:out [(msg/child ctx row)]}
                 (let [t0 (System/nanoTime)
                       c  (llm-classify! row)
                       ms (long (/ (- (System/nanoTime) t0) 1e6))]
                   (trace/emit ctx {:event :classified
                                    :comment-id (:comment-id row)
                                    :ms ms})
                   {:out [(msg/child ctx (merge row c))]})))))

(def aggregate-by-user
  (c/batch-by-group
   :user-id
   (fn [user-id rows]
     (let [real   (remove :empty? rows)
           by-mon (group-by :year-month real)
           months (->> by-mon
                       (sort-by key)
                       (mapv (fn [[ym rs]]
                               (-> (summarize-bucket rs)
                                   (assoc :year-month ym
                                          :user-id user-id)))))]
       {:user-id user-id :months months}))))

;; --- Flow -----------------------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [user-ids workers max-comments]
     :or   {workers      8
            max-comments 10000}
     :as opts}]
   (let [opts' (assoc opts :workers workers :max-comments max-comments
                           :user-ids (or user-ids []))]
     (step/serial :hn-emotion
                  (mk-emit-users opts')
                  (c/stealing-workers :fetchers workers (mk-fetch-history opts'))
                  split-comments
                  (c/stealing-workers :classifiers workers classify-step)
                  aggregate-by-user))))

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
                       (:comment-id ev)       (str "cid=" (:comment-id ev) " ")
                       (:n-comments ev)       (str "n=" (:n-comments ev) " ")
                       (:n-users ev)          (str "users=" (:n-users ev) " ")
                       (:ms ev)               (str "ms=" (:ms ev) " ")
                       (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./emotion.json" {}))
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
