(ns hn-crank-index.core
  "Per-user crank index. For each topic the user comments on, rank by
   the z-score of mean intensity against that user's own
   intensity-baseline. The interesting signal is *relative* — a calm
   commenter who lights up only on one topic is more interesting than
   one who's hot on everything.

   Same source/classifier shape as `hn-emotion`, with a smaller schema
   (just :topic + :intensity) and a different aggregator that does a
   two-pass reduction inside :on-all-input-done.

   Data path:
     emit-users → fetch-history (paginated Algolia)
     split-comments
     classify-llm (Haiku — topic, intensity 0-10)
     aggregate-by-user (per-user μ/σ, then per-topic z-score)

   One-shot:
     clojure -M -e \"(require 'hn-crank-index.core) (hn-crank-index.core/run-once-for-user! \\\"tptacek\\\" \\\"crank.json\\\" {:trace? true})\""
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
(def haiku "claude-haiku-4-5")

;; --- Algolia paginated source by author -----------------------------------

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

;; --- Haiku classifier (small schema) --------------------------------------

(def ^:private classify-schema
  {:type "object"
   :properties {:topic     {:type "string"
                            :description "2-5 words, lowercase, about the subject matter."}
                :intensity {:type "integer" :minimum 0 :maximum 10
                            :description "Emotional intensity, regardless of valence."}}
   :required ["topic" "intensity"]})

(def ^:private classify-system
  "Classify a single Hacker News comment on two axes — TOPIC (2-5 words about the subject matter, lowercase) and INTENSITY (0-10 emotional intensity, regardless of valence). The story title is provided as context only — classify the COMMENT.")

(defn llm-classify!
  "Classify a comment. Returns {:topic s :intensity n}. Stub-friendly."
  [{:keys [text story-title]}]
  (let [user-msg (str (when story-title (str "Story title: " story-title "\n\n"))
                      "Comment:\n" (or text ""))]
    (or (llm/call-json! {:system classify-system
                         :user   user-msg
                         :schema classify-schema
                         :model  haiku})
        {:topic "" :intensity 5})))

;; --- Pure crank computation -----------------------------------------------

(defn- mean ^double [xs]
  (if (empty? xs) 0.0 (double (/ (reduce + 0.0 xs) (count xs)))))

(defn- stdev ^double [xs]
  (if (< (count xs) 2)
    0.0
    (let [m (mean xs)]
      (Math/sqrt (mean (mapv (fn [x] (let [d (- x m)] (* d d))) xs))))))

(defn- clip [s n]
  (if (and s (> (count s) n)) (str (subs s 0 n) "…") s))

(defn cranks-of
  "Compute the crank index for one user given their classified rows.
   Returns {:user-id ... :n-comments N :baseline {...} :topics [...]}."
  [user-id rows {:keys [min-count] :or {min-count 5}}]
  (let [intensities (mapv :intensity rows)
        mu          (mean intensities)
        sigma       (stdev intensities)
        topic-of    (fn [r] (some-> (:topic r) str/lower-case str/trim))
        by-topic    (group-by topic-of (filter #(seq (topic-of %)) rows))
        ranked      (->> by-topic
                         (keep (fn [[topic rs]]
                                 (let [n (count rs)]
                                   (when (>= n min-count)
                                     (let [tm (mean (mapv :intensity rs))
                                           z  (if (zero? sigma) 0.0 (/ (- tm mu) sigma))
                                           samples
                                           (->> rs
                                                (sort-by :intensity >)
                                                (take 3)
                                                (mapv (fn [r]
                                                        {:comment-id (:comment-id r)
                                                         :intensity  (:intensity r)
                                                         :preview    (clip (:text r) 200)})))]
                                       {:topic          topic
                                        :n              n
                                        :mean-intensity tm
                                        :z              z
                                        :samples        samples})))))
                         (sort-by :z >)
                         vec)]
    {:user-id    user-id
     :n-comments (count rows)
     :baseline   {:mean mu :stdev sigma}
     :topics     ranked}))

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
                 {:out (msg/children ctx [{:user-id user-id :empty? true}])}
                 {:out (msg/children
                         ctx
                         (mapv (fn [c]
                                 {:user-id     user-id
                                  :comment-id  (str (:objectID c))
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

(defn- mk-aggregate [{:keys [min-count]}]
  (ca/batch-by-group
   :user-id
   (fn [user-id rows]
     (cranks-of user-id (remove :empty? rows) {:min-count min-count}))))

;; --- Flow -----------------------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [user-ids workers max-comments min-count]
     :or   {workers 8 max-comments 10000 min-count 5}
     :as opts}]
   (let [opts' (assoc opts :workers workers :max-comments max-comments
                           :min-count min-count
                           :user-ids (or user-ids []))]
     (step/serial :hn-crank-index
                  (mk-emit-users opts')
                  (cw/stealing-workers :fetchers workers (mk-fetch-history opts'))
                  split-comments
                  (cw/stealing-workers :classifiers workers classify-step)
                  (mk-aggregate opts')))))

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
  ([] (run-once! "./crank.json" {}))
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
