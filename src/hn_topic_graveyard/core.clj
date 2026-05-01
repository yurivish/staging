(ns hn-topic-graveyard.core
  "For one user, find topics they used to comment about and have
   stopped. Same source/classifier shape as `hn-emotion` (paginated
   Algolia by author + Haiku) with a smaller schema (:topic +
   :is-substantive); the interesting part is the trajectory classifier
   (active / fading / graveyard / ephemeral).

   Data path:
     emit-users → fetch-history (paginated Algolia)
     split-comments
     classify-llm (Haiku — topic + is-substantive)
     aggregate-by-user (group-by user × topic × quarter; classify
                        each (user, topic) trajectory; emit one msg
                        per user with graveyard + fading)

   One-shot:
     clojure -M -e \"(require 'hn-topic-graveyard.core) (hn-topic-graveyard.core/run-once-for-user! \\\"pg\\\" \\\"graveyard.json\\\" {:trace? true})\""
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

;; --- Quarter helpers ------------------------------------------------------

(defn quarter-of
  "epoch seconds → \"YYYY-Qq\"."
  [^long epoch-s]
  (let [d (LocalDate/ofInstant (Instant/ofEpochSecond epoch-s) ZoneOffset/UTC)]
    (format "%04d-Q%d" (.getYear d) (inc (quot (dec (.getMonthValue d)) 3)))))

(defn- parse-quarter [q]
  (let [[_ y qn] (re-matches #"(\d{4})-Q(\d)" q)]
    [(Long/parseLong y) (Long/parseLong qn)]))

(defn- next-quarter [q]
  (let [[y qn] (parse-quarter q)]
    (if (= qn 4)
      (format "%04d-Q1" (inc y))
      (format "%04d-Q%d" y (inc qn)))))

(defn quarters-between
  "Inclusive list of quarter labels from `lo` to `hi`."
  [lo hi]
  (loop [acc [] q lo]
    (cond
      (= q hi)             (conj acc q)
      (pos? (compare q hi)) acc
      :else                (recur (conj acc q) (next-quarter q)))))

;; --- Algolia paginated source ---------------------------------------------

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

;; --- Haiku classifier (topic + substantiveness gate) ----------------------

(def ^:private classify-schema
  {:type "object"
   :properties {:topic           {:type "string"
                                   :description "2-5 words, lowercase, about the subject matter."}
                :is_substantive  {:type "boolean"
                                   :description "True if the comment makes a substantive claim about the topic; false for jokes, one-liners, meta, off-topic."}}
   :required ["topic" "is_substantive"]})

(def ^:private classify-system
  "Classify a single Hacker News comment. Return TOPIC (2-5 words about the subject matter, lowercase) and IS_SUBSTANTIVE (true if the comment makes a substantive claim, false for jokes/one-liners/meta/off-topic). The story title is provided as context only — classify the COMMENT.")

(defn llm-classify!
  "Classify a comment. Returns {:topic s :is-substantive bool}."
  [{:keys [text story-title]}]
  (let [user-msg (str (when story-title (str "Story title: " story-title "\n\n"))
                      "Comment:\n" (or text ""))
        r (or (llm/call-json! {:system classify-system
                               :user   user-msg
                               :schema classify-schema
                               :model  haiku})
              {:topic "" :is-substantive false})]
    {:topic          (:topic r)
     :is-substantive (boolean (:is-substantive r))}))

;; --- Trajectory classifier ------------------------------------------------

(defn classify-trajectory
  "Take a sorted-by-quarter series `[[label n] ...]` filled across the
   user's quarter range; return one of `:active`, `:graveyard`,
   `:fading`, `:ephemeral`."
  [series {:keys [peak-min silent-quarters]}]
  (let [counts        (mapv second series)
        peak          (apply max 0 counts)
        cur           (or (peek counts) 0)
        trailing-zero (count (take-while zero? (rseq counts)))]
    (cond
      (< peak peak-min)                   :ephemeral
      (>= trailing-zero silent-quarters)  :graveyard
      (and (pos? peak)
           (< cur (* 0.25 peak)))         :fading
      :else                               :active)))

;; --- Per-user summarization ------------------------------------------------

(defn- clip [s n]
  (if (and s (> (count s) n)) (str (subs s 0 n) "…") s))

(defn summarize-user
  "Roll up classified comments for one user into a graveyard report."
  [user-id rows {:keys [peak-min silent-quarters] :as cfg}]
  (let [rows-sub (filter :is-substantive rows)
        ;; user's overall quarter range based on substantive comments
        all-qs   (sort (set (map :year-quarter rows-sub)))
        lo       (first all-qs)
        hi       (last  all-qs)
        full-qs  (when (and lo hi) (quarters-between lo hi))
        by-topic (group-by #(some-> (:topic %) str/lower-case str/trim) rows-sub)
        report-of
        (fn [topic rs]
          (let [by-q     (frequencies (map :year-quarter rs))
                series   (mapv (fn [q] [q (get by-q q 0)]) full-qs)
                traj     (classify-trajectory series cfg)
                peak-cnt (apply max 0 (map second series))
                peak-q   (some (fn [[q n]] (when (= n peak-cnt) q)) series)
                non-zero (filter (fn [[_ n]] (pos? n)) series)
                last-q   (some-> (last non-zero) first)
                samples  (->> rs
                              (sort-by :year-quarter #(compare %2 %1))
                              (take 3)
                              (mapv (fn [r]
                                      {:quarter    (:year-quarter r)
                                       :comment-id (:comment-id r)
                                       :preview    (clip (:text r) 200)})))]
            {:topic          topic
             :trajectory     traj
             :peak-quarter   peak-q
             :peak-count     peak-cnt
             :last-seen      last-q
             :total-comments (count rs)
             :samples        samples
             :series         (into {} series)}))
        all       (->> by-topic
                       (keep (fn [[topic rs]]
                               (when (seq topic)
                                 (report-of topic rs)))))
        graveyard (->> all (filter #(= :graveyard (:trajectory %)))
                       (sort-by :peak-count >) vec)
        fading    (->> all (filter #(= :fading (:trajectory %)))
                       (sort-by :peak-count >) vec)]
    {:user-id              user-id
     :n-comments-classified (count rows-sub)
     :n-topics              (count all)
     :graveyard            graveyard
     :fading               fading}))

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
                                 {:user-id     user-id
                                  :comment-id  (str (:objectID c))
                                  :year-quarter (quarter-of (:created_at_i c))
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
                                    :comment-id (:comment-id row) :ms ms})
                   {:out [(msg/child ctx (merge row c))]})))))

(defn- mk-aggregate [{:keys [peak-min silent-quarters] :as cfg}]
  (c/batch-by-group
   :user-id
   (fn [user-id rows]
     (summarize-user user-id (remove :empty? rows) cfg))))

;; --- Flow -----------------------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [user-ids workers max-comments peak-min silent-quarters]
     :or   {workers 8 max-comments 10000
            peak-min 5 silent-quarters 4}
     :as opts}]
   (let [opts' (assoc opts :workers workers :max-comments max-comments
                           :peak-min peak-min :silent-quarters silent-quarters
                           :user-ids (or user-ids []))]
     (step/serial :hn-topic-graveyard
                  (mk-emit-users opts')
                  (c/stealing-workers :fetchers workers (mk-fetch-history opts'))
                  split-comments
                  (c/stealing-workers :classifiers workers classify-step)
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
  ([] (run-once! "./graveyard.json" {}))
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
