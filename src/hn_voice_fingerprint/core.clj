(ns hn-voice-fingerprint.core
  "Voice-fingerprint drift over a HN user's comment history. Pure
   per-comment style features (no LLM), bucketed by quarter (default)
   or month, with a per-user drift series of cosine distances between
   consecutive period mean-vectors.

   Data path:
     emit-users  →  fetch-author-history (paginated Algolia)
     split-and-feature (msg/children — period bucket + features)
     aggregate-by-user (group-by user-id × period; mean+stdev;
                        drift series in the same step)

   No LLM. Mac-mini-trivial.

   One-shot:
     clojure -M -e \"(require 'hn-voice-fingerprint.core) (hn-voice-fingerprint.core/run-once-for-user! \\\"tptacek\\\" \\\"voice.json\\\" {:trace? true})\""
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
  (:import [java.time LocalDate Instant ZoneOffset]))

(def algolia-base "https://hn.algolia.com/api/v1/search_by_date")

;; --- Tokenization & basic helpers ------------------------------------------

(def ^:private token-pat
  ;; Word characters with optional internal apostrophe (don't, won't).
  #"[a-zA-Z0-9]+(?:'[a-zA-Z]+)?")

(defn tokenize
  "Lowercased word tokens. Keeps internal apostrophes."
  [s]
  (if (str/blank? s)
    []
    (mapv str/lower-case (re-seq token-pat s))))

(def ^:private url-pat #"https?://\S+")
(def ^:private number-pat #"\b\d[\d.,]*\b")
(def ^:private code-pat #"`[^`]*`")
(def ^:private sentence-split #"[.!?]+")
(def ^:private para-split #"\n\s*\n+")

;; --- Feature lexicons ------------------------------------------------------

(def ^:private hedge-words
  #{"maybe" "perhaps" "might" "seems" "arguably" "possibly" "probably" "kinda"})
(def ^:private hedge-phrases
  ["i think" "i guess" "sort of" "kind of"])

(def ^:private assertion-words
  #{"obviously" "clearly" "definitely" "always" "never" "undoubtedly" "certainly"})
(def ^:private assertion-phrases
  ["of course" "without question"])

(def ^:private first-person #{"i" "me" "my" "mine"})
(def ^:private second-person #{"you" "your" "yours"})
(def ^:private negation #{"not" "no" "never"})

(defn- count-occurrences [^String text ^String needle]
  (loop [i 0 n 0]
    (let [j (.indexOf text needle (int i))]
      (if (neg? j) n (recur (+ j (count needle)) (inc n))))))

;; --- Period bucket ---------------------------------------------------------

(defn period-of
  "epoch seconds + bucket → \"YYYY-Qq\" or \"YYYY-MM\"."
  [^long epoch-s bucket]
  (let [d (LocalDate/ofInstant (Instant/ofEpochSecond epoch-s) ZoneOffset/UTC)]
    (case bucket
      :month   (format "%04d-%02d" (.getYear d) (.getMonthValue d))
      :quarter (format "%04d-Q%d"
                       (.getYear d)
                       (inc (quot (dec (.getMonthValue d)) 3))))))

;; --- Per-comment features --------------------------------------------------

(defn- per-100 [n n-tokens]
  (if (zero? n-tokens) 0.0 (* 100.0 (/ (double n) n-tokens))))

(defn- mean ^double [xs]
  (if (empty? xs) 0.0 (double (/ (reduce + 0.0 xs) (count xs)))))

(defn- stdev ^double [xs]
  (if (< (count xs) 2)
    0.0
    (let [m (mean xs)]
      (Math/sqrt (mean (mapv (fn [x] (let [d (- x m)] (* d d))) xs))))))

(defn- mattr
  "Moving-average type-token ratio, window=50. Falls back to TTR for
   short token sequences."
  ^double [tokens]
  (let [n (count tokens)
        w 50]
    (cond
      (zero? n) 0.0
      (< n w)   (double (/ (count (set tokens)) n))
      :else
      (let [tv (vec tokens)
            windows (- (inc n) w)]
        (loop [i 0 acc 0.0]
          (if (>= i windows)
            (/ acc windows)
            (let [seg (subvec tv i (+ i w))]
              (recur (inc i)
                     (+ acc (/ (count (set seg)) (double w)))))))))))

(defn- caps-bigram-count
  "Adjacent capitalized tokens. Crude NER proxy."
  [orig-tokens]
  (loop [prev nil ts orig-tokens n 0]
    (if-let [t (first ts)]
      (let [cap? (and (>= (count t) 1)
                      (Character/isUpperCase (.charAt ^String t 0))
                      (not (#{"I"} t)))]
        (recur t (rest ts)
               (if (and cap? prev
                        (>= (count prev) 1)
                        (Character/isUpperCase (.charAt ^String prev 0))
                        (not (#{"I"} prev)))
                 (inc n)
                 n)))
      n)))

(defn features
  "All ~20 style features as a flat map of numbers. Every value is a
   number — no nils — so downstream aggregation is safe."
  [text]
  (let [text       (or text "")
        lower-text (str/lower-case text)
        tokens     (tokenize text)
        orig-toks  (re-seq token-pat text)
        n-tokens   (count tokens)
        n-chars    (count text)
        sentences  (->> (str/split text sentence-split)
                        (map str/trim)
                        (remove str/blank?))
        n-sent     (count sentences)
        sent-lens  (mapv #(count (tokenize %)) sentences)
        word-lens  (mapv count tokens)
        paragraphs (->> (str/split text para-split)
                        (map str/trim)
                        (remove str/blank?))
        n-para     (count paragraphs)
        para-lens  (mapv #(count (tokenize %)) paragraphs)
        ;; lexical-set rates
        tok-set    (frequencies tokens)
        count-set  (fn [s] (reduce + 0 (map #(get tok-set % 0) s)))
        count-phr  (fn [phrases]
                     (reduce + 0
                             (map #(count-occurrences lower-text %) phrases)))
        hedge-n    (+ (count-set hedge-words) (count-phr hedge-phrases))
        assert-n   (+ (count-set assertion-words) (count-phr assertion-phrases))
        ;; n't matches in contractions (don't, won't, isn't, …)
        nt-n       (count-occurrences lower-text "n't")
        neg-n      (+ (count-set negation) nt-n)
        url-n      (count (re-seq url-pat text))
        num-n      (count (re-seq number-pat text))
        code-chars (reduce + 0 (map count (re-seq code-pat text)))
        caps-bg    (caps-bigram-count orig-toks)
        lines      (str/split text #"\n")
        n-lines    (count (remove str/blank? lines))
        quote-ln   (count (filter #(str/starts-with? (str/triml %) ">") lines))
        q-marks    (count (re-seq #"\?" text))
        excl       (count (re-seq #"!" text))]
    {:length-tokens          n-tokens
     :length-chars           n-chars
     :n-sentences            n-sent
     :n-paragraphs           n-para
     :mean-sentence-length   (mean sent-lens)
     :sentence-length-stdev  (stdev sent-lens)
     :mean-word-length       (mean word-lens)
     :mean-paragraph-length  (mean para-lens)
     :vocab-richness         (mattr tokens)
     :hedge-rate             (per-100 hedge-n n-tokens)
     :assertion-rate         (per-100 assert-n n-tokens)
     :first-person-rate      (per-100 (count-set first-person) n-tokens)
     :second-person-rate     (per-100 (count-set second-person) n-tokens)
     :negation-rate          (per-100 neg-n n-tokens)
     :link-rate              (per-100 url-n n-tokens)
     :code-fraction          (if (zero? n-chars) 0.0
                                 (double (/ code-chars n-chars)))
     :numeric-rate           (per-100 num-n n-tokens)
     :capitalized-bigram-rate (per-100 caps-bg n-tokens)
     :quote-rate             (if (zero? n-lines) 0.0
                                 (double (/ quote-ln n-lines)))
     :question-mark-rate     (per-100 q-marks n-tokens)
     :exclamation-rate       (per-100 excl n-tokens)}))

;; --- Period summarization --------------------------------------------------

(defn summarize-period
  "Take a seq of per-comment feature maps, return per-feature mean+stdev."
  [feature-maps]
  (let [ks (some-> feature-maps first keys sort)
        feats (into {}
                    (for [k ks]
                      (let [vs (mapv #(get % k 0) feature-maps)]
                        [k {:mean (mean vs) :stdev (stdev vs)}])))]
    {:n-comments (count feature-maps)
     :features   feats}))

;; --- Drift series ----------------------------------------------------------

(defn- cosine-distance ^double [a b]
  (let [dot   (reduce + 0.0 (map * a b))
        mag-a (Math/sqrt (reduce + 0.0 (map #(* % %) a)))
        mag-b (Math/sqrt (reduce + 0.0 (map #(* % %) b)))]
    (if (or (zero? mag-a) (zero? mag-b))
      0.0
      (- 1.0 (/ dot (* mag-a mag-b))))))

(defn drift-series
  "Per-user drift: cosine distance between consecutive periods'
   z-scored mean-vectors. Empty for <2 periods."
  [periods]
  (if (< (count periods) 2)
    []
    (let [ks         (sort (keys (:features (first periods))))
          period-vec (fn [p] (mapv #(-> p :features (get %) :mean (or 0.0)) ks))
          raw        (mapv period-vec periods)
          n-feats    (count ks)
          feat-stats (vec
                       (for [i (range n-feats)]
                         (let [vs (mapv #(nth % i) raw)]
                           [(mean vs) (stdev vs)])))
          z-of       (fn [v]
                       (mapv (fn [i x]
                               (let [[m sd] (nth feat-stats i)]
                                 (if (zero? sd) 0.0 (/ (- x m) sd))))
                             (range) v))
          z-vecs     (mapv z-of raw)]
      (mapv (fn [a b p1 p2]
              {:from     (:year-period p1)
               :to       (:year-period p2)
               :distance (cosine-distance a b)})
            z-vecs (rest z-vecs) periods (rest periods)))))

;; --- Algolia paginated source ----------------------------------------------

(defn algolia-author-page
  "One page of an author's comments. Returns
   {:hits [...] :nb-pages N}. Returns empty page on transport error."
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
  (loop [page 0
         hits []]
    (let [{ph :hits np :nb-pages} (algolia-author-page user page)
          all (into hits ph)]
      (if (or (>= (count all) max-comments)
              (>= (inc page) (or np 0))
              (empty? ph))
        (vec (take max-comments all))
        (recur (inc page) all)))))

;; --- Steps -----------------------------------------------------------------

(defn- mk-emit-users [{:keys [user-ids]}]
  (step/step :emit-users nil
             (fn [ctx _s _tick]
               {:out (msg/children ctx
                                   (mapv (fn [u] {:user-id u}) user-ids))})))

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

(defn- mk-split-and-feature [{:keys [bucket]}]
  (step/step :split-and-feature nil
             (fn [ctx _s {:keys [user-id comments]}]
               (if (empty? comments)
                 {:out (msg/children ctx
                                     [{:user-id user-id :empty? true}])}
                 {:out (msg/children
                         ctx
                         (mapv (fn [c]
                                 {:user-id user-id
                                  :year-period (period-of (:created_at_i c) bucket)
                                  :features (features (:comment_text c))})
                               comments))}))))

(def aggregate-by-user
  (ca/batch-by-group
   :user-id
   (fn [user-id rows]
     (let [real    (remove :empty? rows)
           by-per  (group-by :year-period real)
           periods (->> by-per
                        (sort-by key)
                        (mapv (fn [[per rs]]
                                (-> (summarize-period (mapv :features rs))
                                    (assoc :year-period per)))))
           drift   (drift-series periods)]
       {:user-id user-id :periods periods :drift drift}))))

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [user-ids bucket workers max-comments]
     :or   {bucket       :quarter
            workers      8
            max-comments 5000}
     :as opts}]
   (let [opts' (assoc opts :bucket bucket :workers workers
                           :max-comments max-comments
                           :user-ids (or user-ids []))]
     (step/serial :hn-voice-fingerprint
                  (mk-emit-users opts')
                  (cw/stealing-workers :fetchers workers (mk-fetch-history opts'))
                  (mk-split-and-feature opts')
                  aggregate-by-user))))

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
                       (:event ev)            (str "event="   (name (:event ev)) " ")
                       (:user-id ev)          (str "user="    (:user-id ev) " ")
                       (:n-comments ev)       (str "ncom="    (:n-comments ev) " ")
                       (:n-users ev)          (str "users="   (:n-users ev) " ")
                       (:ms ev)               (str "ms="      (:ms ev) " ")
                       (contains? ev :data)   (str "data="    (preview (:data ev)) " ")
                       (contains? ev :tokens) (str "tokens="  (preview (:tokens ev))))))))

(defn run-once!
  ([] (run-once! "./voice.json" {}))
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
