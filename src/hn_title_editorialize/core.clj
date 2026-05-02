(ns hn-title-editorialize.core
  "HN submitted titles vs. linked-page <title>: how often, and how, do
   they diverge? No LLM by default — pure string metrics over the pair.

   Pipeline shape:
     fetch-top-ids → filter url-bearing → c/stealing-workers fetch-page-title
                  → compute-metrics → aggregate

   Demonstrates a per-story flow that doesn't touch comment trees, plus
   a 'compute deterministic metrics, no LLM' pattern.

   One-shot:
     clojure -M -e \"(require 'hn-title-editorialize.core) (hn-title-editorialize.core/run-once! \\\"title.json\\\" {:trace? true})\""
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators.workers :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.pubsub :as pubsub]))

(def base "https://hacker-news.firebaseio.com/v0")

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

(defn- http-get-text
  "GET `url` with a 10s timeout; returns the body as a string, or nil on
   non-HTML / non-200 / timeout. Never throws."
  [url]
  (try
    (let [{:keys [status headers body error]}
          @(http/get url {:timeout 10000 :as :text :follow-redirects true})
          ct (or (:content-type headers) "")]
      (when (and (nil? error)
                 (= 200 status)
                 (str/includes? (str/lower-case ct) "text/html")
                 (string? body))
        body))
    (catch Throwable _ nil)))

;; --- HTML extraction --------------------------------------------------------

(defn- ^:private decode-entities
  "Decode the small set of HTML entities we expect in <title>/og:title.
   Numeric character refs handled too. Anything else is left literal."
  [s]
  (when s
    (-> s
        (str/replace #"&#(\d+);"
                     (fn [[_ n]] (str (char (Integer/parseInt n)))))
        (str/replace #"&#[xX]([0-9a-fA-F]+);"
                     (fn [[_ n]] (str (char (Integer/parseInt n 16)))))
        (str/replace "&amp;"  "&")
        (str/replace "&lt;"   "<")
        (str/replace "&gt;"   ">")
        (str/replace "&quot;" "\"")
        (str/replace "&#39;"  "'")
        (str/replace "&apos;" "'")
        (str/replace "&nbsp;" " "))))

(defn- ^:private og-title
  "First <meta property=\"og:title\" content=\"...\"> in `html`."
  [html]
  (when html
    (when-let [m (re-find #"(?is)<meta\s+[^>]*property\s*=\s*[\"']og:title[\"'][^>]*content\s*=\s*[\"']([^\"']*)[\"']"
                          html)]
      (decode-entities (second m)))))

(defn- ^:private twitter-title
  [html]
  (when html
    (when-let [m (re-find #"(?is)<meta\s+[^>]*name\s*=\s*[\"']twitter:title[\"'][^>]*content\s*=\s*[\"']([^\"']*)[\"']"
                          html)]
      (decode-entities (second m)))))

(defn- ^:private html-title
  "Text inside the first <title>...</title>."
  [html]
  (when html
    (when-let [m (re-find #"(?is)<title[^>]*>(.*?)</title>" html)]
      (-> (second m) decode-entities str/trim))))

(defn extract-source-title
  "Pick og:title, then twitter:title, then <title>. Returns nil if all
   are missing or empty."
  [html]
  (->> [(og-title html) (twitter-title html) (html-title html)]
       (some #(when (and % (seq (str/trim %))) (str/trim %)))))

;; --- Suffix stripping (a small set of common boilerplate) -------------------

(def ^:private suffix-patterns
  "Regexes that strip site-name boilerplate from source titles. Order
   matters: the first match wins."
  [#"\s+[\|—–\-]\s+The New York Times\s*$"
   #"\s+–\s+The New York Times\s*$"
   #"\s+- Wikipedia\s*$"
   #"\s+\| Hacker News\s*$"
   #"\s+- BBC News\s*$"
   #"\s+- The Verge\s*$"
   #"\s+\| Ars Technica\s*$"
   #"\s+\| TechCrunch\s*$"
   #"\s+\| The Guardian\s*$"
   #"\s+- The Guardian\s*$"])

(defn strip-source-boilerplate [s]
  (if (string? s)
    (reduce (fn [acc re] (str/replace acc re ""))
            s
            suffix-patterns)
    s))

;; --- Metrics ----------------------------------------------------------------

(def ^:private stop-words
  #{"a" "an" "the" "of" "to" "in" "on" "for" "and" "or" "but"
    "is" "are" "was" "were" "be" "been" "being" "as" "at" "by"
    "with" "from" "this" "that" "these" "those" "it" "its"})

(defn- ^:private tokenize
  "Lowercase word-tokenize, split on whitespace + punctuation. Stop-words
   removed for set-based metrics; raw token list (with stop-words intact)
   used for length-based ones."
  [s]
  (when s
    (->> (-> s str/lower-case (str/split #"[^a-z0-9'À-ſ]+"))
         (remove str/blank?)
         vec)))

(defn- ^:private content-tokens [s]
  (->> (tokenize s) (remove stop-words) vec))

(defn- ^:private edit-distance
  "Levenshtein on character sequences."
  [^String a ^String b]
  (let [m (.length a)
        n (.length b)]
    (cond
      (zero? m) n
      (zero? n) m
      :else
      (let [arr (long-array (* (inc m) (inc n)))]
        (dotimes [i (inc m)] (aset arr (* i (inc n)) (long i)))
        (dotimes [j (inc n)] (aset arr j (long j)))
        (dotimes [i m]
          (dotimes [j n]
            (let [cost (if (= (.charAt a i) (.charAt b j)) 0 1)
                  ip1  (inc i)
                  jp1  (inc j)
                  del  (inc (aget arr (+ (* i (inc n)) jp1)))
                  ins  (inc (aget arr (+ (* ip1 (inc n)) j)))
                  sub  (+ cost (aget arr (+ (* i (inc n)) j)))]
              (aset arr (+ (* ip1 (inc n)) jp1)
                    (long (min del ins sub))))))
        (aget arr (+ (* m (inc n)) n))))))

(defn- ^:private lcs-len
  "Longest common subsequence length over two token vectors."
  [a b]
  (let [m (count a) n (count b)]
    (cond
      (zero? m) 0
      (zero? n) 0
      :else
      (let [arr (long-array (* (inc m) (inc n)))]
        (dotimes [i m]
          (dotimes [j n]
            (let [ip1 (inc i) jp1 (inc j)
                  v   (if (= (nth a i) (nth b j))
                        (inc (aget arr (+ (* i (inc n)) j)))
                        (max (aget arr (+ (* i (inc n)) jp1))
                             (aget arr (+ (* ip1 (inc n)) j))))]
              (aset arr (+ (* ip1 (inc n)) jp1) (long v)))))
        (aget arr (+ (* m (inc n)) n))))))

(def ^:private bracket-tag-patterns
  "Common HN editorial bracket conventions, in detection order. Each
   maps a regex over the HN title to the tag string we'll record."
  {#"^\[Show HN\]"  "Show HN"
   #"^\[Ask HN\]"   "Ask HN"
   #"\[paywall\]"  "paywall"
   #"\[pdf\]"      "pdf"
   #"\[(\d{4})\]"  "year"
   #"\[video\]"    "video"
   #"\[audio\]"    "audio"
   #"\[2006\]|\[2007\]|\[2008\]|\[2009\]|\[201\d\]|\[202\d\]" "year"})

(defn- ^:private bracket-tags
  "Vector of HN bracket-tag strings detected in `hn-title`. Order
   preserved per pattern table; duplicates dropped."
  [hn-title]
  (when hn-title
    (->> bracket-tag-patterns
         (keep (fn [[re tag]]
                 (when (re-find re hn-title) tag)))
         distinct
         vec)))

(defn- ^:private paren-asides
  "Parenthesized fragments in `hn-title` that don't appear in
   `source-title`. Useful for spotting editor-added context like
   '(2018)' or '(announcement)'."
  [hn-title source-title]
  (when hn-title
    (let [parens   (mapv (fn [m] (str/trim (subs m 1 (dec (count m)))))
                         (re-seq #"\([^()]*\)" hn-title))
          src-low  (when source-title (str/lower-case source-title))]
      (->> parens
           (remove str/blank?)
           (remove (fn [p]
                     (and src-low (str/includes? src-low (str/lower-case p)))))
           vec))))

(defn- ^:private count-char [^String s ^Character ch]
  (count (filter #(= % ch) s)))

(defn- ^:private divergence-flag
  [{:keys [exact-match ci-match normalized-edit-distance
           length-ratio lcs-token-fraction token-jaccard]}]
  (cond
    exact-match                 :identical
    (or ci-match
        (and normalized-edit-distance
             (< normalized-edit-distance 0.05))) :near-identical
    (and length-ratio (< length-ratio 0.7)
         lcs-token-fraction (> lcs-token-fraction 0.6))
    :clipped
    (and length-ratio (> length-ratio 1.3)
         lcs-token-fraction (> lcs-token-fraction 0.6))
    :expanded
    (and lcs-token-fraction (> lcs-token-fraction 0.4)
         token-jaccard (> token-jaccard 0.5))
    :rephrased
    :else                       :divergent))

(defn metrics
  "Compute the metric map for one (HN-title, source-title) pair.
   `source-title` may be nil if the page didn't yield one — the result
   is the same shape with `:source-title-missing? true`."
  [hn-title source-title]
  (let [src-stripped (strip-source-boilerplate source-title)]
    (if (or (nil? src-stripped) (str/blank? src-stripped))
      {:exact-match              false
       :ci-match                 false
       :token-jaccard            nil
       :normalized-edit-distance nil
       :lcs-token-fraction       nil
       :length-ratio             nil
       :bracket-tags             (bracket-tags hn-title)
       :paren-asides             (paren-asides hn-title nil)
       :q-mark-delta             nil
       :excl-mark-delta          nil
       :source-title-missing?    true
       :divergence-flag          :source-title-missing}
      (let [hn-low      (str/lower-case (or hn-title ""))
            src-low     (str/lower-case src-stripped)
            hn-tokens   (tokenize hn-title)
            src-tokens  (tokenize src-stripped)
            hn-content  (set (content-tokens hn-title))
            src-content (set (content-tokens src-stripped))
            jacc        (let [u (set/union hn-content src-content)]
                          (if (empty? u)
                            0.0
                            (double (/ (count (set/intersection hn-content src-content))
                                       (count u)))))
            max-len     (max (count hn-title) (count src-stripped))
            ned         (when (pos? max-len)
                          (double (/ (edit-distance hn-title src-stripped)
                                     max-len)))
            lcs         (lcs-len hn-tokens src-tokens)
            ;; lcs / shorter — measures "how much of the shorter title
            ;; survives in the other." For clipped (HN shorter), high
            ;; lcs-frac means HN's tokens are essentially a subsequence
            ;; of the source. For expanded (HN longer), high means the
            ;; source's tokens survive in HN. The plan's "longer-title
            ;; length" denominator was self-contradictory with the
            ;; clipped/expanded rules below, which need both
            ;; length-ratio AND lcs-frac > 0.6 — only achievable if
            ;; we measure preservation of the shorter title.
            shorter-toks (min (count hn-tokens) (count src-tokens))
            lcs-frac    (when (pos? shorter-toks)
                          (double (/ lcs shorter-toks)))
            length-r    (when (pos? (count src-tokens))
                          (double (/ (count hn-tokens) (count src-tokens))))
            base        {:exact-match              (= hn-title src-stripped)
                         :ci-match                 (= hn-low src-low)
                         :token-jaccard            jacc
                         :normalized-edit-distance ned
                         :lcs-token-fraction       lcs-frac
                         :length-ratio             length-r
                         :bracket-tags             (bracket-tags hn-title)
                         :paren-asides             (paren-asides hn-title src-stripped)
                         :q-mark-delta             (- (count-char (or hn-title "") \?)
                                                      (count-char src-stripped \?))
                         :excl-mark-delta          (- (count-char (or hn-title "") \!)
                                                      (count-char src-stripped \!))
                         :source-title-missing?    false}]
        (assoc base :divergence-flag (divergence-flag base))))))

;; --- Steps ------------------------------------------------------------------

(defn- mk-fetch-top-ids [n]
  (step/step :fetch-top-ids
             (fn [_tick]
               (vec (take n (get-json (str base "/topstories.json")))))))

(def split-ids
  (step/step :split-ids nil
             (fn [ctx _s ids] {:out (msg/children ctx ids)})))

(def fetch-story
  (step/step :fetch-story
             (fn [id] (get-json (str base "/item/" id ".json")))))

(def filter-urlled
  "Drop stories without a :url (Show HN / Ask HN / dead). Returns
   `msg/drain` for the dropped ones; otherwise passes through."
  {:procs
   {:filter-urlled
    (step/handler-map
     {:ports {:ins {:in ""} :outs {:out ""}}
      :on-data (fn [ctx s {:keys [url] :as story}]
                 (if (and url (not (str/blank? url)))
                   [s {:out [(msg/child ctx story)]}]
                   [s msg/drain]))})}
   :conns [] :in :filter-urlled :out :filter-urlled})

(def fetch-page
  "HTTP-fetch the linked URL and extract the source title. Emits
   `{:story story :source-title <string-or-nil>}` on :out so downstream
   compute-metrics has both halves."
  (step/step :fetch-page nil
             (fn [ctx _s story]
               (let [url    (:url story)
                     t0     (System/nanoTime)
                     html   (http-get-text url)
                     stitle (extract-source-title html)
                     ms     (long (/ (- (System/nanoTime) t0) 1e6))]
                 (trace/emit ctx {:event :page-fetched
                                  :story-id (:id story)
                                  :url url
                                  :got-title? (some? stitle)
                                  :ms ms})
                 {:out [(msg/child ctx {:story story :source-title stitle})]}))))

(def compute-metrics-step
  (step/step :compute-metrics
             (fn [{:keys [story source-title]}]
               (let [m (metrics (:title story) source-title)]
                 {:story-id     (:id story)
                  :hn-title     (:title story)
                  :source-title source-title
                  :url          (:url story)
                  :metrics      m
                  :flag         (:divergence-flag m)}))))

(defn aggregate
  "Cumulative batch aggregator: on each row, emits the current
   cumulative summary (rate breakdown + most-divergent samples)
   over all rows seen so far. The last emission is the final summary."
  [{:keys [n-divergent-samples] :or {n-divergent-samples 10}}]
  {:procs
   {:agg
    (step/handler-map
     {:ports {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:rows []})
      :on-data
      (fn [ctx s row]
        (let [s'        (update s :rows conj {:msg (:msg ctx) :row row})
              rows-data (mapv :row (:rows s'))
              parents   (mapv :msg (:rows s'))
              n-total   (count rows-data)
              by-flag   (frequencies (map :flag rows-data))
              with-st   (count (remove (comp :source-title-missing? :metrics) rows-data))
              divergent (->> rows-data
                             (filter (fn [r] (#{:divergent :rephrased} (:flag r))))
                             (sort-by (fn [r]
                                        (or (-> r :metrics :token-jaccard) 1.0)))
                             (take n-divergent-samples)
                             vec)
              summary   {:n-stories            n-total
                         :n-with-source-title  with-st
                         :flag-counts          by-flag
                         :flag-rates           (when (pos? n-total)
                                                 (into {} (for [[k v] by-flag]
                                                            [k (double (/ v n-total))])))
                         :most-divergent       divergent
                         :rows                 rows-data}]
          [s' {:out [(msg/merge ctx parents summary)]}]))})}
   :conns [] :in :agg :out :agg})

;; --- Flow -------------------------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories fetch-workers n-divergent-samples]
     :or   {n-stories 100
            fetch-workers 16
            n-divergent-samples 10}}]
   (step/serial :hn-title-editorialize
                (mk-fetch-top-ids n-stories)
                split-ids
                (c/stealing-workers :stories fetch-workers fetch-story)
                filter-urlled
                (c/stealing-workers :pages fetch-workers fetch-page)
                compute-metrics-step
                (aggregate {:n-divergent-samples n-divergent-samples}))))

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
  ([] (run-once! "./title.json" {}))
  ([out-path] (run-once! out-path {}))
  ([out-path {:keys [trace? pubsub] :as opts}]
   (let [ps    (or pubsub (when trace? (pubsub/make)))
         unsub (when trace? (pubsub/sub ps [:>] print-event))
         res   (flow/run-seq (build-flow opts) [:tick]
                             (cond-> {} ps (assoc :pubsub ps)))
         summary (first (first (:outputs res)))]
     (when unsub (unsub))
     (when (= :completed (:state res))
       (spit out-path (with-out-str (json/pprint summary))))
     {:state (:state res)
      :n-stories (:n-stories summary)
      :flag-rates (:flag-rates summary)
      :error (:error res)})))
