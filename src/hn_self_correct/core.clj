(ns hn-self-correct.core
  "HN top stories → per-story self-correcting summarizer agent loop.
   Each story enters its own draft → critique → revise loop, terminating
   when the critic's score crosses :threshold or :max-rounds is reached.
   Loops run concurrently — story A on round 4 while story B is on round 1.

   Stress-tests Datapotamus's recursive-feedback `:work` port pattern
   embedded inside a streaming pipeline (rather than a whole-program loop
   like doublespeak/distill).

   One-shot:
     clojure -M -e \"(require 'hn-self-correct.core) (hn-self-correct.core/run-once! \\\"summary.json\\\" {:trace? true})\""
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.hn.tree-fetch :as tree-fetch]
            [toolkit.llm.cli :as llm]
            [toolkit.pubsub :as pubsub]))

(def ^:private base "https://hacker-news.firebaseio.com/v0")
(def ^:private haiku "claude-haiku-4-5")
(def ^:private sonnet "claude-sonnet-4-5")

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

(defn- loop-inner
  "Inner handler for the cycle. Emits ONLY on `:out` when done, ONLY on
   `:work` when iterating. The wrapper in `c/stealing-workers` flags
   the last `:work` msg as the invocation terminator so the worker is
   freed even on iterations that don't emit on `:out`."
  [{:keys [threshold max-rounds draft! revise! critique!]}]
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out "" :work ""}}
    :on-data
    (fn [ctx _ {:keys [story round prev-draft prev-critique]}]
      (let [draft  (if prev-draft
                     (revise! story prev-draft prev-critique)
                     (draft! story))
            {:keys [score critique]} (critique! story draft)
            round' (inc round)
            hit?   (>= score threshold)
            cap?   (>= round' max-rounds)]
        (if (or hit? cap?)
          {:out [(msg/child ctx {:story-id        (:id story)
                                 :draft           draft
                                 :rounds          round'
                                 :final-score     score
                                 :max-rounds-hit? (and cap? (not hit?))})]}
          {:work [(msg/child ctx {:story         story
                                  :round         round'
                                  :prev-draft    draft
                                  :prev-critique critique})]})))}))

(defn summarize-loop
  "Returns a step that runs a per-item draft/critique/revise loop using
   `c/stealing-workers` with `:work` port for the cycle.

   Config:
     :k          — pool size (parallel concurrent loops, in flight at once)
     :threshold  — score in [0,1] at or above which the loop terminates
     :max-rounds — hard cap on rounds per item
     :draft!     — (story → draft) — first-round draft generator
     :revise!    — (story prev-draft prev-critique → draft) — later rounds
     :critique!  — (story draft → {:score s :critique c}) — every round

   Input msg-data shape:
     {:story s :round n :prev-draft d-or-nil :prev-critique c-or-nil}

   Output msg-data shape:
     {:story-id id :draft final-draft :rounds n
      :final-score s :max-rounds-hit? bool}"
  [{:keys [k] :as cfg}]
  (c/stealing-workers :summarize-loop k (loop-inner cfg)))

;; --- LLM calls ----------------------------------------------------------

(def ^:private draft-schema
  {:type "object"
   :properties {:summary {:type "string"
                          :description "A 3-sentence summary of the discussion's main point and the most interesting tension."}}
   :required ["summary"]})

(def ^:private critique-schema
  {:type "object"
   :properties {:score    {:type "number"
                           :description "A score in [0,1]: how well does the summary capture the discussion's substance and tension?"}
                :critique {:type "string"
                           :description "Concrete, actionable feedback (max 2 sentences) — what's missing or wrong."}}
   :required ["score" "critique"]})

(defn- story-context [story]
  (str "TITLE: " (:title story)
       (when-let [u (:url story)] (str "\nURL: " u))
       "\n\nDISCUSSION:\n" (:comments-text story)))

(defn- draft! [story]
  (when-let [r (llm/call-json!
                {:system "You are summarizing a Hacker News discussion."
                 :user   (str (story-context story)
                              "\n\nWrite a 3-sentence summary.")
                 :schema draft-schema
                 :model  haiku
                 :keys   :snake})]
    {:summary (:summary r)}))

(defn- revise! [story prev-draft prev-critique]
  (when-let [r (llm/call-json!
                {:system "You are revising a Hacker News discussion summary."
                 :user   (str (story-context story)
                              "\n\nPREVIOUS SUMMARY:\n" (:summary prev-draft)
                              "\n\nCRITIQUE:\n" prev-critique
                              "\n\nProduce an improved 3-sentence summary that addresses the critique.")
                 :schema draft-schema
                 :model  haiku
                 :keys   :snake})]
    {:summary (:summary r)}))

(defn- critique! [story draft]
  (when-let [r (llm/call-json!
                {:system "You are scoring a summary of a Hacker News discussion."
                 :user   (str (story-context story)
                              "\n\nSUMMARY:\n" (:summary draft)
                              "\n\nScore in [0,1] and critique.")
                 :schema critique-schema
                 :model  sonnet
                 :keys   :snake})]
    {:score    (some-> r :score double)
     :critique (:critique r)}))

;; --- Tree → loop input -------------------------------------------------

(defn- collect-comments-text
  "Walk a tree and collect all comment :text bodies into a flat seq."
  [node]
  (concat (when (and (= "comment" (:type node)) (:text node))
            [(:text node)])
          (mapcat collect-comments-text (:kid-trees node))))

(defn- tree->loop-input
  "Turn a fetched HN tree into the initial summarize-loop input."
  [tree]
  {:story         {:id            (:id tree)
                   :title         (:title tree)
                   :url           (:url tree)
                   :comments-text (str/join "\n---\n"
                                            (map str/trim
                                                 (collect-comments-text tree)))}
   :round         0
   :prev-draft    nil
   :prev-critique nil})

;; --- Pipeline assembly --------------------------------------------------

(defn- mk-fetch-top-ids [n]
  (step/step :fetch-top-ids
             (fn [_tick]
               (vec (take n (get-json (str base "/topstories.json")))))))

(def ^:private prepare-step
  (step/step :prepare nil
             (fn [ctx _s tree]
               {:out [(msg/child ctx (tree->loop-input tree))]})))

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories tree-workers loop-workers threshold max-rounds]
     :or   {n-stories 30 tree-workers 8 loop-workers 4
            threshold 0.8 max-rounds 5}}]
   (step/serial :hn-self-correct
                (mk-fetch-top-ids n-stories)
                (tree-fetch/step {:k tree-workers :get-json get-json})
                prepare-step
                (summarize-loop {:k          loop-workers
                                 :threshold  threshold
                                 :max-rounds max-rounds
                                 :draft!     draft!
                                 :revise!    revise!
                                 :critique!  critique!}))))

;; --- run-once! ----------------------------------------------------------

(defn- preview [v]
  (let [s (pr-str v)] (if (> (count s) 80) (str (subs s 0 77) "...") s)))

(defn- print-event [_subj ev _match]
  (locking *out*
    (println (format "[%-8s %-6s] %-32s %s"
                     (name (:kind ev))
                     (or (some-> (:msg-kind ev) name) "")
                     (str (:step-id ev) (when-let [p (:port ev)] (str " → " p)))
                     (cond-> ""
                       (contains? ev :data) (str "data=" (preview (:data ev))))))))

(defn run-once!
  ([] (run-once! "./summary.json" {}))
  ([out-path] (run-once! out-path {}))
  ([out-path {:keys [trace? pubsub] :as opts}]
   (let [ps    (or pubsub (when trace? (pubsub/make)))
         unsub (when trace? (pubsub/sub ps [:>] print-event))
         res   (flow/run-seq (build-flow opts) [:tick]
                             (cond-> {} ps (assoc :pubsub ps)))
         rows  (vec (mapcat identity (:outputs res)))]
     (when unsub (unsub))
     (when (= :completed (:state res))
       (spit out-path (with-out-str (json/pprint rows))))
     {:state (:state res) :count (count rows) :error (:error res)})))
