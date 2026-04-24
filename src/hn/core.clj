(ns hn.core
  "Hacker News front-page summarizer as a potam3 flow.

   One tick every 10 min fetches the top N stories, their top K comments,
   summarizes each comment with Claude Haiku, and writes a flat JSON array
   of {:story-id :comment-id :comment-rank :comment :summary}.

   All HTTP and LLM work is parallelized via `c/workers` — each stage's
   I/O runs in K distinct procs (one thread each), with per-worker trace
   scopes visible in the event stream.

   REPL:
     (run-once! \"out.json\")
     (run-once! \"out.json\" {:trace? true})   ; print every event as it fires
     (start-scheduler! \"out.json\")
     (stop-scheduler!)"
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.llm :as llm]
            [toolkit.llm.anthropic :as anthropic]
            [toolkit.potam3.combinators :as c]
            [toolkit.potam3.flow :as flow]
            [toolkit.potam3.msg :as msg]
            [toolkit.potam3.step :as step])
  (:import (java.util.concurrent Executors TimeUnit)))

(def base "https://hacker-news.firebaseio.com/v0")
(def haiku "claude-haiku-4-5")
(def top-n-stories 30)
(def top-k-comments 10)
(def fetch-workers 16)
(def llm-workers 4)

(defonce claude (delay (anthropic/client (str/trim (slurp "claude.key")))))

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

;; --- Steps ------------------------------------------------------------------

(def fetch-top-ids
  (step/step :fetch-top-ids
             (fn [_tick]
               (vec (take top-n-stories
                          (get-json (str base "/topstories.json")))))))

(def split-ids
  (step/step :split-ids nil
             (fn [ctx _s ids]
               {:out (msg/children ctx ids)})))

(def fetch-story
  (step/step :fetch-story
             (fn [id] (get-json (str base "/item/" id ".json")))))

(def split-comments
  (step/step :split-comments nil
             (fn [ctx _s {:keys [id kids]}]
               {:out (msg/children ctx
                                   (map-indexed (fn [i cid]
                                                  {:story-id     id
                                                   :comment-id   cid
                                                   :comment-rank i})
                                                (take top-k-comments kids)))})))

(def fetch-comment
  (step/step :fetch-comment
             (fn [row]
               (assoc row :comment
                      (or (:text (get-json (str base "/item/" (:comment-id row) ".json")))
                          "")))))

(def summarize
  (step/step :summarize
             (fn [row]
               (assoc row :summary
                      (:text (llm/query
                              @claude
                              {:model      haiku
                               :max-tokens 256
                               :system     "Summarize HN comments in one sentence. No preface."
                               :messages   [{:role :user
                                             :content [{:type :text
                                                        :text (:comment row)}]}]}))))))

(defn- collect-into [a]
  (step/step :collect {:ins {:in ""} :outs {}}
             (fn [_ctx _s row]
               (swap! a conj row)
               {})))

;; --- Flow -------------------------------------------------------------------

(defn build-flow [out-atom]
  (step/serial
   fetch-top-ids
   split-ids
   (c/workers :story-fetchers   fetch-workers fetch-story)
   split-comments
   (c/workers :comment-fetchers fetch-workers fetch-comment)
   (c/workers :summarizers      llm-workers   summarize)
   (collect-into out-atom)))

(defn- preview [v]
  (let [s (pr-str v)] (if (> (count s) 80) (str (subs s 0 77) "...") s)))

(defn- print-event [_subject ev _match]
  (locking *out*
    (println
     (format "[%-8s %-6s] %-28s %s"
             (name (:kind ev))
             (some-> (:msg-kind ev) name (or ""))
             (str (:step-id ev) (when-let [p (:port ev)] (str " → " p)))
             (cond-> ""
               (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
               (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

(defn run-once!
  ([out-path] (run-once! out-path {}))
  ([out-path {:keys [trace?]}]
   (let [rows (atom [])
         opts (cond-> {:data :tick}
                trace? (assoc :subscribers {[:>] print-event}))
         res  (flow/run! (build-flow rows) opts)]
     (when (= :completed (:state res))
       (spit out-path (json/write-str @rows)))
     {:state (:state res) :count (count @rows) :error (:error res)})))

;; --- Scheduler --------------------------------------------------------------

(defonce scheduler (atom nil))

(defn start-scheduler! [out-path]
  (let [sched (Executors/newSingleThreadScheduledExecutor
               (-> (Thread/ofVirtual) (.name "hn-tick-") .factory))]
    (.scheduleAtFixedRate sched
                          (fn []
                            (try (run-once! out-path)
                                 (catch Throwable t (.printStackTrace t))))
                          0 10 TimeUnit/MINUTES)
    (reset! scheduler sched)))

(defn stop-scheduler! []
  (some-> @scheduler (.shutdownNow))
  (reset! scheduler nil))
