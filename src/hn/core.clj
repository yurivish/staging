(ns hn.core
  "Hacker News front-page summarizer as a datapotamus flow.

   One tick every 10 min fetches the top N stories, their top K comments,
   summarizes each comment with Claude Haiku, and writes a flat JSON array
   of {:story-id :comment-id :comment-rank :comment :summary}.

   All HTTP and LLM work is parallelized via `c/round-robin-workers` — each stage's
   I/O runs in K distinct procs (one thread each), with per-worker trace
   scopes visible in the event stream.

   One-shot from the shell (no REPL, exits on completion):

     clojure -M -e \"(require 'hn.core) (hn.core/run-once! \\\"out.json\\\" {:trace? true})\"

   REPL (for iteration; file watcher reloads on edit):

     clojure -M:dev
     user=> (require 'hn.core)
     user=> (run-once! \"out.json\" {:trace? true})   ; print every event
     user=> (start-scheduler! \"out.json\")
     user=> (stop-scheduler!)

   Gotcha — do NOT combine `-M:dev` with a shell `-e`: the :dev alias's
   :main-opts (`-e '(user/start!)' -r`) get *prepended* by the CLI, so the
   `-r` REPL main-opt wins and everything after it becomes inert
   *command-line-args*. Your expression never evaluates."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.pubsub :as pubsub])
  (:import [dev.langchain4j.model.anthropic AnthropicChatModel]
           [dev.langchain4j.model.chat.request ChatRequest]
           [dev.langchain4j.data.message UserMessage SystemMessage]
           [java.util.concurrent Executors TimeUnit]))

(def base "https://hacker-news.firebaseio.com/v0")
(def haiku "claude-haiku-4-5")
(def top-n-stories 30)
(def top-k-comments 10)
(def fetch-workers 16)
(def llm-workers 4)

(defonce ^:private summarizer
  (delay (-> (AnthropicChatModel/builder)
             (.apiKey (str/trim (slurp "claude.key")))
             (.modelName haiku)
             (.maxTokens (int 256))
             .build)))

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
               (let [req (-> (ChatRequest/builder)
                             (.messages [(SystemMessage/from "Summarize HN comments in one sentence. No preface.")
                                         (UserMessage/from (:comment row))])
                             .build)]
                 (assoc row :summary
                        (-> (.chat @summarizer req) .aiMessage .text))))))

(defn- fake-summary []
  (apply str (repeatedly (+ 20 (rand-int 40))
                         #(rand-nth "abcdefghijklmnopqrstuvwxyz      "))))

(def fake-summarize
  (step/step :summarize
             (fn [row]
               (Thread/sleep (+ 300 (rand-int 700)))
               (assoc row :summary (fake-summary)))))

;; --- Flow -------------------------------------------------------------------

;; The flow's `:out` boundary is left exposed — `run-once!` uses
;; `flow/run-seq`, which appends its own collector. Callers that want
;; the manual API (e.g. the visualizer in `demo/server.clj`) can wrap
;; this in `(step/serial _ (step/sink))` to terminate the stream.
(defn build-flow []
  (step/serial
   fetch-top-ids
   split-ids
   (c/round-robin-workers :story-fetchers   fetch-workers fetch-story)
   split-comments
   (c/round-robin-workers :comment-fetchers fetch-workers fetch-comment)
   (c/round-robin-workers :summarizers      llm-workers   fake-summarize)))

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
  ([] (run-once! "./out.json" {}))
  ([out-path] (run-once! out-path {}))
  ([out-path {:keys [trace? pubsub]}]
   (let [ps     (or pubsub (when trace? (pubsub/make)))
         unsub  (when trace? (pubsub/sub ps [:>] print-event))
         opts   (cond-> {} ps (assoc :pubsub ps))
         res    (flow/run-seq (build-flow) [:tick] opts)
         rows   (first (:outputs res))]
     (when unsub (unsub))
     (when (= :completed (:state res))
       (spit out-path (with-out-str (json/pprint rows))))
     {:state (:state res) :count (count rows) :error (:error res)})))

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
