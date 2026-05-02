(ns hn-alarm.core
  "Windowed comment-rate alarm. Stream of HN comments → tumbling
   window aggregation → alarm step that fires when a window's count
   exceeds a threshold.

   Stress-tests `c/tumbling-window` and demonstrates how a streaming
   pipeline routes per-window summaries through downstream filters.
   For visualization: each event carries a timestamp, windows close
   when the watermark advances, and alarms fan out only on bursts —
   the trace shows clear per-window punctuation.

   Live-firehose driver:
     clojure -M -e \"(require 'hn-alarm.core) (let [h (hn-alarm.core/run-live! {})] (Thread/sleep 60000) ((:stop! h)))\""
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators.aggregate :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

(def ^:private base "https://hacker-news.firebaseio.com/v0")

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

(defn- get-max-item! [] (get-json (str base "/maxitem.json")))
(defn- get-item!    [id] (get-json (str base "/item/" id ".json")))

;; --- Window summary fn ---------------------------------------------------

(defn- summarize-window
  "Build the per-window summary from a coll of comment events. Each
   event has `:by`, `:text`, `:time`."
  [start end items]
  (let [n     (count items)
        texts (keep :text items)
        mean-len (if (zero? n) 0
                     (long (/ (reduce + 0 (map count texts)) (max 1 (count texts)))))
        top   (when (seq items)
                (->> items (group-by :by) (sort-by (comp count val) >)
                     ffirst))]
    {:kind     :window
     :start    start
     :end      end
     :n        n
     :mean-len mean-len
     :top-by   top}))

;; --- Alarm step ----------------------------------------------------------

(defn- alarm-step [threshold]
  (step/step :alarm nil
             (fn [ctx _ window-summary]
               (if (and (= :window (:kind window-summary))
                        (> (:n window-summary) threshold))
                 ;; Emit BOTH the window summary AND a derived alarm so
                 ;; downstream consumers (and the trace) see both events.
                 {:out [(msg/child ctx window-summary)
                        (msg/child ctx (assoc window-summary :kind :alarm))]}
                 {:out [(msg/child ctx window-summary)]}))))

;; --- Pipeline ------------------------------------------------------------

(defn build-flow
  "Build the alarm pipeline.
   Config:
     :size-ms          — tumbling window size (default 60000 = 1 min)
     :alarm-threshold  — fire alarm when window count exceeds this (default 50)
     :on-late          — `(item → any)` called for late drops (default no-op)"
  ([] (build-flow {}))
  ([{:keys [size-ms alarm-threshold on-late]
     :or   {size-ms 60000 alarm-threshold 50 on-late (fn [_])}}]
   (step/serial :hn-alarm
                (c/tumbling-window {:size-ms   size-ms
                                    :time-fn   :time
                                    :on-late   on-late
                                    :on-window summarize-window})
                (alarm-step alarm-threshold))))

;; --- Live driver ---------------------------------------------------------

(defn- new-comment? [item] (= "comment" (:type item)))

(defn run-live!
  "Live driver: poll HN maxitem, fetch new items, push only comments
   into the alarm pipeline. Returns the polling handle.

   Options:
     :interval-ms      — poll cadence (default 10000)
     :start-id         — initial last-seen ID (default: current maxitem)
     :max-ticks        — optional cap on poll iterations (test-only)
     :size-ms          — window size for the alarm pipeline (default 60000)
     :alarm-threshold  — count above which an alarm is emitted (default 50)"
  [{:keys [interval-ms start-id max-ticks size-ms alarm-threshold]
    :or   {interval-ms 10000 size-ms 60000 alarm-threshold 50}}]
  (let [start (or start-id (get-max-item!))
        pipe  (build-flow {:size-ms size-ms :alarm-threshold alarm-threshold})]
    (flow/run-polling!
     pipe
     {:interval-ms interval-ms
      :init-state  {:last-id start}
      :max-ticks   max-ticks
      :poll-fn     (fn [{:keys [last-id]}]
                     (let [m       (get-max-item!)
                           new-ids (range (inc last-id) (inc m))
                           items   (vec (keep (fn [id]
                                                (try (let [it (get-item! id)]
                                                       (when (new-comment? it) it))
                                                     (catch Throwable _ nil)))
                                              new-ids))]
                       [{:last-id m} items]))})))
