(ns toolkit.datapotamus.daemon
  (:require [clojure.core.async :as a]
            [com.stuartsierra.component :as component]
            [toolkit.datapotamus.registry :as reg]
            [toolkit.datapotamus.runner :as runner]
            [toolkit.datapotamus.store :as store]
            [toolkit.filewatcher :as fw])
  (:import [java.nio.file Files LinkOption Path Paths]
           [java.nio.file.attribute BasicFileAttributes]))

(set! *warn-on-reflection* true)

(defn- ->path ^Path [^String s] (Paths/get s (into-array String [])))

(defn- stat [^Path p]
  (when-let [^BasicFileAttributes attrs
             (try (Files/readAttributes p BasicFileAttributes
                                        ^"[Ljava.nio.file.LinkOption;" (into-array LinkOption []))
                  (catch Throwable _ nil))]
    {:mtime (.toMillis (.lastModifiedTime attrs))
     :size  (.size attrs)}))

(defn- spawn-run! [{:keys [datasource events-ch pipeline idle-complete-ms registry]}
                   slug ^String abs-path]
  (let [p         (->path abs-path)
        s         (stat p)
        rid       (random-uuid)
        cancel-ch (a/chan)
        prior     (reg/install! registry slug {:run-id rid :cancel-ch cancel-ch})]
    (when-let [prior-cancel (:cancel-ch prior)]
      (a/close! prior-cancel))
    (store/insert-run! datasource
                       {:run-id rid :pipeline-id (:pipeline-id pipeline)
                        :input-path abs-path :input-slug slug
                        :input-mtime (:mtime s) :input-size (:size s)
                        :state :pending :started-at (System/currentTimeMillis)})
    (a/thread
      (try
        (runner/run-pipeline!
         {:datasource datasource :events-ch events-ch :run-id rid
          :pipeline pipeline
          :seed {:data {:path abs-path :slug slug}}
          :idle-complete-ms idle-complete-ms
          :cancel-ch cancel-ch})
        (finally (reg/remove-if-matches! registry slug rid))))))

(defn- schedule-spawn!
  "Coalesce the filewatcher's rapid-fire events: record the latest seen
   event for `slug`, then after `debounce-ms` of silence for that slug,
   spawn a single run. If another event arrives first, that one resets
   the clock and this goroutine is a no-op."
  [{:keys [pending debounce-ms] :as ctx} slug ^String abs-path]
  (let [gen (:gen (get (swap! pending update slug
                                (fn [m]
                                  {:gen (inc (:gen m 0)) :path abs-path}))
                       slug))]
    (a/go
      (a/<! (a/timeout debounce-ms))
      (let [cur (get @pending slug)]
        (when (and cur (= gen (:gen cur)))
          (swap! pending dissoc slug)
          (spawn-run! ctx slug (:path cur)))))))

(defn- handle! [{:keys [watch-dir] :as ctx} {:keys [path dir?]}]
  (if dir?
    ;; Directory change: schedule spawns for every file under this subtree.
    ;; The debouncer in schedule-spawn! deduplicates with later per-file events.
    (let [root (->path path)
          ^"[Ljava.nio.file.FileVisitOption;" no-opts (into-array java.nio.file.FileVisitOption [])]
      (try
        (doseq [^Path child (iterator-seq (.iterator (Files/walk root no-opts)))
                :when (not (Files/isDirectory child
                                              ^"[Ljava.nio.file.LinkOption;" (into-array LinkOption [])))]
          (schedule-spawn! ctx
                           (str (.relativize (->path watch-dir) child))
                           (str child)))
        (catch Throwable _ nil)))
    ;; File content change.
    (schedule-spawn! ctx
                     (str (.relativize (->path watch-dir) (->path path)))
                     path)))

(defrecord PipelineDaemon [watch-dir datasource events-ch pipeline
                           stable-gap-ms debounce-ms idle-complete-ms
                           fw registry pending stop-ch done-ch]
  component/Lifecycle
  (start [this]
    ;; interval-ms is the filewatcher's poll period. debounce-ms must be
    ;; comfortably larger than it — otherwise filewatcher events can
    ;; straddle the debounce window and trigger duplicate spawns for the
    ;; same drop. 50ms here; `make` enforces debounce-ms >= stable-gap-ms.
    (let [fw' (-> (fw/make {:interval-ms 50 :safety-gap-ms stable-gap-ms
                            :changes-buffer 64})
                  (fw/watch-dir-recursive watch-dir)
                  fw/start)
          r    (reg/make)
          pend (atom {})
          stop (a/chan)
          done (a/chan)
          ctx  {:watch-dir watch-dir :datasource datasource
                :events-ch events-ch :pipeline pipeline
                :idle-complete-ms idle-complete-ms
                :debounce-ms debounce-ms
                :registry r :pending pend}]
      (a/thread
        (try
          (loop []
            (let [ch    (fw/changes fw')
                  [v c] (a/alts!! [ch stop])]
              (when (and (= c ch) (some? v))
                (try (handle! ctx v)
                     (catch Throwable ex
                       (binding [*out* *err*]
                         (println "datapotamus daemon error:" (ex-message ex)))))
                (recur))))
          (finally (a/close! done))))
      (assoc this :fw fw' :registry r :pending pend
             :stop-ch stop :done-ch done)))
  (stop [this]
    (when stop-ch (a/close! stop-ch))
    (when done-ch (a/<!! done-ch))
    (when fw (fw/stop fw))
    (assoc this :fw nil :registry nil :pending nil
           :stop-ch nil :done-ch nil)))

(defn make
  "Build a PipelineDaemon. Required keys: `:watch-dir` `:datasource`
   `:events-ch` `:pipeline`.

   Timing tunables (all in ms):

   `:stable-gap-ms` — trailing-edge window the filewatcher waits before
     trusting a file's mtime. Default 3000 (chosen to survive coarse-mtime
     filesystems; see `toolkit.filewatcher` — FAT has 2s resolution). Lower
     it to ~200 on modern filesystems for snappier turnaround.

   `:debounce-ms` — how long the daemon waits with no new event for a slug
     before spawning a run. Coalesces the filewatcher's rapid-fire events
     (one per 50ms poll tick while the file is dirty) into a single spawn.
     Must exceed `toolkit.filewatcher`'s poll interval (50ms) by a clear
     margin; defaults to `:stable-gap-ms`, which is already comfortable.

   `:idle-complete-ms` — how long the runner sits in quiescence on
     `report-chan` before declaring a run :completed. Default 500. Raise
     it if procs legitimately pause (e.g., LLM calls); lower it for
     snappier test suites.

   Supersede: if a new event for the same slug debounces into a spawn
   while an earlier run for that slug is in flight, the earlier run's
   `cancel-ch` is closed and it transitions to :cancelled. Sink writes
   already performed are NOT rolled back — filter queries by
   `runs.state = 'completed'` if that matters."
  [{:keys [stable-gap-ms debounce-ms idle-complete-ms]
    :or {stable-gap-ms 3000 idle-complete-ms 500} :as opts}]
  (map->PipelineDaemon (assoc opts
                              :stable-gap-ms stable-gap-ms
                              :debounce-ms (or debounce-ms stable-gap-ms)
                              :idle-complete-ms idle-complete-ms)))
