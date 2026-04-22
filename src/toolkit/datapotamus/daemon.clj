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
  (let [p   (->path abs-path)
        s   (stat p)
        rid (random-uuid)
        prior (reg/install! registry slug {:run-id rid})]
    (when prior
      (store/update-run! datasource (:run-id prior)
                         {:state :cancelled
                          :finished-at (System/currentTimeMillis)}))
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
          :idle-complete-ms idle-complete-ms})
        (finally (reg/remove-if-matches! registry slug rid))))))

(defn- handle! [{:keys [watch-dir registry] :as ctx} {:keys [path dir?]}]
  (if dir?
    ;; Directory change: find files whose slugs have no live registry entry.
    (let [root (->path path)
          ^"[Ljava.nio.file.FileVisitOption;" no-opts (into-array java.nio.file.FileVisitOption [])]
      (try
        (doseq [^Path child (iterator-seq (.iterator (Files/walk root no-opts)))
                :when (not (Files/isDirectory child
                                              ^"[Ljava.nio.file.LinkOption;" (into-array LinkOption [])))]
          (let [abs  (str child)
                slug (str (.relativize (->path watch-dir) child))]
            (when-not (reg/current registry slug)
              (spawn-run! ctx slug abs))))
        (catch Throwable _ nil)))
    ;; File content change.
    (let [slug (str (.relativize (->path watch-dir) (->path path)))]
      (spawn-run! ctx slug path))))

(defrecord PipelineDaemon [watch-dir datasource events-ch pipeline
                           stable-gap-ms idle-complete-ms
                           fw registry stop-ch done-ch]
  component/Lifecycle
  (start [this]
    (let [fw' (-> (fw/make {:interval-ms 50 :safety-gap-ms stable-gap-ms
                            :changes-buffer 64})
                  (fw/watch-dir-recursive watch-dir)
                  fw/start)
          r    (reg/make)
          stop (a/chan)
          done (a/chan)
          ctx  {:watch-dir watch-dir :datasource datasource
                :events-ch events-ch :pipeline pipeline
                :idle-complete-ms idle-complete-ms :registry r}]
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
      (assoc this :fw fw' :registry r :stop-ch stop :done-ch done)))
  (stop [this]
    (when stop-ch (a/close! stop-ch))
    (when done-ch (a/<!! done-ch))
    (when fw (fw/stop fw))
    (assoc this :fw nil :registry nil :stop-ch nil :done-ch nil)))

(defn make
  [{:keys [stable-gap-ms idle-complete-ms]
    :or {stable-gap-ms 3000 idle-complete-ms 500} :as opts}]
  (map->PipelineDaemon (assoc opts
                              :stable-gap-ms stable-gap-ms
                              :idle-complete-ms idle-complete-ms)))
