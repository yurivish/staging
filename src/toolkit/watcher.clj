(ns toolkit.watcher
  "Polling file watcher as a Stuart Sierra component. On any change under
   `:dir`, invokes `:on-change` on a detached thread so a slow callback
   doesn't back up the poll loop (and so the callback is free to stop this
   component as part of its work)."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [com.stuartsierra.component :as component])
  (:import [java.nio.file Files LinkOption]
           [java.nio.file.attribute BasicFileAttributes]
           [java.util.concurrent Executors TimeUnit]))

(defn- path-attrs [path]
  (try
    (let [opts (into-array LinkOption [])
          a    (Files/readAttributes path BasicFileAttributes opts)]
      {:inode (Files/getAttribute path "unix:ino" opts)
       :size  (.size a)
       :mtime (.toMillis (.lastModifiedTime a))})
    (catch Exception _ nil)))

(defn- scan [dir]
  (->> (file-seq (io/file dir))
       (filter #(and (.isFile %) (str/ends-with? (.getName %) ".clj")))
       (keep #(some->> (path-attrs (.toPath %)) (vector (str %))))
       (into {})))

(defn- log-err [msg t]
  (binding [*out* *err*]
    (println "[watcher]" msg)
    (when t (.printStackTrace t *err*))))

(defrecord FileWatcher [dir on-change interval-ms executor]
  component/Lifecycle
  (start [this]
    (let [snap (atom (scan dir))
          exec (Executors/newSingleThreadScheduledExecutor)
          ms   (or interval-ms 100)]
      (.scheduleAtFixedRate
        exec
        (fn poll []
          (try
            (let [t0  (System/currentTimeMillis)
                  now (scan dir)
                  dt  (- (System/currentTimeMillis) t0)]
              (when (> dt 50)
                (log-err (str "scan took " dt "ms") nil))
              (when (not= now @snap)
                (reset! snap now)
                ;; Run the callback off the scheduler thread so it can
                ;; safely stop this watcher, and so a slow callback
                ;; doesn't back up the polling loop.
                (future
                  (try (on-change)
                       (catch Throwable t
                         (log-err "on-change threw" t))))))
            ;; scheduleAtFixedRate silently cancels future runs on throw.
            (catch Throwable t
              (log-err "poll threw" t))))
        ms ms TimeUnit/MILLISECONDS)
      (assoc this :executor exec)))
  (stop [this]
    (some-> executor .shutdown)
    (assoc this :executor nil)))
