(ns toolkit.watcher
  "Polling file watcher as a Stuart Sierra component. Configured with a seq
   of `:watches` — each watch has its own `:dir`, `:include?` predicate, and
   `:on-change` callback. A single scheduler thread polls every watch in
   turn and runs each callback inline on that same thread, so a slow
   callback does back up the poll loop. Thrown callbacks are caught and
   logged; they don't cancel the scheduler."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [com.stuartsierra.component :as component])
  (:import [java.nio.file Files LinkOption]
           [java.nio.file.attribute BasicFileAttributes]
           [java.util.concurrent Executors TimeUnit]))

(defn clj-file? [^java.io.File f]
  (str/ends-with? (.getName f) ".clj"))

(def ^:private ^"[Ljava.nio.file.LinkOption;" no-link-opts
  (into-array LinkOption []))

;; Same runtime feature-detect that filewatcher.clj uses (see
;; filewatcher.clj:107-111) — and the same spirit as the Go toolkit's
;; modkey_unix.go vs modkey_other.go build-tag split. Falls back to
;; size+mtime on filesystems without POSIX inodes.
(def ^:private has-posix?
  (try
    (Files/getAttribute (.toPath (io/file ".")) "unix:ino" no-link-opts)
    true
    (catch Throwable _ false)))

(defn- path-attrs [path]
  (try
    (let [a (Files/readAttributes path BasicFileAttributes no-link-opts)]
      {:inode (when has-posix?
                (try (Files/getAttribute path "unix:ino" no-link-opts)
                     (catch Throwable _ nil)))
       :size  (.size a)
       :mtime (.toMillis (.lastModifiedTime a))})
    (catch Exception _ nil)))

(defn- scan [dir include?]
  (->> (file-seq (io/file dir))
       (filter #(and (.isFile ^java.io.File %) (include? %)))
       (keep #(some->> (path-attrs (.toPath ^java.io.File %)) (vector (str %))))
       (into {})))

(defn- log-err [msg t]
  (binding [*out* *err*]
    (println "[watcher]" msg)
    (when t (.printStackTrace ^Throwable t *err*))))

(defrecord FileWatcher [watches interval-ms executor]
  component/Lifecycle
  (start [this]
    (let [snap (atom (mapv (fn [{:keys [dir include?]}] (scan dir include?)) watches))
          exec (Executors/newSingleThreadScheduledExecutor)
          ms   (or interval-ms 100)]
      (.scheduleAtFixedRate
        exec
        (fn poll []
          (try
            (let [t0 (System/currentTimeMillis)]
              (dotimes [i (count watches)]
                (let [{:keys [dir include? on-change]} (nth watches i)
                      now  (scan dir include?)
                      prev (nth @snap i)]
                  (when (not= now prev)
                    (swap! snap assoc i now)
                    ;; Inline on the scheduler thread: a slow callback
                    ;; intentionally backs up the poll loop.
                    (try (on-change)
                         (catch Throwable t
                           (log-err (str "on-change threw for " dir) t))))))
              (let [dt (- (System/currentTimeMillis) t0)]
                (when (> dt 50)
                  (log-err (str "poll took " dt "ms") nil))))
            ;; scheduleAtFixedRate silently cancels future runs on throw.
            (catch Throwable t
              (log-err "poll threw" t))))
        ms ms TimeUnit/MILLISECONDS)
      (assoc this :executor exec)))
  (stop [this]
    (some-> executor .shutdown)
    (assoc this :executor nil)))
