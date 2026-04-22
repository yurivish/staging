(ns toolkit.filewatcher
  "Polling file watcher with fine-grained per-path registrations and a
   stream of change events. Port of `toolkit/filewatcher` (Go).

   Complements `toolkit.watcher` — which is a coarse 'any change under
   this directory triggers this callback' primitive — by offering
   per-path baselines, a randomized two-tier sampling loop, and
   automatic rewatching of recursive roots.

   Public API:
     (make opts)                  — returns a FileWatcher record
     (watch-file fw path)         — register a file for content changes
     (watch-dir fw path)          — register a dir for entry-set changes
     (watch-dir-recursive fw path)— register a whole subtree
     (changes fw)                 — core.async chan of {:path :dir?}
     (start fw) / (stop fw)       — also via component/Lifecycle

   Canonical usage (channel mode):
     (require '[clojure.core.async :as a])
     (def w (-> (make {:interval-ms 100}) start))
     (watch-dir-recursive w \"src\")
     (a/<!! (changes w))  ; => {:path \"src/foo.clj\" :dir? false}
     (stop w)

   Canonical usage (callback mode — fires inline on scheduler thread):
     (def w (-> (make {:interval-ms 100
                       :on-change (fn [{:keys [path dir?]}]
                                    (println \"changed\" path))})
                (watch-dir-recursive \"src\")
                start))
     (stop w)

   The three `watch-*` functions are interchangeable in shape — all
   take `(fw path)` and mutate the watcher — so swap
   `watch-dir-recursive` for `watch-file` or `watch-dir` in either
   example above; the rest stays the same. You can mix them on one
   watcher.

   Must specify one of `:on-change` (inline callback on scheduler
   thread) or `:changes-buffer` (int; core.async chan buffer size) at
   `make` time. Default is :changes-buffer 16 when neither is given.

   Sampling: on each tick one dirty path is reported. Recently-changed
   paths (cap :max-recent) are checked every tick; the rest are
   Fisher-Yates-shuffled and checked in chunks of roughly
   ceil(N / :max-ticks-per-scan) — bounded below by :min-per-tick — so
   a full scan completes in ~:max-ticks-per-scan ticks. With defaults
   (100ms interval, 20 ticks/scan), worst-case detection latency is
   ~2s; detection on recent paths is one tick.

   Baselines: files use a stat tuple (size, mtime, inode when
   available). A :safety-gap-ms window (default 3000ms) suppresses the
   baseline for files whose mtime is 'too new' to trust — the path is
   reported dirty until it ages out of the gap. Directories use the
   sorted vector of immediate entry names.

   When a dirty path fires, it is re-registered (baseline refreshed)
   before the event is emitted. For recursive roots, re-registration
   re-walks the subtree, so new files and subdirectories are picked up
   automatically.

   Known limitations:
   - `paths` grows monotonically. Stale entries (deleted files) stay
     resident with 'wait for reappearance' closures; they don't fire
     events but do consume memory.
   - Symlinks are not followed (matches Go's filepath.WalkDir).
   - TOCTOU: a file created-then-deleted between a re-walk's listing
     and its per-entry registration is missed.
   - Blocking consumer on the changes channel stalls the poll loop
     (matches Go's backpressure)."
  (:require
    [clojure.core.async :as a]
    [clojure.java.io :as io]
    [com.stuartsierra.component :as component]
    [malli.core :as m]
    [malli.error :as me])
  (:import
    [java.io File]
    [java.nio.file Files LinkOption Path]
    [java.nio.file FileVisitOption]
    [java.nio.file.attribute BasicFileAttributes]
    [java.util.concurrent Executors ScheduledExecutorService TimeUnit]))

(set! *warn-on-reflection* true)

;; --- Constants ---

(def ^:private ^"[Ljava.nio.file.LinkOption;" no-link-opts
  (into-array LinkOption []))

(def ^:private ^"[Ljava.nio.file.FileVisitOption;" no-walk-opts
  (into-array FileVisitOption []))

(def ^:private defaults
  {:interval-ms        100
   :max-recent         16
   :min-per-tick       64
   :max-ticks-per-scan 20
   :safety-gap-ms      3000
   :clock              (fn [] (System/currentTimeMillis))})

(def ^:private changes-buffer-default 16)

;; Feature-detect POSIX extended attrs (for inode). One probe at load time.
;; On filesystems that don't support it (Windows, non-POSIX FSes), we fall
;; back to a (size, mtime) baseline, which is still sufficient to catch
;; same-size same-mtime-but-different-inode races in rare cases.
(def ^:private has-posix?
  (try
    (Files/getAttribute (.toPath (io/file ".")) "unix:ino" no-link-opts)
    true
    (catch Throwable _ false)))

;; --- Path canonicalization ---

(defn- as-path ^Path [x]
  (cond
    (instance? Path x) x
    (instance? File x) (.toPath ^File x)
    :else              (.toPath (io/file x))))

(defn- canonical
  "Canonicalizes to an absolute, normalized Path. Does not resolve
   symlinks (matches Go's filepath.WalkDir default). Works for
   non-existent paths."
  ^Path [x]
  (.normalize (.toAbsolutePath (as-path x))))

;; --- Modkey ---

(defn- file-modkey
  "Returns a stat-tuple baseline for `path`, or :unusable if the file's
   mtime is within `safety-gap-ms` of `now-ms`, or nil if the file is
   missing or otherwise unreadable."
  [^Path path ^long safety-gap-ms ^long now-ms]
  (try
    (let [a      (Files/readAttributes path BasicFileAttributes no-link-opts)
          mtime  (.toMillis (.lastModifiedTime a))]
      (if (< (- now-ms mtime) safety-gap-ms)
        :unusable
        [(.size a)
         mtime
         (when has-posix?
           (try (Files/getAttribute path "unix:ino" no-link-opts)
                (catch Throwable _ nil)))]))
    (catch java.nio.file.NoSuchFileException _ nil)
    (catch Throwable _ nil)))

(defn- dir-entries
  "Returns a sorted vector of entry names in `path`, or nil if the dir
   is missing or unreadable."
  [^Path path]
  (try
    (with-open [stream (Files/newDirectoryStream path)]
      (->> stream
           (map #(.toString (.getFileName ^Path %)))
           sort
           vec))
    (catch Throwable _ nil)))

;; --- Check-fns ---
;;
;; A check-fn is a zero-arg fn that returns the string path if its
;; baseline no longer matches the current filesystem state, or nil if
;; clean. Check-fns are pure reads — they never mutate watcher state,
;; so they can run outside any coordination.

(defn- mk-file-check
  [^Path path path-str ^long safety-gap-ms clock-fn]
  (let [baseline (file-modkey path safety-gap-ms (long (clock-fn)))]
    (cond
      (nil? baseline)
      ;; File missing at registration — detect when it reappears as a file.
      (fn file-missing-check []
        (try
          (when (and (Files/exists path no-link-opts)
                     (not (Files/isDirectory path no-link-opts)))
            path-str)
          (catch Throwable _ nil)))

      (= :unusable baseline)
      ;; File too young; keep reporting dirty until it ages out of the gap.
      (fn file-unusable-check [] path-str)

      :else
      (fn file-modkey-check []
        (let [now (file-modkey path safety-gap-ms (long (clock-fn)))]
          (when-not (= now baseline)
            path-str))))))

(defn- mk-dir-check
  [^Path path path-str]
  (let [baseline (dir-entries path)]
    (if (nil? baseline)
      ;; Dir missing/unreadable at registration — detect reappearance.
      (fn dir-missing-check []
        (when (some? (dir-entries path))
          path-str))
      (fn dir-entries-check []
        (when-not (= baseline (dir-entries path))
          path-str)))))

;; --- State management ---
;;
;; State is a single atom. Shape:
;;   {:paths     {path-str {:check fn :dir? bool}}
;;    :recursive #{path-str}
;;    :recent    [path-str ...]   ; capped at :max-recent
;;    :to-scan   [path-str ...]   ; shuffled scan queue, consumed from front
;;    :per-tick  long}            ; recomputed on refill
;;
;; Only `tick!` runs on the scheduler thread; `watch-*` calls can come
;; from any thread. `tick!` only mutates :recent / :to-scan / :per-tick
;; (plus :paths via re-registration after a hit); `watch-*` only
;; mutates :paths / :recursive / :to-scan-tail. Concurrent appends to
;; :to-scan go to the tail; tick! consumes from the front, so the two
;; never collide.

(defn- initial-state []
  {:paths     {}
   :recursive #{}
   :recent    []
   :to-scan   []
   :per-tick  (:min-per-tick defaults)})

(defn- register
  "Installs `check` as the check-fn for `path-str`. Appends to :to-scan
   if this is a new path; otherwise just refreshes the check-fn."
  [state path-str dir? check]
  (let [new? (not (contains? (:paths state) path-str))]
    (cond-> (assoc-in state [:paths path-str] {:check check :dir? dir?})
      new? (update :to-scan conj path-str))))

(defn- compute-per-tick ^long [^long n ^long min-per-tick ^long max-ticks-per-scan]
  (max min-per-tick (long (quot (+ n max-ticks-per-scan -1) max-ticks-per-scan))))

(defn- rebuild-to-scan
  "If :to-scan is empty, repopulate from (keys :paths), Fisher-Yates
   shuffle (via clojure.core/shuffle), recompute :per-tick."
  [state opts]
  (let [paths (keys (:paths state))
        n     (count paths)]
    (assoc state
           :to-scan (vec (shuffle paths))
           :per-tick (compute-per-tick n
                                       (:min-per-tick opts)
                                       (:max-ticks-per-scan opts)))))

;; --- Tick ---

(declare watch-file!* watch-dir!* watch-dir-recursive!*)

(defn- log-err [msg ^Throwable t]
  (let [pw (java.io.PrintWriter. ^java.io.Writer *err*)]
    (.println pw (str "[filewatcher] " msg))
    (when t (.printStackTrace t pw))
    (.flush pw)))

(defn- find-recent-hit [paths recent]
  (loop [i 0]
    (if (>= i (count recent))
      nil
      (let [path-str (nth recent i)
            check   (:check (get paths path-str))]
        (if (and check (check))
          {:i i :path path-str}
          (recur (inc i)))))))

(defn- find-chunk-hit [paths chunk]
  (loop [i 0]
    (if (>= i (count chunk))
      nil
      (let [path-str (nth chunk i)
            check   (:check (get paths path-str))]
        (if (and check (check))
          {:i i :path path-str}
          (recur (inc i)))))))

(defn- tick! [fw]
  (try
    (let [opts       (:opts fw)
          state-atom (:state fw)
          s0         (deref state-atom)
          ;; Refill the scan queue if empty.
          s          (if (empty? (:to-scan s0)) (rebuild-to-scan s0 opts) s0)
          recent     (:recent s)
          per-tick   (:per-tick s)
          to-scan    (:to-scan s)
          chunk-n    (min per-tick (count to-scan))
          chunk      (subvec to-scan 0 chunk-n)
          paths      (:paths s)
          ;; Phase 1: check recent items. Check-fns run outside any swap.
          rhit       (find-recent-hit paths recent)
          ;; Phase 2: check chunk (only if no recent hit).
          chit       (when-not rhit (find-chunk-hit paths chunk))
          result     (volatile! nil)
          max-recent (:max-recent opts)]
      (swap! state-atom
             (fn [s']
               (let [;; If we rebuilt in the snapshot and state is still empty,
                     ;; apply our rebuilt plan. If watch-* appended in between,
                     ;; use the current :to-scan as-is (our rebuild had the same
                     ;; paths; any new appends are tail-additions).
                     s' (if (and (empty? (:to-scan s0)) (empty? (:to-scan s')))
                          (assoc s' :to-scan to-scan :per-tick per-tick)
                          s')]
                 (cond
                   rhit
                   (let [{:keys [i path]} rhit
                         r  (:recent s')
                         ;; Remove from position i, append at end (MRU).
                         nr (if (and (< i (count r)) (= path (nth r i)))
                              (conj (into (vec (subvec r 0 i))
                                          (subvec r (inc i)))
                                    path)
                              ;; :recent was modified between snapshot and commit
                              ;; (shouldn't happen — tick! is single-threaded —
                              ;; but be defensive).
                              (conj (filterv #(not= % path) r) path))]
                     (vreset! result {:path path :dir? (:dir? (get (:paths s') path))})
                     (assoc s' :recent nr))

                   chit
                   (let [{:keys [path]} chit
                         ;; Drop the chunk (first chunk-n items) from :to-scan.
                         ;; Concurrent appends went to the tail, so they're
                         ;; preserved by subvec'ing from chunk-n.
                         n-ts (count (:to-scan s'))
                         drop-n (min chunk-n n-ts)
                         new-ts (vec (subvec (:to-scan s') drop-n))
                         promoted (conj (:recent s') path)
                         trimmed (if (> (count promoted) max-recent)
                                   (vec (subvec promoted (- (count promoted) max-recent)))
                                   promoted)]
                     (vreset! result {:path path :dir? (:dir? (get (:paths s') path))})
                     (assoc s' :recent trimmed :to-scan new-ts))

                   :else
                   (let [n-ts (count (:to-scan s'))
                         drop-n (min chunk-n n-ts)
                         new-ts (vec (subvec (:to-scan s') drop-n))]
                     (assoc s' :to-scan new-ts))))))
      (when-let [{:keys [path dir?]} @result]
        (let [recursive? (contains? (:recursive @state-atom) path)]
          (cond
            recursive? (watch-dir-recursive!* fw path)
            dir?       (watch-dir!* fw path)
            :else      (watch-file!* fw path))
          (let [ev {:path path :dir? dir?}]
            (if-let [cb (:on-change fw)]
              (try (cb ev)
                   (catch Throwable t
                     (log-err (str "on-change threw for " path) t)))
              (a/>!! (:changes-ch fw) ev))))))
    (catch Throwable t
      (log-err "tick threw" t))))

;; --- Internal mutators ---

(defn- watch-file!* [fw path]
  (let [^Path p (canonical path)
        p-str   (str p)
        {:keys [safety-gap-ms clock]} (:opts fw)
        check   (mk-file-check p p-str safety-gap-ms clock)]
    (swap! (:state fw) register p-str false check)
    nil))

(defn- watch-dir!* [fw path]
  (let [^Path p (canonical path)
        p-str   (str p)
        check   (mk-dir-check p p-str)]
    (swap! (:state fw) register p-str true check)
    nil))

(defn- watch-dir-recursive!* [fw path]
  (let [^Path root  (canonical path)
        root-str    (str root)
        root-check  (mk-dir-check root root-str)
        {:keys [safety-gap-ms clock]} (:opts fw)]
    ;; Register root. If it's missing, the check becomes a "wait for
    ;; reappearance" fn and the walk below no-ops.
    (swap! (:state fw)
           (fn [s]
             (-> s
                 (register root-str true root-check)
                 (update :recursive conj root-str))))
    (try
      (with-open [stream (Files/walk root no-walk-opts)]
        (doseq [^Path child (iterator-seq (.iterator stream))
                :when       (not (.equals child root))]
          (let [c-str (str child)]
            (if (Files/isDirectory child no-link-opts)
              (swap! (:state fw)
                     (fn [s]
                       (-> s
                           (register c-str true (mk-dir-check child c-str))
                           (update :recursive conj c-str))))
              (swap! (:state fw)
                     (fn [s]
                       (register s c-str false
                                 (mk-file-check child c-str safety-gap-ms clock))))))))
      (catch java.nio.file.NoSuchFileException _ nil)
      (catch Throwable t (log-err (str "walk failed under " root-str) t)))
    nil))

;; --- Options schema ---

(def ^:private Options
  [:map
   [:interval-ms        {:optional true} pos-int?]
   [:changes-buffer     {:optional true} pos-int?]
   [:on-change          {:optional true} fn?]
   [:max-recent         {:optional true} pos-int?]
   [:min-per-tick       {:optional true} pos-int?]
   [:max-ticks-per-scan {:optional true} pos-int?]
   [:safety-gap-ms      {:optional true} nat-int?]
   [:clock              {:optional true} fn?]])

(defn- validate-opts! [opts]
  (when-not (m/validate Options opts)
    (throw (ex-info "filewatcher: invalid options"
                    {:reason :invalid-options
                     :errors (me/humanize (m/explain Options opts))})))
  (let [cb? (some? (:on-change opts))
        ch? (some? (:changes-buffer opts))]
    (when (and cb? ch?)
      (throw (ex-info "filewatcher: pass :on-change OR :changes-buffer, not both"
                      {:reason :invalid-options})))))

;; --- Public API ---

(declare start stop)

(defrecord FileWatcher [opts state executor changes-ch on-change]
  component/Lifecycle
  (start [this] (start this))
  (stop  [this] (stop this)))

(defn make
  "Creates a file watcher. See namespace docstring for options.

   Exactly one output mode must be chosen: pass `:on-change` for an
   inline callback, or `:changes-buffer` for a core.async channel. If
   neither is given, defaults to `:changes-buffer 16`.

   Example:
     (make {:interval-ms 50 :changes-buffer 32})
     (make {:on-change (fn [{:keys [path dir?]}] (prn path))})"
  [opts]
  (let [opts (cond-> (merge defaults opts)
               (and (not (contains? opts :on-change))
                    (not (contains? opts :changes-buffer)))
               (assoc :changes-buffer changes-buffer-default))]
    (validate-opts! opts)
    (map->FileWatcher
      {:opts       opts
       :state      (atom (initial-state))
       :executor   nil
       :changes-ch (when-let [n (:changes-buffer opts)] (a/chan n))
       :on-change  (:on-change opts)})))

(defn watch-file
  "Registers `path` (string, File, or Path) for content-change
   detection. Safe to call before or after start. Calling again on the
   same path refreshes the baseline.

   Example:
     (watch-file w \"config.edn\")"
  [fw path]
  (watch-file!* fw path)
  fw)

(defn watch-dir
  "Registers `path` for directory-entry-set changes (additions and
   removals of immediate children). Does NOT detect content changes to
   files within — use `watch-dir-recursive` or pair with `watch-file`
   for those. Safe to call before or after start.

   Example:
     (watch-dir w \"/etc/app/conf.d\")"
  [fw path]
  (watch-dir!* fw path)
  fw)

(defn watch-dir-recursive
  "Registers `path` and its entire subtree. Every directory is watched
   for entry-set changes; every file is watched for content changes.
   When any directory's entries change, the subtree under that
   directory is re-walked, automatically picking up new files and
   subdirectories. Symlinks are not followed. Safe to call before or
   after start.

   Example:
     (watch-dir-recursive w \"src\")"
  [fw path]
  (watch-dir-recursive!* fw path)
  fw)

(defn changes
  "Returns the core.async chan of change events, or nil if the watcher
   was configured with `:on-change`. Each event is `{:path :dir?}`.

   Example:
     (clojure.core.async/<!! (changes w))  ; => {:path \"...\" :dir? false}"
  [fw]
  (:changes-ch fw))

(defn start
  "Launches the polling task. Idempotent — if already started, returns
   the watcher unchanged.

   Example:
     (def w (-> (make {:interval-ms 50}) start))"
  [fw]
  (if (:executor fw)
    fw
    (let [^ScheduledExecutorService exec (Executors/newSingleThreadScheduledExecutor)
          interval (long (:interval-ms (:opts fw)))
          started  (assoc fw :executor exec)]
      (.scheduleWithFixedDelay exec
                               ^Runnable (fn tick-runner [] (tick! started))
                               interval interval TimeUnit/MILLISECONDS)
      started)))

(defn stop
  "Halts the polling task, awaits in-flight tick completion (up to 1s),
   then closes the changes channel. Idempotent.

   Example:
     (stop w)"
  [fw]
  (when-let [^ScheduledExecutorService exec (:executor fw)]
    (.shutdown exec)
    (.awaitTermination exec 1 TimeUnit/SECONDS))
  (when-let [ch (:changes-ch fw)]
    (a/close! ch))
  (assoc fw :executor nil :changes-ch nil))

(comment
  ;; Channel mode
  (require '[clojure.core.async :as a])
  (def w (-> (make {:interval-ms 50}) start))
  (watch-dir-recursive w "src/toolkit")
  (a/go-loop []
    (when-let [ev (a/<! (changes w))]
      (prn ev)
      (recur)))
  ;; edit any file under src/toolkit → see events
  (stop w)

  ;; Callback mode
  (def w (-> (make {:interval-ms 50
                    :on-change (fn [{:keys [path dir?]}]
                                 (println "changed" path "dir?" dir?))})
             (watch-dir-recursive "src/toolkit")
             start)))
