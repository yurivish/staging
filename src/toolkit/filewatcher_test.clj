(ns toolkit.filewatcher-test
  "Mirrors the Go test suite at toolkit/filewatcher/watcher_test.go.
   Tests run against real temp dirs with a short poll interval (30ms)
   and :safety-gap-ms 0 so mtimes taken right after writes are trusted.
   Changes are deliberately to different content *sizes* where
   possible, so detection is robust to coarse-mtime filesystems."
  (:require
    [clojure.core.async :as a]
    [clojure.test :refer [deftest is]]
    [toolkit.filewatcher :as fw])
  (:import
    [java.nio.file Files Path]
    [java.nio.file.attribute FileAttribute FileTime]))

;; --- helpers --------------------------------------------------------------

(def ^:private no-attrs (into-array FileAttribute []))
(def ^:private take-timeout-ms 1500)

(defn- temp-dir ^Path []
  (Files/createTempDirectory "fw-test-" no-attrs))

(defn- delete-tree! [^Path root]
  (when (and root (Files/exists root (into-array java.nio.file.LinkOption [])))
    (try
      (doseq [^Path p (reverse (iterator-seq (.iterator (Files/walk root (into-array java.nio.file.FileVisitOption [])))))]
        (try (Files/delete p) (catch Throwable _ nil)))
      (catch Throwable _ nil))))

(defn- write! [^Path path ^String content]
  (Files/write path (.getBytes content "UTF-8")
               (into-array java.nio.file.OpenOption [])))

(defn- age-file! [^Path path]
  (Files/setLastModifiedTime
    path (FileTime/fromMillis (- (System/currentTimeMillis) 10000))))

(defn- take!! [ch]
  (let [[v _] (a/alts!! [ch (a/timeout take-timeout-ms)])]
    v))

(defn- drain!! [ch]
  (loop [acc []]
    (let [[v _] (a/alts!! [ch (a/timeout 120)])]
      (if (nil? v) acc (recur (conj acc v))))))

(defmacro with-watcher [[binding opts] & body]
  `(let [~binding (fw/start (fw/make ~opts))]
     (try ~@body
          (finally (fw/stop ~binding)))))

(defmacro with-tree [[dir-binding] & body]
  `(let [~dir-binding (temp-dir)]
     (try ~@body
          (finally (delete-tree! ~dir-binding)))))

(defn- child [^Path dir ^String name] (.resolve dir name))

;; --- file modification ----------------------------------------------------

(deftest file-modification-detected
  (with-tree [dir]
    (let [p (child dir "test.txt")]
      (write! p "hello")
      (with-watcher [w {:interval-ms 30 :safety-gap-ms 0 :changes-buffer 16}]
        (fw/watch-file w p)
        (write! p "world!")
        (let [ev (take!! (fw/changes w))]
          (is (some? ev) "got a change event")
          (is (= (str p) (:path ev)))
          (is (false? (:dir? ev))))))))

;; --- directory entry add --------------------------------------------------

(deftest directory-change-detected
  (with-tree [dir]
    (with-watcher [w {:interval-ms 30 :safety-gap-ms 0 :changes-buffer 16}]
      (fw/watch-dir w dir)
      (write! (child dir "newfile.txt") "new")
      (let [ev (take!! (fw/changes w))]
        (is (some? ev))
        (is (= (str dir) (:path ev)))
        (is (true? (:dir? ev)))))))

;; --- file deletion --------------------------------------------------------

(deftest file-deletion-detected
  (with-tree [dir]
    (let [p (child dir "test.txt")]
      (write! p "hello")
      (with-watcher [w {:interval-ms 30 :safety-gap-ms 0 :changes-buffer 16}]
        (fw/watch-file w p)
        (Files/delete p)
        (let [ev (take!! (fw/changes w))]
          (is (some? ev) "got a change event for delete")
          (is (= (str p) (:path ev))))))))

;; --- auto re-registration: two consecutive modifications detected --------

(deftest auto-reregistration
  (with-tree [dir]
    (let [p (child dir "test.txt")]
      (write! p "v1")
      (with-watcher [w {:interval-ms 30 :safety-gap-ms 0 :changes-buffer 16}]
        (fw/watch-file w p)
        (write! p "v22")
        (is (some? (take!! (fw/changes w))) "first modification detected")
        ;; drain any spurious events before the second mod
        (drain!! (fw/changes w))
        (write! p "v333")
        (is (some? (take!! (fw/changes w))) "second modification detected")))))

;; --- recursive: new file in existing subdir -------------------------------

(deftest recursive-new-file-in-subdir
  (with-tree [dir]
    (let [sub (child dir "sub")]
      (Files/createDirectory sub no-attrs)
      (with-watcher [w {:interval-ms 30 :safety-gap-ms 0 :changes-buffer 16}]
        (fw/watch-dir-recursive w dir)
        (write! (child sub "new.txt") "hello")
        (let [ev (take!! (fw/changes w))]
          (is (some? ev))
          (is (true? (:dir? ev)) "directory entry set changed"))))))

;; --- recursive: auto-watches new subdirectory -----------------------------

(deftest recursive-auto-watches-new-subdir
  (with-tree [dir]
    (with-watcher [w {:interval-ms 30 :safety-gap-ms 0 :changes-buffer 16}]
      (fw/watch-dir-recursive w dir)
      (let [newsub (child dir "newsub")]
        (Files/createDirectory newsub no-attrs)
        (is (some? (take!! (fw/changes w))) "root dir change detected")
        (drain!! (fw/changes w))
        (write! (child newsub "file.txt") "hello")
        (let [ev (take!! (fw/changes w))]
          (is (some? ev) "file in new subdir detected via re-walk"))))))

;; --- recursive: file modification deep in tree ----------------------------

(deftest recursive-file-modification
  (with-tree [dir]
    (let [deep (.resolve (.resolve dir "a") "b")]
      (Files/createDirectories deep no-attrs)
      (let [p (child deep "deep.txt")]
        (write! p "v1")
        (with-watcher [w {:interval-ms 30 :safety-gap-ms 0 :changes-buffer 16}]
          (fw/watch-dir-recursive w dir)
          (write! p "v22")
          (let [ev (loop [tries 0]
                     (when (< tries 20)
                       (let [ev (take!! (fw/changes w))]
                         (cond
                           (nil? ev) nil
                           (= (str p) (:path ev)) ev
                           :else (recur (inc tries))))))]
            (is (some? ev) "modification of deep file surfaced")
            (is (= (str p) (:path ev)))
            (is (false? (:dir? ev)))))))))

;; --- recursive: rename — new files in renamed dir are still detected -----

(deftest recursive-rename-picks-up-new-files
  (with-tree [dir]
    (let [sub (child dir "sub")]
      (Files/createDirectory sub no-attrs)
      (write! (child sub "file.txt") "hello")
      (with-watcher [w {:interval-ms 30 :safety-gap-ms 0 :changes-buffer 16}]
        (fw/watch-dir-recursive w dir)
        (let [newsub (child dir "newsub")]
          (Files/move sub newsub (into-array java.nio.file.CopyOption []))
          ;; Drain all changes resulting from the rename. Multiple stale
          ;; paths may fire before their re-registrations install "wait
          ;; for reappearance" checks. We don't assert exactly-one —
          ;; that'd be brittle against sampling order.
          (Thread/sleep 200)
          (drain!! (fw/changes w))
          ;; Now verify the renamed subdir is actively being watched:
          ;; adding a new file inside it surfaces as a change.
          (write! (child newsub "new.txt") "world")
          (is (some? (take!! (fw/changes w)))
              "new file in renamed dir is detected"))))))

;; --- stop halts notifications --------------------------------------------

(deftest stop-prevents-notifications
  (with-tree [dir]
    (let [p (child dir "test.txt")]
      (write! p "hello")
      (let [w (fw/start (fw/make {:interval-ms 30 :safety-gap-ms 0 :changes-buffer 16}))]
        (fw/watch-file w p)
        (let [ch (fw/changes w)]
          (fw/stop w)
          (write! p "world!")
          ;; After stop, channel is closed — alts!! should return nil.
          (let [[v _] (a/alts!! [ch (a/timeout 200)])]
            (is (nil? v) "no event after stop (channel closed or idle)")))))))

;; --- callback mode --------------------------------------------------------

(deftest on-change-callback-fires
  (with-tree [dir]
    (let [p (child dir "test.txt")
          events (atom [])
          latch  (java.util.concurrent.CountDownLatch. 1)]
      (write! p "hello")
      (with-watcher [w {:interval-ms 30
                        :safety-gap-ms 0
                        :on-change (fn [ev]
                                     (swap! events conj ev)
                                     (.countDown latch))}]
        (fw/watch-file w p)
        (write! p "world!")
        (is (.await latch 2 java.util.concurrent.TimeUnit/SECONDS)
            "callback fired within 2s")
        (let [ev (first @events)]
          (is (= (str p) (:path ev)))
          (is (false? (:dir? ev))))
        ;; In callback mode, (changes w) returns nil — no chan allocated.
        (is (nil? (fw/changes w)))))))

;; --- config errors --------------------------------------------------------

(deftest rejects-both-outputs-at-once
  (is (thrown? clojure.lang.ExceptionInfo
               (fw/make {:on-change identity :changes-buffer 16}))))

(deftest invalid-option-type-throws
  (is (thrown? clojure.lang.ExceptionInfo
               (fw/make {:interval-ms "fast"}))))

;; --- safety gap suppresses too-new baselines ------------------------------

(deftest safety-gap-reports-young-files-as-dirty
  ;; A file whose mtime is within the gap has an :unusable baseline,
  ;; which the check-fn treats as "always dirty." Aging the file with
  ;; setLastModifiedTime pushes its mtime back by 10s, past the 3s gap,
  ;; so the baseline becomes usable and the file is quiet.
  (with-tree [dir]
    (let [p      (child dir "test.txt")
          events (atom [])]
      (write! p "hello")
      (age-file! p)
      (with-watcher [w {:interval-ms 30
                        :safety-gap-ms 3000
                        :on-change (fn [ev] (swap! events conj ev))}]
        (fw/watch-file w p)
        (Thread/sleep 150)
        (is (empty? @events) "aged file is quiet under 3s safety gap")))))

;; --- start/stop idempotence ----------------------------------------------

(deftest start-stop-idempotent
  (let [w (fw/make {:interval-ms 30 :safety-gap-ms 0 :changes-buffer 16})
        w1 (fw/start w)
        w2 (fw/start w1)]
    (is (identical? (:executor w1) (:executor w2))
        "start on already-started is a no-op")
    (fw/stop w2)
    ;; Second stop shouldn't throw even though executor is already shutdown.
    (is (nil? (:executor (fw/stop w2))))))
