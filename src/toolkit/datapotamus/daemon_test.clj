(ns toolkit.datapotamus.daemon-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.daemon :as daemon]
            [toolkit.datapotamus.proc :as proc]
            [toolkit.datapotamus.sinks :as sinks]
            [toolkit.datapotamus.store :as store]
            [toolkit.datapotamus.trace :as trace])
  (:import [java.io File]
           [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute FileTime]))

(def ^:dynamic *ds* nil)
(def ^:dynamic ^Path *dir* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-daemon-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn fix [t]
  (let [f (tmp-db)
        d (Files/createTempDirectory "dp-daemon-" (into-array FileAttribute []))
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try
      (store/migrate! ds)
      (jdbc/execute! ds ["CREATE TABLE results (
                           rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                           run_id TEXT, msg_id TEXT, data_id TEXT,
                           written_at INTEGER, data TEXT)"])
      (binding [*ds* ds *dir* d] (t))
      (finally
        (.delete f)
        (doseq [p (reverse (iterator-seq
                              (.iterator (Files/walk d (into-array java.nio.file.FileVisitOption [])))))]
          (try (Files/delete p) (catch Throwable _)))))))

(use-fixtures :each fix)

(defn- write-aged! [^Path p ^String s]
  (Files/write p (.getBytes s "UTF-8") (into-array java.nio.file.OpenOption []))
  (Files/setLastModifiedTime
    p (FileTime/fromMillis (- (System/currentTimeMillis) 10000))))

(defn- read-int [m]
  (assoc m :data (Integer/parseInt (str/trim (slurp (:path (:data m)))))))

(defn- write! [^Path p ^String s]
  (Files/write p (.getBytes s "UTF-8") (into-array java.nio.file.OpenOption [])))

(defn- demo-pipeline [ds]
  {:pipeline-id :demo :entry :read
   :procs {:read (proc/step-proc :read read-int)
           :dec  (proc/step-proc :dec (fn [m] (update m :data dec)))
           :sink (sinks/sqlite-sink :sink {:table :results :datasource ds})}
   :conns [[[:read :out] [:dec :in]]
           [[:dec :out]  [:sink :in]]]})

(deftest daemon-processes-file-drop-end-to-end
  (let [tc (component/start (trace/make {:datasource *ds* :batch-size 10 :flush-ms 20}))
        dae (component/start
              (daemon/make {:watch-dir (str *dir*) :datasource *ds*
                            :events-ch (:events-ch tc) :pipeline (demo-pipeline *ds*)
                            :stable-gap-ms 50 :idle-complete-ms 200}))]
    (try
      (write-aged! (.resolve *dir* "n.txt") "10\n")
      (Thread/sleep 1500)
      (let [runs (store/list-runs *ds* 10)]
        (is (= 1 (count runs)))
        (is (= :completed (:state (first runs))))
        (is (= 1 (count (jdbc/execute! *ds* ["SELECT * FROM results"])))))
      (finally (component/stop dae) (component/stop tc)))))

(deftest daemon-debounces-filewatcher-event-storm
  ;; A real (non-aged) file write lands many :dir? false events inside the
  ;; filewatcher's safety-gap window. The daemon must coalesce those into
  ;; exactly one run.
  (let [tc (component/start (trace/make {:datasource *ds* :batch-size 10 :flush-ms 20}))
        dae (component/start
              (daemon/make {:watch-dir (str *dir*) :datasource *ds*
                            :events-ch (:events-ch tc) :pipeline (demo-pipeline *ds*)
                            :stable-gap-ms 200 :idle-complete-ms 200}))]
    (try
      (write! (.resolve *dir* "n.txt") "10\n")
      (Thread/sleep 1500)
      (is (= 1 (count (store/list-runs *ds* 10))))
      (is (= 1 (count (jdbc/execute! *ds* ["SELECT * FROM results"]))))
      (finally (component/stop dae) (component/stop tc)))))
