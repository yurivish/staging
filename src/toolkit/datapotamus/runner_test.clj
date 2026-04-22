(ns toolkit.datapotamus.runner-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.proc :as proc]
            [toolkit.datapotamus.runner :as runner]
            [toolkit.datapotamus.sinks :as sinks]
            [toolkit.datapotamus.store :as store]
            [toolkit.datapotamus.trace :as trace])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *ds* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-runner-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn with-db [t]
  (let [f (tmp-db)
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try
      (store/migrate! ds)
      (jdbc/execute! ds ["CREATE TABLE results (
                           rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                           run_id TEXT, msg_id TEXT, data_id TEXT,
                           written_at INTEGER, data TEXT)"])
      (binding [*ds* ds] (t))
      (finally (.delete f)))))

(use-fixtures :each with-db)

(defn- run-with [pipeline seed]
  (let [tc (component/start (trace/make {:datasource *ds* :batch-size 10 :flush-ms 20}))
        rid (random-uuid)
        _ (store/insert-run! *ds* {:run-id rid :pipeline-id (:pipeline-id pipeline)
                                    :input-path "x" :input-slug "x"
                                    :state :pending :started-at 0})
        r (runner/run-pipeline!
            {:datasource *ds* :events-ch (:events-ch tc) :run-id rid
             :pipeline pipeline :seed {:data seed} :idle-complete-ms 200})]
    (component/stop tc)
    r))

(deftest linear-pipeline-runs-to-completion
  (let [p {:pipeline-id :arith :entry :dec
           :procs {:dec  (proc/step-proc :dec (fn [m] (update m :data dec)))
                   :sink (sinks/sqlite-sink :sink
                          {:table :results :datasource *ds*})}
           :conns [[[:dec :out] [:sink :in]]]}
        r (run-with p 10)]
    (is (= :completed (:state r)))
    (is (= 1 (count (jdbc/execute! *ds* ["SELECT * FROM results"]))))))

(deftest pipeline-failure-records-failed-state
  (let [p {:pipeline-id :boom :entry :boom
           :procs {:boom (proc/step-proc :boom (fn [_] (throw (ex-info "no" {}))))
                   :sink (sinks/sqlite-sink :sink
                          {:table :results :datasource *ds*})}
           :conns [[[:boom :out] [:sink :in]]]}
        r (run-with p 1)]
    (is (= :failed (:state r)))))
