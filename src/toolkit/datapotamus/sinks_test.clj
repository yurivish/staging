(ns toolkit.datapotamus.sinks-test
  (:require [clojure.core.async.flow :as flow]
            [clojure.test :refer [deftest is use-fixtures]]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.sinks :as sinks])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *ds* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-sinks-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn with-db [t]
  (let [f (tmp-db)
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try
      (jdbc/execute! ds ["CREATE TABLE results (
                           rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                           run_id TEXT, msg_id TEXT, data_id TEXT,
                           written_at INTEGER, data TEXT)"])
      (binding [*ds* ds] (t))
      (finally (.delete f)))))

(use-fixtures :each with-db)

(deftest sqlite-sink-writes-and-emits-sink-wrote
  (let [sfn (sinks/sqlite-sink :sink {:table :results :datasource *ds*})
        m {:msg-id (random-uuid) :data-id (random-uuid)
           :run-id (random-uuid) :data 9}
        _ (sfn) s (sfn {})
        [_ out] (sfn s :in m)
        report (::flow/report out)
        [recv sw success] report]
    (is (empty? (:out out)))
    (is (= 3 (count report)))
    (is (= :recv       (:kind recv)))
    (is (= :sink-wrote (:kind sw)))
    (is (= :success    (:kind success)))
    (is (= :sqlite (get-in sw [:payload-ref :kind])))
    (is (re-matches #"results:\d+" (get-in sw [:payload-ref :location])))
    (is (= 1 (count (jdbc/execute! *ds*
                      ["SELECT * FROM results WHERE run_id=?"
                       (str (:run-id m))]))))))
