(ns toolkit.datapotamus.demo-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is use-fixtures]]
            [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.demo :as demo]
            [toolkit.datapotamus.runner :as runner]
            [toolkit.datapotamus.store :as store]
            [toolkit.datapotamus.trace :as trace])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *ds* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-demo-" ".sqlite" (into-array FileAttribute []))
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

(deftest demo-produces-three-rows-of-decremented-value
  (let [tc (component/start (trace/make {:datasource *ds* :batch-size 10 :flush-ms 20}))
        tmp (doto (File/createTempFile "dp-demo-input-" ".txt")
              (#(spit % "10\n")))
        pipeline (demo/pipeline {:datasource *ds*})
        rid (random-uuid)
        _ (store/insert-run! *ds* {:run-id rid :pipeline-id :arith-demo
                                    :input-path (.getAbsolutePath tmp)
                                    :input-slug (.getName tmp)
                                    :state :pending :started-at 0})
        r (runner/run-pipeline!
            {:datasource *ds* :events-ch (:events-ch tc) :run-id rid
             :pipeline pipeline
             :seed {:data {:path (.getAbsolutePath tmp) :slug (.getName tmp)}}
             :idle-complete-ms 200})]
    (try
      (is (= :completed (:state r)))
      (let [rows (jdbc/execute! *ds* ["SELECT data FROM results"])
            vs   (mapv #(json/read-str (:results/data %)) rows)]
        (is (= 3 (count vs)))
        (is (every? #{9} vs)))
      (finally (.delete tmp) (component/stop tc)))))
