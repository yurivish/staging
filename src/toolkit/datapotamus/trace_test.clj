(ns toolkit.datapotamus.trace-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is use-fixtures]]
            [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.store :as store]
            [toolkit.datapotamus.trace :as trace])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *ds* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-trace-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn with-db [t]
  (let [f (tmp-db)
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try (store/migrate! ds) (binding [*ds* ds] (t))
         (finally (.delete f)))))

(use-fixtures :each with-db)

(defn- seed [ds run-id]
  (store/insert-run! ds {:run-id run-id :pipeline-id :d
                          :input-path "p" :input-slug "p"
                          :state :running :started-at 0}))

(deftest collector-batches-events-to-sqlite
  (let [run-id (random-uuid) _ (seed *ds* run-id)
        tc (component/start (trace/make {:datasource *ds* :batch-size 10 :flush-ms 20}))
        ch (:events-ch tc)]
    (try
      (dotimes [i 3]
        (a/>!! ch {:event-id (random-uuid) :run-id run-id :at i :kind :recv}))
      (Thread/sleep 100)
      (is (= 3 (count (store/get-events *ds* run-id nil))))
      (finally (component/stop tc)))))

(deftest collector-keeps-running-across-flush-cycles
  (let [run-id (random-uuid) _ (seed *ds* run-id)
        tc (component/start (trace/make {:datasource *ds* :batch-size 10 :flush-ms 20}))
        ch (:events-ch tc)]
    (try
      (a/>!! ch {:event-id (random-uuid) :run-id run-id :at 1 :kind :recv})
      (Thread/sleep 80)
      (a/>!! ch {:event-id (random-uuid) :run-id run-id :at 2 :kind :recv})
      (Thread/sleep 80)
      (is (= 2 (count (store/get-events *ds* run-id nil))))
      (finally (component/stop tc)))))
