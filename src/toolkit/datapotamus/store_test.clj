(ns toolkit.datapotamus.store-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.store :as store])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *ds* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-store-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn with-db [t]
  (let [f (tmp-db)
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try (store/migrate! ds) (binding [*ds* ds] (t))
         (finally (.delete f)))))

(use-fixtures :each with-db)

(deftest migrate-creates-tables
  (let [names (->> (jdbc/execute! *ds* ["SELECT name FROM sqlite_master WHERE type='table'"])
                   (map :sqlite_master/name) set)]
    (is (contains? names "runs"))
    (is (contains? names "trace_events"))))

(deftest migrate-is-idempotent
  (store/migrate! *ds*) (store/migrate! *ds*))

(deftest run-insert-get-update
  (let [run-id (random-uuid)]
    (store/insert-run! *ds* {:run-id run-id :pipeline-id :demo
                              :input-path "p" :input-slug "p"
                              :state :pending :started-at 1})
    (is (= :pending (:state (store/get-run *ds* run-id))))
    (store/update-run! *ds* run-id {:state :running})
    (is (= :running (:state (store/get-run *ds* run-id))))
    (store/update-run! *ds* run-id {:state :completed :finished-at 9})
    (let [r (store/get-run *ds* run-id)]
      (is (= :completed (:state r)))
      (is (= 9 (:finished-at r))))))

(deftest list-runs-desc
  (dotimes [i 3]
    (store/insert-run! *ds* {:run-id (random-uuid) :pipeline-id :d
                              :input-path (str i) :input-slug (str i)
                              :state :completed :started-at i}))
  (is (= [2 1 0] (map :started-at (store/list-runs *ds* 10)))))

(deftest events-batch-insert-and-cursor
  (let [run-id (random-uuid)]
    (store/insert-run! *ds* {:run-id run-id :pipeline-id :d
                              :input-path "p" :input-slug "p"
                              :state :running :started-at 0})
    (store/insert-events! *ds*
      (mapv (fn [i] {:event-id (random-uuid) :run-id run-id :at i
                     :kind :recv :step-id :a :msg-id (random-uuid)})
            (range 5)))
    (let [all (store/get-events *ds* run-id nil)]
      (is (= 5 (count all)))
      (is (= [0 1 2 3 4] (map :at all))))
    (let [cursor (:seq (nth (store/get-events *ds* run-id nil) 2))
          tail   (store/get-events *ds* run-id cursor)]
      (is (= 2 (count tail))))))

(deftest events-store-json-fields
  (let [run-id (random-uuid) m1 (random-uuid) m2 (random-uuid)]
    (store/insert-run! *ds* {:run-id run-id :pipeline-id :d
                              :input-path "p" :input-slug "p"
                              :state :running :started-at 0})
    (store/insert-events! *ds*
      [{:event-id (random-uuid) :run-id run-id :at 1 :kind :send-out
        :step-id :a :to-step-id :b :msg-id m1 :data-id (random-uuid)
        :parent-msg-ids [m2] :provenance {:k 1}
        :payload-ref {:kind :sqlite :location "results:7"}}])
    (let [e (first (store/get-events *ds* run-id nil))]
      (is (= [m2] (:parent-msg-ids e)))
      (is (= {:k 1} (:provenance e)))
      (is (= {:kind "sqlite" :location "results:7"} (:payload-ref e))))))
