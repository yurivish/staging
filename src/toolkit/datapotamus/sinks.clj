(ns toolkit.datapotamus.sinks
  (:require [clojure.core.async.flow :as flow]
            [clojure.data.json :as json]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.proc :as proc]))

(set! *warn-on-reflection* true)

(defn sqlite-sink
  "Step-fn: write each msg as a row into `:table` of `:datasource`.
   Table is expected to have run_id/msg_id/data_id/written_at/data TEXT cols."
  [step-id {:keys [table datasource]}]
  (fn
    ([] {:params {} :ins {:in ""} :outs {}})
    ([_] {})
    ([s _] s)
    ([s _ m]
     (let [now (System/currentTimeMillis)
           res (jdbc/execute-one! datasource
                 (sql/format {:insert-into table
                              :values [{:run_id (str (:run-id m))
                                        :msg_id (str (:msg-id m))
                                        :data_id (str (:data-id m))
                                        :written_at now
                                        :data (json/write-str (:data m))}]})
                 {:return-keys true})
           rowid (or (get res (keyword "last_insert_rowid()"))
                     (:last_insert_rowid res)
                     (get res (keyword (name table) "rowid"))
                     (:next.jdbc/update-count res))
           ref {:kind :sqlite :location (str (name table) ":" rowid)}]
       [s {::flow/report [(proc/sink-wrote-event step-id m ref)]}]))))
