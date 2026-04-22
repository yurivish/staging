(ns toolkit.datapotamus.demo
  "Arithmetic demo: file → read → sub1 → repeat3 → sqlite-sink."
  (:require [clojure.core.async.flow :as flow]
            [clojure.string :as str]
            [toolkit.datapotamus.proc :as proc]
            [toolkit.datapotamus.sinks :as sinks]))

(set! *warn-on-reflection* true)

(defn- read-file [m]
  (assoc m :data (Integer/parseInt (str/trim (slurp (:path (:data m)))))))

(defn- repeat3
  ([] {:params {} :ins {:in ""} :outs {:out ""}})
  ([_] {})
  ([s _] s)
  ([s _ m]
   (let [kids (mapv (fn [k]
                      (-> (proc/derive-msg m (:data m))
                          (vary-meta assoc :k k)))
                    (range 3))
         evs (-> [(proc/recv-event :repeat3 m)]
                 (into (map-indexed
                        (fn [k c]
                          (proc/send-out-event :repeat3 c
                                               :parent-msg-ids [(:msg-id m)]
                                               :provenance {:idx k}))
                        kids))
                 (conj (proc/success-event :repeat3 m)))]
     [s {:out kids ::flow/report evs}])))

(defn pipeline [{:keys [datasource]}]
  {:pipeline-id :arith-demo
   :entry :read
   :procs {:read    (proc/step-proc :read read-file)
           :sub1    (proc/step-proc :sub1 (fn [m] (update m :data dec)))
           :repeat3 repeat3
           :sink    (sinks/sqlite-sink :sink
                                       {:table :results :datasource datasource})}
   :conns [[[:read :out]    [:sub1 :in]]
           [[:sub1 :out]    [:repeat3 :in]]
           [[:repeat3 :out] [:sink :in]]]})
