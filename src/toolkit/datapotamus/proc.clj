(ns toolkit.datapotamus.proc
  (:require [clojure.core.async.flow :as flow]))

(set! *warn-on-reflection* true)

(defn- now [] (System/currentTimeMillis))

(defn derive [parent new-data]
  {:msg-id (random-uuid) :data-id (random-uuid)
   :run-id (:run-id parent) :data new-data})

(defn- base [kind m]
  {:event-id (random-uuid) :run-id (:run-id m) :at (now) :kind kind
   :msg-id (:msg-id m) :data-id (:data-id m)})

(defn recv-event    [step-id m] (assoc (base :recv    m) :step-id step-id))
(defn success-event [step-id m] (assoc (base :success m) :step-id step-id))

(defn failure-event [step-id m ^Throwable ex]
  (assoc (base :failure m) :step-id step-id
         :error {:message (ex-message ex) :data (ex-data ex)
                 :stack   (with-out-str (.printStackTrace ex))}))

(defn send-out-event [step-id child & {:keys [parent-msg-ids to-step-id provenance]}]
  (cond-> (assoc (base :send-out child) :step-id step-id
                 :parent-msg-ids (vec parent-msg-ids))
    to-step-id (assoc :to-step-id to-step-id)
    provenance (assoc :provenance provenance)))

(defn merge-event [step-id child parent-msg-ids]
  (assoc (base :merge child) :step-id step-id
         :parent-msg-ids (vec parent-msg-ids)))

(defn sink-wrote-event [step-id m payload-ref]
  (assoc (base :sink-wrote m) :step-id step-id :payload-ref payload-ref))

(defn step-proc
  "Wrap a pure (msg -> msg') fn into a one-in-one-out flow step-fn that
   auto-emits :recv / :send-out / :success (or :failure + rethrow)."
  [step-id f]
  (fn
    ([] {:params {} :ins {:in ""} :outs {:out ""}})
    ([_] {})
    ([s _] s)
    ([s _ m]
     (try
       (let [c (derive m (:data (f m)))
             evs [(recv-event step-id m)
                  (send-out-event step-id c :parent-msg-ids [(:msg-id m)])
                  (success-event step-id c)]]
         [s {:out [c] ::flow/report evs}])
       (catch Throwable ex
         (throw (ex-info (ex-message ex)
                          (assoc (ex-data ex)
                                 ::failure-event (failure-event step-id m ex))
                          ex)))))))
