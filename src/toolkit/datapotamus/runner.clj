(ns toolkit.datapotamus.runner
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow]
            [toolkit.datapotamus.store :as store]))

(set! *warn-on-reflection* true)

(defn- emit! [ch run-id kind & {:as extra}]
  (a/>!! ch (merge {:event-id (random-uuid) :run-id run-id
                    :at (System/currentTimeMillis) :kind kind} extra)))

(defn- forward! [ch run-id evs]
  (doseq [e evs] (a/>!! ch (assoc e :run-id run-id))))

(defn run-pipeline!
  "Runs `pipeline` to completion, failure, or idle-quiescence. Returns the final run map."
  [{:keys [datasource events-ch run-id pipeline seed idle-complete-ms]
    :or {idle-complete-ms 500}}]
  (let [{:keys [entry procs conns]} pipeline
        g (flow/create-flow
            {:procs (into {} (map (fn [[k sfn]] [k {:proc (flow/process sfn)}]) procs))
             :conns conns})
        {:keys [report-chan error-chan]} (flow/start g)
        seed-msg {:msg-id (random-uuid) :data-id (random-uuid)
                  :run-id run-id :data (:data seed)}]
    (try
      (store/update-run! datasource run-id {:state :running})
      (emit! events-ch run-id :run-started)
      (flow/resume g)
      @(flow/inject g [entry :in] [seed-msg])
      (loop []
        (let [timeout (a/timeout idle-complete-ms)
              [v ch] (a/alts!! [report-chan error-chan timeout])]
          (cond
            (and (= ch error-chan) (some? v))
            (let [ex (::flow/ex v)
                  fail (or (some-> ex ex-data :toolkit.datapotamus.proc/failure-event)
                           {:event-id (random-uuid) :run-id run-id
                            :at (System/currentTimeMillis) :kind :failure
                            :error {:message (some-> ex ex-message)}})]
              (a/>!! events-ch (assoc fail :run-id run-id))
              (flow/stop g)
              (store/update-run! datasource run-id
                                  {:state :failed
                                   :finished-at (System/currentTimeMillis)
                                   :error (get-in fail [:error :message])})
              (store/get-run datasource run-id))

            (and (= ch report-chan) (some? v))
            (let [evs (if (sequential? v) v [v])]
              (forward! events-ch run-id evs)
              (recur))

            (= ch timeout)
            (do (flow/stop g)
                (emit! events-ch run-id :run-finished)
                (store/update-run! datasource run-id
                                    {:state :completed
                                     :finished-at (System/currentTimeMillis)})
                (store/get-run datasource run-id))

            :else (recur))))
      (catch Throwable ex
        (try (flow/stop g) (catch Throwable _))
        (store/update-run! datasource run-id
                            {:state :failed :finished-at (System/currentTimeMillis)
                             :error (ex-message ex)})
        (store/get-run datasource run-id)))))
