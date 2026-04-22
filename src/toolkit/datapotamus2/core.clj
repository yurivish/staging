(ns toolkit.datapotamus2.core
  "Datapotamus2 core: envelope helpers, the `instrument` transformation
   and `run!` runner.

   `instrument : FlowSpec → FlowSpec` wraps every proc's step-fn so the
   wrapper owns all trace-event emission. Step-fns never touch
   `::flow/report`. They return a data description:

     [new-state {out-port [children]
                 ::dp2/derivations [intermediate-msgs]
                 ::dp2/merges      [{:msg-id ... :parents [...]}]}]

   and the wrapper derives :recv, :send-out, :merge, :success / :failure.

   Tokens live on the msg envelope but are not touched by the wrapper;
   combinators manage token math (see toolkit.datapotamus2.combinators)."
  (:refer-clojure :exclude [run!])
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit]))

(set! *warn-on-reflection* true)

;; --- envelope ---------------------------------------------------------------

(defn new-msg
  "Root msg: no parent. Starts with empty tokens."
  [data]
  {:msg-id (random-uuid) :data-id (random-uuid) :data data
   :tokens {} :parent-msg-ids []})

(defn child-with-data
  "Child msg with new ids, given data, parent tokens inherited.
   1-arity: parent left nil — the wrapper resolves it to the current
   input msg's id at step-call boundary."
  ([data] (child-with-data nil data))
  ([parent data]
   {:msg-id (random-uuid) :data-id (random-uuid) :data data
    :tokens (:tokens parent {})
    :parent-msg-ids (when parent [(:msg-id parent)])}))

(defn child-with-parents
  "Child msg with explicit multi-parent lineage (for merge outputs)."
  [parent-msg-ids data]
  {:msg-id (random-uuid) :data-id (random-uuid) :data data
   :tokens {} :parent-msg-ids (vec parent-msg-ids)})

(defn child-same-data
  "Child msg with new ids, same data as parent, tokens inherited.
   1-arity shorthand like child-with-data."
  ([] (child-same-data nil))
  ([parent]
   {:msg-id (random-uuid) :data-id (random-uuid) :data (:data parent)
    :tokens (:tokens parent {})
    :parent-msg-ids (when parent [(:msg-id parent)])}))

;; --- event constructors (internal) ------------------------------------------

(defn- now [] (System/currentTimeMillis))

(defn- recv-event [step-id m]
  {:kind :recv :step-id step-id :msg-id (:msg-id m) :data-id (:data-id m) :at (now)})

(defn- success-event [step-id m]
  {:kind :success :step-id step-id :msg-id (:msg-id m) :at (now)})

(defn- failure-event [step-id m ^Throwable ex]
  {:kind :failure :step-id step-id :msg-id (:msg-id m)
   :error {:message (ex-message ex) :data (ex-data ex)} :at (now)})

(defn- send-out-event [step-id port child]
  {:kind :send-out :step-id step-id :port port
   :msg-id (:msg-id child) :data-id (:data-id child)
   :parent-msg-ids (vec (:parent-msg-ids child))
   :tokens (:tokens child) :at (now)})

(defn- merge-event [step-id msg-id parents]
  {:kind :merge :step-id step-id :msg-id msg-id
   :parent-msg-ids (vec parents) :at (now)})

;; --- wrapper helpers --------------------------------------------------------

(defn- resolve-msg-nils
  "Replace a nil in :parent-msg-ids with [input-msg-id]."
  [m input-id]
  (cond-> m
    (or (nil? (:parent-msg-ids m))
        (some nil? (:parent-msg-ids m)))
    (update :parent-msg-ids
            (fn [ps]
              (if (or (nil? ps) (empty? ps))
                [input-id]
                (mapv #(or % input-id) ps))))))

(defn- port-output-keys
  "Port keys in the output map — anything that's not a namespaced keyword."
  [out]
  (filter (fn [k] (and (keyword? k) (not (namespace k)))) (keys out)))

(defn- dp2-key? [k]
  (and (keyword? k) (= "toolkit.datapotamus2.core" (namespace k))))

(defn- resolve-output-nils [out input-id]
  (let [resolved-ports
        (reduce (fn [m k]
                  (assoc m k (mapv #(resolve-msg-nils % input-id) (get m k))))
                out (port-output-keys out))
        resolved-derivs
        (update resolved-ports ::derivations
                (fn [ds] (when ds (mapv #(resolve-msg-nils % input-id) ds))))
        resolved-merges
        (update resolved-derivs ::merges
                (fn [ms]
                  (when ms
                    (mapv (fn [{:keys [msg-id parents]}]
                            {:msg-id msg-id
                             :parents (if (empty? parents)
                                        [input-id]
                                        (mapv #(or % input-id) parents))})
                          ms))))]
    resolved-merges))

(defn- build-events [step-id m output]
  (let [ports      (port-output-keys output)
        port-ids   (into #{} (mapcat (fn [p] (map :msg-id (get output p)))) ports)
        derivs     (->> (get output ::derivations)
                        ;; dedup: if a derivation msg was also sent on a port,
                        ;; skip its nil-port send-out (the port one covers it)
                        (remove #(port-ids (:msg-id %))))
        merges     (get output ::merges)
        ev-recv    [(recv-event step-id m)]
        ev-merges  (mapv (fn [{:keys [msg-id parents]}]
                           (merge-event step-id msg-id parents))
                         merges)
        ev-derivs  (mapv (fn [c] (send-out-event step-id nil c)) derivs)
        ev-sends   (into []
                         (mapcat
                          (fn [p]
                            (map (fn [c] (send-out-event step-id p c)) (get output p))))
                         ports)
        ev-success [(success-event step-id m)]]
    (into [] cat [ev-recv ev-merges ev-derivs ev-sends ev-success])))

(defn- strip-dp2-keys [out]
  (reduce-kv (fn [m k v] (if (dp2-key? k) m (assoc m k v))) {} out))

;; --- wrap-proc / instrument -------------------------------------------------

(defn- wrap-proc [step-id user-step-fn]
  (fn
    ([] (user-step-fn))
    ([arg] (user-step-fn arg))
    ([s arg] (user-step-fn s arg))
    ([s in-id m]
     (try
       (let [[s' raw]  (user-step-fn s in-id m)
             resolved  (resolve-output-nils (or raw {}) (:msg-id m))
             events    (build-events step-id m resolved)
             port-map  (strip-dp2-keys resolved)]
         [s' (assoc port-map ::flow/report events)])
       (catch Throwable ex
         (throw (ex-info (ex-message ex)
                         {::wrapped-output
                          {::flow/report [(failure-event step-id m ex)]}}
                         ex)))))))

(defn instrument
  "Transform a flow spec: wrap every proc's step-fn for auto-instrumentation.
   Leaves :conns and other keys untouched."
  [{:keys [procs] :as spec}]
  (assoc spec :procs
         (into {} (map (fn [[sid pfn]] [sid (wrap-proc sid pfn)])) procs)))

;; --- run! -------------------------------------------------------------------

(defn- update-counters [counters ev]
  (case (:kind ev)
    :recv     (update counters :recv inc)
    :success  (update counters :completed inc)
    :failure  (update counters :completed inc)
    :send-out (if (:port ev)
                (update counters :sent inc)
                counters)                ; :port nil = internal derivation, no channel
    counters))

(defn- done? [{:keys [sent recv completed]}]
  (and (pos? sent) (= sent recv) (= recv completed)))

(defn- start-drainer!
  "Spawn a dedicated thread that drains flow's report-chan and error-chan
   into an unbounded LinkedBlockingQueue. This defeats the sliding-buffer
   on flow's report-chan (hardcoded to size 100) — since we always have
   a reader ready, the buffer never fills, so no events are ever dropped.
   Each queue entry is [:report event] or [:error exception]."
  [report-chan error-chan]
  (let [queue (LinkedBlockingQueue.)]
    (a/thread
      (try
        (loop []
          (let [[v ch] (a/alts!! [report-chan error-chan])]
            (when (some? v)
              (.put queue (if (identical? ch error-chan) [:error v] [:report v]))
              (recur))))
        (catch Throwable _ nil)))
    queue))

(defn run!
  "Run an instrumented spec to completion. Seeds a single msg into
   [entry :in] (or [entry port] if given). Returns:

     {:state :completed | :failed
      :events [event-map ...]
      :error  Throwable or nil}

   Tracing is lossless: a dedicated drainer thread moves every event
   off flow's report-chan into an unbounded LinkedBlockingQueue the
   moment flow emits it. flow's report-chan is hardcoded to a
   sliding-buffer(100) — without this drainer, bursts could drop trace
   events, which would be unacceptable for a provenance system.

   Completion: Clojure's core.async.flow puts msgs and trace events on
   separate channels with no ordering guarantee between them, so
   counters can transiently balance mid-run. We guard against this with
   a short grace window — after counters balance, we wait `quiesce-ms`
   (default 50) for more events. In practice this is 3+ orders of
   magnitude above flow's scheduling latency."
  [{:keys [procs conns]}
   {:keys [entry port data quiesce-ms]
    :or {port :in quiesce-ms 50}}]
  (let [g (flow/create-flow
           {:procs (into {} (map (fn [[sid pfn]] [sid {:proc (flow/process pfn)}])) procs)
            :conns conns})
        {:keys [report-chan error-chan]} (flow/start g)
        queue (start-drainer! report-chan error-chan)
        seed (new-msg data)
        initial-events [{:kind :run-started :at (now)}
                        {:kind :seed :msg-id (:msg-id seed) :data-id (:data-id seed)
                         :entry entry :port port :at (now)}]
        poll (fn [timeout-ms]
               (.poll ^LinkedBlockingQueue queue
                      (long timeout-ms) TimeUnit/MILLISECONDS))]
    (flow/resume g)
    @(flow/inject g [entry port] [seed])
    (try
      (loop [events   initial-events
             counters {:sent 1 :recv 0 :completed 0}]
        (if (done? counters)
          ;; Counters balance; wait quiesce-ms for more events.
          (if-let [[kind v] (poll quiesce-ms)]
            (case kind
              :error
              (let [ex (::flow/ex v)
                    failure-evs (get-in (ex-data ex)
                                        [::wrapped-output ::flow/report])
                    events' (into events
                                  (or failure-evs
                                      [{:kind :failure
                                        :error {:message (ex-message ex)}
                                        :at (now)}]))]
                (flow/stop g)
                {:state :failed
                 :events (conj events' {:kind :run-failed :at (now)})
                 :error ex})
              :report
              (let [batch (if (sequential? v) v [v])]
                (recur (into events batch)
                       (reduce update-counters counters batch))))
            ;; Quiesce window elapsed with no new events → done.
            (do (flow/stop g)
                {:state :completed
                 :events (conj events {:kind :run-finished :at (now)})
                 :error nil}))
          ;; Counters haven't balanced; block for the next event.
          (let [[kind v] (.take ^LinkedBlockingQueue queue)]
            (case kind
              :error
              (let [ex (::flow/ex v)
                    failure-evs (get-in (ex-data ex)
                                        [::wrapped-output ::flow/report])
                    events' (into events
                                  (or failure-evs
                                      [{:kind :failure
                                        :error {:message (ex-message ex)}
                                        :at (now)}]))]
                (flow/stop g)
                {:state :failed
                 :events (conj events' {:kind :run-failed :at (now)})
                 :error ex})
              :report
              (let [batch (if (sequential? v) v [v])]
                (recur (into events batch)
                       (reduce update-counters counters batch)))))))
      (catch Throwable ex
        (try (flow/stop g) (catch Throwable _))
        {:state :failed :events [] :error ex}))))
