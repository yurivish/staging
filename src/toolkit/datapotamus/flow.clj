(ns toolkit.datapotamus.flow
  "The interpreter.

   Takes a step (pure data) and animates it on core.async.flow channels.
   This is the only namespace that requires `core.async.flow`.

   Responsibilities:
     * `run-step`   — pure dispatch: envelope → handler → synthesis → events.
     * `proc-fn`    — adapter to core.async.flow's 4-arity proc-fn shape.
     * `instrument-flow` — inline subflows, resolve endpoints, wrap procs.
     * `start!` / `inject!` / `stop!` / `await-quiescent!` / `run!` / `run-seq`.

   Every effect — pubsub publishes, atom updates, channel injects — is
   confined to this namespace. The msg / step / trace namespaces are
   pure."
  (:refer-clojure :exclude [run!])
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow]
            [clojure.set :as set]
            [toolkit.datapotamus.counters :as ctrs]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.pubsub :as pubsub]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; run-step — pure dispatch
;; ============================================================================

(defn- validate-ports! [trace-sid port-map outs]
  (let [unknown (seq (remove #(contains? outs %) (keys port-map)))]
    (when unknown
      (throw (ex-info (str "step " trace-sid
                           " emitted on undeclared port(s): " (vec unknown)
                           " (declared: " (vec (keys outs)) ")")
                      {:step-id trace-sid
                       :unknown (vec unknown)
                       :declared (vec (keys outs))})))))

(defn- send-out-events [trace-sid port-map]
  (mapv (fn [[port mm]] (trace/send-out-event trace-sid port mm))
        (for [[port msgs] port-map, mm msgs] [port mm])))

(defn- normalize-return
  "Handler return is either [state' outputs] (vector) or just outputs (map,
   implying state unchanged). Return [state' outputs] uniformly."
  [s ret]
  (if (vector? ret) [(first ret) (second ret)] [s ret]))

(defn- auto-signal-outputs
  "When a handler returns an empty port-map, synthesize a signal on every
   declared output port. Preserves the input's tokens for downstream
   closure (filters, dedups, and skips are the motivating cases)."
  [ctx outs]
  (into {} (map (fn [p] [p [(msg/signal ctx)]])) (keys outs)))

(defn- run-data-or-signal
  "Data and signal share a code path. Both call the handler-map,
   synthesize, validate, and collect events.

   Empty port-map from the handler ⇒ auto-signal on every output port
   (preserves tokens). Return `msg/drain` instead to suppress that —
   e.g., when deferring propagation to a later invocation via stashed
   parent refs and an eventual `msg/merge`."
  [hmap m s ctx trace-sid outs kind]
  (try
    (let [ret                  (case kind
                                 :signal ((:on-signal hmap) ctx s)
                                 :data   ((:on-data   hmap) ctx s (:data m)))
          [s' msgs]            (normalize-return s ret)
          msgs'                (cond
                                 (identical? msgs msg/drain) {}
                                 (and (= kind :data)
                                      (map? msgs)
                                      (every? empty? (vals msgs)))
                                 (auto-signal-outputs ctx outs)
                                 :else msgs)
          [port-map synth-evs] (msg/synthesize msgs' m trace-sid)
          _                    (validate-ports! trace-sid port-map outs)
          events (vec (concat synth-evs
                              (send-out-events trace-sid port-map)
                              [(trace/success-event trace-sid m)]))]
      [s' port-map events])
    (catch Throwable ex
      [s {} [(trace/failure-event trace-sid m ex)]])))

(defn- run-done
  "Done cascade: track closed-ins in framework state. When all inputs are
   closed, call :on-all-closed (drain hook), then forward (new-done) on
   every output port."
  [hmap m ctx s trace-sid ins outs]
  (let [closed  (conj (::closed-ins s #{}) (:in-port ctx))
        all-ins (set (keys ins))]
    (if (= closed all-ins)
      (try
        (let [ret                  ((:on-all-closed hmap) ctx s)
              [s' msgs]            (normalize-return s ret)
              [synth-pm synth-evs] (msg/synthesize msgs m trace-sid)
              port-map             (reduce (fn [pm p]
                                             (update pm p (fnil conj []) (msg/new-done)))
                                           synth-pm (keys outs))
              events (vec (concat synth-evs
                                  (send-out-events trace-sid port-map)
                                  [(trace/success-event trace-sid m)]))]
          [(assoc s' ::closed-ins closed) port-map events])
        (catch Throwable ex
          [(assoc s ::closed-ins closed)
           {}
           [(trace/failure-event trace-sid m ex)]]))
      [(assoc s ::closed-ins closed)
       {}
       [(trace/success-event trace-sid m)]])))

(defn run-step
  "Pure dispatch. Returns [new-state port-map events].
   Never throws (user exceptions become :failure events)."
  [hmap m in-port s trace-sid step-sp cancel-p]
  (let [ins  (-> hmap :ports :ins)
        outs (-> hmap :ports :outs)
        ctx  {:pubsub step-sp :step-id trace-sid :cancel cancel-p
              :in-port in-port :msg m :ins ins :outs outs}]
    (case (msg/envelope-kind m)
      :done   (run-done               hmap m ctx s trace-sid ins outs)
      :signal (run-data-or-signal     hmap m s ctx trace-sid outs :signal)
      :data   (run-data-or-signal     hmap m s ctx trace-sid outs :data))))

;; ============================================================================
;; proc-fn — the core.async.flow 4-arity adapter
;; ============================================================================

(defn- proc-fn
  "Adapt a handler-map to a core.async.flow proc-fn (four arities).

   Arity 0: describe ports.
   Arity 1: init — calls (:on-init hmap) to build initial state.
   Arity 2: transition — on ::flow/stop, calls (:on-stop hmap ctx state)
            for resource cleanup; otherwise returns state unchanged.
   Arity 3: process a message — delegates to run-step.

   Publishing, counter updates, and quiescence signalling all happen here
   on the publish path — no self-subscription."
  [hmap trace-sid step-sp cancel-p counters done-p]
  (let [ports (:ports hmap)
        ins   (:ins ports)
        outs  (:outs ports)
        emit! (fn [ev]
                (trace/sp-pub step-sp ev)
                (when (ctrs/record-event! counters ev)
                  (deliver @done-p :quiescent)))]
    (fn
      ([]      {:params {} :ins ins :outs outs})
      ([_]     ((:on-init hmap)))
      ([s transition]
       (if (= transition :clojure.core.async.flow/stop)
         (let [ctx {:pubsub step-sp :step-id trace-sid :cancel cancel-p
                    :ins ins :outs outs}]
           (try ((:on-stop hmap) ctx s) (catch Throwable _ nil))
           s)
         s))
      ([s in-port m]
       ;; Publish :recv on arrival, before the handler runs. Emitting it
       ;; alongside :success (as we used to) made every pair atomic in the
       ;; event log, so live consumers couldn't derive in-flight counts.
       (emit! (trace/recv-event trace-sid m in-port))
       (let [[s' port-map events] (run-step hmap m in-port s trace-sid step-sp cancel-p)]
         (doseq [e events] (emit! e))
         [s' port-map])))))

;; ============================================================================
;; Instrumentation — subflow inlining + endpoint resolution
;; ============================================================================

;; Post-instrumentation, every proc id is a path vector ([:foo] for top-level,
;; [:bar :foo] for nested) and every conn endpoint / boundary ref is uniformly
;; [path port]. User-facing step data still uses keyword sids; normalize-ref
;; converts at the boundary.

(defn- normalize-ref
  "User-form ref (kw | [kw port] | [path port]) → [path port]."
  [ref default-port]
  (if (vector? ref)
    (let [[s p] ref] [(if (vector? s) s [s]) p])
    [[ref] default-port]))

(defn- prefix-endpoint [outer-sid [path port]]
  [(into [outer-sid] path) port])

(declare instrument-flow)

(defn- inline-subflow [outer-sid subflow outer-sp cancel-p counters done-p]
  (let [inner-sp (trace/push-scope outer-sp [:flow (name outer-sid)])
        inner    (instrument-flow subflow inner-sp cancel-p counters done-p)]
    {:procs (update-keys (:procs inner) #(into [outer-sid] %))
     :conns (mapv (fn [[from to opts]]
                    (cond-> [(prefix-endpoint outer-sid from)
                             (prefix-endpoint outer-sid to)]
                      opts (conj opts)))
                  (:conns inner))
     :in    (prefix-endpoint outer-sid (:in inner))
     :out   (prefix-endpoint outer-sid (or (:out inner) (:in inner)))}))

(defn- resolve-ref
  "Look up a normalized [path port] endpoint against the alias map keyed by
   the bare outer sid. Returns the alias's [path port] resolution or the
   endpoint unchanged."
  [aliases which [path _ :as ep]]
  (or (when (= 1 (count path))
        (some-> (get aliases (first path)) (get which)))
      ep))

(defn- instrument-flow
  "Recursively inline subflows and wrap handler-maps with proc-fn. Returns a
   flat step map with concrete proc-fns under :procs (keyed by path vectors)
   and conns / :in / :out as [path port] pairs."
  [stepmap outer-sp cancel-p counters done-p]
  (let [{:keys [procs inner-conns aliases]}
        (reduce (fn [acc [sid p]]
                  (cond
                    (step/step? p)
                    (let [{:keys [procs conns in out]}
                          (inline-subflow sid p outer-sp cancel-p counters done-p)]
                      (-> acc
                          (update :procs into procs)
                          (update :inner-conns into conns)
                          (assoc-in [:aliases sid] {:in in :out out})))

                    (step/handler-map? p)
                    (let [step-sp (trace/push-scope outer-sp [:step sid])
                          wrapped (proc-fn p sid step-sp cancel-p counters done-p)]
                      (assoc-in acc [:procs [sid]] wrapped))

                    :else
                    (throw (ex-info (str "Unrecognized proc value at " sid)
                                    {:sid sid :value p}))))
                {:procs {} :inner-conns [] :aliases {}}
                (:procs stepmap))
        resolved-conns (mapv (fn [[from to opts]]
                               (cond-> [(resolve-ref aliases :out (normalize-ref from :out))
                                        (resolve-ref aliases :in  (normalize-ref to   :in))]
                                 opts (conj opts)))
                             (:conns stepmap))
        in-resolved  (when-let [r (:in  stepmap)] (resolve-ref aliases :in  (normalize-ref r :in)))
        out-resolved (when-let [r (:out stepmap)] (resolve-ref aliases :out (normalize-ref r :out)))]
    (cond-> (assoc stepmap
                   :procs procs
                   :conns (into resolved-conns inner-conns))
      (:in  stepmap) (assoc :in  in-resolved)
      (:out stepmap) (assoc :out out-resolved))))

;; ============================================================================
;; Graph build + validation
;; ============================================================================

(defn- collect-chan-opts
  "Fold per-conn opts into `{sid {port opts}}`. A conn attaches its opts
   to the consumer's input port, so multiple conns targeting the same
   `[to in]` must agree (or only one supplies opts) — otherwise the
   underlying channel's buffer is ambiguous."
  [conns]
  (reduce (fn [acc [_ [to in] opts]]
            (if (nil? opts)
              acc
              (let [existing (get-in acc [to in])]
                (cond
                  (nil? existing)    (assoc-in acc [to in] opts)
                  (= existing opts)  acc
                  :else              (throw (ex-info
                                             (str "Conflicting chan-opts for " [to in]
                                                  ": " existing " vs " opts)
                                             {:to to :in in
                                              :existing existing :new opts}))))))
          {} conns))

(defn- build-graph [instrumented]
  (let [chan-opts (collect-chan-opts (:conns instrumented))]
    (flow/create-flow
     {:procs (into {}
                   (map (fn [[sid pfn]]
                          [sid (cond-> {:proc (flow/process pfn)}
                                 (chan-opts sid) (assoc :chan-opts (chan-opts sid)))]))
                   (:procs instrumented))
      :conns (mapv (fn [[from to]] [from to]) (:conns instrumented))})))

(defn- port-index-of [instrumented]
  (into {}
        (map (fn [[sid pfn]]
               (let [spec (pfn)]
                 [sid {:ins  (set (keys (:ins  spec)))
                       :outs (set (keys (:outs spec)))}])))
        (:procs instrumented)))

(defn- validate-wired-outs!
  "Every declared output port must be consumed by a conn, or match the
   flow's :out boundary. An unwired port would block `flow/inject` forever."
  [instrumented port-index]
  (let [out-ep (:out instrumented)
        used   (cond-> (set (map first (:conns instrumented)))
                 out-ep (conj out-ep))
        unwired (vec (for [[sid {:keys [outs]}] port-index
                           port outs
                           :when (not (used [sid port]))]
                       [sid port]))]
    (when (seq unwired)
      (throw (ex-info (str "step has unwired output port(s): " (pr-str unwired))
                      {:unwired unwired})))))

;; ============================================================================
;; Lifecycle
;; ============================================================================

(defn start!
  "Instantiate a step and start it running. Returns a handle."
  ([stepmap] (start! stepmap {}))
  ([stepmap opts]
   (let [fid          (or (:flow-id opts) (str (random-uuid)))
         raw-ps       (or (:pubsub opts) (pubsub/make))
         outer-sp     {:raw raw-ps :prefix [[:flow fid]]}
         cancel-p     (promise)
         scope        [[:flow fid]]
         counters     (ctrs/make)
         done-p       (atom (promise))
         error        (atom nil)
         instrumented (instrument-flow stepmap outer-sp cancel-p counters done-p)
         port-index   (port-index-of instrumented)
         _            (validate-wired-outs! instrumented port-index)
         g            (build-graph instrumented)
         {:keys [error-chan]} (flow/start g)
         err-done     (a/io-thread
                       (loop []
                         (when-let [m (a/<!! error-chan)]
                           (let [ex  (:clojure.core.async.flow/ex m)
                                 err {:message (ex-message ex) :data (ex-data ex)}]
                             (reset! error err)
                             (pubsub/pub raw-ps
                                         (trace/run-subject-for scope :flow-error)
                                         {:kind      :flow-error
                                          :pid       (:clojure.core.async.flow/pid m)
                                          :cid       (:clojure.core.async.flow/cid m)
                                          :msg-id    (get-in m [:clojure.core.async.flow/msg :msg-id])
                                          :error     err
                                          :scope     scope
                                          :flow-path [fid]
                                          :at        (trace/now)})
                             (deliver @done-p [:failed err]))
                           (recur))))]
     (pubsub/pub raw-ps (trace/run-subject-for scope :run-started)
                 {:kind :run-started :flow-path [fid] :scope scope
                  :at   (trace/now)})
     (flow/resume g)
     {::step       instrumented
      ::graph      g
      ::pubsub     raw-ps
      ::scope      scope
      ::fid        fid
      ::cancel     cancel-p
      ::counters   counters
      ::done-p     done-p
      ::error      error
      ::err-done   err-done
      ::port-index port-index})))

(defn inject!
  "Route a work item into the running step. Returns the handle.

   Long-running use: call repeatedly to route items in as they arrive;
   observe via counters/events/pubsub; `stop!` when done.

   Opts:
     :in    — step id or [step-id port] to target (defaults to flow's :in)
     :port  — override the port (when :in is a bare sid)
     :data  — data value; presence ⇒ data envelope
     :tokens — token map; presence without :data ⇒ signal envelope
     (neither) ⇒ done marker"
  [handle {:keys [in port] :as opts}]
  (let [{::keys [step graph pubsub scope fid counters done-p port-index]} handle
        ref                 (or in (:in step))
        [flow-in flow-port] (normalize-ref ref :in)
        port                (or port flow-port)
        step-ports          (get port-index flow-in)
        _                   (when-not step-ports
                              (throw (ex-info
                                      (str "inject!: unknown step " flow-in
                                           " (known: " (sort (keys port-index)) ")")
                                      {:step flow-in :known (set (keys port-index))})))
        _                   (when-not (contains? (:ins step-ports) port)
                              (throw (ex-info
                                      (str "inject!: step " flow-in
                                           " does not declare input port " port
                                           " (declared: " (:ins step-ports) ")")
                                      {:step flow-in :port port :declared (:ins step-ports)})))
        has-data?           (contains? opts :data)
        has-tokens?         (contains? opts :tokens)
        item                (cond
                              has-data?   (cond-> (msg/new-msg (:data opts))
                                            has-tokens? (assoc :tokens (:tokens opts)))
                              has-tokens? (msg/new-signal (:tokens opts))
                              :else       (msg/new-done))]
    (swap! done-p (fn [p] (if (realized? p) (promise) p)))
    (ctrs/record-inject! counters)
    (pubsub/pub pubsub (trace/run-subject-for scope :inject)
                (assoc (trace/msg-envelope item)
                       :kind :inject
                       :flow-path [fid] :scope scope
                       :in flow-in :port port
                       :at (trace/now)))
    @(flow/inject graph [flow-in port] [item])
    handle))

(defn counters   [handle] (ctrs/snapshot (::counters handle)))
(defn quiescent? [handle] (ctrs/balanced? (::counters handle)))

(defn await-quiescent!
  "Block until quiescence or error. Returns :quiescent, [:failed err], or :timeout."
  ([handle] (await-quiescent! handle nil))
  ([handle timeout-ms]
   (let [p @(::done-p handle)]
     (if timeout-ms
       (deref p timeout-ms :timeout)
       @p))))

(defn stop!
  "Tear down the graph. Returns {:state :counters :error}."
  [handle]
  (let [{::keys [graph cancel err-done counters error]} handle]
    (when-not (realized? cancel) (deliver cancel :stopped))
    (flow/stop graph)
    (a/<!! err-done)
    (let [err @error]
      {:state    (if err :failed :completed)
       :counters (ctrs/snapshot counters)
       :error    err})))

(defn run!
  "Start, inject one message, wait for quiescence, stop."
  [stepmap opts]
  (let [handle (start! stepmap (select-keys opts [:pubsub :flow-id]))]
    (inject! handle (select-keys opts [:in :port :data :tokens]))
    (let [signal (await-quiescent! handle)]
      (-> (stop! handle)
          (assoc :state (if (= :quiescent signal) :completed :failed))))))

;; ============================================================================
;; run-seq — run a flow against a collection, attribute outputs to inputs
;; ============================================================================

(defn- collector-step [id a]
  (step/step id {:ins {:in ""} :outs {}}
             (fn [ctx _s d]
               (swap! a conj {:msg-id (:msg-id (:msg ctx)) :data d})
               {})))

(defn- inject-attribution
  "Forward pass over time-ordered events. Returns {msg-id #{inject-idx ...}}."
  [events inject->idx]
  (reduce (fn [acc ev]
            (case (:kind ev)
              :inject
              (update acc (:msg-id ev) (fnil conj #{}) (inject->idx (:msg-id ev)))
              (:split :merge :send-out)
              (let [parent-injects (reduce set/union #{} (map acc (:parent-msg-ids ev)))]
                (update acc (:msg-id ev) (fnil set/union #{}) parent-injects))
              acc))
          {} events))

(defn run-seq
  "Run `stepmap` against each input in `coll`. Appends an internal collector
   at the step's :out boundary. Returns a map like `run!` plus :outputs —
   a vector aligned with `coll` where each element is the vector of data
   values whose ancestry traces back to that input."
  ([stepmap coll] (run-seq stepmap coll {}))
  ([stepmap coll opts]
   (if (empty? coll)
     {:state :completed :outputs [] :counters {:sent 0 :recv 0 :completed 0}}
     (let [collected (atom [])
           ps        (or (:pubsub opts) (pubsub/make))
           provenance (atom [])
           prov-unsub (pubsub/sub ps [:>]
                                  (fn [_ ev _]
                                    (when (#{:inject :split :merge :send-out} (:kind ev))
                                      (swap! provenance conj ev))))
           wf        (step/serial stepmap (collector-step ::collector collected))
           handle    (start! wf (assoc (select-keys opts [:flow-id]) :pubsub ps))]
       (doseq [d coll] (inject! handle {:data d}))
       (let [signal    (await-quiescent! handle)
             result    (-> (stop! handle)
                           (assoc :state (if (= :quiescent signal) :completed :failed))
                           (cond-> (vector? signal) (assoc :error (second signal))))
             _           (prov-unsub)
             evs         @provenance
             inject-ids  (mapv :msg-id (filter #(= :inject (:kind %)) evs))
             inject->idx (zipmap inject-ids (range))
             attribution (inject-attribution evs inject->idx)
             outputs     (reduce (fn [acc {:keys [msg-id data]}]
                                   (reduce (fn [a s] (update a s conj data))
                                           acc
                                           (attribution msg-id)))
                               (vec (repeat (count coll) []))
                               @collected)]
         (assoc result :outputs outputs))))))
