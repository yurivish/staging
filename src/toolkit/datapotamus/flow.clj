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
   parent refs and an eventual `msg/merge`.

   Note: input-done is a per-port lifecycle signal, not a processing
   barrier. Data and signals continue to be processed on a port even
   after that port has been input-done'd (e.g. via a cyclic feedback
   edge that re-feeds the port). The system terminates on counter
   quiescence, not on input-done."
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

(defn- run-input-done
  "Input-done cascade: track closed-ins in framework state. When all
   declared input ports have been input-done'd, call :on-all-closed
   (drain hook), then forward (new-input-done) on every output port.
   Closed ports stay closed — a redundant input-done on an already-closed
   port is a no-op (matters for cyclic flows where a feedback edge can
   re-deliver input-done after on-all-closed has fired).

   :on-all-closed may return msg/drain (or [s' msg/drain]) to suppress
   the auto-append of new-input-done on every output port — symmetric
   with msg/drain's role in run-data-or-signal. Combinators that gate
   their own close (closing an internal channel only after in-flight
   tokens drain) use this to keep the framework from racing extra
   input-done envelopes into the channel."
  [hmap m ctx s trace-sid ins outs]
  (let [in-port (:in-port ctx)
        closed  (::closed-ins s #{})]
    (if (contains? closed in-port)
      [s {} [(trace/success-event trace-sid m)]]
      (let [closed' (conj closed in-port)
            all-ins (set (keys ins))]
        (if (= closed' all-ins)
          (try
            (let [ret                  ((:on-all-closed hmap) ctx s)
                  [s' msgs]            (normalize-return s ret)
                  drain?               (identical? msgs msg/drain)
                  msgs'                (if drain? {} msgs)
                  [synth-pm synth-evs] (msg/synthesize msgs' m trace-sid)
                  port-map             (if drain?
                                         synth-pm
                                         (reduce (fn [pm p]
                                                   (update pm p (fnil conj []) (msg/new-input-done)))
                                                 synth-pm (keys outs)))
                  events (vec (concat synth-evs
                                      (send-out-events trace-sid port-map)
                                      [(trace/success-event trace-sid m)]))]
              [(assoc s' ::closed-ins closed') port-map events])
            (catch Throwable ex
              [(assoc s ::closed-ins closed')
               {}
               [(trace/failure-event trace-sid m ex)]]))
          [(assoc s ::closed-ins closed')
           {}
           [(trace/success-event trace-sid m)]])))))

(defn run-step
  "Pure dispatch. Returns [new-state port-map events].
   Never throws (user exceptions become :failure events)."
  [hmap m in-port s trace-sid step-sp cancel-p]
  (let [ins  (-> hmap :ports :ins)
        outs (-> hmap :ports :outs)
        ctx  {:pubsub step-sp :step-id trace-sid :cancel cancel-p
              :in-port in-port :msg m :ins ins :outs outs}]
    (case (msg/envelope-kind m)
      :input-done (run-input-done           hmap m ctx s trace-sid ins outs)
      :signal     (run-data-or-signal       hmap m s ctx trace-sid outs :signal)
      :data       (run-data-or-signal       hmap m s ctx trace-sid outs :data))))

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
  [hmap trace-sid step-sp {:keys [cancel-p counters done-p]}]
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
       (cond
         ;; Channel closed on a port not in the handler's declared :ins.
         ;; Happens when ::flow/in-ports injects an input under a name
         ;; the handler doesn't list. Don't dispatch — that would add a
         ;; non-declared key to ::closed-ins and break run-input-done's
         ;; (= closed all-ins) test, permanently blocking cascade.
         ;; Framework's read loop already dissocs the chan on the first
         ;; nil read.
         (and (nil? m) (not (contains? ins in-port)))
         [s {}]

         :else
         ;; `or` branch: when m is nil, the channel was closed and
         ;; core.async.flow has called us once with nil before dropping
         ;; the chan. Synthesize a fresh input-done envelope so the rest
         ;; of the pipeline (recv-event, run-step → run-input-done,
         ;; ::closed-ins, cascade, auto-append) runs identically to an
         ;; envelope-input-done arrival. Lineage gap is intrinsic — close
         ;; has no upstream message to attribute to.
         (let [m (or m (msg/new-input-done))]
           ;; Publish :recv on arrival, before the handler runs. Emitting it
           ;; alongside :success (as we used to) made every pair atomic in the
           ;; event log, so live consumers couldn't derive in-flight counts.
           (emit! (trace/recv-event trace-sid m in-port))
           (let [[s' port-map events] (run-step hmap m in-port s trace-sid step-sp cancel-p)]
             (doseq [e events] (emit! e))
             [s' port-map])))))))

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

(defn- inline-subflow [outer-sid subflow outer-sp rt]
  (let [inner-sp (trace/push-scope outer-sp [:scope (name outer-sid)])
        inner    (instrument-flow subflow inner-sp rt)]
    (when-not (and (:in inner) (:out inner))
      (throw (ex-info (str "Subflow " outer-sid " is missing :in or :out boundary"
                           " — `beside` produces no boundaries; use `serial`,"
                           " `input-at`/`output-at`, or set them explicitly.")
                      {:subflow-sid outer-sid
                       :has-in?  (some? (:in inner))
                       :has-out? (some? (:out inner))})))
    {:procs (update-keys (:procs inner) #(into [outer-sid] %))
     :conns (mapv (fn [[from to opts]]
                    (cond-> [(prefix-endpoint outer-sid from)
                             (prefix-endpoint outer-sid to)]
                      opts (conj opts)))
                  (:conns inner))
     :in    (prefix-endpoint outer-sid (:in inner))
     :out   (prefix-endpoint outer-sid (:out inner))}))

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
  [stepmap outer-sp rt]
  (let [{:keys [procs inner-conns aliases]}
        (reduce (fn [acc [sid p]]
                  (cond
                    (step/step? p)
                    (let [{:keys [procs conns in out]}
                          (inline-subflow sid p outer-sp rt)]
                      (-> acc
                          (update :procs into procs)
                          (update :inner-conns into conns)
                          (assoc-in [:aliases sid] {:in in :out out})))

                    (step/handler-map? p)
                    (let [step-sp (trace/push-scope outer-sp [:step sid])
                          wrapped (proc-fn p sid step-sp rt)]
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

(defn- start-error-pump!
  "Drain the flow's error-chan: stamp each error onto the `error` atom and
   publish a :flow-error event. On the first error, also halt the flow —
   deliver the cancel promise and call flow/stop so done-p truthfully means
   done. error-chan closes on stop, so the loop exits naturally afterward.
   Returns the io-thread channel; closes when the pump exits."
  [error-chan graph {:keys [raw-ps scope fid error cancel-p done-p]}]
  (a/io-thread
   (loop [first? true]
     (when-let [m (a/<!! error-chan)]
       (let [ev  (trace/flow-error-event scope fid m)
             err (:error ev)]
         (reset! error err)
         (pubsub/pub raw-ps (trace/run-subject-for scope :flow-error) ev)
         (when first?
           (deliver @done-p [:failed err])
           (when-not (realized? cancel-p) (deliver cancel-p :failed))
           (flow/stop graph)))
       (recur false)))))

(defn start!
  "Instantiate a step and start it running. Returns a handle."
  ([stepmap] (start! stepmap {}))
  ([stepmap opts]
   (let [fid          (or (:flow-id opts) (str (random-uuid)))
         raw-ps       (or (:pubsub opts) (pubsub/make))
         outer-sp     {:raw raw-ps :prefix [[:scope fid]]}
         cancel-p     (promise)
         scope        [[:scope fid]]
         counters     (ctrs/make)
         done-p       (atom (promise))
         error        (atom nil)
         rt           {:cancel-p cancel-p :counters counters :done-p done-p}
         instrumented (instrument-flow stepmap outer-sp rt)
         port-index   (port-index-of instrumented)
         _            (validate-wired-outs! instrumented port-index)
         g            (build-graph instrumented)
         {:keys [error-chan]} (flow/start g)
         err-done     (start-error-pump! error-chan g
                                         {:raw-ps raw-ps :scope scope :fid fid
                                          :error error :cancel-p cancel-p :done-p done-p})]
     (pubsub/pub raw-ps (trace/run-subject-for scope :run-started)
                 {:kind :run-started :scope-path [fid] :scope scope
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
     (neither) ⇒ input-done marker"
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
        item                (msg/from-opts opts)]
    (swap! done-p (fn [p] (if (realized? p) (promise) p)))
    (ctrs/record-inject! counters)
    (pubsub/pub pubsub (trace/run-subject-for scope :inject)
                (assoc (trace/msg-envelope item)
                       :kind :inject
                       :scope-path [fid] :scope scope
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
   values whose ancestry traces back to that input.

   When the caller supplies an external `:pubsub` (e.g. to share trace
   events with a recorder/viz across multiple concurrent runs), the
   internal provenance subscription is scoped to events whose
   `scope-path` starts with this run's `flow-id` — otherwise sibling
   runs' events leak into our attribution and corrupt the per-input
   output buckets."
  ([stepmap coll] (run-seq stepmap coll {}))
  ([stepmap coll opts]
   (if (empty? coll)
     {:state :completed :outputs [] :counters {:sent 0 :recv 0 :completed 0}}
     (let [collected (atom [])
           ps        (or (:pubsub opts) (pubsub/make))
           fid       (or (:flow-id opts) (str (random-uuid)))
           provenance (atom [])
           prov-unsub (pubsub/sub ps [:>]
                                  (fn [_ ev _]
                                    (when (and (= fid (first (:scope-path ev)))
                                               (#{:inject :split :merge :send-out} (:kind ev)))
                                      (swap! provenance conj ev))))
           wf        (step/serial stepmap (collector-step ::collector collected))
           handle    (start! wf {:pubsub ps :flow-id fid})]
       (doseq [d coll] (inject! handle {:data d}))
       (inject! handle {})  ; close boundary input — fires :on-all-closed cascade
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
