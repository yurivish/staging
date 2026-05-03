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

   The system terminates on counter quiescence (sent = recv = completed).
   Stashing aggregators flush via the broadcast protocol — see
   `cast!` and `flush-and-drain!`."
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

(defn- run-broadcast
  "Broadcast lifecycle: an externally-cast signal arrives. `signal-id` is
   the cast key the framework matched against this proc's :signal-select.
   Calls :on-broadcast (default no-op), synthesizes any returned outputs
   normally, and emits :recv/:send-out/:success events as for regular
   inputs — broadcasts are first-class trace citizens."
  [hmap m signal-id ctx s trace-sid outs]
  (try
    (let [ret                  ((:on-broadcast hmap) ctx s signal-id m)
          [s' msgs]            (normalize-return s ret)
          msgs'                (if (identical? msgs msg/drain) {} msgs)
          [port-map synth-evs] (msg/synthesize msgs' m trace-sid)
          _                    (validate-ports! trace-sid port-map outs)
          events (vec (concat synth-evs
                              (send-out-events trace-sid port-map)
                              [(trace/success-event trace-sid m)]))]
      [s' port-map events])
    (catch Throwable ex
      [s {} [(trace/failure-event trace-sid m ex)]])))

(defn run-step
  "Pure dispatch. Returns [new-state port-map events].
   Never throws (user exceptions become :failure events)."
  [hmap m in-port s trace-sid step-sp cancel-p]
  (let [ins  (-> hmap :ports :ins)
        outs (-> hmap :ports :outs)
        ctx  {:pubsub step-sp :step-id trace-sid :cancel cancel-p
              :in-port in-port :msg m :ins ins :outs outs}]
    (case (msg/envelope-kind m)
      :broadcast  (run-broadcast      hmap m (:signal-id m) ctx s trace-sid outs)
      :signal     (run-data-or-signal hmap m s ctx trace-sid outs :signal)
      :data       (run-data-or-signal hmap m s ctx trace-sid outs :data))))

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
  (let [ports         (:ports hmap)
        ins           (:ins ports)
        outs          (:outs ports)
        signal-select (:signal-select hmap)
        emit! (fn [ev]
                (trace/sp-pub step-sp ev)
                (when (ctrs/record-event! counters ev)
                  (deliver @done-p :quiescent)))]
    (fn
      ([]      (cond-> {:params {} :ins ins :outs outs}
                 signal-select (assoc :signal-select signal-select)))
      ([_]     ((:on-init hmap)))
      ([s transition]
       (if (= transition :clojure.core.async.flow/stop)
         (let [ctx {:pubsub step-sp :step-id trace-sid :cancel cancel-p
                    :ins ins :outs outs}]
           (try ((:on-stop hmap) ctx s) (catch Throwable _ nil))
           s)
         s))
      ([s in-port m]
       (if (nil? m)
         ;; Channel closed (nil from core.async.flow's read loop). The
         ;; framework's read loop already dissocs the chan from read-ins
         ;; on first nil read; we just acknowledge with no events.
         ;; Termination is via quiescence (counter balance), not via a
         ;; per-port close cascade — see flush-and-drain! for the
         ;; flush-on-quiescence broadcast protocol used by stashing
         ;; aggregators.
         [s {}]
         (do
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
                 [sid (cond-> {:ins  (set (keys (:ins  spec)))
                               :outs (set (keys (:outs spec)))}
                        (:signal-select spec) (assoc :signal-select
                                                     (:signal-select spec)))])))
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

(defn- castees-of
  "Procs whose :signal-select matches `signal-id`. Returns a vector of
   sids — used to determine how many :sent counters to bump so quiescence
   stays reachable after the broadcast."
  [port-index signal-id]
  (vec (for [[sid {:keys [signal-select]}] port-index
             :when (and signal-select (signal-select signal-id))]
         sid)))

(defn cast!
  "Broadcast `payload` under `signal-id` to every proc whose
   :signal-select matches. The framework's flow/inject special-cases
   `[::flow/cast signal-id]` as a broadcast target.

   Counter accounting: each subscribed proc will fire one :recv +
   one :success when it processes the broadcast. We pre-bump :sent
   once per matching castee so quiescence (sent = recv = completed)
   remains reachable after the broadcast drains.

   Trace accounting: an :inject event is published for the broadcast
   so its descendants (e.g. flush emissions from stashing aggregators)
   have an :inject ancestor in the trace — required for `run-seq`'s
   per-input attribution to work and for retroactive lineage walks
   via obs.store.

   Returns the handle. No-op (other than returning) if no proc
   subscribes to `signal-id` — keeps shutdown loops well-behaved
   on flows with no broadcast consumers."
  ([handle signal-id]         (cast! handle signal-id nil))
  ([handle signal-id payload]
   (let [{::keys [graph counters done-p port-index pubsub scope fid]} handle
         castees   (castees-of port-index signal-id)
         n         (count castees)]
     (when (pos? n)
       (let [item (msg/new-broadcast signal-id payload)]
         (swap! done-p (fn [p] (if (realized? p) (promise) p)))
         (dotimes [_ n] (ctrs/record-inject! counters))
         (pubsub/pub pubsub (trace/run-subject-for scope :inject)
                     (assoc (trace/msg-envelope item)
                            :kind :inject
                            :scope-path [fid] :scope scope
                            :signal-id signal-id
                            :at (trace/now)))
         @(flow/inject graph [::flow/cast signal-id] [item])))
     handle)))

(defn flush-and-drain!
  "Iterative broadcast-flush shutdown protocol. Replaces the previous
   `(inject! handle {})` close-cascade.

   Loop: wait for quiescence, broadcast `signal-id` (default
   `::flow/flush`), wait for quiescence again. Repeat until a round
   produces no emissions beyond the broadcast itself (i.e., no
   downstream propagation happened). Bounded by the longest chain of
   stashing aggregators in the topology — typically ≤ 5 rounds.

   Why iteration: a single broadcast hits all procs simultaneously, so
   a chain A→B of stashing aggregators flushes A in round 1 (B sees
   nothing yet) and B in round 2 (now buffered with A's flushed data).

   Progress detection: the broadcast itself bumps :sent by N (the
   number of subscribed procs) and :recv/:completed by N too. Real
   work happened only if :sent grew by MORE than N — i.e., somebody
   emitted in response. If sent-delta == N, every proc was a no-op
   (returned {} or msg/drain), so we're done.

   Returns `{:rounds n :state :completed|:failed :final-counters {...}}`."
  ([handle]           (flush-and-drain! handle ::flow/flush))
  ([handle signal-id]
   (let [{::keys [port-index]} handle]
     (loop [rounds 0]
       (let [signal (await-quiescent! handle)]
         (cond
           (vector? signal)                   ; [:failed err]
           {:rounds rounds :state :failed :error (second signal)
            :final-counters (counters handle)}

           (= :quiescent signal)
           (let [n           (count (castees-of port-index signal-id))
                 sent-before (:sent (counters handle))
                 _           (cast! handle signal-id)
                 _           (await-quiescent! handle)
                 sent-after  (:sent (counters handle))
                 emissions   (- sent-after sent-before n)]
             (if (pos? emissions)
               (recur (inc rounds))
               {:rounds rounds :state :completed
                :final-counters (counters handle)}))

           :else
           {:rounds rounds :state :unknown :signal signal
            :final-counters (counters handle)}))))))

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
  "Start, inject one message, drain via broadcast-flush, stop.
   Stashing aggregators downstream are flushed by `flush-and-drain!`'s
   iterative broadcast loop (replaces the older empty-inject /
   input-done cascade)."
  [stepmap opts]
  (let [handle (start! stepmap (select-keys opts [:pubsub :flow-id]))]
    (inject! handle (select-keys opts [:in :port :data :tokens]))
    (let [{:keys [state error]} (flush-and-drain! handle)]
      (cond-> (stop! handle)
        true       (assoc :state state)
        error      (assoc :error error)))))

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
       (let [{:keys [state error]} (flush-and-drain! handle)
             result    (cond-> (stop! handle)
                         true  (assoc :state state)
                         error (assoc :error error))
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

;; ============================================================================
;; run-polling! — driver for unbounded polling sources
;; ============================================================================

(defn run-polling!
  "Drive `stepmap` from a virtual-thread polling loop. Use for
   unbounded streaming sources (e.g. an HN maxitem firehose) where
   `run-seq` doesn't apply because there is no natural close.

   Wraps the step in an output collector, starts the flow, and
   spawns a virtual thread that ticks `poll-fn` every
   `:interval-ms`. Each tick:
     `(poll-fn state) → [state' values]`
   yields zero or more values that are `inject!`ed into the flow's
   `:in`. `state` defaults to `:init-state`.

   Returns `{:handle h :collected a :stop! (fn [] ...)}`:
     :handle    — the flow handle (for inject!, counters, pubsub)
     :collected — atom of `{:msg-id :data}` maps, one per :out emission
                  (live updates as the flow processes)
     :stop!     — idempotent halt — terminates the polling thread,
                  then `stop!`s the flow.

   Options:
     :interval-ms — poll cadence (default 1000)
     :poll-fn     — required, see above
     :init-state  — initial poll state (default {})
     :max-ticks   — optional cap on poll iterations (test-only)
     :pubsub      — external pubsub (default new)"
  [stepmap {:keys [interval-ms poll-fn init-state max-ticks pubsub]
            :or   {interval-ms 1000 init-state {}}}]
  (assert poll-fn "run-polling!: :poll-fn is required")
  (let [collected (atom [])
        wf        (step/serial stepmap (collector-step ::collector collected))
        handle    (start! wf (cond-> {} pubsub (assoc :pubsub pubsub)))
        stop?     (atom false)
        ticks     (atom 0)
        thread
        (Thread/startVirtualThread
         (fn []
           (try
             (loop [s init-state]
               (when (and (not @stop?)
                          (or (nil? max-ticks) (< @ticks max-ticks)))
                 (let [[s' values] (poll-fn s)]
                   (swap! ticks inc)
                   (doseq [v values]
                     (when-not @stop?
                       (inject! handle {:data v})))
                   (when (and (not @stop?)
                              (or (nil? max-ticks) (< @ticks max-ticks)))
                     (Thread/sleep (long interval-ms)))
                   (recur s'))))
             (catch InterruptedException _ nil))))]
    {:handle    handle
     :collected collected
     :ticks     ticks
     :thread    thread
     :stop!     (let [stopped? (atom false)]
                  (fn []
                    (when (compare-and-set! stopped? false true)
                      (reset! stop? true)
                      (.interrupt ^Thread thread)
                      (stop! handle))))}))
