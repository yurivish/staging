(ns toolkit.potam3.flow
  "The interpreter.

   Takes a step (pure data) and animates it on core.async.flow channels.
   This is the only namespace that requires `core.async.flow`.

   Responsibilities:
     * `run-step`   — pure dispatch: envelope → arrow → synthesis → events.
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
            [toolkit.potam3.msg :as msg]
            [toolkit.potam3.step :as step]
            [toolkit.potam3.trace :as trace]
            [toolkit.pubsub :as pubsub]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; run-step — pure dispatch
;; ============================================================================

(defn- mk-ctx [step-sp trace-sid cancel-p in-port m ins outs]
  {:pubsub  step-sp
   :step-id trace-sid
   :cancel  cancel-p
   :in-port in-port
   :msg     m
   :ins     ins
   :outs    outs})

(defn- validate-ports! [trace-sid port-map outs]
  (let [unknown (seq (filter #(not (contains? outs %)) (keys port-map)))]
    (when unknown
      (throw (ex-info (str "step " trace-sid
                           " emitted on undeclared port(s): " (vec unknown)
                           " (declared: " (vec (keys outs)) ")")
                      {:step-id trace-sid
                       :unknown (vec unknown)
                       :declared (vec (keys outs))})))))

(defn- send-out-events [trace-sid port-map]
  (mapv (fn [[port mm]]
          (trace/send-out-event trace-sid (msg/envelope-kind mm) port mm))
        (for [[port msgs] port-map, mm msgs] [port mm])))

(defn- normalize-return
  "Handler return is either [state' outputs] (vector) or just outputs (map,
   implying state unchanged). Return [state' outputs] uniformly."
  [s ret]
  (if (vector? ret) [(first ret) (second ret)] [s ret]))

(defn- run-data-or-signal
  "Data and signal share a code path. Both call the arrow, synthesize,
   validate, and collect events."
  [arrow m s ctx trace-sid outs kind]
  (try
    (let [ret                  (case kind
                                 :signal ((:on-signal arrow) ctx s)
                                 :data   ((:on-data   arrow) ctx s (:data m)))
          [s' drafts]          (normalize-return s ret)
          [port-map synth-evs] (msg/synthesize drafts m trace-sid)
          _                    (validate-ports! trace-sid port-map outs)
          events (vec (concat [(trace/recv-event trace-sid kind m)]
                              synth-evs
                              (send-out-events trace-sid port-map)
                              [(trace/success-event trace-sid kind m)]))]
      [s' port-map events])
    (catch Throwable ex
      [s {} [(trace/recv-event trace-sid kind m)
             (trace/failure-event trace-sid m ex)]])))

(defn- run-done
  "Done cascade: track closed-ins in framework state. When all inputs are
   closed, call :on-all-closed (drain hook), then forward (new-done) on
   every output port."
  [arrow m ctx s trace-sid ins outs]
  (let [closed  (conj (::closed-ins s #{}) (:in-port ctx))
        all-ins (set (keys ins))]
    (if (= closed all-ins)
      (try
        (let [ret                  ((:on-all-closed arrow) ctx s)
              [s' drafts]          (normalize-return s ret)
              [synth-pm synth-evs] (msg/synthesize drafts m trace-sid)
              port-map             (reduce (fn [pm p]
                                             (update pm p (fnil conj []) (msg/new-done)))
                                           synth-pm (keys outs))
              events (vec (concat [(trace/recv-event trace-sid :done m (:in-port ctx))]
                                  synth-evs
                                  (send-out-events trace-sid port-map)
                                  [(trace/success-event trace-sid :done m)]))]
          [(assoc s' ::closed-ins closed) port-map events])
        (catch Throwable ex
          [(assoc s ::closed-ins closed)
           {}
           [(trace/recv-event trace-sid :done m (:in-port ctx))
            (trace/failure-event trace-sid m ex)]]))
      [(assoc s ::closed-ins closed)
       {}
       [(trace/recv-event trace-sid :done m (:in-port ctx))
        (trace/success-event trace-sid :done m)]])))

(defn run-step
  "Pure dispatch. Returns [new-state port-map events].
   Never throws (user exceptions become :failure events)."
  [arrow m in-port s trace-sid step-sp cancel-p]
  (let [ins  (-> arrow :ports :ins)
        outs (-> arrow :ports :outs)
        ctx  (mk-ctx step-sp trace-sid cancel-p in-port m ins outs)]
    (case (msg/envelope-kind m)
      :done   (run-done               arrow m ctx s trace-sid ins outs)
      :signal (run-data-or-signal     arrow m s ctx trace-sid outs :signal)
      :data   (run-data-or-signal     arrow m s ctx trace-sid outs :data))))

;; ============================================================================
;; proc-fn — the core.async.flow 4-arity adapter
;; ============================================================================

(defn- proc-fn
  "Adapt an arrow to a 4-arity core.async.flow proc-fn. All publishing
   happens here; arrows are pure."
  [arrow trace-sid step-sp cancel-p]
  (let [ports (:ports arrow)
        ins   (:ins ports)
        outs  (:outs ports)]
    (fn
      ([]      {:params {} :ins ins :outs outs})
      ([_]     {})
      ([s _]   s)
      ([s in-port m]
       (let [[s' port-map events] (run-step arrow m in-port s trace-sid step-sp cancel-p)]
         (doseq [e events] (trace/sp-pub step-sp e))
         [s' port-map])))))

;; ============================================================================
;; Instrumentation — subflow inlining + endpoint resolution
;; ============================================================================

(defn- prefix-sid [prefix sid]
  (keyword (str (name prefix) "." (name sid))))

(defn- prefix-endpoint [prefix [sid port]]
  [(prefix-sid prefix sid) port])

(defn- prefix-ref [prefix ref]
  (if (vector? ref)
    [(prefix-sid prefix (first ref)) (second ref)]
    (prefix-sid prefix ref)))

(declare instrument-flow)

(defn- inline-subflow [sid subflow outer-sp cancel-p]
  (let [inner-sp      (update outer-sp :prefix conj [:flow (name sid)])
        inner-inst    (instrument-flow subflow inner-sp cancel-p)
        renamed-procs (into {}
                            (for [[k v] (:procs inner-inst)]
                              [(prefix-sid sid k) v]))
        renamed-conns (mapv (fn [[from to]]
                              [(prefix-endpoint sid from)
                               (prefix-endpoint sid to)])
                            (:conns inner-inst))
        in-ref        (prefix-ref sid (:in inner-inst))
        out-ref       (prefix-ref sid (or (:out inner-inst) (:in inner-inst)))]
    {:procs renamed-procs :conns renamed-conns :in in-ref :out out-ref}))

(defn- resolve-endpoint [aliases which [sid port]]
  (if-let [a (get aliases sid)]
    (let [target (get a which)]
      (if (vector? target) target [target port]))
    [sid port]))

(defn- resolve-flow-ref [ref aliases which]
  (let [sid (if (vector? ref) (first ref) ref)]
    (if-let [a (get aliases sid)]
      (get a which)
      ref)))

(defn- instrument-flow
  "Recursively inline subflows and wrap arrows with proc-fn. Returns a
   flat step map with concrete proc-fns under :procs."
  [stepmap outer-sp cancel-p]
  (let [{:keys [procs inner-conns aliases]}
        (reduce (fn [acc [sid p]]
                  (cond
                    (step/step? p)
                    (let [{:keys [procs conns in out]}
                          (inline-subflow sid p outer-sp cancel-p)]
                      (-> acc
                          (update :procs clojure.core/merge procs)
                          (update :inner-conns into conns)
                          (assoc-in [:aliases sid] {:in in :out out})))

                    (step/arrow? p)
                    (let [step-sp (update outer-sp :prefix conj [:step sid])
                          wrapped (proc-fn p sid step-sp cancel-p)]
                      (assoc-in acc [:procs sid] wrapped))

                    :else
                    (throw (ex-info (str "Unrecognized proc value at " sid)
                                    {:sid sid :value p}))))
                {:procs {} :inner-conns [] :aliases {}}
                (:procs stepmap))
        resolved-conns (mapv (fn [[from to]]
                               [(resolve-endpoint aliases :out from)
                                (resolve-endpoint aliases :in to)])
                             (:conns stepmap))
        in-resolved  (when-let [r (:in  stepmap)] (resolve-flow-ref r aliases :in))
        out-resolved (when-let [r (:out stepmap)] (resolve-flow-ref r aliases :out))]
    (cond-> (assoc stepmap
                   :procs procs
                   :conns (into resolved-conns inner-conns))
      (:in  stepmap) (assoc :in  in-resolved)
      (:out stepmap) (assoc :out out-resolved))))

;; ============================================================================
;; Graph build + validation
;; ============================================================================

(defn- build-graph [instrumented]
  (flow/create-flow
   {:procs (into {}
                 (map (fn [[sid pfn]] [sid {:proc (flow/process pfn)}]))
                 (:procs instrumented))
    :conns (:conns instrumented)}))

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
  (let [out-ref (:out instrumented)
        out-ep  (when out-ref (if (vector? out-ref) out-ref [out-ref :out]))
        used    (cond-> (set (map first (:conns instrumented)))
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
         subscribers  (:subscribers opts {})
         outer-sp     {:raw raw-ps :prefix [[:flow fid]]}
         cancel-p     (promise)
         instrumented (instrument-flow stepmap outer-sp cancel-p)
         port-index   (port-index-of instrumented)
         _            (validate-wired-outs! instrumented port-index)
         scope        [[:flow fid]]
         events-atom  (atom [])
         counters     (atom {:sent 0 :recv 0 :completed 0})
         done-p       (atom (promise))
         main-sub     (pubsub/sub raw-ps (trace/scope->glob scope)
                                  (fn [_ ev _]
                                    (swap! events-atom conj ev)
                                    (let [c' (swap! counters trace/update-counters ev)]
                                      (when (trace/balanced? c')
                                        (deliver @done-p :quiescent)))))
         user-unsubs  (mapv (fn [[pat h]] (pubsub/sub raw-ps pat h)) subscribers)
         g            (build-graph instrumented)
         {:keys [error-chan]} (flow/start g)
         err-done     (a/io-thread
                       (loop []
                         (when-let [m (a/<!! error-chan)]
                           (let [ex  (:clojure.core.async.flow/ex m)
                                 err {:message (ex-message ex) :data (ex-data ex)}]
                             (pubsub/pub raw-ps
                                         (trace/run-subject-for scope :flow-error)
                                         {:kind      :flow-error
                                          :pid       (:clojure.core.async.flow/pid m)
                                          :cid       (:clojure.core.async.flow/cid m)
                                          :msg-id    (get-in m [:clojure.core.async.flow/msg :msg-id])
                                          :error     err
                                          :scope     scope
                                          :flow-path [fid]
                                          :at        (System/currentTimeMillis)})
                             (deliver @done-p [:failed err]))
                           (recur))))]
     (pubsub/pub raw-ps (trace/run-subject-for scope :run-started)
                 {:kind :run-started :flow-path [fid] :scope scope
                  :at   (System/currentTimeMillis)})
     (flow/resume g)
     {::step        instrumented
      ::graph       g
      ::pubsub      raw-ps
      ::scope       scope
      ::fid         fid
      ::cancel      cancel-p
      ::events      events-atom
      ::counters    counters
      ::done-p      done-p
      ::main-sub    main-sub
      ::user-unsubs user-unsubs
      ::err-done    err-done
      ::port-index  port-index})))

(defn inject!
  "Seed a message into the running step. Returns the handle.

   Opts:
     :in    — step id or [step-id port] to target (defaults to flow's :in)
     :port  — override the port (when :in is bare sid)
     :data  — data value; presence ⇒ data envelope
     :tokens — token map; presence without :data ⇒ signal envelope
     (neither) ⇒ done marker"
  [handle {:keys [in port] :as opts}]
  (let [{::keys [step graph pubsub scope fid counters done-p port-index]} handle
        ref                 (or in (:in step))
        [flow-in flow-port] (if (vector? ref) ref [ref :in])
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
        seed                (cond
                              has-data?
                              (cond-> (msg/new-msg (:data opts))
                                has-tokens? (assoc :tokens (:tokens opts)))

                              has-tokens?
                              {:msg-id (random-uuid) :data-id (random-uuid)
                               :tokens (:tokens opts) :parent-msg-ids []}

                              :else
                              (msg/new-done))
        msg-kind            (cond has-data? :data has-tokens? :signal :else :done)]
    (swap! done-p (fn [p] (if (realized? p) (promise) p)))
    (swap! counters update :sent inc)
    (pubsub/pub pubsub (trace/run-subject-for scope :seed)
                (cond-> {:kind :seed :msg-kind msg-kind
                         :flow-path [fid] :scope scope
                         :msg-id (:msg-id seed)
                         :in flow-in :port port
                         :at (System/currentTimeMillis)}
                  has-data?   (assoc :data-id (:data-id seed))
                  has-tokens? (assoc :tokens  (:tokens seed))))
    @(flow/inject graph [flow-in port] [seed])
    handle))

(defn counters   [handle] @(::counters handle))
(defn events     [handle] @(::events handle))
(defn quiescent? [handle] (trace/balanced? (counters handle)))

(defn await-quiescent!
  "Block until quiescence or error. Returns :quiescent, [:failed err], or :timeout."
  ([handle] (await-quiescent! handle nil))
  ([handle timeout-ms]
   (let [p @(::done-p handle)]
     (if timeout-ms
       (deref p timeout-ms :timeout)
       @p))))

(defn stop!
  "Tear down the graph. Returns {:state :events :counters :error}."
  [handle]
  (let [{::keys [graph main-sub user-unsubs cancel err-done events counters]} handle]
    (when-not (realized? cancel) (deliver cancel :stopped))
    (flow/stop graph)
    (a/<!! err-done)
    (main-sub)
    (doseq [u user-unsubs] (u))
    (let [err (some #(when (= :flow-error (:kind %)) %) @events)]
      {:state    (if err :failed :completed)
       :events   @events
       :counters @counters
       :error    (:error err)})))

(defn run!
  "Start, inject one message, wait for quiescence, stop."
  [stepmap opts]
  (let [handle (start! stepmap (select-keys opts [:pubsub :flow-id :subscribers]))]
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

(defn- seed-attribution
  "Forward pass over time-ordered events. Returns {msg-id #{seed-idx ...}}."
  [events seed->idx]
  (reduce (fn [acc ev]
            (case (:kind ev)
              :seed
              (update acc (:msg-id ev) (fnil conj #{}) (seed->idx (:msg-id ev)))
              (:split :merge :send-out)
              (let [parent-seeds (reduce set/union #{} (map acc (:parent-msg-ids ev)))]
                (update acc (:msg-id ev) (fnil set/union #{}) parent-seeds))
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
     {:state :completed :outputs [] :events [] :counters {:sent 0 :recv 0 :completed 0}}
     (let [collected (atom [])
           wf        (step/serial stepmap (collector-step ::collector collected))
           handle    (start! wf (select-keys opts [:pubsub :flow-id :subscribers]))]
       (doseq [d coll] (inject! handle {:data d}))
       (let [signal    (await-quiescent! handle)
             result    (-> (stop! handle)
                           (assoc :state (if (= :quiescent signal) :completed :failed))
                           (cond-> (vector? signal) (assoc :error (second signal))))
             evs       (:events result)
             seed-ids  (mapv :msg-id (filter #(= :seed (:kind %)) evs))
             seed->idx (zipmap seed-ids (range))
             seed-map  (seed-attribution evs seed->idx)
             outputs   (reduce (fn [acc {:keys [msg-id data]}]
                                 (reduce (fn [a s] (update a s conj data))
                                         acc
                                         (seed-map msg-id)))
                               (vec (repeat (count coll) []))
                               @collected)]
         (assoc result :outputs outputs))))))
