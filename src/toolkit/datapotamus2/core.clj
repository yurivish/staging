(ns toolkit.datapotamus2.core
  "Datapotamus2: a thin layer over clojure.core.async.flow adding a
   scope-prefixed pubsub for trace emission and a span primitive for
   observing sequential work inside a step.

   A **flow** is a map:

     {:procs  {sid (fn [ctx] step-fn), ...}
      :conns  [[[from-sid from-port] [to-sid to-port]], ...]
      :in  sid         ; required for run!
      :out sid}        ; optional; when this flow is composed into a
                          ; larger one, :out's :out port is the flow's
                          ; external output.

   Every step-fn is a **factory** — a one-arg fn that takes a ctx and
   returns a 4-arity core.async.flow process-fn. ctx carries:

     :pubsub   — ScopedPubsub prefixed with this step's scope
     :step-id  — this step's id, as it appears in trace events
     :cancel   — promise, delivered on stop!; poll with `realized?`

   Users call `with-span` (a fn taking ctx) to annotate work inside a
   step with named spans.

   Subjects compose uniformly across flow / step / span:

     recv.flow.<fid>.step.<sid>
     recv.flow.<fid>.step.<sid>.span.<name>
     recv.flow.<fid>.flow.<sub>.step.<sid>

   Subflows — a `:procs` entry that is itself a flow map — are flattened
   at instrument time, not driven per message. References to a subflow's
   id in outer conns resolve to its `:in` (for to-endpoints) or
   `:out` (for from-endpoints). Subflow inner steps get namespaced
   graph ids (e.g. `:sub.inc`) and a `[:flow <sub>]` scope segment, so
   their trace events nest correctly under the outer flow.

   `run!` drives a flow to completion with a single seeded message."
  (:refer-clojure :exclude [run!])
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow]
            [clojure.string :as str]
            [toolkit.pubsub :as pubsub]))

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
  "Child msg with new ids, same data as parent, tokens inherited."
  ([] (child-same-data nil))
  ([parent]
   {:msg-id (random-uuid) :data-id (random-uuid) :data (:data parent)
    :tokens (:tokens parent {})
    :parent-msg-ids (when parent [(:msg-id parent)])}))

;; --- event constructors (internal) ------------------------------------------

(defn- now [] (System/currentTimeMillis))

(defn- recv-event [step-id m]
  {:kind :recv :step-id step-id :msg-id (:msg-id m) :data-id (:data-id m)
   :data (:data m) :at (now)})

(defn- success-event [step-id m]
  {:kind :success :step-id step-id :msg-id (:msg-id m) :at (now)})

(defn- failure-event [step-id m ^Throwable ex]
  {:kind :failure :step-id step-id :msg-id (:msg-id m)
   :error {:message (ex-message ex) :data (ex-data ex)} :at (now)})

(defn- send-out-event [step-id port child]
  {:kind :send-out :step-id step-id :port port
   :msg-id (:msg-id child) :data-id (:data-id child)
   :parent-msg-ids (vec (:parent-msg-ids child))
   :tokens (:tokens child) :data (:data child) :at (now)})

(defn- merge-event [step-id msg-id parents]
  {:kind :merge :step-id step-id :msg-id msg-id
   :parent-msg-ids (vec parents) :at (now)})

;; --- output-map helpers -----------------------------------------------------

(defn- resolve-msg-nils [m input-id]
  (cond-> m
    (or (nil? (:parent-msg-ids m))
        (some nil? (:parent-msg-ids m)))
    (update :parent-msg-ids
            (fn [ps]
              (if (or (nil? ps) (empty? ps))
                [input-id]
                (mapv #(or % input-id) ps))))))

(defn- port-output-keys [out]
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

(defn- build-middle-events [step-id output]
  (let [ports     (port-output-keys output)
        port-ids  (into #{} (mapcat (fn [p] (map :msg-id (get output p)))) ports)
        derivs    (->> (get output ::derivations)
                       (remove #(port-ids (:msg-id %))))
        merges    (get output ::merges)
        ev-merges (mapv (fn [{:keys [msg-id parents]}]
                          (merge-event step-id msg-id parents))
                        merges)
        ev-derivs (mapv (fn [c] (send-out-event step-id nil c)) derivs)
        ev-sends  (into []
                        (mapcat
                         (fn [p]
                           (map (fn [c] (send-out-event step-id p c)) (get output p))))
                        ports)]
    (into [] cat [ev-merges ev-derivs ev-sends])))

(defn- strip-dp2-keys [out]
  (reduce-kv (fn [m k v] (if (dp2-key? k) m (assoc m k v))) {} out))

;; --- scope + subject --------------------------------------------------------

(defn- scope->segments [scope]
  (mapcat (fn [[k id]]
            [(name k) (if (keyword? id) (name id) (str id))])
          scope))

(defn- subject-for [scope kind]
  (->> (cons (name kind) (scope->segments scope))
       (str/join ".")))

(defn- run-subject-for [scope kind]
  (->> (cons (name kind) (concat (scope->segments scope) ["run"]))
       (str/join ".")))

(defn- flow-path-of [scope]
  (mapv (fn [[_ id]] (if (keyword? id) (name id) id))
        (filter (fn [[k _]] (= k :flow)) scope)))

;; --- scoped pubsub ----------------------------------------------------------

(defrecord ScopedPubsub [raw prefix])

(defn- scoped-pubsub [raw prefix]
  (->ScopedPubsub raw (vec prefix)))

(defn- sp-pub [^ScopedPubsub sp ev]
  (let [prefix (.prefix sp)]
    (pubsub/pub (.raw sp)
                (subject-for prefix (:kind ev))
                (assoc ev :scope prefix :flow-path (flow-path-of prefix)))))

(defn- sp-extend [^ScopedPubsub sp segment]
  (->ScopedPubsub (.raw sp) (conj (.prefix sp) segment)))

;; --- spans ------------------------------------------------------------------

(defn with-span
  "Run body-fn inside a span scope. Emits :span-start, then runs body-fn,
   then emits :span-success (carrying body-fn's return value) or
   :span-failure (rethrowing the exception). Returns body-fn's result.

   `body-fn` is `(fn [inner-ctx] -> result)`. The inner ctx has its
   pubsub scope extended by [:span span-name]; nested `with-span` calls
   made with inner-ctx therefore nest correctly.

   Span events are observability only — they do not affect run
   completion counters."
  [{:keys [pubsub step-id] :as ctx} span-name metadata body-fn]
  (let [inner-sp  (sp-extend pubsub [:span span-name])
        inner-ctx (assoc ctx :pubsub inner-sp)]
    (sp-pub inner-sp {:kind :span-start :step-id step-id :span-name span-name
                      :metadata metadata :at (now)})
    (try
      (let [result (body-fn inner-ctx)]
        (sp-pub inner-sp {:kind :span-success :step-id step-id :span-name span-name
                          :result result :at (now)})
        result)
      (catch Throwable ex
        (sp-pub inner-sp {:kind :span-failure :step-id step-id :span-name span-name
                          :error {:message (ex-message ex) :data (ex-data ex)}
                          :at (now)})
        (throw ex)))))

;; --- handler factory -------------------------------------------------------

(defn handler-factory
  "Lift a 3-arg message handler `(fn [ctx s m] -> [s' port-map])` into a
   full dp2 factory `(fn [ctx] step-fn)` that satisfies core.async.flow's
   4-arity proc-fn shape. Ports default to `{:in \"\"}` / `{:out \"\"}`.

     (handler-factory h)
     (handler-factory {:ins {...} :outs {...}} h)"
  ([handler] (handler-factory nil handler))
  ([ports handler]
   (let [ins  (:ins  ports {:in  ""})
         outs (:outs ports {:out ""})]
     (fn [ctx]
       (fn ([]      {:params {} :ins ins :outs outs})
           ([_]     {})
           ([s _]   s)
           ([s _ m] (handler ctx s m)))))))

;; --- wrap-proc --------------------------------------------------------------

(defn- wrap-proc [trace-sid step-sp user-step-fn]
  (fn
    ([] (user-step-fn))
    ([arg] (user-step-fn arg))
    ([s arg] (user-step-fn s arg))
    ([s in-id m]
     (sp-pub step-sp (recv-event trace-sid m))
     (try
       (let [[s' raw]      (user-step-fn s in-id m)
             resolved      (resolve-output-nils (or raw {}) (:msg-id m))
             middle-events (build-middle-events trace-sid resolved)
             port-map      (strip-dp2-keys resolved)]
         (doseq [ev middle-events] (sp-pub step-sp ev))
         (sp-pub step-sp (success-event trace-sid m))
         [s' port-map])
       (catch Throwable ex
         (sp-pub step-sp (failure-event trace-sid m ex))
         [s {}])))))

;; --- counter logic ----------------------------------------------------------

(defn- update-counters [counters ev]
  (case (:kind ev)
    :recv     (update counters :recv inc)
    :success  (update counters :completed inc)
    :failure  (update counters :completed inc)
    :send-out (if (:port ev)
                (update counters :sent inc)
                counters)
    counters))

(defn- done? [{:keys [sent recv completed]}]
  (and (pos? sent) (= sent recv) (= recv completed)))

;; --- instrument (flattening) ------------------------------------------------

(defn- subflow? [v]
  (and (map? v) (contains? v :procs) (contains? v :conns)))

(defn- prefix-sid [prefix sid]
  (keyword (str (name prefix) "." (name sid))))

(defn- prefix-endpoint [prefix [sid port]]
  [(prefix-sid prefix sid) port])

(defn- prefix-ref
  "Prefix a flow-boundary ref (either a step-id keyword or a [sid port]
   vector) with `prefix`."
  [prefix ref]
  (if (vector? ref)
    [(prefix-sid prefix (first ref)) (second ref)]
    (prefix-sid prefix ref)))

(defn- instrument-step [trace-sid factory step-sp cancel-p]
  (let [proc-ctx {:pubsub step-sp :step-id trace-sid :cancel cancel-p}
        step-fn  (factory proc-ctx)]
    (wrap-proc trace-sid step-sp step-fn)))

(declare instrument-flow)

(defn- inline-subflow
  "Recursively instrument a subflow with extended scope, rename its graph
   ids to avoid collision in the outer graph, and return the inlined
   pieces plus entry/output aliases."
  [sid subflow outer-sp cancel-p]
  (let [inner-sp      (sp-extend outer-sp [:flow (name sid)])
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
    {:procs renamed-procs
     :conns renamed-conns
     :in in-ref
     :out out-ref}))

(defn- resolve-endpoint
  "Resolve a conn endpoint against `aliases`. If `sid` is a subflow alias,
   use the alias's `:in`/`:out` target: if that target is `[sid port]`,
   substitute it wholesale (the subflow's declared boundary port wins);
   if it's a bare sid, keep the outer conn's port."
  [aliases which [sid port]]
  (if-let [a (get aliases sid)]
    (let [target (get a which)]
      (if (vector? target) target [target port]))
    [sid port]))

(defn- resolve-flow-ref
  "Resolve the flow's own :in/:out field against `aliases`. If it
   references a subflow, replace with that subflow's resolved boundary
   ref; otherwise return as-is."
  [ref aliases which]
  (let [sid (if (vector? ref) (first ref) ref)]
    (if-let [a (get aliases sid)]
      (get a which)
      ref)))

(defn- instrument-flow [flow outer-sp cancel-p]
  (let [{:keys [procs inner-conns aliases]}
        (reduce (fn [acc [sid p]]
                  (if (subflow? p)
                    (let [{:keys [procs conns in out]}
                          (inline-subflow sid p outer-sp cancel-p)]
                      (-> acc
                          (update :procs merge procs)
                          (update :inner-conns into conns)
                          (assoc-in [:aliases sid] {:in in :out out})))
                    (let [step-sp (sp-extend outer-sp [:step sid])
                          wrapped (instrument-step sid p step-sp cancel-p)]
                      (assoc-in acc [:procs sid] wrapped))))
                {:procs {} :inner-conns [] :aliases {}}
                (:procs flow))
        resolved-conns (mapv (fn [[from to]]
                               [(resolve-endpoint aliases :out from)
                                (resolve-endpoint aliases :in to)])
                             (:conns flow))
        in-resolved  (when-let [r (:in flow)]  (resolve-flow-ref r aliases :in))
        out-resolved (when-let [r (:out flow)] (resolve-flow-ref r aliases :out))]
    (cond-> (assoc flow
                   :procs procs
                   :conns (into resolved-conns inner-conns))
      (:in flow)  (assoc :in  in-resolved)
      (:out flow) (assoc :out out-resolved))))

;; --- start! / inject! / quiescent? / stop! ---------------------------------

(defn- build-graph [flow]
  (flow/create-flow
   {:procs (into {} (map (fn [[sid pfn]] [sid {:proc (flow/process pfn)}])) (:procs flow))
    :conns (:conns flow)}))

(defn- scope->glob [scope]
  (str "*." (str/join "." (scope->segments scope)) ".>"))

(defn start!
  "Instantiate a flow and start it running. Returns a handle for
   `inject!` / `quiescent?` / `status` / `stop!`.

   No messages are seeded. Call `inject!` to add inputs. Unlike `run!`,
   this does not block.

   Opts:
     :pubsub       existing pubsub instance (default: fresh)
     :flow-id      this flow's id (default: fresh uuid)
     :subscribers  {pattern handler-fn} — extra subscriptions that live
                   for the handle's lifetime"
  ([flow] (start! flow {}))
  ([flow opts]
   (let [fid         (or (:flow-id opts) (str (random-uuid)))
         raw-ps      (or (:pubsub opts) (pubsub/make))
         subscribers (:subscribers opts {})
         outer-sp    (scoped-pubsub raw-ps [[:flow fid]])
         cancel-p    (promise)
         instrumented (instrument-flow flow outer-sp cancel-p)
         scope       [[:flow fid]]
         events      (atom [])
         counters    (atom {:sent 0 :recv 0 :completed 0})
         quiescent-p (atom (promise))
         main-sub    (pubsub/sub raw-ps (scope->glob scope)
                                 (fn [_ ev _]
                                   (swap! events conj ev)
                                   (let [c' (swap! counters update-counters ev)]
                                     (when (done? c')
                                       (deliver @quiescent-p :quiescent)))))
         user-unsubs (mapv (fn [[pat h]] (pubsub/sub raw-ps pat h)) subscribers)
         g           (build-graph instrumented)
         {:keys [error-chan]} (flow/start g)
         err-p       (promise)]
     (pubsub/pub raw-ps (run-subject-for scope :run-started)
                 {:kind :run-started :flow-path [fid] :scope scope :at (now)})
     (flow/resume g)
     (a/thread (when-let [ex (a/<!! error-chan)] (deliver err-p ex)))
     {::flow        instrumented
      ::graph       g
      ::pubsub      raw-ps
      ::scope       scope
      ::fid         fid
      ::cancel      cancel-p
      ::events      events
      ::counters    counters
      ::quiescent-p quiescent-p
      ::main-sub    main-sub
      ::user-unsubs user-unsubs
      ::err-p       err-p})))

(defn inject!
  "Seed a message into the running flow. Returns the handle.

   Args:
     :in  step-id (required unless the flow declares :in)
     :port   port keyword (default :in)
     :data   seed data"
  [handle {:keys [in port data]}]
  (let [{::keys [flow graph pubsub scope fid counters quiescent-p]} handle
        ref              (or in (:in flow))
        [flow-in flow-port] (if (vector? ref) ref [ref :in])
        port             (or port flow-port)
        seed             (new-msg data)]
    ;; Fresh promise for the next quiescence window; bump :sent before
    ;; injecting so any :recv the inner sub sees can't beat us.
    (swap! quiescent-p (fn [p] (if (realized? p) (promise) p)))
    (swap! counters update :sent inc)
    (pubsub/pub pubsub (run-subject-for scope :seed)
                {:kind :seed :flow-path [fid] :scope scope
                 :msg-id (:msg-id seed) :data-id (:data-id seed)
                 :in flow-in :port port :at (now)})
    @(flow/inject graph [flow-in port] [seed])
    handle))

(defn counters
  "Current counter snapshot {:sent :recv :completed}."
  [handle]
  @(::counters handle))

(defn events
  "Snapshot of events collected so far."
  [handle]
  @(::events handle))

(defn quiescent?
  "True iff the handle's counters currently balance and at least one
   message has been injected."
  [handle]
  (done? (counters handle)))

(defn await-quiescent!
  "Block until the handle reaches quiescence or the underlying graph
   errors. Returns :quiescent, [:failed ex], or :timeout (with timeout-ms).

   Implemented by polling both promises every 10ms — avoids go-blocks
   and doesn't leak waiting threads across repeated calls on a handle."
  ([handle] (await-quiescent! handle nil))
  ([handle timeout-ms]
   (let [{::keys [quiescent-p err-p]} handle
         deadline (when timeout-ms (+ (System/currentTimeMillis) timeout-ms))]
     (loop []
       (cond
         (realized? err-p)
         [:failed @err-p]

         (and deadline (>= (System/currentTimeMillis) deadline))
         :timeout

         :else
         (let [current @quiescent-p
               v       (deref current 10 ::pending)]
           (if (= ::pending v) (recur) v)))))))

(defn stop!
  "Tear down the flow graph, deliver the cancel promise, and return a
   final result map {:state :events :counters :error}."
  [handle]
  (let [{::keys [graph main-sub user-unsubs cancel err-p events counters]} handle]
    (when-not (realized? cancel) (deliver cancel :stopped))
    (flow/stop graph)
    (main-sub)
    (doseq [u user-unsubs] (u))
    (let [err (when (realized? err-p) @err-p)]
      {:state    (if err :failed :completed)
       :events   @events
       :counters @counters
       :error    err})))

(defn run!
  "Convenience: start, inject one message, wait for quiescence, stop.

   Opts:
     :in        entry step-id (required if not in flow's :in)
     :port         entry port (default :in)
     :data         seed data
     :pubsub       existing pubsub instance (default: fresh)
     :flow-id      this flow's id (default: fresh uuid)
     :subscribers  {pattern handler-fn} — extra pubsub subscriptions"
  [flow opts]
  (let [handle (start! flow (select-keys opts [:pubsub :flow-id :subscribers]))]
    (inject! handle (select-keys opts [:in :port :data]))
    (let [signal (await-quiescent! handle)]
      (-> (stop! handle)
          (assoc :state (if (= :quiescent signal) :completed :failed))
          (cond-> (vector? signal) (assoc :error (second signal)))))))
