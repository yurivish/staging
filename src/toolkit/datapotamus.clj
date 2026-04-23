(ns toolkit.datapotamus
  "Datapotamus: a thin layer over clojure.core.async.flow that adds a
   scope-prefixed pubsub for trace emission and a span primitive for
   observing sequential work inside a step.

   The unit of composition is a **step**: a map of the shape

     {:procs  {sid factory, ...}   ; factory is (fn [ctx] step-fn)
      :conns  [[[from-sid from-port] [to-sid to-port]], ...]
      :in  sid-or-[sid port]       ; required for run!
      :out sid-or-[sid port]}      ; boundary for outer composition

   A step with one proc is still a step; `serial`, `merge-steps`, and
   `as-step` all produce steps. When a step is run at the top level, it
   plays the role of a workflow — but there's only one data type.

   Factories are 1-arg fns that take a ctx and return a 4-arity
   core.async.flow process-fn. ctx carries:

     :pubsub   — ScopedPubsub prefixed with this step's scope
     :step-id  — this step's id, as it appears in trace events
     :cancel   — promise, delivered on stop!; poll with `realized?`

   Users call `with-span` (a fn taking ctx) to annotate work inside a
   step with named spans.

   Subjects compose uniformly across flow / step / span:

     recv.flow.<fid>.step.<sid>
     recv.flow.<fid>.step.<sid>.span.<name>
     recv.flow.<fid>.flow.<sub>.step.<sid>

   Subflows — a `:procs` entry that is itself a step map — are flattened
   at instrument time. References to a subflow's id in outer conns
   resolve to its `:in` (for to-endpoints) or `:out` (for from-endpoints).
   Subflow inner steps get namespaced graph ids (e.g. `:sub.inc`) and a
   `[:flow <sub>]` scope segment, so their trace events nest correctly.

   File layout (narrative order, top to bottom):

     Part 1 — Messages      envelope, spawn, emit, fcatch
     Part 2 — Steps         step/proc + single-step blueprints
     Part 3 — Composition   serial, merge-steps, connect, input-at, output-at
     Part 4 — Combinators   fan-out, fan-in, router, retry
     Part 5 — Tracing       events, counters, scope+pubsub, with-span
     Part 6 — wrap-proc     the step-execution wrapper that drives handlers
     Part 7 — Flow lifecycle  instrument, start!, inject!, stop!, run!"
  (:refer-clojure :exclude [run!])
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow]
            [clojure.set :as set]
            [clojure.string :as str]
            [toolkit.datapotamus.token :as tok]
            [toolkit.pubsub :as pubsub]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Part 1 — Messages
;;
;; What flows through a graph. Three message kinds — `data`, `signal`,
;; `done` — are a structural sum identified by key absence: data has
;; :data; signal has :tokens but no :data; done has neither. Every
;; child carries parent tokens; when N children come from one parent
;; their tokens are split (never copied) so XOR mass is conserved —
;; this is the foundation that makes fan-in a well-defined morphism.
;; ============================================================================

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

(defn signal?
  "True iff `m` is a signal message — a coordination primitive that
   carries tokens and lineage but no payload. Identified by absence of
   the `:data` key (not by a sentinel value), so `nil` stays a
   legitimate data value."
  [m]
  (not (contains? m :data)))

(defn child-signal
  "Build a signal child of `parent`: carries parent tokens and lineage,
   no `:data` key. Signal messages flow through `wrap-proc` transparently
   — user handlers on downstream steps are not invoked — which makes them
   useful for per-group coordination (e.g. realizing the token-monoid's
   identity for an empty fan-out)."
  ([] (child-signal nil))
  ([parent]
   {:msg-id (random-uuid) :data-id (random-uuid)
    :tokens (:tokens parent {})
    :parent-msg-ids (when parent [(:msg-id parent)])}))

(defn done?
  "True iff `m` is a done message — a port-closure marker. No `:data`,
   no `:tokens`. Arrival means: no more messages will come on this
   input port. `wrap-proc` tracks done per-input-port and cascades a
   fresh done on every declared output port once all inputs have closed."
  [m]
  (and (not (contains? m :data))
       (not (contains? m :tokens))))

(defn new-done
  "Fresh port-closure marker: no data, no tokens, no lineage. Inject
   via `(inject! handle {})` (the empty-opts call) when a source has
   exhausted or a terminator combinator has decided to stop."
  []
  {:msg-id (random-uuid) :at (System/currentTimeMillis)})

(defn- spawn
  "Produce N children of `parent`, splitting parent tokens N ways so
   the children's pointwise XOR equals parent's. Each entry in `specs`
   shapes one child: `{:data d}` builds a data child, `{:signal? true}`
   a signal child. An optional `:extra-tokens` on a spec is merged onto
   its split slice — fan-out uses this to inject the new group's
   zero-sum slice on top of the inherited mass.

   The sibling-token-split invariant lives here and only here: any
   primitive that produces N children from one parent routes through
   this fn, so token conservation across siblings is a property of the
   construction, not of each callsite's discipline."
  [parent specs]
  (let [splits (tok/split-tokens (:tokens parent) (count specs))]
    (mapv (fn [i {:keys [signal? extra-tokens] :as spec}]
            (let [base  (if signal?
                          (child-signal parent)
                          (child-with-data parent (:data spec)))
                  slice (nth splits i)]
              (assoc base :tokens
                     (if extra-tokens (merge slice extra-tokens) slice))))
          (range (count specs)) specs)))

(defn emit
  "Build a handler-output port-map from port/data pairs. Each `data`
   becomes one child msg carrying `m`'s lineage. Repeating a port key
   emits multiple children on that port, in the order they appear in
   the port-map vector at emit time.

     (emit m :out result)            ;; one port, one msg
     (emit m :a x :b y)              ;; multi-port, one msg each
     (emit m :out x :out y :out z)   ;; one port, three msgs

   When more than one child is emitted, `m`'s tokens are split across
   them (via `tok/split-tokens`) so that their pointwise XOR
   reconstructs the original token map. This preserves any upstream
   zero-sum group: N siblings collectively contribute the same token
   mass their parent did, so a downstream `fan-in` correlates
   correctly. For token-free flows the split is a no-op (`{}` splits
   to `n` copies of `{}`).

   Ordering: siblings are independent messages once emitted. Their
   arrival order at a downstream step is NOT guaranteed to match the
   order they appeared in the emit call. The argument order is a local
   property of the port-map vector, not a promise about the flow."
  [m & port-data-pairs]
  (let [pairs (vec (partition 2 port-data-pairs))]
    (if (empty? pairs)
      {}
      (let [kids (spawn m (mapv (fn [[_ data]] {:data data}) pairs))]
        (reduce (fn [acc [[port _] kid]]
                  (update acc port (fnil conj []) kid))
                {}
                (map vector pairs kids))))))

(defn fcatch
  "Wrap f so it returns exceptions rather than throwing them. Useful for
   lifting exceptions into the data domain, where they become routable
   like any other value — compose with `router` (or a custom handler) to
   split success and error streams downstream.

     (dp/step :work (dp/fcatch risky-fn))
     (dp/router :split [:ok :err]
       (fn [v] [{:data v :port (if (instance? Throwable v) :err :ok)}]))

   Catches `Throwable` (not just `Exception`), matching `wrap-proc`."
  [f]
  (fn [& args]
    (try (apply f args)
         (catch Throwable t t))))

;; ============================================================================
;; Part 2 — Steps
;;
;; A step is a blueprint: a map with :procs, :conns, :in, :out. The
;; constructors in this section each build a step with exactly one
;; proc; Part 3 combines steps into larger ones.
;; ============================================================================

(def ^:private auto-id-counters (atom {}))

(defn- gen-id [kind]
  (let [n (-> (swap! auto-id-counters update kind (fnil inc 0)) (get kind))]
    (keyword (str (name kind) "-" n))))

(defn- handler-factory
  "Lift a 3-arg message handler `(fn [ctx s m] -> [s' port-map])` into a
   full factory `(fn [ctx] step-fn)` that satisfies core.async.flow's
   4-arity proc-fn shape. Ports default to `{:in \"\"}` / `{:out \"\"}`.

   Each message invocation passes `ctx` with `:in-port` assoc'd to the
   port keyword the message arrived on — useful for multi-input
   handlers that need to dispatch on the source port."
  ([handler] (handler-factory nil handler))
  ([ports handler]
   (let [ins  (:ins  ports {:in  ""})
         outs (:outs ports {:out ""})]
     (fn [ctx]
       (fn ([]            {:params {} :ins ins :outs outs})
         ([_]           {})
         ([s _]         s)
         ([s in-port m] (handler (assoc ctx :in-port in-port) s m)))))))

(defn- pure-fn-handler
  "Handler that lifts a pure (data -> data) fn: reads :data, emits the
   result on :out with `child-with-data`."
  [f]
  (fn [_ctx s m]
    [s {:out [(child-with-data m (f (:data m)))]}]))

(defn proc
  "Low-level: wrap a raw core.async.flow factory `(fn [ctx] step-fn)`
   into a 1-proc step. Most users want `step` instead — use `proc` only
   when you need access to `ctx` at factory time (rare)."
  [id factory]
  {:procs {id factory} :conns [] :in id :out id})

(defn step
  "Build a 1-proc step.

   2-arity — pure-fn form:
     (step id f)        ; f is (data -> data), ports default to :in / :out

   3-arity — handler form:
     (step id ports handler)
       ports   = {:ins {port-kw \"\"} :outs {port-kw \"\"}}
       handler = (fn [ctx s m] -> [s' port-map])
       port-map shape: {port-kw [msg ...]}; use `emit` to build it.

   For raw factory access (stateful ctx capture at factory time), use
   `proc`."
  ([id f]
   (proc id (handler-factory (pure-fn-handler f))))
  ([id ports handler]
   (proc id (handler-factory ports handler))))

(defn sink
  "Terminal step — consumes input, emits nothing. Default id: :sink."
  ([] (sink :sink))
  ([id]
   (proc id (handler-factory {:outs {}} (fn [_ctx s _m] [s {}])))))

(defn passthrough
  "Identity step: receives on :in, forwards data unchanged on :out with
   fresh ids and inherited lineage. Useful as a placeholder in `serial`."
  ([] (passthrough (gen-id :passthrough)))
  ([id]
   (proc id
         (fn [_ctx]
           (fn ([]      {:params {} :ins {:in ""} :outs {:out ""}})
             ([_]     {})
             ([s _]   s)
             ([s _ m] [s {:out [(child-same-data m)]}]))))))

(defn as-step
  "Black-box `inner-step` as a single proc under `id` in the containing
   step. Inner proc ids get namespaced by `id` at instrument time, and
   inner trace events nest under a [:flow id] scope segment."
  [id inner-step]
  {:procs {id inner-step} :conns [] :in id :out id})

;; ============================================================================
;; Part 3 — Composition
;;
;; Union procs, glue :out → :in, rebind the boundary. Produces new
;; steps out of old; closes under `serial`/`merge-steps` so arbitrary
;; pipelines stay first-class blueprints.
;; ============================================================================

(defn- assert-no-collision! [a b context]
  (let [coll (set/intersection (set (keys (:procs a))) (set (keys (:procs b))))]
    (when (seq coll)
      (throw (ex-info (str context ": colliding proc ids")
                      {:colliding coll})))))

(defn- ref->endpoint [ref default-port]
  (if (vector? ref) ref [ref default-port]))

(defn- conn-glue [from-step to-step]
  [(ref->endpoint (:out from-step) :out)
   (ref->endpoint (:in to-step) :in)])

(defn serial
  "Compose steps sequentially. Glues each step's :out to the next step's
   :in. The composite's :in is the first step's :in; :out is the last
   step's :out."
  [& steps]
  (when (empty? steps)
    (throw (ex-info "serial needs at least one step" {})))
  (reduce (fn [acc f]
            (assert-no-collision! acc f "serial")
            {:procs  (merge (:procs acc) (:procs f))
             :conns  (conj (into (vec (:conns acc)) (:conns f))
                           (conn-glue acc f))
             :in  (:in acc)
             :out (:out f)})
          (first steps)
          (rest steps)))

(defn merge-steps
  "Union procs and conns of several steps without auto-wiring. :in /
   :out of the result come from the first step. Use with `connect` for
   explicit multi-port wiring."
  [& steps]
  (when (empty? steps)
    (throw (ex-info "merge-steps needs at least one step" {})))
  (reduce (fn [acc f]
            (assert-no-collision! acc f "merge-steps")
            (-> acc
                (update :procs merge (:procs f))
                (update :conns (fnil into []) (:conns f))))
          (first steps)
          (rest steps)))

(defn connect
  "Add an explicit conn. `from` and `to` are refs: a keyword (uses the
   default port) or a [sid port] vector."
  [step from to]
  (update step :conns (fnil conj [])
          [(ref->endpoint from :out) (ref->endpoint to :in)]))

(defn input-at
  "Set the step's :in boundary. `ref` is either a step-id keyword
   (default :in port) or `[step-id port]` for an explicit port."
  [step ref]
  (assoc step :in ref))

(defn output-at
  "Set the step's :out boundary. `ref` is either a step-id keyword
   (default :out port) or `[step-id port]` for an explicit port."
  [step ref]
  (assoc step :out ref))

;; ============================================================================
;; Part 4 — Combinators
;;
;; Named algebraic patterns over `step`/`proc` + `emit`/`spawn`.
;; `fan-out` and `fan-in` are duals on the token-group structure —
;; every N-way split gets a fresh zero-sum group; fan-in closes it
;; when the XOR returns to zero. `router` is emit with a runtime-
;; computed port list; `retry` is a local loop inside one handler.
;; ============================================================================

(defn fan-out
  "Emit N copies of the input on :out with a fresh zero-sum token group
   keyed `[group-id input-msg-id]`. Pair with `(fan-in group-id)`
   downstream to detect completion of all N branches.

   2-arg form uses the group-id as the step-id (common case).
   3-arg form lets you set the step-id distinctly, e.g. when fan-out
   and fan-in share a group-id inside the same composition.

   Composition: fan-out splits the incoming message's EXISTING tokens
   across the N children (via `tok/split-tokens`) and adds its own
   fresh zero-sum slice for the new group. This preserves upstream
   zero-sum groups through nesting, so `(fan-out :outer ...) → inner
   processing that itself fans out and in → (fan-in :outer)` composes
   correctly — each outer-group slice traverses the inner topology as
   the algebra dictates. Well-nested fan-out/fan-in pairs are the
   recommended shape; interleaving pairs still yields algebraically
   correct tokens but produces merged data vectors whose nesting
   reflects fire-order at each fan-in rather than user intent, which
   is usually unwanted.

   Ordering: children flow independently through the graph after
   emission; any downstream step (including the paired `fan-in`) sees
   them in arrival order, NOT in emission / child-index order. Tokens
   conserve cardinality, not ordinality. If your pipeline depends on
   position, encode it in the data payload yourself."
  ([group-id n] (fan-out group-id group-id n))
  ([id group-id n]
   (proc id
         (handler-factory
          (fn [_ctx s m]
            (let [gid    [group-id (:msg-id m)]
                  specs  (mapv (fn [v] {:data (:data m) :extra-tokens {gid v}})
                               (tok/split-value 0 n))]
              [s {:out (spawn m specs)}]))))))

(defn fan-in
  "Accumulate inputs grouped by token-ids produced by `(fan-out
   group-id …)`. When a group's XOR reaches zero, emit a merge msg on
   :out whose:
     - parents are the accumulated input msg-ids
     - data is a vector of accumulated datas (in arrival order)
     - tokens are the XOR-merge of all inputs' tokens, with the
       completing group removed.

   1-arg form uses the group-id as the step-id (common case).
   2-arg form lets you set the step-id distinctly — useful when fan-in
   and fan-out share a group-id inside the same composition.

   Ordering: the `:data` vector is in ARRIVAL order, NOT input/
   collection order. Tokens conserve cardinality (multiset semantics),
   not ordinality, so the merged vector reflects the order children
   happened to reach this step through the graph — which depends on
   scheduler, branch latency, and intermediate step behavior. If
   position matters, encode it into each child's data payload before
   fan-out and reconstruct after fan-in."
  ([group-id] (fan-in group-id group-id))
  ([id group-id]
   (proc id
         (handler-factory
          (fn [_ctx s m]
            (let [gids (filterv (fn [k] (and (vector? k) (= group-id (first k))))
                                (keys (:tokens m)))]
              (reduce
               (fn [[s' output] gid]
                 (let [v     (long (get (:tokens m) gid))
                       grp   (get-in s' [:groups gid] {:value 0 :msgs []})
                       grp'  (-> grp
                                 (update :value (fn [x] (bit-xor (long x) v)))
                                 (update :msgs conj m))]
                   (if (zero? (long (:value grp')))
                     (let [merge-id (random-uuid)
                           parents  (mapv :msg-id (:msgs grp'))
                           mtokens  (-> (reduce tok/merge-tokens {} (map :tokens (:msgs grp')))
                                        (dissoc gid))
                           out-msg  {:msg-id        (random-uuid)
                                     :data-id       (random-uuid)
                                     :data          (mapv :data (:msgs grp'))
                                     :tokens        mtokens
                                     :parent-msg-ids [merge-id]}]
                       [(update s' :groups dissoc gid)
                        (-> output
                            (update :out (fnil conj []) out-msg)
                            (update ::merges (fnil conj [])
                                    {:msg-id merge-id :parents parents}))])
                     [(assoc-in s' [:groups gid] grp') output])))
               [s {}]
               gids)))))))

(defn router
  "Route input to multiple ports based on `(route-fn data) →
   [{:data d :port p} ...]`. Splits existing tokens across outputs; does
   not introduce a new zero-group (routing is dispatch, not coordination)."
  [id ports route-fn]
  (let [port-set (set ports)]
    (proc id
          (handler-factory
           {:outs (zipmap ports (repeat ""))}
           (fn [_ctx s m]
             (let [routes (vec (route-fn (:data m)))]
               (doseq [{:keys [port]} routes]
                 (when-not (port-set port)
                   (throw (IllegalArgumentException.
                           (str "router: unknown port " port)))))
               (let [kids (spawn m (mapv #(select-keys % [:data]) routes))]
                 [s (reduce (fn [acc [route kid]]
                              (update acc (:port route) (fnil conj []) kid))
                            {} (map vector routes kids))])))))))

(defn retry
  "Wrap a (data -> data) fn. On exception, retry up to `max-attempts`
   times. Retries are invisible in the trace (internal to the step);
   on exhausted attempts the final exception propagates, surfacing as
   a :failure event."
  [id f max-attempts]
  (proc id
        (handler-factory
         (fn [_ctx s m]
           (loop [attempt 1]
             (let [result (try {:ok (f (:data m))}
                               (catch Throwable t {:err t}))]
               (if (contains? result :ok)
                 [s {:out [(child-with-data m (:ok result))]}]
                 (if (< attempt max-attempts)
                   (recur (inc attempt))
                   (throw (:err result))))))))))

;; ============================================================================
;; Part 5 — Tracing (events, counters, scoped pubsub, spans)
;;
;; Every step run produces a stream of events — recv/success/failure
;; for data; recv-signal/success-signal for signals; done-in/done-out/
;; done-complete for port closures; span-start/success/failure for
;; in-step spans. Each event is published on a subject derived from
;; the step's scope, so subscribers can filter by flow / step / span.
;; `with-span` (user-facing) extends the current scope for the
;; duration of a body-fn.
;; ============================================================================

(defn- now [] (System/currentTimeMillis))

;; --- event constructors (internal) ------------------------------------------

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

(defn- recv-signal-event [step-id m]
  {:kind :recv-signal :step-id step-id :msg-id (:msg-id m) :data-id (:data-id m)
   :tokens (:tokens m) :at (now)})

(defn- success-signal-event [step-id m]
  {:kind :success-signal :step-id step-id :msg-id (:msg-id m) :at (now)})

(defn- send-out-signal-event [step-id port child]
  {:kind :send-out-signal :step-id step-id :port port
   :msg-id (:msg-id child) :data-id (:data-id child)
   :parent-msg-ids (vec (:parent-msg-ids child))
   :tokens (:tokens child) :at (now)})

(defn- done-in-event [step-id in-port m]
  {:kind :done-in :step-id step-id :in-port in-port
   :msg-id (:msg-id m) :at (now)})

(defn- done-out-event [step-id port child]
  {:kind :done-out :step-id step-id :port port
   :msg-id (:msg-id child) :at (now)})

(defn- done-complete-event [step-id m]
  {:kind :done-complete :step-id step-id :msg-id (:msg-id m) :at (now)})

;; --- counter logic ----------------------------------------------------------

(defn- update-counters [counters ev]
  (case (:kind ev)
    :recv             (update counters :recv inc)
    :recv-signal      (update counters :recv inc)
    :done-in          (update counters :recv inc)
    :success          (update counters :completed inc)
    :success-signal   (update counters :completed inc)
    :done-complete    (update counters :completed inc)
    :failure          (update counters :completed inc)
    :send-out         (if (:port ev)
                        (update counters :sent inc)
                        counters)
    :send-out-signal  (if (:port ev)
                        (update counters :sent inc)
                        counters)
    :done-out         (if (:port ev)
                        (update counters :sent inc)
                        counters)
    counters))

(defn- balanced?
  "True iff the step/flow counters indicate all work so far has resolved:
   at least one message was injected, and every sent message has been
   received and completed."
  [{:keys [sent recv completed]}]
  (and (pos? sent) (= sent recv) (= recv completed)))

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

(defn- scope->glob [scope]
  (str "*." (str/join "." (scope->segments scope)) ".>"))

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

;; ============================================================================
;; Part 6 — wrap-proc
;;
;; The step-execution wrapper. Each user step-fn is wrapped once at
;; instrument time; the wrapper drives data/signal/done dispatch,
;; publishes events, resolves handler output-maps (filling in nil
;; parents, separating port data from ::merges/::derivations trace
;; annotations), and enforces that emitted ports are among declared
;; outs.
;; ============================================================================

;; --- handler-output resolution ---------------------------------------------
;;
;; Handlers return `[s' port-map]`. The port-map can contain:
;;   - declared-port keys       — real outgoing messages
;;   - ::merges / ::derivations — trace-only annotations for fan-in
;;     merges and side-channel derivations (no port routing)
;; These helpers resolve nil parent-ids against the current input msg,
;; split the two layers apart, and produce the intermediate events.

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

(defn- internal-key? [k]
  (and (keyword? k) (= "toolkit.datapotamus" (namespace k))))

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

(defn- strip-internal-keys [out]
  (reduce-kv (fn [m k v] (if (internal-key? k) m (assoc m k v))) {} out))

;; --- wrap-proc --------------------------------------------------------------

(defn- wrap-proc [trace-sid step-sp user-step-fn]
  ;; Peek at the step's declared ports once (the 0-arity call is pure per
  ;; the core.async.flow proc-fn contract). `outs` determines where
  ;; signals and done cascades forward to; `ins` defines the done quorum.
  (let [ports         (user-step-fn)
        ins           (:ins ports)
        outs          (:outs ports)
        all-in-ports  (set (keys ins))
        declared-outs (set (keys outs))
        ;; Per-step accumulator of which input ports have closed. When
        ;; this set equals all-in-ports, cascade done on every declared
        ;; output. Multi-input steps hold until all inputs are closed.
        done-ins      (atom #{})]
    (fn
      ([] (user-step-fn))
      ([arg] (user-step-fn arg))
      ([s arg] (user-step-fn s arg))
      ([s in-id m]
       (cond
         (done? m)
         ;; Done short-circuit: framework-managed control signal; user
         ;; handler is not called. Track per-input-port receipt; cascade
         ;; done on all declared output ports once every input has closed.
         (let [closed-ins (swap! done-ins conj in-id)]
           (sp-pub step-sp (done-in-event trace-sid in-id m))
           (if (= closed-ins all-in-ports)
             (let [forwards (into {} (map (fn [p] [p [(new-done)]])) (keys outs))]
               (doseq [[p [d]] forwards]
                 (sp-pub step-sp (done-out-event trace-sid p d)))
               (sp-pub step-sp (done-complete-event trace-sid m))
               [s forwards])
             (do
               (sp-pub step-sp (done-complete-event trace-sid m))
               [s {}])))

         (signal? m)
         ;; Signal short-circuit: skip the user handler, forward on EVERY
         ;; declared output port with parent tokens split across them via
         ;; `spawn`. This preserves any upstream fan-out group's XOR
         ;; invariant through multi-out intermediaries. Terminal steps
         ;; (no :outs) absorb the signal.
         (let [_         (sp-pub step-sp (recv-signal-event trace-sid m))
               out-ports (vec (keys outs))]
           (if (seq out-ports)
             (let [kids     (spawn m (vec (repeat (count out-ports) {:signal? true})))
                   forwards (zipmap out-ports (map vector kids))]
               (doseq [[p [sig]] forwards]
                 (sp-pub step-sp (send-out-signal-event trace-sid p sig)))
               (sp-pub step-sp (success-signal-event trace-sid m))
               [s forwards])
             (do
               (sp-pub step-sp (success-signal-event trace-sid m))
               [s {}])))

         :else
         (do
           (sp-pub step-sp (recv-event trace-sid m))
           (try
             (let [[s' raw]       (user-step-fn s in-id m)
                   user-ports     (into #{}
                                        (filter (fn [k]
                                                  (and (keyword? k)
                                                       (not (namespace k)))))
                                        (keys (or raw {})))
                   unknown-ports  (set/difference user-ports declared-outs)
                   _              (when (seq unknown-ports)
                                    (throw (ex-info
                                            (str "step " trace-sid
                                                 " emitted on undeclared port(s): "
                                                 unknown-ports
                                                 " (declared: " declared-outs ")")
                                            {:step           trace-sid
                                             :unknown-ports  unknown-ports
                                             :declared-ports declared-outs})))
                   resolved       (resolve-output-nils (or raw {}) (:msg-id m))
                   middle-events  (build-middle-events trace-sid resolved)
                   port-map      (strip-internal-keys resolved)]
               (doseq [ev middle-events] (sp-pub step-sp ev))
               (sp-pub step-sp (success-event trace-sid m))
               [s' port-map])
             (catch Throwable ex
               (sp-pub step-sp (failure-event trace-sid m ex))
               [s {}]))))))))

;; ============================================================================
;; Part 7 — Flow lifecycle
;;
;; `instrument-flow` flattens subflows and wraps each leaf factory
;; with `wrap-proc`, producing a plain core.async.flow graph spec.
;; `start!` materializes it; `inject!` seeds messages; `stop!` tears
;; it down. `run!` is a one-shot convenience.
;; ============================================================================

;; --- instrument (flattening) ------------------------------------------------

(defn- subflow? [v]
  (and (map? v) (contains? v :procs) (contains? v :conns)))

(defn- prefix-sid [prefix sid]
  (keyword (str (name prefix) "." (name sid))))

(defn- prefix-endpoint [prefix [sid port]]
  [(prefix-sid prefix sid) port])

(defn- prefix-ref
  "Prefix a step-boundary ref (either a step-id keyword or a [sid port]
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
  "Resolve the step's own :in/:out field against `aliases`. If it
   references a subflow, replace with that subflow's resolved boundary
   ref; otherwise return as-is."
  [ref aliases which]
  (let [sid (if (vector? ref) (first ref) ref)]
    (if-let [a (get aliases sid)]
      (get a which)
      ref)))

(defn- instrument-flow [step outer-sp cancel-p]
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
                (:procs step))
        resolved-conns (mapv (fn [[from to]]
                               [(resolve-endpoint aliases :out from)
                                (resolve-endpoint aliases :in to)])
                             (:conns step))
        in-resolved  (when-let [r (:in step)]  (resolve-flow-ref r aliases :in))
        out-resolved (when-let [r (:out step)] (resolve-flow-ref r aliases :out))]
    (cond-> (assoc step
                   :procs procs
                   :conns (into resolved-conns inner-conns))
      (:in step)  (assoc :in  in-resolved)
      (:out step) (assoc :out out-resolved))))

;; --- start! / inject! / quiescent? / stop! ---------------------------------

(defn- build-graph [step]
  (flow/create-flow
   {:procs (into {} (map (fn [[sid pfn]] [sid {:proc (flow/process pfn)}])) (:procs step))
    :conns (:conns step)}))

(defn start!
  "Instantiate a step and start it running. Returns a handle for
   `inject!` / `quiescent?` / `counters` / `stop!`.

   No messages are seeded. Call `inject!` to add inputs. Unlike `run!`,
   this does not block.

   Opts:
     :pubsub       existing pubsub instance (default: fresh)
     :flow-id      this step's run id (default: fresh uuid)
     :subscribers  {pattern handler-fn} — extra subscriptions that live
                   for the handle's lifetime"
  ([step] (start! step {}))
  ([step opts]
   (let [fid         (or (:flow-id opts) (str (random-uuid)))
         raw-ps      (or (:pubsub opts) (pubsub/make))
         subscribers (:subscribers opts {})
         outer-sp    (scoped-pubsub raw-ps [[:flow fid]])
         cancel-p    (promise)
         instrumented (instrument-flow step outer-sp cancel-p)
         ;; Port index: {sid {:ins #{port-kw} :outs #{port-kw}}}. Built from
         ;; each wrapped proc-fn's 0-arity (pure per the c.a.flow contract)
         ;; and used by `inject!` to validate :in / :port args — turns typos
         ;; into ex-info with a helpful message instead of failures from
         ;; flow/inject's internals.
         port-index  (into {}
                           (map (fn [[sid pfn]]
                                  (let [p (pfn)]
                                    [sid {:ins  (set (keys (:ins  p)))
                                          :outs (set (keys (:outs p)))}])))
                           (:procs instrumented))
         scope       [[:flow fid]]
         events      (atom [])
         counters    (atom {:sent 0 :recv 0 :completed 0})
         quiescent-p (atom (promise))
         main-sub    (pubsub/sub raw-ps (scope->glob scope)
                                 (fn [_ ev _]
                                   (swap! events conj ev)
                                   (let [c' (swap! counters update-counters ev)]
                                     (when (balanced? c')
                                       (deliver @quiescent-p :quiescent)))))
         user-unsubs (mapv (fn [[pat h]] (pubsub/sub raw-ps pat h)) subscribers)
         g           (build-graph instrumented)
         {:keys [error-chan]} (flow/start g)
         err-p       (promise)]
     (pubsub/pub raw-ps (run-subject-for scope :run-started)
                 {:kind :run-started :flow-path [fid] :scope scope :at (now)})
     (flow/resume g)
     (a/thread (when-let [ex (a/<!! error-chan)] (deliver err-p ex)))
     {::step        instrumented
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
      ::err-p       err-p
      ::port-index  port-index})))

(defn inject!
  "Seed a message into the running step. Returns the handle.

   The kind of message injected mirrors the envelope's own shape:
   presence or absence of `:data` and `:tokens` in opts directly
   determines which kind of message is built.

     (inject! h {:data 5})                  ; data msg  (with :tokens {})
     (inject! h {:data 5 :tokens {g v}})    ; data msg  with pre-set tokens
     (inject! h {:tokens {g v}})            ; signal    (no data, has tokens)
     (inject! h {})                         ; done      (no data, no tokens)

   Note the empty-opts case: `(inject! h {})` injects a done, by design.
   Make sure you want that — typos here silently shift semantics.

   Other opts:
     :in     step-id (required unless the step declares :in)
     :port   port keyword (default :in)"
  [handle {:keys [in port] :as opts}]
  (let [{::keys [step graph pubsub scope fid counters quiescent-p port-index]} handle
        ref              (or in (:in step))
        [flow-in flow-port] (if (vector? ref) ref [ref :in])
        port             (or port flow-port)
        step-ports       (get port-index flow-in)
        _                (when-not step-ports
                           (throw (ex-info
                                   (str "inject!: unknown step " flow-in
                                        " (known: " (sort (keys port-index)) ")")
                                   {:step  flow-in
                                    :known (set (keys port-index))})))
        _                (when-not (contains? (:ins step-ports) port)
                           (throw (ex-info
                                   (str "inject!: step " flow-in
                                        " does not declare input port " port
                                        " (declared: " (:ins step-ports) ")")
                                   {:step     flow-in
                                    :port     port
                                    :declared (:ins step-ports)})))
        has-data?        (contains? opts :data)
        has-tokens?      (contains? opts :tokens)
        seed             (cond
                           has-data?
                           (cond-> (new-msg (:data opts))
                             has-tokens? (assoc :tokens (:tokens opts)))

                           has-tokens?
                           {:msg-id (random-uuid) :data-id (random-uuid)
                            :tokens (:tokens opts) :parent-msg-ids []}

                           :else
                           (new-done))
        kind             (cond has-data?   :seed
                               has-tokens? :seed-signal
                               :else       :seed-done)]
    (swap! quiescent-p (fn [p] (if (realized? p) (promise) p)))
    (swap! counters update :sent inc)
    (pubsub/pub pubsub (run-subject-for scope kind)
                (cond-> {:kind kind
                         :flow-path [fid] :scope scope
                         :msg-id (:msg-id seed)
                         :in flow-in :port port :at (now)}
                  has-data?   (assoc :data-id (:data-id seed))
                  has-tokens? (assoc :tokens  (:tokens seed))))
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
  (balanced? (counters handle)))

(defn await-quiescent!
  "Block until the handle reaches quiescence or the underlying graph
   errors. Returns :quiescent, [:failed ex], or :timeout (with timeout-ms)."
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
  "Tear down the graph, deliver the cancel promise, and return a final
   result map {:state :events :counters :error}."
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
     :in           entry step-id (required if not in step's :in)
     :port         entry port (default :in)
     :data         seed data
     :pubsub       existing pubsub instance (default: fresh)
     :flow-id      this step's run id (default: fresh uuid)
     :subscribers  {pattern handler-fn} — extra pubsub subscriptions"
  [step opts]
  (let [handle (start! step (select-keys opts [:pubsub :flow-id :subscribers]))]
    (inject! handle (select-keys opts [:in :port :data]))
    (let [signal (await-quiescent! handle)]
      (-> (stop! handle)
          (assoc :state (if (= :quiescent signal) :completed :failed))
          (cond-> (vector? signal) (assoc :error (second signal)))))))
