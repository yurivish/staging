(ns toolkit.datapotamus
  "Datapotamus: a thin layer over clojure.core.async.flow that adds a
   scope-prefixed pubsub for trace emission and a DAG-based message
   algebra for composable token accounting.

   The unit of composition is a **step**: a map of the shape

     {:procs  {sid factory, ...}   ; factory is (fn [ctx] step-fn)
      :conns  [[[from-sid from-port] [to-sid to-port]], ...]
      :in  sid-or-[sid port]       ; required for run!
      :out sid-or-[sid port]}      ; boundary for outer composition

   A step with one proc is still a step; `serial`, `merge-steps`, and
   `as-step` all produce steps.

   Factories are 1-arg fns that take a ctx and return a 4-arity
   core.async.flow process-fn. ctx carries:

     :pubsub   — scoped pubsub ({:raw :prefix}) for this step's trace emission
     :step-id  — this step's id, as it appears in trace events
     :cancel   — promise, delivered on stop!; poll with `realized?`

   At message time the ctx is further extended with

     :in-port  — the port this invocation arrived on
     :msg      — the current input message envelope

   The message-construction helpers (`child`, `children`, `pass`,
   `signal`, `merge`) are pure — they return draft messages
   carrying in-memory refs to their direct parents under `::parents`.
   The wrapper walks those refs at handler return to reconstruct the
   DAG and distribute tokens.

   Handlers return `[s' [[port msg-or-data] ...]]` — a new state plus
   a seq of port/value pairs. Handler signature is `(fn [ctx s d])`
   where `d` is the input data (shorthand for `(:data (:msg ctx))`).
   A `value` in the outputs that is a message built via one of the
   helpers is routed as-is; a bare value is sugar for
   `(child ctx value)` and is coerced by the wrapper before synthesis.

   ## Token conservation

   Tokens live in an abelian group under XOR. Three invariants compose:

     (1) 1-to-1 preserves tokens.
         (child p d).tokens = p.tokens

     (2) N-way split preserves XOR sum.
         XOR over (children p [d_1..d_N]).tokens = p.tokens

     (3) Merge combines tokens.
         (merge [p_1..p_N] d).tokens = XOR over p_i.tokens

   Fan-out mints a fresh zero-sum group: N children tagged with values
   XOR-summing to 0. Fan-in closes the group when it sees arrivals whose
   XOR for that group returns to 0 — emitting a merge and stripping the
   group's key. (1)(2)(3) compose, so any chain of steps preserves every
   upstream group's XOR sum; downstream fan-ins fire when their groups
   actually close.

   ### Single-consumption invariant

   Conservation holds *per handler invocation*: XOR over (input +
   stashed `merge` parents pulled from state) = XOR over emitted
   outputs, per group. Users who stay within the helpers
   (`child`/`children`/`pass`/`signal`/`merge`) satisfy this by
   construction.

   State-stashing across invocations is fine — it's just deferred
   consumption. The rule: when you derive from a stashed message, emit
   all its derivatives in that same invocation and remove it from
   state. Pair-merger (collect two, emit one):

     (fn [ctx s _d]
       (let [pending (:pending s)]
         (if (nil? pending)
           ;; First half: stash, emit nothing.
           [(assoc s :pending (:msg ctx)) []]
           ;; Second half: consume both in one invocation.
           [(dissoc s :pending)
            [[:out (merge ctx [pending (:msg ctx)]
                          [(:data pending) (:data (:msg ctx))])]]])))

   ### Escape hatch

   For patterns the helpers can't express — custom group minting,
   dribbling a source's tokens across many invocations — stamp
   `assoc-tokens` / `dissoc-tokens` on drafts. Synthesis
   XOR-merges extras into final tokens and dissocs any drop-keys. The
   combinator is responsible for XOR-balancing stamped values so that
   global conservation holds. Dribble sketch (emit K chunks carrying
   a source's tokens across K invocations):

     ;; On :start: stash residual in state, emit nothing.
     [(assoc s :residual (:tokens (:msg ctx)) :left K) []]

     ;; On each :tick: emit a chunk. Last tick carries the residual
     ;; to close the XOR equation.
     (let [{:keys [residual left]} s
           last? (= 1 left)
           chunk (if last? residual (random-tokens-like residual))
           out   (-> (child ctx {:i left}) (assoc-tokens chunk))]
       [(-> s (assoc :residual (tok/merge-tokens residual chunk))
              (update :left dec))
        [[:out out]]])

   Fan-out and fan-in are the canonical in-repo escape-hatch users
   (Part 6) — both call `assoc-tokens` / `dissoc-tokens`.

   ## Trace events

   The event stream is orthogonal on two axes:

     :kind       — lifecycle role (:recv :success :failure :send-out
                   :split :merge :seed :run-started)
     :msg-kind   — message type (:data :signal :done) when applicable

   This means `:kind :recv :msg-kind :signal` replaces what was a
   separate `:recv-signal` event, and so on. Subjects are built from
   `:kind` only — consumers who care about `:msg-kind` filter in the
   handler.

   File layout (narrative order, top to bottom):

     Part 1 — Messages      envelope, derivation helpers, fcatch
     Part 2 — Tracing       events, counters, scope+pubsub
     Part 3 — Synthesis     DAG walk, token synthesis, event emission
     Part 4 — Steps         step/proc + single-step blueprints
     Part 5 — Composition   serial, merge-steps, connect, input-at, output-at
     Part 6 — Combinators   fan-out, fan-in, router, retry
     Part 7 — wrap-proc     the step-execution wrapper
     Part 8 — Flow lifecycle  instrument, start!, inject!, stop!, run!, run-seq"
  (:refer-clojure :exclude [merge run!])
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow]
            [clojure.set :as set]
            [toolkit.datapotamus.token :as tok]
            [toolkit.pubsub :as pubsub]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Part 1 — Messages
;;
;; Three structural message kinds distinguished by key absence:
;;   data:   has :data,  has :tokens
;;   signal: no  :data,  has :tokens
;;   done:   no  :data,  no  :tokens
;;
;; Done messages are port-closure markers generated by the wrapper when
;; all declared input ports have closed. They bypass the user handler
;; and the synthesis pass entirely.
;;
;; Data and signal messages go through synthesis — the helpers return
;; drafts with an in-memory `::parents` field, and the wrapper walks
;; that ref graph at handler return to assign tokens. `::pending` marks
;; a draft's tokens field until synthesis stamps the real value.
;; ============================================================================

(defn new-msg
  "Root msg: no parent. Starts with empty tokens."
  [data]
  {:msg-id (random-uuid) :data-id (random-uuid) :data data
   :tokens {} :parent-msg-ids []})

(defn signal?
  "True iff `m` is a signal message — carries tokens and lineage but no
   payload. Identified by absence of the `:data` key (not a sentinel
   value), so `nil` is a legitimate data value."
  [m]
  (not (contains? m :data)))

(defn done?
  "True iff `m` is a done message — a port-closure marker. No `:data`,
   no `:tokens`."
  [m]
  (and (not (contains? m :data))
       (not (contains? m :tokens))))

(defn new-done
  "Fresh port-closure marker."
  []
  {:msg-id (random-uuid) :at (System/currentTimeMillis)})

;; --- derivation envelopes ----------------------------------------------------
;;
;; Helpers return draft messages: the normal wire-format envelope plus
;; an in-memory `::parents` field holding refs to the direct parent
;; value(s). Each entry in `::parents` is either another draft
;; (`:tokens ::pending`) or a vanilla message — the handler's input or
;; an external message reached via `merge`.
;;
;; Synthesis at handler return walks outputs backward through
;; `::parents` to recover the in-handler DAG, distributes tokens
;; K-ways from each parent to its children, and strips the `::parents`
;; ref before emitting.

(defn- derive-msg
  "Build a derived message envelope: fresh msg-id and data-id, given
   tokens and parent-msg-ids. Tokens are `::pending` for handler-land
   helpers (synthesis stamps the real value) or a real token-map for
   eager construction in wrap-proc."
  [parent-ids tokens]
  {:msg-id (random-uuid)
   :data-id (random-uuid)
   :tokens tokens
   :parent-msg-ids (vec parent-ids)})

(defn- draft?
  "True iff `x` is an in-handler draft built by a helper."
  [x]
  (and (map? x) (= ::pending (:tokens x))))

;; --- public helpers ---------------------------------------------------------

(defn child
  "Single-parent derive. 2-arity uses `(:msg ctx)` as parent; 3-arity
   takes an explicit parent. Returns a draft message carrying
   `::pending` tokens; the wrapper stamps final tokens after the
   handler returns."
  ([ctx data] (child ctx (:msg ctx) data))
  ([_ctx parent data]
   (-> (derive-msg [(:msg-id parent)] ::pending)
       (assoc :data data
              ::parents [parent]))))

(defn children
  "N-way derive of `(:msg ctx)` (or the explicit `parent`)."
  ([ctx datas] (children ctx (:msg ctx) datas))
  ([ctx parent datas]
   (mapv #(child ctx parent %) datas)))

(defn pass
  "Same data as `(:msg ctx)`, preserves its `:data-id`. Use when a
   step routes or coordinates without transforming the payload."
  [ctx]
  (let [parent (:msg ctx)]
    (-> (derive-msg [(:msg-id parent)] ::pending)
        (assoc :data-id (:data-id parent)
               :data    (:data parent)
               ::parents [parent]))))

(defn signal
  "Signal child of `(:msg ctx)` — carries tokens and lineage but no
   payload. Downstream steps' user handlers are not invoked on signal
   messages; they are used for per-group coordination."
  [ctx]
  (let [parent (:msg ctx)]
    (-> (derive-msg [(:msg-id parent)] ::pending)
        (assoc ::parents [parent]))))

(defn merge
  "Multi-parent derive. XOR-combines parent tokens; publishes a `:merge`
   trace event. External parents contribute their full :tokens; in-handler
   parents contribute their K-way slice via synthesis."
  [_ctx parents data]
  (-> (derive-msg (mapv :msg-id parents) ::pending)
      (assoc :data data
             ::parents (vec parents))))

;; --- escape hatch -----------------------------------------------------------
;;
;; For patterns the helpers can't express — custom group minting,
;; dribbling a source across multiple invocations — stamp tokens
;; directly on a draft. Synthesis XOR-merges `::assoc-tokens`
;; into the final tokens and dissocs any keys in `::dissoc-tokens`.
;; The user is responsible for XOR-balancing stamped values so that
;; global conservation holds.

(defn assoc-tokens
  "XOR-merge `token-map` into the draft's final tokens during synthesis.
   Like `assoc` on the token-map, except XOR-merged on key collision
   (which is how token-maps combine throughout the system). Repeated
   tagging composes via XOR-merge.

   Example — mint a fresh zero-sum pair on two siblings:
     (let [gid     [::my-group (random-uuid)]
           [v1 v2] (tok/split-value 0 2)
           a (-> (child ctx data-a) (assoc-tokens {gid v1}))
           b (-> (child ctx data-b) (assoc-tokens {gid v2}))]
       [s [[:a a] [:b b]]])"
  [draft token-map]
  (update draft ::assoc-tokens #(tok/merge-tokens (or % {}) token-map)))

(defn dissoc-tokens
  "Remove `group-keys` from the draft's final tokens during synthesis.
   Like `dissoc` on the token-map. Repeated tagging composes via
   set-union.

   Example — strip a closed group after its zero-sum members have merged:
     (-> (merge ctx (:msgs grp) (mapv :data (:msgs grp)))
         (dissoc-tokens [gid]))"
  [draft group-keys]
  (update draft ::dissoc-tokens (fnil into #{}) group-keys))

;; --- fcatch -----------------------------------------------------------------

(defn fcatch
  "Wrap `f` so it returns exceptions rather than throwing them."
  [f]
  (fn [& args]
    (try (apply f args)
         (catch Throwable t t))))

;; ============================================================================
;; Part 2 — Tracing (events, counters, scoped pubsub)
;;
;; :kind is the lifecycle role; :msg-kind is the message type where
;; applicable. This gives a 4×3 grid for message lifecycle events
;; (:recv/:success/:send-out/:failure × :data/:signal/:done, minus
;; :failure on non-data) plus :split / :merge for DAG derivations
;; and :seed / :run-started for run-level events.
;; ============================================================================

(defn- now [] (System/currentTimeMillis))

;; --- event constructors (internal) ------------------------------------------

(defn- recv-event
  "Build a :recv event for any msg-kind. For :done, pass `in-port`."
  ([step-id msg-kind m] (recv-event step-id msg-kind m nil))
  ([step-id msg-kind m in-port]
   (cond-> {:kind :recv :msg-kind msg-kind :step-id step-id
            :msg-id (:msg-id m) :at (now)}
     (= :data msg-kind)   (assoc :data-id (:data-id m) :data (:data m))
     (= :signal msg-kind) (assoc :data-id (:data-id m) :tokens (:tokens m))
     (= :done msg-kind)   (assoc :in-port in-port))))

(defn- success-event [step-id msg-kind m]
  {:kind :success :msg-kind msg-kind :step-id step-id
   :msg-id (:msg-id m) :at (now)})

(defn- failure-event [step-id m ^Throwable ex]
  {:kind :failure :msg-kind :data :step-id step-id :msg-id (:msg-id m)
   :error {:message (ex-message ex) :data (ex-data ex)} :at (now)})

(defn- send-out-event [step-id msg-kind port child]
  (cond-> {:kind :send-out :msg-kind msg-kind :port port :step-id step-id
           :msg-id (:msg-id child) :at (now)}
    (not= :done msg-kind)
    (assoc :data-id (:data-id child)
           :parent-msg-ids (vec (:parent-msg-ids child))
           :tokens (:tokens child))

    (= :data msg-kind)
    (assoc :data (:data child))))

(defn- split-event [step-id child]
  {:kind :split :step-id step-id
   :msg-id (:msg-id child) :data-id (:data-id child)
   :parent-msg-ids (vec (:parent-msg-ids child))
   :data (:data child) :at (now)})

(defn- merge-event [step-id msg-id parents]
  {:kind :merge :step-id step-id :msg-id msg-id
   :parent-msg-ids (vec parents) :at (now)})

;; --- counter logic ----------------------------------------------------------

(defn- update-counters [counters ev]
  (case (:kind ev)
    :recv     (update counters :recv inc)
    :success  (update counters :completed inc)
    :failure  (update counters :completed inc)
    :send-out (if (:port ev) (update counters :sent inc) counters)
    counters))

(defn- balanced?
  "True iff counters indicate all work so far has resolved."
  [{:keys [sent recv completed]}]
  (and (pos? sent) (= sent recv) (= recv completed)))

;; --- scope + subject --------------------------------------------------------
;;
;; Subjects are vectors (per toolkit.pubsub/sublist). A scope is a vector
;; of `[key id]` tuples, e.g. `[[:flow fid] [:step sid]]`, flattened to
;; string tokens. A subject prepends the event kind, keeping kind-first
;; ordering so `[kind flow <fid> :>]` cleanly filters by kind.

(defn- scope->tokens [scope]
  (vec (mapcat (fn [[k id]]
                 [(name k) (if (keyword? id) (name id) (str id))])
               scope)))

(defn- subject-for [scope kind]
  (into [(name kind)] (scope->tokens scope)))

(defn- run-subject-for [scope kind]
  (-> (subject-for scope kind) (conj "run")))

(defn- flow-path-of [scope]
  (mapv (fn [[_ id]] (if (keyword? id) (name id) id))
        (filter (fn [[k _]] (= k :flow)) scope)))

(defn- scope->glob [scope]
  (-> [:*] (into (scope->tokens scope)) (conj :>)))

;; --- scoped pubsub ----------------------------------------------------------
;;
;; A scoped pubsub is a plain map `{:raw raw-ps :prefix [[:flow fid] ...]}`.
;; `sp-pub` stamps the prefix into the subject and event metadata. To
;; extend a scope with a child segment, `update :prefix conj <segment>`.

(defn- sp-pub [{:keys [raw prefix]} ev]
  (pubsub/pub raw
              (subject-for prefix (:kind ev))
              (assoc ev :scope prefix :flow-path (flow-path-of prefix))))

;; ============================================================================
;; Part 3 — Synthesis
;;
;; After a handler returns, we walk outputs backward through `::parents`
;; refs to enumerate the in-handler DAG and find each emitted draft's
;; set of "ultimate sources" — the vanilla-message ancestors (the handler
;; input, or external messages reached via `merge`). Token assignment is
;; then: for each source S, let K = number of emissions reached from S;
;; split S.tokens K-ways across those emissions and XOR each slice into
;; the emission's running total.
;;
;; Intermediate drafts (in-handler nodes that are parents-of-outputs
;; but not themselves emitted) get `:split` / `:merge` trace events for
;; lineage purposes but do NOT get tokens assigned — tokens live on
;; wire-format emissions only, and trace events are token-free for
;; structural `:split` / `:merge` kinds (see Part 2).
;;
;; Any assignment that satisfies per-source XOR conservation is valid;
;; the ultimate-source K-way split is the simplest such assignment.
;; ============================================================================

(defn- coerce-bare-outputs
  "Replace bare-data values in `raw-outputs` with draft children."
  [ctx raw-outputs]
  (mapv (fn [[port v]]
          (if (draft? v) [port v] [port (child ctx v)]))
        raw-outputs))

(defn- collect-dag
  "Walk outputs backward through `::parents`. Returns [topo nodes sources]
   where topo lists draft msg-ids parents-before-children, nodes
   maps msg-id → draft, and sources maps msg-id → #{vanilla-msg ...}
   (its set of ultimate-source ancestors)."
  [outputs]
  (letfn [(visit [[topo nodes sources :as acc] node]
            (let [id (:msg-id node)]
              (if (contains? nodes id)
                acc
                (let [[topo' nodes' sources'] (reduce visit acc
                                                     (filter draft?
                                                             (::parents node)))
                      own-sources (reduce (fn [s p]
                                            (if (draft? p)
                                              (into s (get sources' (:msg-id p)))
                                              (conj s p)))
                                          #{}
                                          (::parents node))]
                  [(conj topo' id)
                   (assoc nodes' id node)
                   (assoc sources' id own-sources)]))))]
    (reduce visit [[] {} {}] (map second outputs))))

(defn- synthesize-tokens
  "Return {emission-msg-id tokens} by grouping emitted drafts
   under each of their ultimate sources and splitting each source's
   tokens K-ways across the emissions that reach it."
  [outputs sources]
  (let [emits (keep (fn [[_ v]] (when (draft? v) v)) outputs)
        by-source (reduce (fn [acc e]
                            (reduce (fn [a s]
                                      (update a s (fnil conj []) e))
                                    acc
                                    (get sources (:msg-id e))))
                          {} emits)]
    (reduce-kv
     (fn [tokens src emissions]
       (let [slices (tok/split-tokens (:tokens src {}) (count emissions))]
         (reduce (fn [t [e slice]]
                   (update t (:msg-id e) #(tok/merge-tokens (or % {}) slice)))
                 tokens
                 (map vector emissions slices))))
     {} by-source)))

(defn- materialize-msg
  "Strip the in-memory `::parents` ref and the `::assoc-tokens` /
   `::dissoc-tokens` overlays, then stamp final `:tokens` (with those
   overlays applied)."
  [node tokens]
  (let [added    (::assoc-tokens node)
        dropped  (::dissoc-tokens node)
        tokens'  (tok/merge-tokens tokens (or added {}))
        tokens'' (if (seq dropped) (apply dissoc tokens' dropped) tokens')]
    (-> node
        (dissoc ::parents ::assoc-tokens ::dissoc-tokens)
        (assoc :tokens tokens''))))

(defn- emit-event [step-sp step-id node]
  (if (> (count (::parents node)) 1)
    (sp-pub step-sp (merge-event step-id (:msg-id node) (:parent-msg-ids node)))
    (sp-pub step-sp (split-event step-id node))))

(defn- process-outputs
  "Post-handler pipeline. Returns `{port [final-msg ...]}` and publishes
   :split / :merge events in topological order."
  [ctx raw-outputs]
  (let [outputs      (coerce-bare-outputs ctx (or raw-outputs []))
        [topo nodes sources] (collect-dag outputs)
        tokens-by    (synthesize-tokens outputs sources)
        step-sp      (:pubsub ctx)
        step-id      (:step-id ctx)]
    (doseq [id topo]
      (emit-event step-sp step-id (nodes id)))
    (reduce (fn [acc [port prov]]
              (let [final (materialize-msg prov (tokens-by (:msg-id prov) {}))]
                (update acc port (fnil conj []) final)))
            {} outputs)))

;; ============================================================================
;; Part 4 — Steps
;; ============================================================================

(def ^:private auto-id-counters (atom {}))

(defn- gen-id [kind]
  (let [n (-> (swap! auto-id-counters update kind (fnil inc 0)) (get kind))]
    (keyword (str (name kind) "-" n))))

(defn- handler-factory
  "Lift a 3-arg message handler `(fn [ctx s d] -> [s' outputs])` into a
   full factory `(fn [ctx] step-fn)` that satisfies core.async.flow's
   4-arity proc-fn shape.

   `ports` is `{:ins {port-kw \"\"} :outs {port-kw \"\"}}`; pass `nil` for
   the default `{:in \"\"} / {:out \"\"}`.

   `d` is the input data (shorthand for `(:data (:msg ctx))`). The
   full envelope is on `(:msg ctx)` if needed (tokens, lineage, etc.).

   `outputs` is a seq of `[port msg-or-data]` pairs. Provisional
   messages returned by the derivation helpers carry `::parents` refs
   to their direct parents; synthesis walks that ref graph on handler
   return to distribute tokens. Bare data in outputs is coerced via
   `child` during that pass."
  [ports handler]
  (let [ins  (:ins  ports {:in  ""})
        outs (:outs ports {:out ""})]
    (fn [factory-ctx]
      (fn ([]             {:params {} :ins ins :outs outs})
        ([_]            {})
        ([s _]          s)
        ([s in-port m]
         (let [ctx      (assoc factory-ctx :in-port in-port :msg m)
               [s' raw] (handler ctx s (:data m))]
           [s' (process-outputs ctx raw)]))))))

(defn proc
  "Low-level: wrap a raw core.async.flow factory `(fn [ctx] step-fn)`
   into a 1-proc step. Most users want `step` instead."
  [id factory]
  {:procs {id factory} :conns [] :in id :out id})

(defn step
  "Build a 1-proc step.

   2-arity — pure-fn form:
     (step id f)        ; f is (data -> data), ports default to :in / :out

   3-arity — handler form:
     (step id ports handler)
       ports   = {:ins {port-kw \"\"} :outs {port-kw \"\"}}
       handler = (fn [ctx s d] -> [s' [[port msg-or-data] ...]])

   `d` is the incoming data (the `:data` field of the input message).
   Full envelope is on `(:msg ctx)`."
  ([id f]
   (proc id (handler-factory nil (fn [_ s d] [s [[:out (f d)]]]))))
  ([id ports handler]
   (proc id (handler-factory ports handler))))

(defn sink
  "Terminal step — consumes input, emits nothing."
  ([] (sink :sink))
  ([id]
   (proc id (handler-factory {:outs {}} (fn [_ctx s _d] [s []])))))

(defn passthrough
  "Identity step: forwards data unchanged with preserved data-id."
  ([] (passthrough (gen-id :passthrough)))
  ([id]
   (proc id (handler-factory nil (fn [ctx s _d] [s [[:out (pass ctx)]]])))))

(defn as-step
  "Black-box `inner-step` as a single proc under `id`."
  [id inner-step]
  {:procs {id inner-step} :conns [] :in id :out id})

;; ============================================================================
;; Part 5 — Composition
;; ============================================================================

(defn- assert-no-collision! [a b context]
  (let [coll (set/intersection (set (keys (:procs a))) (set (keys (:procs b))))]
    (when (seq coll)
      (throw (ex-info (str context ": colliding proc ids") {:colliding coll})))))

(defn- ref->endpoint [ref default-port]
  (if (vector? ref) ref [ref default-port]))

(defn- conn-glue [from-step to-step]
  [(ref->endpoint (:out from-step) :out)
   (ref->endpoint (:in to-step) :in)])

(defn serial
  "Compose steps sequentially."
  [& steps]
  (when (empty? steps)
    (throw (ex-info "serial needs at least one step" {})))
  (reduce (fn [acc f]
            (assert-no-collision! acc f "serial")
            {:procs  (clojure.core/merge (:procs acc) (:procs f))
             :conns  (conj (into (vec (:conns acc)) (:conns f))
                           (conn-glue acc f))
             :in  (:in acc)
             :out (:out f)})
          (first steps)
          (rest steps)))

(defn merge-steps
  "Union procs and conns without auto-wiring."
  [& steps]
  (when (empty? steps)
    (throw (ex-info "merge-steps needs at least one step" {})))
  (reduce (fn [acc f]
            (assert-no-collision! acc f "merge-steps")
            (-> acc
                (update :procs clojure.core/merge (:procs f))
                (update :conns (fnil into []) (:conns f))))
          (first steps)
          (rest steps)))

(defn connect
  "Add an explicit conn."
  [step from to]
  (update step :conns (fnil conj [])
          [(ref->endpoint from :out) (ref->endpoint to :in)]))

(defn input-at
  "Set the step's :in boundary."
  [step ref]
  (assoc step :in ref))

(defn output-at
  "Set the step's :out boundary."
  [step ref]
  (assoc step :out ref))

;; ============================================================================
;; Part 6 — Combinators
;; ============================================================================

(defn fan-out
  "Emit N copies of the input on :out with a fresh zero-sum token group."
  ([group-id n] (fan-out group-id group-id n))
  ([id group-id n]
   (proc id
         (handler-factory
          nil
          (fn [ctx s d]
            (let [gid    [group-id (:msg-id (:msg ctx))]
                  values (tok/split-value 0 n)
                  kids   (children ctx (repeat n d))
                  kids'  (mapv (fn [k v] (assoc-tokens k {gid v}))
                               kids values)]
              [s (mapv (fn [k] [:out k]) kids')]))))))

(defn fan-in
  "Accumulate inputs grouped by token-ids produced by `fan-out`."
  ([group-id] (fan-in group-id group-id))
  ([id group-id]
   (proc id
         (handler-factory
          nil
          (fn [ctx s _d]
            (let [m    (:msg ctx)
                  gids (filterv (fn [k] (and (vector? k) (= group-id (first k))))
                                (keys (:tokens m)))]
              (reduce
               (fn [[s' output] gid]
                 (let [v    (long (get (:tokens m) gid))
                       grp  (get-in s' [:groups gid] {:value 0 :msgs []})
                       grp' (-> grp
                                (update :value (fn [x] (bit-xor (long x) v)))
                                (update :msgs conj m))]
                   (if (zero? (long (:value grp')))
                     (let [merged (-> (merge ctx (:msgs grp') (mapv :data (:msgs grp')))
                                      (dissoc-tokens [gid]))]
                       [(update s' :groups dissoc gid)
                        (conj output [:out merged])])
                     [(assoc-in s' [:groups gid] grp') output])))
               [s []]
               gids)))))))

(defn router
  "Route input to multiple ports based on `(route-fn data)`."
  [id ports route-fn]
  (let [port-set (set ports)]
    (proc id
          (handler-factory
           {:outs (zipmap ports (repeat ""))}
           (fn [ctx s d]
             (let [routes (vec (route-fn d))]
               (doseq [{:keys [port]} routes]
                 (when-not (port-set port)
                   (throw (IllegalArgumentException.
                           (str "router: unknown port " port)))))
               (let [kids (children ctx (mapv :data routes))]
                 [s (mapv (fn [route kid] [(:port route) kid])
                          routes kids)])))))))

(defn retry
  "Wrap a (data -> data) fn. Retries on exception up to `max-attempts`."
  [id f max-attempts]
  (proc id
        (handler-factory
         nil
         (fn [_ctx s d]
           (loop [attempt 1]
             (let [result (try {:ok (f d)}
                               (catch Throwable t {:err t}))]
               (if (contains? result :ok)
                 [s [[:out (:ok result)]]]
                 (if (< attempt max-attempts)
                   (recur (inc attempt))
                   (throw (:err result))))))))))

;; ============================================================================
;; Part 7 — wrap-proc
;; ============================================================================

(defn- wrap-proc [trace-sid step-sp user-step-fn]
  (let [ports         (user-step-fn)
        ins           (:ins ports)
        outs          (:outs ports)
        all-in-ports  (set (keys ins))
        declared-outs (set (keys outs))
        done-ins      (atom #{})]
    (fn
      ([] (user-step-fn))
      ([arg] (user-step-fn arg))
      ([s arg] (user-step-fn s arg))
      ([s in-id m]
       (cond
         (done? m)
         (let [closed-ins (swap! done-ins conj in-id)]
           (sp-pub step-sp (recv-event trace-sid :done m in-id))
           (if (= closed-ins all-in-ports)
             (let [forwards (into {} (map (fn [p] [p [(new-done)]])) (keys outs))]
               (doseq [[p [d]] forwards]
                 (sp-pub step-sp (send-out-event trace-sid :done p d)))
               (sp-pub step-sp (success-event trace-sid :done m))
               [s forwards])
             (do
               (sp-pub step-sp (success-event trace-sid :done m))
               [s {}])))

         (signal? m)
         (let [_         (sp-pub step-sp (recv-event trace-sid :signal m))
               out-ports (vec (keys outs))]
           (if (seq out-ports)
             (try
               (let [ctx      {:pubsub step-sp :step-id trace-sid :msg m}
                     raw      (mapv (fn [p] [p (signal ctx)]) out-ports)
                     port-map (process-outputs ctx raw)]
                 (doseq [[port msgs] port-map
                         msg msgs]
                   (sp-pub step-sp (send-out-event trace-sid :signal port msg)))
                 (sp-pub step-sp (success-event trace-sid :signal m))
                 [s port-map])
               (catch Throwable ex
                 (sp-pub step-sp (failure-event trace-sid m ex))
                 [s {}]))
             (do
               (sp-pub step-sp (success-event trace-sid :signal m))
               [s {}])))

         :else
         (do
           (sp-pub step-sp (recv-event trace-sid :data m))
           (try
             (let [[s' port-map] (user-step-fn s in-id m)
                   user-ports    (set (keys port-map))
                   unknown-ports (set/difference user-ports declared-outs)
                   _             (when (seq unknown-ports)
                                   (throw (ex-info
                                           (str "step " trace-sid
                                                " emitted on undeclared port(s): "
                                                unknown-ports
                                                " (declared: " declared-outs ")")
                                           {:step           trace-sid
                                            :unknown-ports  unknown-ports
                                            :declared-ports declared-outs})))]
               (doseq [[port msgs] port-map
                       msg msgs]
                 (sp-pub step-sp (send-out-event trace-sid :data port msg)))
               (sp-pub step-sp (success-event trace-sid :data m))
               [s' port-map])
             (catch Throwable ex
               (sp-pub step-sp (failure-event trace-sid m ex))
               [s {}]))))))))

;; ============================================================================
;; Part 8 — Flow lifecycle
;; ============================================================================

(defn- subflow? [v]
  (and (map? v) (contains? v :procs) (contains? v :conns)))

(defn- prefix-sid [prefix sid]
  (keyword (str (name prefix) "." (name sid))))

(defn- prefix-endpoint [prefix [sid port]]
  [(prefix-sid prefix sid) port])

(defn- prefix-ref [prefix ref]
  (if (vector? ref)
    [(prefix-sid prefix (first ref)) (second ref)]
    (prefix-sid prefix ref)))

(defn- instrument-step [trace-sid factory step-sp cancel-p]
  (let [proc-ctx {:pubsub step-sp :step-id trace-sid :cancel cancel-p}
        step-fn  (factory proc-ctx)]
    (wrap-proc trace-sid step-sp step-fn)))

(declare instrument-flow)

(defn- inline-subflow
  [sid subflow outer-sp cancel-p]
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

(defn- instrument-flow [step outer-sp cancel-p]
  (let [{:keys [procs inner-conns aliases]}
        (reduce (fn [acc [sid p]]
                  (if (subflow? p)
                    (let [{:keys [procs conns in out]}
                          (inline-subflow sid p outer-sp cancel-p)]
                      (-> acc
                          (update :procs clojure.core/merge procs)
                          (update :inner-conns into conns)
                          (assoc-in [:aliases sid] {:in in :out out})))
                    (let [step-sp (update outer-sp :prefix conj [:step sid])
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

(defn- build-graph [step]
  (flow/create-flow
   {:procs (into {} (map (fn [[sid pfn]] [sid {:proc (flow/process pfn)}])) (:procs step))
    :conns (:conns step)}))

(defn- validate-wired-outs!
  "Throw iff any proc declares an output port that is neither consumed by
   a conn nor exposed via the flow's :out boundary. An unwired port is
   a latent hang — `flow/inject` on it would block forever."
  [instrumented port-index]
  (let [out-ref      (:out instrumented)
        out-endpoint (when out-ref
                       (if (vector? out-ref) out-ref [out-ref :out]))
        used         (cond-> (set (map first (:conns instrumented)))
                       out-endpoint (conj out-endpoint))
        unwired      (vec (for [[sid {:keys [outs]}] port-index
                                port outs
                                :when (not (used [sid port]))]
                            [sid port]))]
    (when (seq unwired)
      (throw (ex-info
              (str "step has unwired output port(s): " (pr-str unwired)
                   "; every declared :out must be consumed by a conn or "
                   "match the step's :out boundary")
              {:unwired unwired})))))

(defn start!
  "Instantiate a step and start it running. Returns a handle."
  ([step] (start! step {}))
  ([step opts]
   (let [fid          (or (:flow-id opts) (str (random-uuid)))
         raw-ps       (or (:pubsub opts) (pubsub/make))
         subscribers  (:subscribers opts {})
         outer-sp     {:raw raw-ps :prefix [[:flow fid]]}
         cancel-p     (promise)
         instrumented (instrument-flow step outer-sp cancel-p)
         port-index   (into {}
                            (map (fn [[sid pfn]]
                                   (let [p (pfn)]
                                     [sid {:ins  (set (keys (:ins  p)))
                                           :outs (set (keys (:outs p)))}])))
                            (:procs instrumented))
         _            (validate-wired-outs! instrumented port-index)
         scope        [[:flow fid]]
         events       (atom [])
         counters     (atom {:sent 0 :recv 0 :completed 0})
         done-p       (atom (promise))
         main-sub     (pubsub/sub raw-ps (scope->glob scope)
                                  (fn [_ ev _]
                                    (swap! events conj ev)
                                    (let [c' (swap! counters update-counters ev)]
                                      (when (balanced? c')
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
                                          (run-subject-for scope :flow-error)
                                          {:kind      :flow-error
                                           :pid       (:clojure.core.async.flow/pid m)
                                           :cid       (:clojure.core.async.flow/cid m)
                                           :msg-id    (get-in m [:clojure.core.async.flow/msg :msg-id])
                                           :error     err
                                           :scope     scope
                                           :flow-path [fid]
                                           :at        (now)})
                              (deliver @done-p [:failed err]))
                            (recur))))]
     (pubsub/pub raw-ps (run-subject-for scope :run-started)
                 {:kind :run-started :flow-path [fid] :scope scope :at (now)})
     (flow/resume g)
     {::step        instrumented
      ::graph       g
      ::pubsub      raw-ps
      ::scope       scope
      ::fid         fid
      ::cancel      cancel-p
      ::events      events
      ::counters    counters
      ::done-p      done-p
      ::main-sub    main-sub
      ::user-unsubs user-unsubs
      ::err-done    err-done
      ::port-index  port-index})))

(defn inject!
  "Seed a message into the running step. Returns the handle."
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
                              (cond-> (new-msg (:data opts))
                                has-tokens? (assoc :tokens (:tokens opts)))

                              has-tokens?
                              {:msg-id (random-uuid) :data-id (random-uuid)
                               :tokens (:tokens opts) :parent-msg-ids []}

                              :else
                              (new-done))
        msg-kind            (cond has-data? :data has-tokens? :signal :else :done)]
    (swap! done-p (fn [p] (if (realized? p) (promise) p)))
    (swap! counters update :sent inc)
    (pubsub/pub pubsub (run-subject-for scope :seed)
                (cond-> {:kind :seed :msg-kind msg-kind
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
  "True iff counters balance and at least one message has been injected."
  [handle]
  (balanced? (counters handle)))

(defn await-quiescent!
  "Block until quiescence or error. Returns :quiescent, [:failed err], or :timeout."
  ([handle] (await-quiescent! handle nil))
  ([handle timeout-ms]
   (let [p @(::done-p handle)]
     (if timeout-ms
       (deref p timeout-ms :timeout)
       @p))))

(defn stop!
  "Tear down the graph."
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
  "Convenience: start, inject one message, wait for quiescence, stop."
  [step opts]
  (let [handle (start! step (select-keys opts [:pubsub :flow-id :subscribers]))]
    (inject! handle (select-keys opts [:in :port :data :tokens]))
    (let [signal (await-quiescent! handle)]
      (-> (stop! handle)
          (assoc :state (if (= :quiescent signal) :completed :failed))))))

(defn- collector-step
  "Terminal step that appends `{:msg-id ... :data ...}` to `a` for every
   data message it receives. Internal to `run-seq`."
  [id a]
  (proc id
        (handler-factory
         {:outs {}}
         (fn [ctx s d]
           (swap! a conj {:msg-id (:msg-id (:msg ctx)) :data d})
           [s []]))))

(defn- seed-attribution
  "Single forward pass over time-ordered `events`: returns
   `{msg-id #{seed-idx ...}}` mapping every message to the set of
   seed indices whose ancestry reaches it. Relies on events arriving
   in causal order (a derived message's event follows its parents')."
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
  "Run `step` against each input in `coll`. Returns a map like `run!`
   plus `:outputs` — a vector aligned with `coll`; each element is the
   vector of data values whose message ancestry traces back to that
   input. An output that traces to multiple inputs (cross-input merge)
   appears under every matching index.

   An internal collector is appended at the step's `:out` boundary, so
   the caller's step should NOT already include its own sink."
  ([step coll] (run-seq step coll {}))
  ([step coll opts]
   (if (empty? coll)
     {:state :completed :outputs [] :events [] :counters {:sent 0 :recv 0 :completed 0}}
     (let [collected (atom [])
           wf        (serial step (collector-step ::collector collected))
           handle    (start! wf (select-keys opts [:pubsub :flow-id :subscribers]))]
       (doseq [d coll] (inject! handle {:data d}))
       (let [signal    (await-quiescent! handle)
             result    (-> (stop! handle)
                           (assoc :state (if (= :quiescent signal) :completed :failed))
                           (cond-> (vector? signal) (assoc :error (second signal))))
             events    (:events result)
             seed-ids  (mapv :msg-id (filter #(= :seed (:kind %)) events))
             seed->idx (zipmap seed-ids (range))
             seed-map  (seed-attribution events seed->idx)
             outputs   (reduce (fn [acc {:keys [msg-id data]}]
                                 (reduce (fn [a s] (update a s conj data))
                                         acc
                                         (seed-map msg-id)))
                               (vec (repeat (count coll) []))
                               @collected)]
         (assoc result :outputs outputs))))))
