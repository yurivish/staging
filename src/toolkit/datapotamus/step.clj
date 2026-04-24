(ns toolkit.datapotamus.step
  "Steps + composition. Pure, no runtime.

   A **step** is a wiring container:

     {:procs {sid handler-map-or-step}   ; nested steps compose recursively
      :conns [[[from-sid port] [to-sid port]] ...]
      :in    sid-or-[sid port]
      :out   sid-or-[sid port]}

   Most users build steps with `step/step` (1-proc) and compose them with
   `serial`, `merge-steps`, `connect`, `input-at`, `output-at`, and
   `as-step`. The handler function you pass to `step/step` has signature
   `(fn [ctx state data] → return)` and returns one of:

     • {port [msg-or-data ...]}                 ; no state change
     • [state' {port [msg-or-data ...]}]        ; state updated
     • msg/drain | [state' msg/drain]           ; drop tokens too

   Msgs come from `toolkit.datapotamus.msg`. Bare data values auto-wrap as
   `child` msgs of the input message during synthesis.

   Two non-obvious rules — see `step` for details:
     * An empty port-map auto-propagates a signal on every output port
       (so the input's tokens flow onward); return `msg/drain` to
       suppress that.
     * Every child ever derived from a given parent input must be
       emitted from a single handler invocation.

   Under the hood, each terminal entry in `:procs` is a **handler-map** —
   a plain map keyed by message-kind/lifecycle slot:

     {:on-data       (fn [ctx state data] → return)
      :on-signal     (fn [ctx state]      → return)   ; default: broadcast
      :on-all-closed (fn [ctx state]      → return)   ; default: emit nothing
      :on-init       (fn []               → initial-state)   ; default: {}
      :on-stop       (fn [ctx state]      → any)             ; default: nil
      :ports         {:ins {...} :outs {...}}}

   `step/step` fills in defaults for everything except `:on-data` and
   builds a 1-proc step around it. Construct a handler-map by hand only
   when you need custom signal / all-closed behavior, or want
   resource-lifecycle hooks (`:on-init` / `:on-stop`) for long-running
   pipelines — see `handler-map`.

   Nothing in this namespace touches pubsub or core.async."
  (:require [toolkit.datapotamus.msg :as msg]))

;; ============================================================================
;; Handler-map defaults
;; ============================================================================
;;
;; The interpreter uses these when a handler-map doesn't supply its own.
;; Signal / all-closed defaults read the current port spec from ctx, which
;; the interpreter injects before every call.

(defn- default-on-signal
  "Broadcast one signal msg to each output port."
  [ctx _state]
  (into {} (map (fn [p] [p [(msg/signal ctx)]])) (keys (:outs ctx))))

(defn- default-on-all-closed
  "Nothing to drain. Interpreter additionally cascades (new-done) on all outs."
  [_ctx _state]
  {})

(defn- default-on-init
  "Fresh state — empty map."
  []
  {})

(defn- default-on-stop
  "No resources to release."
  [_ctx _state]
  nil)

(defn- ensure-defaults [m]
  (cond-> m
    (nil? (:on-signal m))     (assoc :on-signal default-on-signal)
    (nil? (:on-all-closed m)) (assoc :on-all-closed default-on-all-closed)
    (nil? (:on-init m))       (assoc :on-init default-on-init)
    (nil? (:on-stop m))       (assoc :on-stop default-on-stop)))

;; ============================================================================
;; Predicates
;; ============================================================================

(defn handler-map? [v] (and (map? v) (contains? v :on-data) (contains? v :ports)))
(defn step?        [v] (and (map? v) (contains? v :procs)   (contains? v :conns)))

;; ============================================================================
;; Handler-map + step constructors
;; ============================================================================

(defn handler-map
  "Build a handler-map from a partial spec, filling in defaults.

   Tier-3 escape hatch: use this when you need a custom :on-signal,
   :on-all-closed, :on-init, or :on-stop. For ordinary handlers prefer
   `step`, which wraps a handler-map in a 1-proc step for you."
  [m]
  (ensure-defaults
   (cond-> m
     (nil? (:ports m)) (assoc :ports {:ins {:in ""} :outs {:out ""}}))))

(defn- mk-step [id proc-value]
  {:procs {id proc-value} :conns [] :in id :out id})

(defn step
  "Build a 1-proc step.

   2-arity — pure-fn form:
     (step id f)      ; f :: data → data; ports default to :in / :out

   3-arity — handler form:
     (step id ports handler)
       ports   = {:ins {port-kw doc} :outs {port-kw doc}}  ; nil ⇒ defaults
       handler = (fn [ctx state data] → return)
       return  = {port [msg-or-data ...]}        ; no state change
               | [state' {port [msg-or-data ...]}] ; state updated
               | msg/drain | [state' msg/drain]    ; see below

   Empty-port-map semantics. An empty return like `{}` or `[state' {}]`
   does NOT drop the input's tokens — the interpreter synthesizes a
   signal on every declared output port so downstream closure still
   works. Return `msg/drain` (or `[state' msg/drain]`) to suppress that
   and truly drop the tokens — the usual reason is that you've stashed
   the input's msg in state and will emit a `msg/merge` from it later.

   Single-invocation invariant for derived msgs. Every child message
   ever derived from a given parent input must be emitted from a single
   handler invocation. Synthesis K-way-splits a parent's tokens across
   the descendants present in this invocation's output; if you stash a
   parent and derive more children from it in a later call, the two
   splits will conflict and conservation will silently break. If you
   need deferred derivation, stash the parent ref, return `msg/drain`,
   and emit a single `msg/merge` that lists all the eventual parents
   in one later invocation (the pair-merger pattern)."
  ([id f]
   (step id nil (fn [_ctx _s d] {:out [(f d)]})))
  ([id ports handler]
   (mk-step id (handler-map (cond-> {:on-data handler}
                              ports (assoc :ports ports))))))

(defn sink
  "Terminal step — consumes input, emits nothing."
  ([] (sink :sink))
  ([id]
   (mk-step id (handler-map {:on-data (fn [_ctx _s _d] {})
                             :ports   {:ins {:in ""} :outs {}}}))))

(defn passthrough
  "Forward data unchanged, preserving :data-id through lineage."
  [id]
  (mk-step id (handler-map {:on-data (fn [ctx _s _d] {:out [(msg/pass ctx)]})})))

(defn as-step
  "Wrap any step into a single-entry step under `id`. Use to give a subflow
   a stable outer name (scope prefixing derives from it)."
  [id inner]
  (mk-step id inner))

;; ============================================================================
;; Composition
;; ============================================================================

(defn- endpoint [ref default-port]
  (if (vector? ref) ref [ref default-port]))

(defn- assert-no-collision! [a b]
  (when-let [shared (seq (filter (set (keys (:procs a))) (keys (:procs b))))]
    (throw (ex-info (str "Step composition: proc-id collision " (vec shared))
                    {:colliding-ids (vec shared)}))))

(defn merge-steps
  "Union procs + conns. Composable via explicit `connect`."
  [& steps]
  (reduce (fn [acc s]
            (assert-no-collision! acc s)
            (-> acc
                (update :procs #(clojure.core/merge % (:procs s)))
                (update :conns into (:conns s))))
          {:procs {} :conns []}
          steps))

(defn connect
  "Add one [from → to] conn to a step."
  [s from to]
  (update s :conns conj
          [(endpoint from :out) (endpoint to :in)]))

(defn serial
  "Chain steps sequentially, auto-wiring each :out to next :in."
  [& steps]
  (let [merged (apply merge-steps steps)
        glues  (partition 2 1 steps)
        wired  (reduce (fn [acc [a b]]
                         (connect acc
                                  (endpoint (:out a) :out)
                                  (endpoint (:in  b) :in)))
                       merged glues)]
    (cond-> wired
      (seq steps) (assoc :in  (:in  (first steps))
                         :out (:out (last  steps))))))

(defn input-at  [s ref] (assoc s :in ref))
(defn output-at [s ref] (assoc s :out ref))
