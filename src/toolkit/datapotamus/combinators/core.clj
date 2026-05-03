(ns toolkit.datapotamus.combinators.core
  "The scatter-gather algebra. Three primitives that everything else
   builds on:

     `fan-out` — split a message into one sibling per declared port,
                 minting a fresh zero-sum token group.
     `fan-in`  — wait for a fan-out's group to close (XOR-balance), then
                 emit a single merge keyed by arrival port.
     `parallel`— scatter-gather bracket. Reach for this whenever the
                 shape is \"one input → N specialists → one aggregated
                 output\" — it wraps fan-out, the inner steps, fan-in,
                 and all the wiring between them in one call.

   `fan-out`/`fan-in` are exposed as user primitives because they are
   the construction material for patterns that don't fit a port-keyed
   single-input bracket — groups that span multiple top-level inputs,
   closure protocols that aren't \"every sibling emits once\", custom
   token groups designed via `assoc-tokens`/`dissoc-tokens` (see the
   dribble / pair-merger exemplars in the tests). A fan-out's id
   doubles as its group name; `fan-in` references the fan-out by that
   id."
  (:require [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.token :as tok]))

(defn fan-out
  "Low-level primitive. For the common scatter-gather case — one input
   to a set of per-port steps (each of which may itself be a composed
   pipeline) and gather the results into a `{port data}` map — reach
   for `parallel` instead; it wraps `fan-out` + inner steps + `fan-in`
   in one call with no repeated port list.

   Emit one child per declared port, each tagged with a slice of a fresh
   zero-sum group keyed on `[id parent-msg-id]`. Downstream `fan-in`
   referencing `id` closes the group when all siblings arrive.

   2-arity — static: broadcast the input payload identically to every
   port in `ports`.
     (fan-out :dispatch [:solver :facts :skeptic :second])

   3-arity — dynamic: `selector-fn` is called on each input's data and
   returns either
     • a vector/seq of port keywords — broadcast input payload to those
       ports (a subset of `ports`), or
     • a map {port-kw payload} — distinct payloads per port.
   The selector may only pick among `ports`, which is declared at
   graph-construction time; core.async.flow requires fixed output ports."
  ([id ports]
   (fan-out id ports (fn [d] (zipmap ports (repeat d)))))
  ([id ports selector-fn]
   (step/step id
              {:ins {:in ""} :outs (zipmap ports (repeat ""))}
              (fn [ctx _s d]
                (let [gid     [id (:msg-id (:msg ctx))]
                      sel     (selector-fn d)
                      by-port (if (map? sel) sel (zipmap sel (repeat d)))
                      n       (count by-port)
                      values  (tok/split-value 0 n)]
                  (into {}
                        (map (fn [[port payload] v]
                               [port [(-> (msg/child ctx payload)
                                          (msg/assoc-tokens {gid v}))]])
                             by-port values)))))))

(defn fan-in
  "Low-level primitive, paired with `fan-out`. The common scatter-gather
   case — fan-out + inner steps + fan-in in one shot — is `parallel`.

   Accumulate inputs whose tokens carry a group minted by the fan-out
   named `fan-out-id`. When a group's XOR sum reaches 0, emit one
   `msg/merge` whose parents are all the collected messages and whose
   data is a `{port data}` map — keyed by the input port each sibling
   arrived on. Declare one input port per corresponding fan-out output
   port so arrival port-of-origin is structurally preserved (mirrors
   Go's FanIn). If a port receives multiple siblings the value under
   that port key becomes a vector; single-arrival ports stay scalar.

   Optional `post-fn` runs on the `{port data}` map before emission —
   use `vals` to drop port keys, or any custom combine. Defaults to
   `identity`.

   State is keyed on the per-invocation gid `[fan-out-id parent-msg-id]`
   so multiple groups can be in flight simultaneously. While
   accumulating (no group closed this invocation) returns `msg/drain`
   to suppress the default auto-signal; the stashed parent refs carry
   tokens forward via the eventual merge."
  ([id fan-out-id ports]         (fan-in id fan-out-id ports identity))
  ([id fan-out-id ports post-fn]
   (step/step id
              {:ins (zipmap ports (repeat "")) :outs {:out ""}}
              (fn [ctx s _d]
                (let [m    (:msg ctx)
                      port (:in-port ctx)
                      gids (filterv (fn [k] (and (vector? k) (= fan-out-id (first k))))
                                    (keys (:tokens m)))
                      [s' output]
                      (reduce
                       (fn [[s' output] gid]
                         (let [v    (long (get (:tokens m) gid))
                               grp  (get-in s' [:groups gid] {:value 0 :entries []})
                               grp' (-> grp
                                        (update :value (fn [x] (bit-xor (long x) v)))
                                        (update :entries conj [port m]))]
                           (if (zero? (long (:value grp')))
                             (let [entries (:entries grp')
                                   parents (mapv second entries)
                                   by-port (reduce
                                            (fn [acc [p mm]]
                                              (update acc p
                                                      (fn [x]
                                                        (cond
                                                          (nil? x)    (:data mm)
                                                          (vector? x) (conj x (:data mm))
                                                          :else       [x (:data mm)]))))
                                            {} entries)
                                   merged  (-> (msg/merge ctx parents (post-fn by-port))
                                               (msg/dissoc-tokens [gid]))]
                               [(update s' :groups dissoc gid)
                                (update output :out (fnil conj []) merged)])
                             [(assoc-in s' [:groups gid] grp') output])))
                       [s {}]
                       gids)]
                  (if (seq output) [s' output] [s' msg/drain]))))))

(defn parallel
  "Run a set of steps simultaneously on the same input and collect their
   outputs into a single map keyed by port. One message in, one message
   out — the fan-out, fan-in, and all the wiring between them are
   hidden inside.

   `port->step` is a map whose keys become the parallel port names and
   whose values are the step that runs under each port.

   What happens, step by step, when a message hits the bracket:
     1. The input is copied once per port.
     2. Each copy is routed to its port's step.
     3. The bracket waits for every port to finish.
     4. Their outputs are collected into a `{port data}` map.
     5. That map is emitted as one output message.

   Options (defaults in parentheses):
     :select  (every port runs with an unchanged copy of the input)
       A fn `data → [port] | {port payload}`. Picks which ports run
       this round, and optionally gives each port its own payload.
       Ports not returned stay idle for this message.
     :post    (identity — the `{port data}` map is emitted as-is)
       A fn that reshapes the collected outputs before emission.
       `vals` is the usual choice when downstream only wants the
       outputs and not the port-of-origin labels.

   The whole bracket is packaged as a single nested step under `id`.
   That means (a) the inner port steps can have any ids they like with
   no risk of colliding with ids outside the bracket, and (b) trace
   events from inside carry a `[:scope id]` segment, so a pubsub
   subscriber can filter on \"everything that happened in this bracket\"
   and nothing else.

   Examples

     ;; Ask a solver and a skeptic the same question; emit a single
     ;; map keyed by role so downstream can tell them apart.
     ;;   =>  {:solver \"...solver's answer...\"
     ;;        :skeptic \"...skeptic's answer...\"}
     (cc/parallel :roles {:solver  solver-step
                          :skeptic skeptic-step})

     ;; Run three workers on the same question (say, each worker
     ;; closes over a different sampling temperature) and emit the
     ;; answers as a flat list. :post vals drops the :w0/:w1/:w2 port
     ;; keys because the judge downstream only cares what the
     ;; candidates look like, not which worker produced which.
     ;;   =>  (cand-w0 cand-w1 cand-w2)
     (cc/parallel :ensemble {:w0 w0 :w1 w1 :w2 w2} :post vals)

     ;; A planner decides at runtime which workers to dispatch to and
     ;; what each one gets. plan-fn is called on the input and must
     ;; return a {port payload} map; ports not listed stay idle this
     ;; round. Output is a flat list (port keys dropped).
     ;;   =>  (answer-for-subtask-0 answer-for-subtask-1 ...)
     (cc/parallel :plan worker-pool-map :select plan-fn :post vals)"
  [id port->step & {:keys [select post] :or {post identity}}]
  (let [ports    (vec (keys port->step))
        fo-id    (keyword (str (name id) "-fan-out"))
        fi-id    (keyword (str (name id) "-fan-in"))
        fo       (if select
                   (fan-out fo-id ports select)
                   (fan-out fo-id ports))
        fi       (fan-in fi-id fo-id ports post)
        base     (apply step/beside fo fi (vals port->step))
        wired    (reduce-kv
                  (fn [wf port inner]
                    (-> wf
                        (step/connect [fo-id port] (:in inner))
                        (step/connect (:out inner) [fi-id port])))
                  base port->step)
        bracket  (-> wired
                     (step/input-at fo-id)
                     (step/output-at fi-id))]
    (-> (step/serial id bracket)
        (update-in [:procs id] assoc :combinator :parallel))))
