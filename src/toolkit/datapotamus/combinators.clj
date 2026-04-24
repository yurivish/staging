(ns toolkit.datapotamus.combinators
  "Combinators, organised in two layers.

   High-level, common case:
     `parallel` — scatter-gather bracket (broadcast one input to a map
                  of named inner steps and collect their outputs).
                  Reach for this whenever the shape is \"one input → N
                  specialists → one aggregated output.\"

   Low-level, the algebra underneath:
     `fan-out` — split a message into one sibling per declared port,
                 minting a fresh zero-sum token group.
     `fan-in`  — wait for a fan-out's group to close (XOR-balance), then
                 emit a single merge keyed by arrival port.

   `fan-out`/`fan-in` are exposed as user primitives because they are
   the construction material for patterns `parallel` can't express —
   iterative rounds that keep a group open across intermediate steps,
   ad-hoc routing, custom coordination protocols (see the dribble /
   pair-merger exemplars in the tests). A fan-out's id doubles as its
   group name; `fan-in` references the fan-out by that id.

   `workers` is a different axis: K round-robin copies of one inner
   step for stream-level throughput parallelism — unrelated to the
   scatter-gather pair above."
  (:refer-clojure :exclude [merge])
  (:require [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.token :as tok]))

(defn fan-out
  "Low-level primitive. If your downstream is a simple scatter-gather
   with no work between split and merge that needs the group open,
   reach for `parallel` instead — it wraps `fan-out` + inner steps +
   `fan-in` in one call with no repeated port list.

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
  "Scatter-gather bracket: broadcast one input to each port, run the step
   under that port, collect outputs into a `{port data}` map on the far
   side. The whole bracket is a single wrapped step with one `:in` and
   one `:out`, self-scoped under `id`.

   `port->step` is a map whose keys become the parallel ports and whose
   values are the inner step run on each port.

   Keyword options (defaults shown):
     :select identity  — selector fn passed to fan-out's 3-arity form;
                         `data → [port] | {port payload}`. When provided,
                         the runtime subset / distinct-payload form is used.
     :post   identity  — reshape fn run on the `{port data}` map before
                         fan-in emits (e.g. `vals` to drop port keys).

   The user never writes the fan-out's ports, the fan-in's ports, or the
   N × 2 inter-connections — they're derived from `port->step`'s keys.

   Example:
     (c/parallel :roles {:solver  solver-step
                         :skeptic skeptic-step})
     (c/parallel :ens   {:w0 w0 :w1 w1 :w2 w2} :post vals)
     (c/parallel :plan  workers-map :select plan-fn :post vals)"
  [id port->step & {:keys [select post] :or {post identity}}]
  (let [ports    (vec (keys port->step))
        fo-id    id
        fi-id    (keyword (str (name id) "*"))
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
    (step/serial id bracket)))

(defn workers
  "K parallel copies of `inner` behind a round-robin router.

   Each worker runs in its own proc (core.async.flow gives each proc one
   thread), so per-message parallelism is real. The workers, router, and
   join live inside a single nested step named `id`, so every worker event
   carries `[:scope id]` in its scope — structural aggregation per logical
   stage, no name parsing. Per-worker distinction adds another `[:scope wN]`
   segment below that. Backpressure arrives via channel buffers — a slow
   worker blocks the router only on its own port.

   `inner` may be a handler-map or a step (any shape; internal sids are
   prefixed per-worker so state is isolated)."
  ([k inner]     (workers (gensym "workers-") k inner))
  ([id k inner]
   (assert (pos-int? k) "workers: k must be a positive integer")
   (let [ws        (mapv #(keyword (str "w" %)) (range k))
         join-ins  (mapv #(keyword (str "in" %)) (range k))
         route     (fn [ctx s msg-fn]
                     (let [i (mod (long (:i s 0)) k)]
                       [(assoc s :i (unchecked-inc (long i)))
                        {(ws i) [(msg-fn ctx)]}]))
         router    (step/handler-map
                    {:ports     {:ins {:in ""} :outs (zipmap ws (repeat ""))}
                     :on-data   (fn [ctx s _d] (route ctx s msg/pass))
                     :on-signal (fn [ctx s]    (route ctx s msg/signal))})
         join      (step/handler-map
                    {:ports   {:ins (zipmap join-ins (repeat "")) :outs {:out ""}}
                     :on-data (fn [ctx _s _d] {:out [(msg/pass ctx)]})})
         procs     (clojure.core/merge
                    {:router router :join join}
                    (zipmap ws (repeat inner)))
         conns     (vec (mapcat (fn [i w]
                                  [[[:router w] [w :in]]
                                   [[w :out]    [:join (join-ins i)]]])
                                (range) ws))
         pool      {:procs procs :conns conns :in :router :out :join}]
     {:procs {id pool} :conns [] :in id :out id})))
