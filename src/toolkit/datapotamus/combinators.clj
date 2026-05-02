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
   the construction material for patterns that don't fit a port-keyed
   single-input bracket — groups that span multiple top-level inputs,
   closure protocols that aren't \"every sibling emits once\", custom
   token groups designed via `assoc-tokens`/`dissoc-tokens` (see the
   dribble / pair-merger exemplars in the tests). Per-port multi-step
   work and nested/serial `parallel`s are already `parallel`'s job —
   hand it a composed step as a port's value. A fan-out's id doubles
   as its group name; `fan-in` references the fan-out by that id.

   `round-robin-workers` and `stealing-workers` are a different axis:
   K parallel copies of one inner step for stream-level throughput
   parallelism — unrelated to the scatter-gather pair above.
   `round-robin-workers` partitions work statically (round-robin);
   `stealing-workers` lets workers race for a coordinator-owned queue
   (work-stealing) and optionally supports recursive `:work` feedback
   when the inner is a handler-map declaring a `:work` output port."
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as-alias flow]
            [toolkit.datapotamus.msg :as msg]
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
     (c/parallel :roles {:solver  solver-step
                         :skeptic skeptic-step})

     ;; Run three workers on the same question (say, each worker
     ;; closes over a different sampling temperature) and emit the
     ;; answers as a flat list. :post vals drops the :w0/:w1/:w2 port
     ;; keys because the judge downstream only cares what the
     ;; candidates look like, not which worker produced which.
     ;;   =>  (cand-w0 cand-w1 cand-w2)
     (c/parallel :ensemble {:w0 w0 :w1 w1 :w2 w2} :post vals)

     ;; A planner decides at runtime which workers to dispatch to and
     ;; what each one gets. plan-fn is called on the input and must
     ;; return a {port payload} map; ports not listed stay idle this
     ;; round. Output is a flat list (port keys dropped).
     ;;   =>  (answer-for-subtask-0 answer-for-subtask-1 ...)
     (c/parallel :plan worker-pool-map :select plan-fn :post vals)"
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

(defn batch-by-group
  "Aggregator step. Buffers every input row until input is exhausted,
   then groups by `key-fn` and emits one summary per group as a
   `msg/merge` over all parent msgs in that group.

   Returns `msg/drain` from :on-data to suppress the framework's
   auto-signal on :out — required because we stash the parent ref
   and derive a single `msg/merge` child later in
   `:on-all-input-done`. Without drain, the auto-signal would
   double-count parent tokens (see the deferred-derivation pattern
   in `msg.clj`).

   - `key-fn`         : (row → group-key)
   - `summarize-rows` : (group-key, rows → summary-data) — called
                        once per group on input-done."
  [key-fn summarize-rows]
  {:procs
   {:agg
    (step/handler-map
     {:ports   {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:rows []})
      :on-data
      (fn [ctx s row]
        [(update s :rows conj {:msg (:msg ctx) :row row}) msg/drain])
      :on-all-input-done
      (fn [ctx s]
        (let [grouped (group-by (comp key-fn :row) (:rows s))
              out-msgs (mapv (fn [[k entries]]
                               (let [parents (mapv :msg entries)
                                     rows    (mapv :row entries)]
                                 (msg/merge ctx parents
                                            (summarize-rows k rows))))
                             grouped)]
          {:out out-msgs}))})}
   :conns [] :in :agg :out :agg})

(defn cumulative-by-group
  "Aggregator step. On each input, accumulates the row in its
   per-group state and emits a cumulative summary for the row's
   group as a `msg/merge` over all parent msgs seen for that group
   so far.

   Use this when upstream eager-propagates input-done while
   in-flight work is still arriving — i.e. downstream of a
   `stealing-workers` whose inner declares `:work` (recursive
   feedback mode). The last emission per group is the final
   summary, and quiescence (counter balance, surfaced by
   `flow/await-quiescent!`) is the signal that no more emissions
   are coming. Tests should take the last per group, e.g.
   `(->> outputs (group-by k) vals (mapv last))`.

   For the common case (downstream of plain
   `round-robin-workers` / `stealing-workers` chains, where
   input-done is a reliable barrier), prefer `batch-by-group`:
   it calls `summarize-rows` once per group instead of once per
   input row.

   - `key-fn`         : (row → group-key) — how to group rows.
   - `summarize-rows` : (group-key, rows-so-far → summary-data) — the
                        data emitted via `msg/merge` of all parent
                        msgs seen for that group."
  [key-fn summarize-rows]
  {:procs
   {:agg
    (step/handler-map
     {:ports   {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:groups {}})
      :on-data
      (fn [ctx s row]
        (let [k       (key-fn row)
              s'      (update-in s [:groups k] (fnil conj [])
                                 {:msg (:msg ctx) :row row})
              entries (get-in s' [:groups k])
              parents (mapv :msg entries)
              rows    (mapv :row entries)]
          [s' {:out [(msg/merge ctx parents (summarize-rows k rows))]}]))})}
   :conns [] :in :agg :out :agg})

(defn round-robin-workers
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
  ([k inner]     (round-robin-workers (gensym "round-robin-workers-") k inner))
  ([id k inner]
   (assert (pos-int? k) "round-robin-workers: k must be a positive integer")
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

(defn- wrap-worker-multiplexed
  "Wrap an inner handler-map so its :on-data return is rewritten to a
   single output port `:to-coord`, with messages tagged `::class
   :work` (recursive) or `::class :result` (forward + free worker)
   and a `::worker-id` pointer. Order is preserved: work msgs come
   first, then the result msg — within one invocation, all work
   emissions land at the coordinator before the terminator, so coord
   can mark the worker free without racing in-flight :work.

   Terminator invariant: every invocation must produce a terminator
   so the coord frees the worker. The wrapper guarantees this by:
     1. If `:out` is non-empty: each :result IS a terminator.
     2. Else if `:work` is non-empty: flag the LAST work msg with
        `::terminates? true` so coord frees on its arrival. Avoids
        minting a synthetic msg that would split the parent's tokens.
     3. Else (empty / drop case): synthesize a `:result`-tagged
        signal so the coord frees the worker AND a signal flows
        downstream to keep XOR-balance for the parent's tokens (the
        same role the framework's auto-signal would play, but routed
        through the worker-pool's tag protocol)."
  [inner worker-id]
  (let [user-on-data   (:on-data inner)
        user-on-signal (or (:on-signal inner) (fn [_ s] [s {}]))
        tag (fn [m cls]
              (assoc m ::class cls ::worker-id worker-id))]
    (-> inner
        (assoc-in [:ports :ins] {:in ""})
        (assoc-in [:ports :outs] {:to-coord ""})
        (assoc :on-data
               (fn [ctx s d]
                 (let [ret (user-on-data ctx s d)
                       [s' pm] (if (vector? ret) ret [s ret])
                       drain?  (identical? pm msg/drain)]
                   (if drain?
                     [s' msg/drain]
                     (let [work-msgs (mapv #(tag % :work)   (get pm :work []))
                           out-msgs  (mapv #(tag % :result) (get pm :out []))
                           work-msgs (if (and (seq work-msgs)
                                              (empty? out-msgs))
                                       (update work-msgs
                                               (dec (count work-msgs))
                                               assoc ::terminates? true)
                                       work-msgs)
                           ;; Empty case (inner returned {} or equivalent):
                           ;; emit a tagged :ack signal so the coord's
                           ;; :on-signal can free the worker AND forward
                           ;; a plain signal downstream for token
                           ;; balance. Without this, the framework's
                           ;; auto-signal would carry the tokens forward
                           ;; but never free the worker → deadlock.
                           ack       (when (and (empty? work-msgs)
                                                (empty? out-msgs))
                                       [(tag (msg/signal ctx) :ack)])
                           combined  (vec (concat work-msgs out-msgs ack))]
                       [s' {:to-coord combined}])))))
        (assoc :on-signal
               (fn [ctx s]
                 (let [ret (user-on-signal ctx s)
                       [s' pm] (if (vector? ret) ret [s ret])
                       drain?  (identical? pm msg/drain)]
                   (if drain?
                     [s' msg/drain]
                     ;; Forward any signal as a tagged-:result so coord
                     ;; can pass it through to :out.
                     [s' {:to-coord [(tag (msg/signal ctx) :result)]}])))))))

(defn- mk-exit-wrap
  "Per-worker exit-wrapper proc used when the inner is a step (not a
   handler-map). Tags every msg arriving on its :in as `::class :result`
   with `::worker-id wk` and forwards on :to-coord so the coordinator
   can mark the worker free. Step inners only support the result path
   (no `:work` recursion); for that, use a handler-map inner which
   bypasses this wrapper and tags emissions atomically inside its
   own :on-data — the single-port trick that preserves within-
   invocation FIFO between :work and :result emissions."
  [wk]
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:to-coord ""}}
    :on-data   (fn [ctx _ _]
                 {:to-coord [(-> (msg/pass ctx)
                                 (assoc ::class :result ::worker-id wk))]})
    :on-signal (fn [ctx _]
                 {:to-coord [(-> (msg/signal ctx)
                                 (assoc ::class :result ::worker-id wk))]})}))

(defn stealing-workers
  "K parallel copies of `inner` racing for items from a shared queue,
   with optional recursive-feedback. Wall-clock approaches `total-work
   / k` rather than `max-item × ceil(n/k)` because slow items don't
   gate fast ones — whichever worker finishes its previous item pulls
   the next.

   Best fit: I/O-bound stages with embarrassingly-parallel,
   homogeneous items where per-item cost varies (LLM calls, HTTP
   fetches, batch jobs).

   Use `round-robin-workers` instead when:
     - Workers hold per-worker state (warmed contexts, primed
       caches, sticky connections) — work-stealing throws away
       locality.
     - Each worker has its own rate-limit bucket (one API key per
       worker) — a free worker can take consecutive items and blow
       through one bucket while the others sit idle.
     - Items must route to a specific worker (consistent hashing,
       sticky-by-key, ordered-by-key).
     - Items are sub-millisecond (queue contention dominates).

   Order is NOT preserved. Tag items upstream and re-sort downstream
   if you need order.

   `inner` may be a handler-map or a step.

   - Handler-map inner: directly multiplexed onto a single
     :to-coord port via the worker wrapper. May declare a `:work`
     output port; items emitted on `:work` are routed back to the
     coordinator's queue, indistinguishable from external work
     (recursive-feedback mode).

   - Step inner: kept intact and connected through a per-worker
     exit-wrapper proc that tags `:out` emissions for the
     coordinator. Step inners do NOT support `:work` recursion in
     this implementation (the within-invocation FIFO that makes
     recursion safe relies on a single tagged output port; with a
     step inner, :work and :out are separate channels with
     unspecified ordering). If you need recursion, refactor the
     inner to a handler-map.

   Implementation: 1 coordinator proc + K shims + K worker inners
   (+ K exit-wrappers, for step inners). The coordinator owns the
   queue as state and dispatches to free workers, eliminating the
   shared-channel race that affected the prior shared-queue design.
   Each worker emission arrives at the coordinator's :rec port
   tagged `::class :work` (queue + dispatch) or `::class :result`
   (mark worker free + forward on :out).

   Worker-free invariants (every invocation must produce exactly one
   terminator so the coord frees the worker):
     - :out non-empty → each :result is a terminator (existing path).
     - :work-only      → wrapper flags the LAST work msg with
                         `::terminates? true`; coord frees on it.
     - empty / drop    → wrapper emits a tagged :ack signal; coord's
                         :on-signal frees the worker AND forwards a
                         plain signal downstream for token balance.
   Without these, work-only or empty invocations would deadlock the
   pool — the worker would stay in `:busy` forever and the K-th
   consecutive non-:result invocation would stall dispatch.

   Closure semantics: when `:in` reports input-done, the coordinator
   sets an `:ext-done?` flag and waits — once the queue is empty
   AND no workers are busy (and, in recursive mode, no `:work` is in
   flight), it writes a fresh input-done envelope to each
   per-worker dispatch channel (which it owns via
   `::flow/out-ports`) using `a/put!`. Workers cascade input-done
   back through their outputs (the framework's auto-append on
   `:on-all-input-done`), eventually closing all of the coordinator's
   input ports and triggering the coordinator's own auto-append on
   `:out`. So input-done at the downstream `:out` port is a
   reliable barrier — by the time it arrives, every dispatched item
   has been processed and every recursive `:work` emission has been
   absorbed into a result. Downstream aggregators can therefore use
   `batch-by-group` even in recursive-feedback mode.

   Trace topology: coordinator proc `:coord`, ext-wrap proc `:ext`,
   shim procs `:s0`..`:sK-1`, worker procs `:w0`..`:wK-1` (and, for
   step inners, exit-wrapper procs `:e0`..`:eK-1`)."
  ([k inner] (stealing-workers (gensym "stealing-workers-") k inner))
  ([id k inner]
   (assert (pos-int? k) "stealing-workers: k must be a positive integer")
   (assert (or (step/handler-map? inner) (step/step? inner))
           "stealing-workers: inner must be a handler-map or step")
   (let [step-inner? (step/step? inner)
         to-w-ports  (mapv #(keyword (str "to-w" %)) (range k))
         shim-ids    (mapv #(keyword (str "s" %)) (range k))
         worker-ids  (mapv #(keyword (str "w" %)) (range k))
         exit-ids    (when step-inner?
                       (mapv #(keyword (str "e" %)) (range k)))
         to-w-chans  (vec (repeatedly k #(a/chan 1)))

         ;; Try-dispatch: pop items from the queue to free workers until
         ;; one of them runs out. Returns [s' port-map] where port-map
         ;; has dispatches keyed by :to-wK ports.
         try-dispatch
         (fn try-dispatch [s]
           (loop [s s, dispatch-pm {}]
             (let [free-idxs (remove (fn [i] (contains? (:busy s) (worker-ids i)))
                                     (range k))
                   queue     (:queue s)]
               (if (or (empty? queue) (empty? free-idxs))
                 [s dispatch-pm]
                 (let [item       (peek queue)
                       wk-idx     (first free-idxs)
                       to-port    (to-w-ports wk-idx)
                       wk         (worker-ids wk-idx)
                       dispatched (msg/pass-of item)]
                   (recur (-> s
                              (update :queue pop)
                              (update :busy conj wk))
                          (update dispatch-pm to-port (fnil conj []) dispatched)))))))

         ;; When ext is exhausted AND nothing is pending, emit a fresh
         ;; input-done envelope on each :to-wK port. The framework
         ;; passes pre-built :input-done envelopes through synthesis
         ;; unchanged (see msg/coerce-data) and writes them to the
         ;; per-worker dispatch channels via the ::flow/out-ports
         ;; override. Workers receive input-done on their :queue
         ;; port (through the shim's ::flow/in-ports merge) and run
         ;; the framework's normal input-done lifecycle — auto-append
         ;; cascades input-done downstream through :to-coord →
         ;; coord :rec. Once :rec reports input-done, coord's default
         ;; :on-all-input-done auto-appends input-done on :out.
         shutdown-pm
         (fn [s]
           (if (and (:ext-done? s)
                    (empty? (:queue s))
                    (empty? (:busy s))
                    (not (:shutdown-sent? s)))
             [(assoc s :shutdown-sent? true)
              (zipmap to-w-ports
                      (repeat [(msg/new-input-done)]))]
             [s {}]))

         coord
         (step/handler-map
          {:ports {:ins  {:in "" :rec ""}
                   :outs (clojure.core/merge {:out ""}
                                             (zipmap to-w-ports (repeat "")))}
           :on-init       (fn []
                            {::flow/out-ports (zipmap to-w-ports to-w-chans)
                             :queue           clojure.lang.PersistentQueue/EMPTY
                             :busy            #{}
                             :ext-done?       false})
           :on-data
           (fn [ctx s _d]
             (let [in-port (:in-port ctx)
                   msg     (:msg ctx)]
               (cond
                 ;; External work — always queue. (No tagging needed;
                 ;; ext-wrap forwards external msgs verbatim.)
                 (= in-port :in)
                 (let [s'             (update s :queue conj msg)
                       [s'' dispatch] (try-dispatch s')]
                   [s'' dispatch])

                 ;; Worker emission on :rec — dispatch on ::class.
                 ;; Worker wrapper guarantees within-invocation FIFO:
                 ;; all :work msgs arrive before the invocation's
                 ;; terminator (a :result OR the LAST :work flagged
                 ;; ::terminates? for work-only invocations), so
                 ;; freeing the worker on either is safe.
                 (= (::class msg) :work)
                 (let [s'             (cond-> (update s :queue conj msg)
                                        (::terminates? msg)
                                        (update :busy disj (::worker-id msg)))
                       [s'' dispatch]  (try-dispatch s')
                       [s''' shutdown] (if (::terminates? msg)
                                         (shutdown-pm s'')
                                         [s'' {}])]
                   [s''' (clojure.core/merge dispatch shutdown)])

                 (= (::class msg) :result)
                 (let [wk     (::worker-id msg)
                       s'     (update s :busy disj wk)
                       forward {:out [(msg/pass ctx)]}
                       [s'' dispatch]    (try-dispatch s')
                       [s''' shutdown]   (shutdown-pm s'')]
                   [s''' (clojure.core/merge forward dispatch shutdown)])

                 :else
                 ;; Untagged msg on :rec — shouldn't happen with the
                 ;; current wrapper, but forward defensively.
                 [s {:out [(msg/pass ctx)]}])))
           :on-signal
           (fn [ctx s]
             (let [msg (:msg ctx)]
               (if (= (::class msg) :ack)
                 ;; Worker-pool internal: a worker emitted nothing, so the
                 ;; wrapper sent us a tagged :ack signal. Free the worker;
                 ;; forward a plain signal for token-balance.
                 (let [wk     (::worker-id msg)
                       s'     (update s :busy disj wk)
                       [s'' dispatch]  (try-dispatch s')
                       [s''' shutdown] (shutdown-pm s'')]
                   [s''' (clojure.core/merge {:out [(msg/signal ctx)]}
                                             dispatch shutdown)])
                 [s {:out [(msg/signal ctx)]}])))
           :on-input-done
           (fn [_ s port]
             (case port
               :in  (let [s'             (assoc s :ext-done? true)
                          [s'' shutdown] (shutdown-pm s')]
                      [s'' shutdown])
               :rec [s {}]))})

         ;; ext-wrap: passes external work through to coord :in
         ;; verbatim (no tagging needed since :in identifies it).
         ext-wrap (step/handler-map
                   {:ports     {:ins {:in ""} :outs {:out ""}}
                    :on-data   (fn [ctx _ _] {:out [(msg/pass ctx)]})
                    :on-signal (fn [ctx _]   {:out [(msg/signal ctx)]})})

         shims
         (mapv (fn [i]
                 (let [c (to-w-chans i)]
                   (step/handler-map
                    {:ports     {:ins {:queue ""} :outs {:to-inner ""}}
                     :on-init   (fn [] {::flow/in-ports {:queue c}})
                     :on-data   (fn [ctx _ _]   {:to-inner [(msg/pass ctx)]})
                     :on-signal (fn [ctx _]     {:to-inner [(msg/signal ctx)]})})))
               (range k))

         ;; Drop sink for the stub conns from coord's :to-wK ports.
         ;; Coord's actual writes go to to-w-chans via ::flow/out-ports
         ;; override; the framework-allocated chans behind these conns
         ;; never receive writes.
         drop-sink (step/handler-map
                    {:ports     {:ins {:in ""} :outs {}}
                     :on-data   (fn [_ _ _] {})
                     :on-signal (fn [_ _]   {})})

         worker-procs (if step-inner?
                        (vec (repeat k inner))
                        (mapv (fn [wk] (wrap-worker-multiplexed inner wk))
                              worker-ids))

         exit-procs (when step-inner?
                      (mapv mk-exit-wrap worker-ids))

         procs (cond-> {:coord coord :ext ext-wrap :drop drop-sink}
                 true        (into (map vector shim-ids shims))
                 true        (into (map vector worker-ids worker-procs))
                 step-inner? (into (map vector exit-ids exit-procs)))

         conns (vec (concat
                     ;; ext-wrap → coord :in (external work entry)
                     [[[:ext :out] [:coord :in]]]
                     ;; Stub conns: coord :to-wK → drop. Real writes go
                     ;; to to-w-chans via ::flow/out-ports override.
                     (mapv (fn [tp] [[:coord tp] [:drop :in]]) to-w-ports)
                     ;; Shim → worker (per-pair) — buffer 1 so a fast
                     ;; shim can't drain to-w-chan ahead of inner.
                     (mapcat (fn [s w] [[[s :to-inner] [w :in] {:buf-or-n 1}]])
                             shim-ids worker-ids)
                     ;; Worker → coord :rec routing differs by inner type.
                     ;; Handler-map workers: their wrapped :to-coord
                     ;; port carries tagged emissions; one channel per
                     ;; worker into coord :rec. Buffer 1024 holds bursts
                     ;; (1 terminator — :result or :ack — plus N work
                     ;; msgs) so the worker doesn't block on chan-write
                     ;; while coord is still busy.
                     ;; Step workers: their unmodified :out goes to a
                     ;; per-worker exit-wrapper, which then writes
                     ;; tagged :result msgs onto :to-coord → coord :rec.
                     (if step-inner?
                       (concat
                        (mapcat (fn [w e] [[[w :out] [e :in]]])
                                worker-ids exit-ids)
                        (mapv (fn [e] [[e :to-coord] [:coord :rec] {:buf-or-n 1024}])
                              exit-ids))
                       (mapv (fn [w] [[w :to-coord] [:coord :rec] {:buf-or-n 1024}])
                             worker-ids))))

         pool {:procs procs :conns conns :in :ext :out :coord}]
     {:procs {id pool} :conns [] :in id :out id})))

;; ============================================================================
;; Rate limiting — shared token bucket gate
;; ============================================================================

(defn- acquire-blocking!
  "Atomically refill and consume one token from `bucket` (an atom over
   `{:tokens d :last-ms long}`), blocking until a token is available.
   Refill rate `rps` (tokens/sec); cap at `burst`."
  [bucket rps burst]
  (loop []
    (let [now      (System/currentTimeMillis)
          old      @bucket
          elapsed  (max 0.0 (/ (- now (:last-ms old)) 1000.0))
          refill   (* elapsed (double rps))
          tokens'  (min (+ (:tokens old) refill) (double burst))]
      (if (>= tokens' 1.0)
        (if (compare-and-set! bucket old {:tokens (- tokens' 1.0) :last-ms now})
          true
          (recur))
        (let [need     (- 1.0 tokens')
              wait-ms  (max 1 (long (Math/ceil (* (/ need (double rps)) 1000))))]
          (Thread/sleep wait-ms)
          (recur))))))

(defn rate-limited
  "Passthrough step that paces messages through a shared token bucket.
   Insert it before a worker pool to bound the pool's effective RPS
   regardless of K. Each `:on-data` invocation consumes 1 token; the
   step blocks (Thread/sleep) until a token is available.

   Because all dispatched messages flow through the single proc that
   owns this step, the bucket is naturally shared — there is no
   per-worker subdivision. This is the missing piece called out at
   `combinators.clj:403`: a free worker can no longer 'blow through'
   its own bucket because there are no per-worker buckets.

   Config:
     :rps   — refill rate in tokens per second
     :burst — bucket capacity (max immediate tokens)
     :id    — sid for this step (default :rate-limited)

   Usage:
     (step/serial
       upstream
       (c/rate-limited {:rps 10 :burst 5})
       (c/stealing-workers k expensive-step))"
  [{:keys [rps burst id]
    :or   {id :rate-limited}}]
  (assert (pos? rps) "rate-limited: :rps must be positive")
  (assert (pos? burst) "rate-limited: :burst must be positive")
  (let [bucket (atom {:tokens (double burst)
                      :last-ms (System/currentTimeMillis)})]
    (step/step id nil
               (fn [ctx _s _d]
                 (acquire-blocking! bucket rps burst)
                 {:out [(msg/pass ctx)]}))))

;; ============================================================================
;; Keyed join — N-input batch join on a per-port key
;; ============================================================================

(defn join-by-key
  "Step that joins N input ports by a per-port key extractor. Each
   incoming msg is buffered under (port, key); on `:on-all-input-done`
   (after every declared input port has reported done), `on-match` is
   called once per key with the per-port items.

   Config:
     :ports        — vector of input port keywords (required, ≥2)
     :key-fns      — `{port-kw key-fn}` per declared port (required)
     :on-match     — `(key port→items → summary)` where `port→items` is
                     a map `{port-kw [item ...]}` (required)
     :require-all? — when true, only emit keys whose `port→items` has a
                     non-empty entry under EVERY declared port. Default
                     false: emit any key seen on any port.
     :id           — sid for the step (default :join-by-key)

   Note: this is a *batch* join — emissions happen on input-done, not
   per-arrival. Use `cumulative-by-group` for incremental aggregation
   over a single input."
  [{:keys [ports key-fns on-match require-all? id]
    :or   {require-all? false id :join-by-key}}]
  (assert (and (sequential? ports) (>= (count ports) 2))
          "join-by-key: :ports must be a vector of ≥2 keywords")
  (assert (every? (set ports) (keys key-fns))
          "join-by-key: :key-fns keys must match :ports")
  (assert on-match "join-by-key: :on-match is required")
  {:in id :out id :conns []
   :procs
   {id
    (step/handler-map
     {:ports   {:ins  (zipmap ports (repeat ""))
                :outs {:out ""}}
      :on-init (fn []
                 {;; {key {port [{:msg :row} ...]}}
                  :buf {}
                  ;; Set of ports we've seen :on-input-done on.
                  :done-ports #{}})
      :on-data
      (fn [ctx s row]
        (let [port  (:in-port ctx)
              k     ((get key-fns port) row)]
          [(update-in s [:buf k port] (fnil conj [])
                      {:msg (:msg ctx) :row row})
           msg/drain]))
      :on-input-done
      (fn [_ctx s port]
        (let [done' (conj (:done-ports s) port)
              s'    (assoc s :done-ports done')]
          [s' {}]))
      :on-all-input-done
      (fn [ctx s]
        (let [emissions
              (vec
               (for [[k port-map] (sort-by key (:buf s))
                     :let [items   (into {}
                                         (for [p ports]
                                           [p (mapv :row (get port-map p []))]))
                           parents (mapv :msg
                                         (mapcat val port-map))]
                     :when (or (not require-all?)
                               (every? #(seq (get items %)) ports))]
                 (msg/merge ctx parents (on-match k items))))]
          {:out emissions}))})}})

;; ============================================================================
;; Time windows — tumbling and sliding, watermark-driven
;; ============================================================================

(defn- window-start-of
  "Floor a time-ms to its tumbling-window-start: `(quot t size) * size`."
  [t size-ms]
  (* (long size-ms) (long (quot (long t) (long size-ms)))))

(defn tumbling-window
  "Step that aggregates messages into non-overlapping fixed-size windows
   keyed by an event-time extracted from the data. A window
   `[start, start+size-ms)` closes when the watermark (max event-time
   seen) reaches `start+size-ms`; on close, `on-window` is called as
   `(on-window start end items)` and its return is emitted as one
   message on `:out`. Empty windows are not emitted.

   Late events (event-time falls into an already-closed window) are
   passed to `on-late` (default: drop silently). On `:on-all-input-done`,
   any remaining open windows are flushed in start-time order.

   Config:
     :size-ms   — window length in ms (required, positive)
     :time-fn   — `(data → ms)` extractor (default: `:time`)
     :on-window — `(start end items → summary)` (required)
     :on-late   — `(item → any)` side-effect on a late drop (default
                  no-op); items are also dropped from the windowed flow
     :id        — sid for the step (default :tumbling-window)"
  [{:keys [size-ms time-fn on-window on-late id]
    :or   {time-fn :time on-late (fn [_]) id :tumbling-window}}]
  (assert (and (number? size-ms) (pos? size-ms))
          "tumbling-window: :size-ms must be positive")
  (assert on-window "tumbling-window: :on-window is required")
  (let [size (long size-ms)
        emit-closed
        (fn [ctx s wm]
          (let [closed-keys (sort (filter #(<= (+ % size) wm) (keys (:windows s))))
                emissions  (mapv (fn [start]
                                   (msg/child ctx
                                              (on-window start (+ start size)
                                                         (get-in s [:windows start]))))
                                 closed-keys)
                s'         (update s :windows
                                   (fn [ws] (apply dissoc ws closed-keys)))]
            [s' emissions]))]
    {:procs
     {id
      (step/handler-map
       {:ports   {:ins {:in ""} :outs {:out ""}}
        :on-init (fn [] {:windows {} :watermark Long/MIN_VALUE})
        :on-data
        (fn [ctx s d]
          (let [t     (long (time-fn d))
                start (window-start-of t size)
                wm    (max (:watermark s) t)
                ;; Late if the event's window is already closed.
                late? (<= (+ start size) (:watermark s))]
            (if late?
              (do (on-late d) {})
              (let [s1            (-> s
                                      (assoc :watermark wm)
                                      (update-in [:windows start] (fnil conj []) d))
                    [s2 emissions] (emit-closed ctx s1 wm)]
                [s2 (if (seq emissions) {:out emissions} {})]))))
        :on-all-input-done
        (fn [ctx s]
          (let [open-keys (sort (keys (:windows s)))]
            {:out (mapv (fn [start]
                          (msg/child ctx
                                     (on-window start (+ start size)
                                                (get-in s [:windows start]))))
                        open-keys)}))})}
     :conns [] :in id :out id}))

(defn sliding-window
  "Step that aggregates messages into overlapping fixed-size windows
   keyed by event-time. Window starts step every `:slide-ms`; each
   window covers `[start, start+size-ms)`. An event at time `t`
   belongs to every window containing `t`. A window closes when the
   watermark (max event-time seen) reaches its end; on close,
   `on-window` is called.

   Config (same shape as `tumbling-window`):
     :size-ms   — window length in ms (required, positive)
     :slide-ms  — distance between consecutive window starts (required,
                  positive; for tumbling behavior, set equal to size-ms)
     :time-fn   — `(data → ms)` extractor (default: `:time`)
     :on-window — `(start end items → summary)` (required)
     :on-late   — `(item → any)` side-effect on late events
     :id        — sid for the step (default :sliding-window)"
  [{:keys [size-ms slide-ms time-fn on-window on-late id]
    :or   {time-fn :time on-late (fn [_]) id :sliding-window}}]
  (assert (and (number? size-ms) (pos? size-ms))
          "sliding-window: :size-ms must be positive")
  (assert (and (number? slide-ms) (pos? slide-ms))
          "sliding-window: :slide-ms must be positive")
  (assert on-window "sliding-window: :on-window is required")
  (let [size  (long size-ms)
        slide (long slide-ms)
        ;; All window-starts that contain time t.
        starts-of
        (fn [t]
          ;; First start whose end > t: smallest k*slide such that k*slide+size > t.
          ;; → k > (t - size) / slide → k_min = floor((t - size) / slide) + 1.
          ;; Last start that contains t: k*slide <= t → k_max = floor(t / slide).
          (let [k-max (long (quot (long t) slide))
                k-min (inc (long (quot (- (long t) size) slide)))
                k-min (max 0 k-min)]
            (mapv #(* slide ^long %) (range k-min (inc k-max)))))
        emit-closed
        (fn [ctx s wm]
          (let [closed-keys (sort (filter #(<= (+ ^long % size) wm)
                                          (keys (:windows s))))
                emissions  (mapv (fn [start]
                                   (msg/child ctx
                                              (on-window start (+ start size)
                                                         (get-in s [:windows start]))))
                                 closed-keys)
                s'         (update s :windows
                                   (fn [ws] (apply dissoc ws closed-keys)))]
            [s' emissions]))]
    {:procs
     {id
      (step/handler-map
       {:ports   {:ins {:in ""} :outs {:out ""}}
        :on-init (fn [] {:windows {} :watermark Long/MIN_VALUE})
        :on-data
        (fn [ctx s d]
          (let [t       (long (time-fn d))
                wm      (max (:watermark s) t)
                ;; Each candidate-start: late if its window already closed.
                cands   (starts-of t)
                live    (remove #(<= (+ ^long % size) (:watermark s)) cands)]
            (if (and (empty? live) (seq cands))
              ;; All candidate windows are closed → fully late.
              (do (on-late d) {})
              (let [s1 (reduce (fn [acc start]
                                 (update-in acc [:windows start] (fnil conj []) d))
                               (assoc s :watermark wm)
                               live)
                    [s2 emissions] (emit-closed ctx s1 wm)]
                [s2 (if (seq emissions) {:out emissions} {})]))))
        :on-all-input-done
        (fn [ctx s]
          (let [open-keys (sort (keys (:windows s)))]
            {:out (mapv (fn [start]
                          (msg/child ctx
                                     (on-window start (+ start size)
                                                (get-in s [:windows start]))))
                        open-keys)}))})}
     :conns [] :in id :out id}))

;; ============================================================================
;; Backoff / retry — exponential with jitter, dead-letter on exhaustion
;; ============================================================================

(defn with-backoff
  "Wrap an inner handler-map's `:on-data` so that exceptions matching
   `:retry?` trigger exponential-backoff retry. After `:max-retries`
   exhausted (or a non-matching exception), the failure is emitted on
   the inner's `:out` port as a dead-letter map tagged
   `:dead-letter? true` (instead of propagating the exception).

   Single-port design: dead-letters share the inner's `:out` port,
   marked by the `:dead-letter?` flag. Downstream code routes by
   inspecting the flag — keeps the handler-map shape compatible with
   any consumer that expects a single output port (including
   `c/stealing-workers`). For a clean two-port routing, follow
   `with-backoff` with a `(step/step :route …)` that splits on the
   flag.

   Backoff schedule: attempt N (0-indexed) sleeps for
   `min(base-ms * 2^N, max-ms) * (1 + jitter * rand)`.

   Config:
     :max-retries — number of retries after the first attempt (default 3,
                    so up to 4 attempts total)
     :base-ms     — initial backoff (default 100)
     :max-ms      — cap on a single sleep (default 30000)
     :jitter      — multiplier for additive randomness in [0, jitter * cap]
                    (default 0.5; pass 0 for deterministic timing)
     :retry?      — predicate on the thrown Throwable. Default: retry
                    everything. To retry on a flag in `ex-data`, pass
                    e.g. `(comp :transient ex-data)`.

   Dead-letter payload shape (on `:out`):
     {:dead-letter? true :error string :ex-data map-or-nil
      :data input-data :attempts n}"
  [{:keys [max-retries base-ms max-ms jitter retry? id]
    :or   {max-retries 3 base-ms 100 max-ms 30000 jitter 0.5
           retry? (constantly true) id :backoff}}
   inner]
  (assert (>= max-retries 0) "with-backoff: :max-retries must be non-negative")
  (assert (pos? base-ms) "with-backoff: :base-ms must be positive")
  (assert (step/handler-map? inner)
          "with-backoff: inner must be a handler-map")
  (let [user-on-data (:on-data inner)
        wrapped
        (assoc inner :on-data
               (fn [ctx s d]
                 (loop [attempt 0]
                   (let [outcome (try {:ok (user-on-data ctx s d)}
                                      (catch Throwable t {:err t}))]
                     (cond
                       (contains? outcome :ok)
                       (:ok outcome)

                       (and (retry? (:err outcome))
                            (< attempt max-retries))
                       (let [base (* (double base-ms) (Math/pow 2 attempt))
                             cap  (min base (double max-ms))
                             jit  (if (zero? jitter) 0.0 (* cap jitter (rand)))
                             wait (max 1 (long (+ cap jit)))]
                         (Thread/sleep wait)
                         (recur (inc attempt)))

                       :else
                       (let [err (:err outcome)]
                         {:out
                          [(msg/child ctx
                                      {:dead-letter? true
                                       :error        (.getMessage err)
                                       :ex-data      (ex-data err)
                                       :data         d
                                       :attempts     (inc attempt)})]}))))))]
    {:procs {id wrapped} :conns [] :in id :out id}))
