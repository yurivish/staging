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

   `workers` and `stealing-workers` are a different axis: K parallel
   copies of one inner step for stream-level throughput parallelism —
   unrelated to the scatter-gather pair above. `workers` partitions
   work statically (round-robin); `stealing-workers` lets workers race
   for a single shared queue (work-stealing)."
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

(defn stealing-workers
  "K parallel copies of `inner` sharing a single bounded queue. Workers
   race for each item — whichever finishes its previous one pulls the
   next, so slow items don't gate fast ones and overall wall-clock
   approaches `total-work / k` rather than `max-item × ceil(n/k)`.

   Best fit: I/O-bound stages with embarrassingly-parallel, homogeneous
   items where per-item cost varies (LLM calls, HTTP fetches, batch
   jobs).

   Use `workers` instead when:
     - Workers hold per-worker state (warmed contexts, primed caches,
       sticky connections) — work-stealing throws away locality.
     - Each worker has its own rate-limit bucket (one API key per
       worker) — a free worker can take consecutive items and blow
       through one bucket while the others sit idle.
     - Items must route to a specific worker (consistent hashing,
       sticky-by-key, ordered-by-key).
     - Items are sub-millisecond (queue contention dominates).

   Order is NOT preserved. Tag items upstream and re-sort downstream
   if you need order. Same trace topology as `workers`: each worker
   runs in its own proc inside a nested step named `id`, so events
   carry `[:scope id]` and per-worker events add `[:scope wN]` below
   that. Backpressure arrives via the queue's bound: when every
   worker is busy and the queue is full, the feeder blocks.

   `inner` may be a handler-map or a step; internal sids are prefixed
   per-worker so state is isolated.

   Implementation note. K worker procs share a single `core.async`
   channel via `::flow/in-ports`. The feeder declares K output ports,
   all merged onto the shared channel via `::flow/out-ports`, so the
   framework's input-done auto-append (one per declared output) writes
   K input-done envelopes — one for each worker. Each worker shim takes
   exactly one input-done (by setting `::flow/input-filter` after its
   first close, so subsequent input-dones are taken by sibling shims).
   This gives work-stealing for the data path and a clean per-shim
   cascade for the close path, all within flow's normal counter
   accounting."
  ([k inner]    (stealing-workers (gensym "stealing-workers-") k inner))
  ([id k inner]
   (assert (pos-int? k) "stealing-workers: k must be a positive integer")
   (let [shared-q (a/chan k)
         q-ports  (mapv #(keyword (str "q" %)) (range k))
         shim-ids (mapv #(keyword (str "s" %)) (range k))
         ws       (mapv #(keyword (str "w" %)) (range k))
         join-ins (mapv #(keyword (str "in" %)) (range k))

         ;; Feeder: 1 input from upstream, K declared outputs all
         ;; pointing at `shared-q` via the in-ports merge. :on-data
         ;; emits on :q0 (one write per item). :on-all-closed default
         ;; → auto-append fires on all K outputs → K input-done envelopes
         ;; written to `shared-q`, one for each shim to consume.
         feeder    (step/handler-map
                    {:ports {:ins {:in ""} :outs (zipmap q-ports (repeat ""))}
                     :on-init (fn []
                                {::flow/out-ports
                                 (zipmap q-ports (repeat shared-q))})
                     :on-data   (fn [ctx _ _]
                                  {(first q-ports) [(msg/pass ctx)]})
                     :on-signal (fn [ctx _]
                                  {(first q-ports) [(msg/signal ctx)]})})

         ;; Idle sink wired to the feeder's K declared output ports so
         ;; validate-wired-outs! sees them as connected. The actual writes
         ;; from the feeder go to `shared-q` via the ::flow/out-ports
         ;; init-time merge override, so the framework-allocated channel
         ;; behind these conns is never written to and `:drop` never wakes.
         drop-sink (step/handler-map
                    {:ports     {:ins {:in ""} :outs {}}
                     :on-data   (fn [_ _ _] {})
                     :on-signal (fn [_ _]   {})})

         ;; Shim: K independent procs all reading from `shared-q` via
         ;; in-ports. Forwards data/signals to its own :to-inner port,
         ;; which is wired by :conns to its inner's :in. On :on-all-closed
         ;; (fired by an input-done arriving on :queue), sets an
         ;; ::flow/input-filter that excludes :queue so a fast shim
         ;; can't grab a sibling's input-done envelope on its next iteration.
         shim      (step/handler-map
                    {:ports     {:ins {:queue ""} :outs {:to-inner ""}}
                     :on-init   (fn [] {::flow/in-ports {:queue shared-q}})
                     :on-data   (fn [ctx _ _]   {:to-inner [(msg/pass ctx)]})
                     :on-signal (fn [ctx _]     {:to-inner [(msg/signal ctx)]})
                     :on-all-closed
                     (fn [_ s]
                       [(assoc s ::flow/input-filter (fn [cid] (not= cid :queue)))
                        {}])})

         join      (step/handler-map
                    {:ports {:ins (zipmap join-ins (repeat ""))
                             :outs {:out ""}}
                     :on-data (fn [ctx _ _] {:out [(msg/pass ctx)]})})

         procs     (clojure.core/merge
                    {:feeder feeder :drop drop-sink :join join}
                    (zipmap shim-ids (repeat shim))
                    (zipmap ws (repeat inner)))

         conns     (vec (concat
                         ;; Stub conns to satisfy validate-wired-outs! for
                         ;; the K declared feeder outputs. The framework
                         ;; allocates one channel for `[:drop :in]` and
                         ;; points each `[:feeder :qN]`'s out-chan at it
                         ;; (1:1 connect → shared chan); the ::flow/out-ports
                         ;; merge then overrides each cid's write target to
                         ;; `shared-q`, so nothing actually flows through
                         ;; the drop chan.
                         (mapv (fn [q] [[:feeder q] [:drop :in]]) q-ports)
                         ;; Shim→inner buffer is 1: a shim must block until
                         ;; its inner has consumed the previous item before
                         ;; pulling the next from `shared-q`. Without this
                         ;; (default buf=10), a fast shim drains many items
                         ;; from shared-q into inner's queue before inner
                         ;; finishes one, defeating work-stealing — items
                         ;; end up statically partitioned among the shims
                         ;; rather than rebalancing on inner-cost variation.
                         (mapcat (fn [i s w]
                                   [[[s :to-inner] [w :in] {:buf-or-n 1}]
                                    [[w :out]      [:join (join-ins i)]]])
                                 (range) shim-ids ws)))

         pool      {:procs procs :conns conns :in :feeder :out :join}]
     {:procs {id pool} :conns [] :in id :out id})))

(defn recursive-pool
  "Coordinator-driven work-stealing pool with optional recursive feedback.

   Same conceptual goal as `stealing-workers` (K parallel copies of an
   inner step racing for items from a shared queue), but implemented as
   1 coordinator proc + K shims + K worker inners. The coordinator owns
   the queue as state and dispatches to free workers, eliminating the
   shared-channel race that affects close-cascade interactions.

   `inner` must be a handler-map. If `inner` declares an output port
   `:work`, items emitted on `:work` are routed back to the
   coordinator's `:in` port — same port external work arrives on. From
   the coordinator's perspective, recursive work and external work are
   indistinguishable; both just go on the queue.

   Closure: when external `:in` reports input-done (via the per-port
   `:on-input-done` hook) AND the queue is empty AND no workers are
   busy, the coordinator closes the channels feeding worker inputs
   (which it owns via `::flow/out-ports` on its `:to-wK` ports). Workers
   then cascade input-done back through the standard channel-close
   mechanism, eventually closing all of the coordinator's input ports
   and triggering `:on-all-closed`'s default auto-append on `:out`.

   Trace topology: coordinator proc `:coord`, shim procs `:s0`..`:sK-1`,
   worker procs `:w0`..`:wK-1`. Every dispatch generates a `:send-out`
   from coordinator and a `:recv` at the corresponding worker (via the
   shim); every result generates a `:send-out` from the worker and a
   `:recv` at the coordinator — same observability surface as
   `stealing-workers`.

   Backward compat with `stealing-workers`'s step-inner support is NOT
   guaranteed — pass a handler-map only."
  ([k inner] (recursive-pool (gensym "recursive-pool-") k inner))
  ([id k inner]
   (assert (pos-int? k) "recursive-pool: k must be a positive integer")
   (assert (step/handler-map? inner)
           "recursive-pool: inner must be a handler-map (use stealing-workers for multi-proc step inners)")
   (let [recursive?   (contains? (-> inner :ports :outs) :work)
         to-w-ports   (mapv #(keyword (str "to-w" %)) (range k))
         from-w-ports (mapv #(keyword (str "from-w" %)) (range k))
         shim-ids     (mapv #(keyword (str "s" %)) (range k))
         worker-ids   (mapv #(keyword (str "w" %)) (range k))
         port->wk-idx (zipmap from-w-ports (range k))
         to-w-chans   (vec (repeatedly k #(a/chan 1)))
         ;; Pending-work counter: incremented in the worker wrapper
         ;; *before* the framework processes the return port-map, so it
         ;; counts :work emissions that exist but haven't yet arrived
         ;; at coord :rec. Coord decrements on each :rec arrival. This
         ;; closes the race where :out arrives at coord before its
         ;; co-emitted :work — without the counter, coord could see
         ;; busy=#{} and queue=∅ and close worker chans while :work is
         ;; still in transit.
         pending-work (when recursive? (java.util.concurrent.atomic.LongAdder.))

         ;; Wrap the inner so each :on-data invocation increments
         ;; pending-work by (count :work emissions) atomically with
         ;; its return value. Synchronous: happens in the worker's
         ;; proc-fn before the framework writes :work to chans.
         inner' (if recursive?
                  (let [user-on-data (:on-data inner)]
                    (assoc inner :on-data
                           (fn [ctx s d]
                             (let [ret (user-on-data ctx s d)
                                   pm (if (vector? ret) (second ret) ret)
                                   n  (if (or (nil? pm) (identical? pm msg/drain))
                                        0
                                        (count (get pm :work [])))]
                               (when (pos? n)
                                 (.add ^java.util.concurrent.atomic.LongAdder pending-work n))
                               ret))))
                  inner)

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

         maybe-close-workers!
         (fn [s]
           (when (and (:ext-done? s)
                      (empty? (:queue s))
                      (empty? (:busy s))
                      (or (nil? pending-work)
                          (zero? (.sum ^java.util.concurrent.atomic.LongAdder pending-work))))
             (doseq [c to-w-chans] (a/close! c))))

         coord
         (step/handler-map
          {:ports {:ins  (cond-> (clojure.core/merge {:in ""}
                                                     (zipmap from-w-ports (repeat "")))
                           recursive? (assoc :rec ""))
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
                 (or (= in-port :in) (= in-port :rec))
                 ;; New work (external on :in OR recursive on :rec).
                 ;; Queue + try dispatch. Two ports because core.async.flow
                 ;; doesn't reliably merge multiple conns into one input
                 ;; port — keep them separate, treat them identically.
                 ;; If on :rec, this is a :work msg that the worker
                 ;; wrapper pre-counted; decrement now that it's arrived.
                 (let [_ (when (and (= in-port :rec) pending-work)
                           (.decrement ^java.util.concurrent.atomic.LongAdder pending-work))
                       s'             (update s :queue conj msg)
                       [s'' dispatch] (try-dispatch s')]
                   [s'' dispatch])

                 :else
                 ;; Worker result on :from-wK.
                 (let [wk-idx (port->wk-idx in-port)
                       wk     (worker-ids wk-idx)
                       s'     (update s :busy disj wk)
                       forward {:out [(msg/pass ctx)]}
                       [s'' dispatch] (try-dispatch s')]
                   (maybe-close-workers! s'')
                   [s'' (clojure.core/merge forward dispatch)]))))
           :on-signal
           (fn [ctx s]
             (let [in-port (:in-port ctx)]
               (if (= in-port :in)
                 ;; Signals from external/recursive go straight to :out.
                 ;; (We don't queue them; signals carry tokens but no
                 ;; payload, so they don't represent work.)
                 [s {:out [(msg/signal ctx)]}]
                 ;; Signals from workers also forward straight through.
                 [s {:out [(msg/signal ctx)]}])))
           :on-input-done
           (fn [_ s port]
             (if (= port :in)
               ;; First input-done on :in is from external (workers'
               ;; :work feedback only emits input-done after coord has
               ;; closed their inputs). Mark ext-done? and check terminal.
               (let [s' (assoc s :ext-done? true)]
                 (maybe-close-workers! s')
                 [s' {}])
               [s {}]))
           :on-all-closed
           (fn [_ s] [s {}])})

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

         procs (-> {:coord coord :drop drop-sink}
                   (into (map vector shim-ids shims))
                   (into (map vector worker-ids (repeat inner'))))

         conns (vec (concat
                     ;; Stub conns: coord :to-wK → drop. Real writes go
                     ;; to to-w-chans via ::flow/out-ports override.
                     (mapv (fn [tp] [[:coord tp] [:drop :in]]) to-w-ports)
                     ;; Shim → worker (per-pair) — buffer 1 so a fast
                     ;; shim can't drain to-w-chan ahead of inner.
                     (mapcat (fn [s w] [[[s :to-inner] [w :in] {:buf-or-n 1}]])
                             shim-ids worker-ids)
                     ;; Worker :out → coord :from-wK
                     (mapcat (fn [w fp] [[[w :out] [:coord fp]]])
                             worker-ids from-w-ports)
                     ;; Recursive: worker :work → coord :rec
                     (when recursive?
                       (mapv (fn [w] [[w :work] [:coord :rec]])
                             worker-ids))))

         pool {:procs procs :conns conns :in :coord :out :coord}]
     {:procs {id pool} :conns [] :in id :out id})))
