(ns toolkit.datapotamus.combinators.workers
  "K parallel copies of one step for stream-level throughput parallelism.

   `round-robin-workers` partitions work statically (round-robin);
   `stealing-workers` lets workers race for a coordinator-owned queue
   (work-stealing) and optionally supports recursive `:work` feedback
   when the inner is a handler-map declaring a `:work` output port.

   Orthogonal axis to `combinators.core/parallel` (scatter-gather across
   heterogeneous specialists)."
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as-alias flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

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
