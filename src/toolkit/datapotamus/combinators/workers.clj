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
            [toolkit.datapotamus.counters :as ctrs]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]
            [toolkit.pubsub :as pubsub]))

(defn- has-work-port?
  "True iff `inner` is a handler-map declaring a `:work` output port —
   the structural signal that this inner wants recursive feedback into
   the pool. stealing-workers uses this to decide whether to install
   the try-put-with-peer-fallback dispatcher on `:work`."
  [inner]
  (and (step/handler-map? inner)
       (contains? (-> inner :ports :outs) :work)))

(defn- mk-pool
  "Internal builder used by both `round-robin-workers` and
   `stealing-workers`. Always builds: 1 router proc that mod-K
   routes external work to one of K worker channels, K worker procs
   each running `inner`, and 1 join proc collecting per-worker outputs.

   Options:
     :steal? — if true, all K worker channels are shared across all
               workers (each worker reads from own + every peer via
               alts! with own-first priority). Off-default; off ⇒
               classic round-robin (each channel has exactly one reader).
     :combinator — symbol stamped on the topology for trace/render."
  [id k inner {:keys [steal? combinator]
               :or   {steal? false combinator :round-robin-workers}}]
  (assert (pos-int? k) "mk-pool: k must be a positive integer")
  (let [ws         (mapv #(keyword (str "worker-" %)) (range k))
        join-ins   (mapv #(keyword (str "in" %)) (range k))
        ;; Pre-allocate the K worker-input channels when stealing.
        ;; Buffer of 16 gives slack for bursty fan-out without blocking
        ;; the router; tunable later if this proves too tight.
        worker-chs (when steal? (vec (repeatedly k #(a/chan 16))))
        ;; Recursive `:work` feedback also writes into worker-chs when
        ;; stealing is on AND inner has a :work port. Same pool, same
        ;; routing — producers (worker handlers emitting :work) try-put
        ;; on own first, then peers, with bounded spillover on total
        ;; failure (handled inside the worker wrapper).
        work-feedback?  (and steal? (has-work-port? inner))

        router-out-ports (when steal?
                           (zipmap ws worker-chs))
        ;; Router proc — same mod-K dispatch for both modes. When
        ;; stealing, outputs go to manually-allocated worker-chs via
        ;; ::flow/out-ports override (the framework-allocated channels
        ;; behind the stub conns are unused).
        router (step/handler-map
                (cond->
                 {:ports     {:ins {:in ""} :outs (zipmap ws (repeat ""))}
                  :on-data   (fn [ctx s _d]
                               (let [i (mod (long (:i s 0)) k)]
                                 [(assoc s :i (unchecked-inc (long i)))
                                  {(ws i) [(msg/pass ctx)]}]))
                  :on-signal (fn [ctx s]
                               (let [i (mod (long (:i s 0)) k)]
                                 [(assoc s :i (unchecked-inc (long i)))
                                  {(ws i) [(msg/signal ctx)]}]))}
                  steal?
                  (assoc :on-init (fn [] {::flow/out-ports router-out-ports}))))
        join   (step/handler-map
                {:ports   {:ins (zipmap join-ins (repeat "")) :outs {:out ""}}
                 :on-data (fn [ctx _s _d] {:out [(msg/pass ctx)]})})

        ;; Worker wrapper: when stealing, attach :own + :peer-N input
        ;; bindings via ::flow/in-ports (giving each worker an alts!
        ;; over its own channel + all peer channels with :own first).
        ;; When inner declares :work, install a side-channel dispatcher
        ;; that takes :work emissions and try-puts them across the worker
        ;; channel array (own first, peer fallback). The dispatcher runs
        ;; in a daemon thread launched from :on-init and torn down by
        ;; :on-stop.
        ;;
        ;; For STEP inners (vs handler-map), we can't modify :on-init
        ;; or :ports of the user's step directly. Instead we build a
        ;; 2-proc sub-step: a small handler-map shim that owns the
        ;; ::flow/in-ports binding and forwards to the user step's :in.
        ;; Step inners cannot use :work feedback (no clean way to
        ;; override their internal output ports) — this is enforced by
        ;; has-work-port? returning false for non-handler-maps.
        mk-shim
        (when steal?
          (fn [own-ch peer-chs]
            (let [peer-ports   (mapv #(keyword (str "peer-" %))
                                     (range (count peer-chs)))
                  in-bindings  (assoc (zipmap peer-ports peer-chs) :own own-ch)
                  in-port-spec (assoc (zipmap peer-ports (repeat "stolen-from-peer"))
                                      :own "preferred-queue")]
              (step/handler-map
               {:ports     {:ins in-port-spec :outs {:out ""}}
                :on-init   (fn [] {::flow/in-ports in-bindings})
                :on-data   (fn [ctx _s _d] {:out [(msg/pass ctx)]})
                :on-signal (fn [ctx _s]    {:out [(msg/signal ctx)]})}))))
        wrap-worker
        (if-not steal?
          (fn [_i v] v)
          (fn [i v]
            (let [own-ch       (worker-chs i)
                  peer-chs     (vec (for [j (range k) :when (not= j i)]
                                      (worker-chs j)))
                  peer-ports   (mapv #(keyword (str "peer-" %)) (range (dec k)))
                  in-bindings  (assoc (zipmap peer-ports peer-chs) :own own-ch)
                  in-port-spec (assoc (zipmap peer-ports (repeat "stolen-from-peer"))
                                      :own "preferred-queue")]
              (cond
                ;; --- Step inner: build a 2-proc sub-step (shim → step). ---
                ;; The shim handler-map owns the ::flow/in-ports binding
                ;; for shared channels and forwards on :out to the user's
                ;; step :in. User's step is unchanged. Step inners cannot
                ;; declare :work feedback (work-feedback? is always false
                ;; for non-handler-maps).
                (step/step? v)
                {:procs {:shim (mk-shim own-ch peer-chs)
                         :inner v}
                 :conns [[[:shim :out] [:inner :in]]]
                 :in    [:shim :own]
                 :out   :inner}

                ;; --- Handler-map inner: modify ports + on-init in place. ---
                (step/handler-map? v)
                (let [hmap         v
                      ;; Recursive feedback: a private unbounded channel that
                      ;; the framework writes :work emissions to (via
                      ;; ::flow/out-ports override). A daemon dispatcher
                      ;; reads from it and uses offer! (non-blocking) to
                      ;; place each msg on one of K worker channels —
                      ;; own first, peers in fixed order. If all K
                      ;; channels are at buffer capacity, the daemon
                      ;; blocks on own via >!!. Blocking the daemon
                      ;; thread is safe (workers drain own-ch via alts!
                      ;; on a separate thread, so the daemon eventually
                      ;; unblocks). work-ch is effectively unbounded so
                      ;; the framework's write never blocks; memory is
                      ;; bounded by total work-set, which is finite for
                      ;; tree-fetch-shaped workloads.
                      work-ch      (when work-feedback?
                                     (a/chan (a/buffer Integer/MAX_VALUE)))
                      targets      (when work-feedback?
                                     (into [own-ch] peer-chs))
                      current-init (:on-init hmap (fn [] {}))
                      current-stop (:on-stop hmap (fn [_ _] nil))]
                  (assoc hmap
                         :ports
                         (-> (:ports hmap)
                             (update :ins
                                     (fn [m]
                                       (-> (apply dissoc m
                                                  (keys (:ins (:ports hmap))))
                                           (merge in-port-spec)))))
                         ;; When the inner has a :work output port, override
                         ;; :on-signal so signals are forwarded ONLY on :out,
                         ;; never on :work. The framework's default broadcasts
                         ;; signals on every declared output port — but for a
                         ;; recursive-feedback pool, broadcasting on :work
                         ;; pumps the signal back into the worker pool, which
                         ;; broadcasts again, forever. This terminates that
                         ;; loop. Token semantics preserved: each input msg
                         ;; produces exactly one downstream signal on :out.
                         :on-signal
                         (if-not work-feedback?
                           (:on-signal hmap)
                           (fn [ctx _s]
                             {:out [(msg/signal ctx)]}))
                         :on-init
                         (fn []
                           (let [base (current-init)
                                 init-with-in (assoc base ::flow/in-ports in-bindings)]
                             (if-not work-feedback?
                               init-with-in
                               (let [stop?    (volatile! false)
                                     t        (Thread.
                                               ^Runnable
                                               (fn []
                                                 (loop []
                                                   (when-not @stop?
                                                     (when-let [m (a/<!! work-ch)]
                                                       ;; Try offer! across own then peers.
                                                       ;; If all decline (channels at buffer
                                                       ;; capacity), block on own via >!!.
                                                       ;; Blocking the dispatcher thread
                                                       ;; is safe — it has no cyclic
                                                       ;; dependency with worker procs
                                                       ;; (the worker's alts! drains
                                                       ;; own-ch independently), so the
                                                       ;; worker frees a slot and the
                                                       ;; dispatcher unblocks. Using
                                                       ;; a/put! here would overflow
                                                       ;; core.async's 1024-pending-puts
                                                       ;; assertion at high producer:
                                                       ;; consumer ratios (e.g. K=1 with
                                                       ;; 5-way fanout × 3000+ nodes).
                                                       (loop [tgts targets]
                                                         (cond
                                                           (empty? tgts)
                                                           (a/>!! own-ch m)
                                                           (a/offer! (first tgts) m)
                                                           nil
                                                           :else
                                                           (recur (rest tgts))))
                                                       (recur))))))]
                                 (.setDaemon t true)
                                 (.start t)
                                 (-> init-with-in
                                     (update ::flow/out-ports assoc :work work-ch)
                                     (assoc ::work-dispatcher {:thread t :stop? stop?}))))))
                         :on-stop
                         (fn [ctx s]
                           (when-let [d (::work-dispatcher s)]
                             (vreset! (:stop? d) true)
                             (when work-ch (a/close! work-ch)))
                           (current-stop ctx s))))

                :else
                (throw (ex-info (str "stealing-workers: inner must be a step or handler-map, got " (type v))
                                {:inner-type (type v)}))))))
        ;; Worker procs: same proc registered K times (the framework
        ;; gives each its own state because they're distinct procs).
        worker-procs (mapv #(wrap-worker % inner) (range k))

        procs (clojure.core/merge
               {:router router :join join}
               (zipmap ws worker-procs))
        conns (vec (mapcat
                    (fn [i w]
                      (cond-> []
                        (not steal?)
                        (conj [[:router w] [w :in]])
                        true
                        (conj [[w :out] [:join (join-ins i)]])))
                    (range) ws))
        ;; When stealing, ports overridden via ::flow/out-ports still
        ;; need stub conns to satisfy the framework's "every declared
        ;; output must be wired" check. The stub-allocated channels are
        ;; never written to (the override redirects all writes). Each
        ;; conn must target a UNIQUE input port — multiple conns into
        ;; the same dest port confuses core.async.flow's allocation.
        ;;
        ;; Routes: K router outputs (router :worker-N → sink :rdrop-N)
        ;;       + K worker :work outputs if recursive (worker :work
        ;;         → sink :wdrop-N)
        sink-ins  (when steal?
                    (-> (zipmap (mapv #(keyword (str "rdrop-" %)) (range k))
                                (repeat ""))
                        (cond-> work-feedback?
                          (into (zipmap
                                  (mapv #(keyword (str "wdrop-" %)) (range k))
                                  (repeat ""))))))
        sink-proc (when steal?
                    (step/handler-map
                     {:ports     {:ins sink-ins :outs {}}
                      :on-data   (fn [_ _ _] {})
                      :on-signal (fn [_ _]   {})}))
        procs (cond-> procs steal? (assoc :sink sink-proc))
        conns (cond-> conns
                steal?
                (into (mapv (fn [i w]
                              [[:router w]
                               [:sink (keyword (str "rdrop-" i))]])
                            (range) ws))
                (and steal? work-feedback?)
                (into (mapv (fn [i w]
                              [[w :work]
                               [:sink (keyword (str "wdrop-" i))]])
                            (range) ws)))
        pool  {:procs procs :conns conns :in :router :out :join
               :combinator combinator}]
    {:procs {id pool} :conns [] :in id :out id}))

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
   prefixed per-worker so state is isolated).

   Use `stealing-workers` instead when you want load-balancing
   (workers race for any free item) or recursive `:work` feedback."
  ([k inner]     (round-robin-workers (gensym "round-robin-workers-") k inner))
  ([id k inner]  (mk-pool id k inner {:combinator :round-robin-workers})))

(defn stealing-workers
  "K parallel copies of `inner` racing for items from a shared channel
   array. Same router + join skeleton as `round-robin-workers`, but each
   worker reads from EVERY channel in the array (its own preferred
   channel first, then peers), so an idle worker can grab an item the
   router placed in a busy worker's queue. No central coordinator —
   stealing falls out of `alts!` priority on shared channels.

   When `inner` declares a `:work` output port (handler-maps only), the
   wrapper installs a daemon dispatcher that routes recursive emissions
   straight back into the channel array (own first via `offer!`, peer
   fallback in fixed order, async `put!` if all are at capacity). The
   dispatcher itself never blocks, so wide recursive fan-out — which
   used to deadlock the centralized coord version of this combinator —
   completes correctly even at depth=5 fanout=5 (3906 nodes).

   Order is NOT preserved (an item placed in worker-3's queue may be
   consumed by worker-7).

   Best fit: I/O-bound stages with embarrassingly-parallel,
   homogeneous items where per-item cost varies (LLM calls, HTTP
   fetches, recursive tree fetches).

   `inner` may be a handler-map or a step. Step inners cannot use
   `:work` recursive feedback (their internal output ports can't be
   cleanly overridden) — convert to a handler-map if you need it.

   Prefer `round-robin-workers` when:
     - Workers hold per-worker state (warmed contexts, primed caches)
     - Each worker has its own rate-limit bucket
     - Items must route to a specific worker (consistent hashing)
     - Items are sub-millisecond (queue contention dominates)"
  ([k inner]     (stealing-workers (gensym "stealing-workers-") k inner))
  ([id k inner]  (mk-pool id k inner {:steal? true
                                      :combinator :stealing-workers})))

