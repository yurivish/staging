(ns toolkit.potam3.combinators
  "The primitive token-algebra combinators — `fan-out` and `fan-in`.

   These are the user's reliable surface for working with zero-sum
   groups. Everything else users want (router, retry, filter, batch...)
   composes from `step` + the message helpers; those recipes live in
   potam3.md, not here."
  (:refer-clojure :exclude [merge])
  (:require [toolkit.potam3.msg :as msg]
            [toolkit.potam3.step :as step]
            [toolkit.potam3.token :as tok]))

(defn fan-out
  "Emit `n` children, each tagged with a fresh zero-sum group. Downstream
   `fan-in` on the same group-id closes the group when all n arrivals have
   been seen (their XOR-sum for the group returns to 0)."
  ([n]       (fan-out (gensym "fan-out-") n))
  ([id n]    (fan-out id id n))
  ([id group-id n]
   (step/step id nil
              (fn [ctx _s d]
                (let [gid    [group-id (:msg-id (:msg ctx))]
                      values (tok/split-value 0 n)
                      kids   (msg/children ctx (repeat n d))
                      kids'  (mapv (fn [k v] (msg/assoc-tokens k {gid v}))
                                   kids values)]
                  {:out kids'})))))

(defn fan-in
  "Accumulate inputs grouped by token-ids minted by `fan-out`. When a
   group's XOR sum for its value reaches 0, emit one `merge` draft whose
   parents are all the collected messages, carrying a vector of their
   data. The group key is stripped from the final tokens.

   State is keyed on group-id so multiple groups can be in flight
   simultaneously."
  ([group-id] (fan-in group-id group-id))
  ([id group-id]
   (step/step id nil
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
                         (let [merged (-> (msg/merge ctx
                                                     (:msgs grp')
                                                     (mapv :data (:msgs grp')))
                                          (msg/dissoc-tokens [gid]))]
                           [(update s' :groups dissoc gid)
                            (update output :out (fnil conj []) merged)])
                         [(assoc-in s' [:groups gid] grp') output])))
                   [s {}]
                   gids))))))

(defn workers
  "K parallel copies of `inner` behind a round-robin router.

   Each worker runs in its own proc (core.async.flow gives each proc one
   thread), so per-message parallelism is real. Per-worker trace scopes
   (<id>.w0, <id>.w1, ...) surface utilization and latency directly in
   the event stream. Backpressure arrives via channel buffers — a slow
   worker blocks the router only on its own port.

   `inner` may be a handler-map or a step (any shape; internal sids are
   prefixed per-worker so state is isolated)."
  ([k inner]     (workers (gensym "workers-") k inner))
  ([id k inner]
   (assert (pos-int? k) "workers: k must be a positive integer")
   (let [nm        #(keyword (str (name id) "." %))
         ws        (mapv #(nm (str "w" %)) (range k))
         router-id (nm "router")
         join-id   (nm "join")
         join-ins  (mapv #(keyword (str "in" %)) (range k))
         route     (fn [ctx s draft-fn]
                     (let [i (mod (long (:i s 0)) k)]
                       [(assoc s :i (unchecked-inc (long i)))
                        {(ws i) [(draft-fn ctx)]}]))
         router    (step/handler-map
                    {:ports     {:ins {:in ""} :outs (zipmap ws (repeat ""))}
                     :on-data   (fn [ctx s _d] (route ctx s msg/pass))
                     :on-signal (fn [ctx s]    (route ctx s msg/signal))})
         join      (step/handler-map
                    {:ports   {:ins (zipmap join-ins (repeat "")) :outs {:out ""}}
                     :on-data (fn [ctx _s _d] {:out [(msg/pass ctx)]})})
         procs     (clojure.core/merge
                    {router-id router join-id join}
                    (zipmap ws (repeat inner)))
         conns     (vec (mapcat (fn [i w]
                                  [[[router-id w] [w :in]]
                                   [[w :out]      [join-id (join-ins i)]]])
                                (range) ws))]
     {:procs procs :conns conns :in router-id :out join-id})))
