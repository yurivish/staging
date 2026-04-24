(ns toolkit.datapotamus.combinators
  "The primitive token-algebra combinators — `fan-out` and `fan-in`.

   These are the user's reliable surface for working with zero-sum
   groups. Everything else users want (router, retry, filter, batch...)
   composes from `step` + the message helpers; those recipes live in
   datapotamus.md, not here."
  (:require [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.token :as tok]))

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
   group's XOR sum for its value reaches 0, emit one `merge` msg whose
   parents are all the collected messages, carrying a vector of their
   data. The group key is stripped from the final tokens.

   State is keyed on group-id so multiple groups can be in flight
   simultaneously. While accumulating (no group has closed this
   invocation) returns `msg/drain` to suppress the default auto-signal;
   the stashed parent refs carry tokens forward via the eventual merge."
  ([group-id] (fan-in group-id group-id))
  ([id group-id]
   (step/step id nil
              (fn [ctx s _d]
                (let [m    (:msg ctx)
                      gids (filterv (fn [k] (and (vector? k) (= group-id (first k))))
                                    (keys (:tokens m)))
                      [s' output]
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
                       gids)]
                  (if (seq output) [s' output] [s' msg/drain]))))))

(defn workers
  "K parallel copies of `inner` behind a round-robin router.

   Each worker runs in its own proc (core.async.flow gives each proc one
   thread), so per-message parallelism is real. The workers, router, and
   join live inside a single nested step named `id`, so every worker event
   carries `[:flow id]` in its scope — structural aggregation per logical
   stage, no name parsing. Per-worker distinction adds another `[:flow wN]`
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
