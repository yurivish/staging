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
