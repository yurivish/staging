(ns toolkit.datapotamus2.combinators
  "Factory helpers that produce dp2 step-fns (fn [ctx] step-fn). Drop
   these into a :procs map or wrap with `dsl/step` for DSL-style
   composition."
  (:require [clojure.string :as str]
            [toolkit.datapotamus2.core :as dp2]
            [toolkit.datapotamus2.token :as tok]))

(set! *warn-on-reflection* true)

(defn wrap
  "1-in 1-out. Lift a pure (data → data) fn to a step factory."
  [f]
  (dp2/handler-factory
   (fn [_ctx s m]
     [s {:out [(dp2/child-with-data m (f (:data m)))]}])))

(defn absorb-sink
  "Terminal sink — consumes input, emits nothing."
  []
  (dp2/handler-factory
   {:outs {}}
   (fn [_ctx s _m] [s {}])))

(defn fan-out
  "Emit N copies of the input on :out with a fresh zero-sum token group
   keyed `\"<group-id>-<input-msg-id>\"`. Pair with `(fan-in group-id)`
   downstream to detect completion of all N branches."
  [group-id n]
  (let [group-prefix (str (name group-id) "-")]
    (dp2/handler-factory
     (fn [_ctx s m]
       (let [gid    (str group-prefix (:msg-id m))
             values (tok/split-value 0 n)
             kids   (mapv (fn [v]
                            (-> (dp2/child-same-data m)
                                (assoc :tokens (assoc (:tokens m) gid v))))
                          values)]
         [s {:out kids}])))))

(defn router
  "Route input to multiple ports based on `(route-fn data) →
   [{:data d :port p} ...]`. Splits existing tokens across outputs; does
   not introduce a new zero-group (routing is dispatch, not coordination)."
  [ports route-fn]
  (let [port-set (set ports)]
    (dp2/handler-factory
     {:outs (zipmap ports (repeat ""))}
     (fn [_ctx s m]
       (let [routes  (vec (route-fn (:data m)))
             n       (count routes)
             split-t (tok/split-tokens (:tokens m) n)
             out-map (reduce (fn [acc i]
                               (let [{:keys [data port]} (nth routes i)]
                                 (when-not (port-set port)
                                   (throw (IllegalArgumentException.
                                           (str "router: unknown port " port))))
                                 (let [kid (-> (dp2/child-with-data m data)
                                               (assoc :tokens (nth split-t i)))]
                                   (update acc port (fnil conj []) kid))))
                             {} (range n))]
         [s out-map])))))

(defn retry
  "Wrap a (data → data) fn. On exception, retry up to `max-attempts`
   times. Retries are invisible in the trace (internal to the step);
   on exhausted attempts the final exception propagates, surfacing as
   a :failure event."
  [f max-attempts]
  (dp2/handler-factory
   (fn [_ctx s m]
     (loop [attempt 1]
       (let [result (try {:ok (f (:data m))}
                         (catch Throwable t {:err t}))]
         (if (contains? result :ok)
           [s {:out [(dp2/child-with-data m (:ok result))]}]
           (if (< attempt max-attempts)
             (recur (inc attempt))
             (throw (:err result)))))))))

(defn fan-in
  "Accumulate inputs grouped by token-ids produced by `(fan-out group-id …)`.
   When a group's XOR reaches zero, emit a merge msg on :out whose:
     - parents are the accumulated input msg-ids
     - data is a vector of accumulated datas (in arrival order)
     - tokens are the XOR-merge of all inputs' tokens, with the
       completing group removed."
  [group-id]
  (let [group-prefix (str (name group-id) "-")]
    (dp2/handler-factory
     (fn [_ctx s m]
       (let [gids (filterv (fn [k] (and (string? k) (str/starts-with? k group-prefix)))
                           (keys (:tokens m)))]
         (reduce
          (fn [[s' output] gid]
            (let [v     (long (get (:tokens m) gid))
                  grp   (get-in s' [:groups gid] {:value 0 :msgs []})
                  grp'  (-> grp
                            (update :value (fn [x] (bit-xor (long x) v)))
                            (update :msgs conj m))]
              (if (zero? (long (:value grp')))
                (let [merge-id (random-uuid)
                      parents  (mapv :msg-id (:msgs grp'))
                      mtokens  (-> (reduce tok/merge-tokens {} (map :tokens (:msgs grp')))
                                   (dissoc gid))
                      out-msg  {:msg-id        (random-uuid)
                                :data-id       (random-uuid)
                                :data          (mapv :data (:msgs grp'))
                                :tokens        mtokens
                                :parent-msg-ids [merge-id]}]
                  [(update s' :groups dissoc gid)
                   (-> output
                       (update :out (fnil conj []) out-msg)
                       (update ::dp2/merges (fnil conj [])
                               {:msg-id merge-id :parents parents}))])
                [(assoc-in s' [:groups gid] grp') output])))
          [s {}]
          gids))))))
