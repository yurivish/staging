(ns toolkit.datapotamus2.combinators
  "User-land combinators that produce flow step-fns. These live at the
   same layer as any user-written raw step-fn — no framework privilege."
  (:require [clojure.string :as str]
            [toolkit.datapotamus2.core :as dp2]
            [toolkit.datapotamus2.token :as tok]))

(set! *warn-on-reflection* true)

(defn wrap
  "1-in 1-out. Wrap a pure (data → data) fn."
  [_step-id f]
  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
      ([_] {}) ([s _] s)
      ([s _ m] [s {:out [(dp2/child-with-data m (f (:data m)))]}])))

(defn absorb-sink
  "Terminal sink — consumes input, emits nothing."
  [_step-id]
  (fn ([] {:params {} :ins {:in ""} :outs {}})
      ([_] {}) ([s _] s)
      ([s _ _] [s {}])))

(defn fan-out
  "Emit N copies of the input on :out. Introduces a fresh zero-sum token
   group (key = \"<step-id>-<input-msg-id>\") so a downstream fan-in can
   detect when all N children have arrived (group XORs to zero)."
  [step-id n]
  (let [group-prefix (str (name step-id) "-")]
    (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
        ([_] {}) ([s _] s)
        ([s _ m]
         (let [gid    (str group-prefix (:msg-id m))
               values (tok/split-value 0 n)
               kids   (mapv (fn [v]
                              (-> (dp2/child-same-data m)
                                  (assoc :tokens (assoc (:tokens m) gid v))))
                            values)]
           [s {:out kids}])))))

(defn router
  "Route input to multiple ports based on (route-fn data) →
   [{:data d :port p} ...]. Splits existing tokens across outputs; does
   not introduce a new zero-group (routing is dispatch, not coordination)."
  [_step-id ports route-fn]
  (let [port-set (set ports)]
    (fn ([] {:params {} :ins {:in ""} :outs (zipmap ports (repeat ""))})
        ([_] {}) ([s _] s)
        ([s _ m]
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
  "Wrap a (data → data) fn. On exception, retry up to max-attempts
   times. Retries are invisible in the trace (internal to the step);
   on exhausted attempts the final exception propagates, surfacing as
   a :failure event."
  [_step-id f max-attempts]
  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
      ([_] {}) ([s _] s)
      ([s _ m]
       (loop [attempt 1]
         (let [result (try {:ok (f (:data m))}
                           (catch Throwable t {:err t}))]
           (if (contains? result :ok)
             [s {:out [(dp2/child-with-data m (:ok result))]}]
             (if (< attempt max-attempts)
               (recur (inc attempt))
               (throw (:err result)))))))))

(defn fan-in
  "Accumulate inputs grouped by token-id matching `ns-prefix`. When a
   group's XOR reaches zero, emit a merge msg on :out whose:
     - parents are the accumulated input msg-ids
     - data is a vector of accumulated datas (in arrival order)
     - tokens are the XOR-merge of all inputs' tokens, with the
       completing group removed."
  [_step-id ns-prefix]
  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
      ([_] {}) ([s _] s)
      ([s _ m]
       (let [gids (filterv (fn [k] (and (string? k) (str/starts-with? k ns-prefix)))
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
          gids)))))
