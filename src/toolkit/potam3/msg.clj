(ns toolkit.potam3.msg
  "Message envelopes + the free algebra of drafts + pure synthesis.

   A **message envelope** is a plain map. Three shapes distinguished by
   key absence:

     data:    has :data, has :tokens
     signal:  no  :data, has :tokens
     done:    no  :data, no  :tokens

   `nil` is a valid data value; envelope kind is structural, not a
   sentinel. Token conservation laws are XOR-based (see `token.clj`).

   Handlers return outputs in a per-port map. Each output is either a
   **draft** (built by the free-algebra constructors below — `child`,
   `children`, `pass`, `signal`, `merge`) or a plain value that gets
   auto-wrapped as a `child` of the input message. Drafts carry an
   in-memory `::parents` ref to their direct parents (real messages or
   other drafts), forming a DAG whose leaves are the handler's input
   messages.

   `synthesize` is a single pure fold over that DAG. For each leaf,
   it counts how many drafts reference it, XOR-splits the leaf's
   tokens K-ways, and XOR-merges the slices into each referring
   draft. Then it applies any stamped `::assoc-tokens` / `::dissoc-tokens`
   metadata and strips the ref graph. Output is concrete messages
   partitioned by port, plus a list of :split/:merge trace events."
  (:refer-clojure :exclude [merge])
  (:require [toolkit.potam3.token :as tok]))

;; ============================================================================
;; Envelope constructors + predicates
;; ============================================================================

(defn new-msg
  "Root data msg: no parent, empty tokens."
  [data]
  {:msg-id (random-uuid) :data-id (random-uuid) :data data
   :tokens {} :parent-msg-ids []})

(defn new-done
  "Done marker: no :data, no :tokens."
  []
  {:msg-id (random-uuid) :parent-msg-ids []})

(defn signal?
  "True iff `m` is a signal envelope (tokens + lineage, no :data)."
  [m]
  (and (map? m) (not (contains? m :data)) (contains? m :tokens)))

(defn done?
  "True iff `m` is a done marker (no :data, no :tokens)."
  [m]
  (and (map? m) (not (contains? m :data)) (not (contains? m :tokens))))

(defn data?
  "True iff `m` is a data envelope."
  [m]
  (and (map? m) (contains? m :data)))

(defn envelope-kind
  "One of :data, :signal, :done."
  [m]
  (cond (done? m) :done (signal? m) :signal :else :data))

;; ============================================================================
;; Drafts — the free algebra over input messages
;; ============================================================================

(defn draft?
  "True iff `x` is a draft (not yet materialized)."
  [x]
  (and (map? x) (contains? x ::parents)))

(defn- derive-skeleton
  [parent-msg-ids]
  {:msg-id         (random-uuid)
   :data-id        (random-uuid)
   :parent-msg-ids (vec parent-msg-ids)})

(defn child
  "1-to-1 derive. 2-arity uses `(:msg ctx)` as parent; 3-arity takes explicit."
  ([ctx data]              (child ctx (:msg ctx) data))
  ([_ctx parent data]
   (-> (derive-skeleton [(:msg-id parent)])
       (assoc :data data ::parents [parent]))))

(defn children
  "N-way derive — one child per element of `datas`."
  ([ctx datas]         (children ctx (:msg ctx) datas))
  ([ctx parent datas]  (mapv #(child ctx parent %) datas)))

(defn pass
  "Preserve parent's :data and :data-id in a new child envelope."
  [ctx]
  (let [parent (:msg ctx)]
    (-> (derive-skeleton [(:msg-id parent)])
        (assoc :data-id (:data-id parent)
               :data    (:data parent)
               ::parents [parent]))))

(defn signal
  "Signal child of `(:msg ctx)` — carries lineage but no :data."
  [ctx]
  (let [parent (:msg ctx)]
    (-> (derive-skeleton [(:msg-id parent)])
        (assoc ::parents [parent]))))

(defn merge
  "N-parent derive. Synthesis XOR-merges all parents' tokens."
  [_ctx parents data]
  (-> (derive-skeleton (mapv :msg-id parents))
      (assoc :data data ::parents (vec parents))))

;; --- Escape hatch ------------------------------------------------------------
;; Stamped decorations applied during synthesis AFTER token distribution.
;; The combinator is responsible for XOR-balancing stamped values so
;; global conservation holds.

(defn assoc-tokens
  "XOR-merge `token-map` into the draft's final tokens during synthesis."
  [draft token-map]
  (update draft ::assoc-tokens #(tok/merge-tokens (or % {}) token-map)))

(defn dissoc-tokens
  "Remove `group-keys` from the draft's final tokens during synthesis."
  [draft group-keys]
  (update draft ::dissoc-tokens (fnil into #{}) group-keys))

;; ============================================================================
;; Synthesis — one pure fold from drafts to concrete messages
;; ============================================================================

(defn- leaves-of
  "The set of ultimate (non-draft) ancestors reachable from `d`."
  [d]
  (if (draft? d)
    (into #{} (mapcat leaves-of) (::parents d))
    #{d}))

(defn- coerce-plain
  "Replace plain output values with `child` drafts of `parent-msg`."
  [outputs parent-msg]
  (mapv (fn [[port v]]
          (if (draft? v) [port v] [port (child nil parent-msg v)]))
        outputs))

(defn- materialize
  "Strip refs + stamped meta; assoc final :tokens."
  [draft tokens]
  (let [added   (::assoc-tokens draft)
        dropped (::dissoc-tokens draft)
        t1      (if added   (tok/merge-tokens tokens added) tokens)
        t2      (if (seq dropped) (apply dissoc t1 dropped) t1)]
    (-> draft
        (dissoc ::parents ::assoc-tokens ::dissoc-tokens)
        (assoc :tokens t2))))

(defn- split-event [step-id draft]
  {:kind           :split
   :step-id        step-id
   :msg-id         (:msg-id draft)
   :data-id        (:data-id draft)
   :parent-msg-ids (:parent-msg-ids draft)})

(defn- merge-event [step-id draft]
  {:kind           :merge
   :step-id        step-id
   :msg-id         (:msg-id draft)
   :parent-msg-ids (:parent-msg-ids draft)})

(defn- flatten-outputs
  "Map {port [d ...]} → seq [[port d] ...]. Within-port order preserved;
   across-port order is arbitrary (map iteration)."
  [outputs]
  (vec (for [[port ds] outputs, d ds] [port d])))

(defn synthesize
  "Post-handler fold. Pure.

   `outputs`    : {port [draft-or-plain ...]}
   `input-msg`  : the message that triggered the handler (parent for plain values)
   `step-id`    : trace subject for generated events

   Returns [msgs-per-port events]
     msgs-per-port : {port [concrete-msg ...]}
     events        : [split-or-merge-event ...]"
  [outputs input-msg step-id]
  (let [pairs       (coerce-plain (flatten-outputs outputs) input-msg)
        leaf-sets   (into {}
                          (map (fn [[_ d]] [(:msg-id d) (leaves-of d)]))
                          pairs)
        all-leaves  (into #{} (mapcat val) leaf-sets)
        ;; leaf-id → vector of draft-ids that reach it
        referrers   (reduce (fn [acc [draft-id leaves]]
                              (reduce (fn [a l]
                                        (update a (:msg-id l) (fnil conj []) draft-id))
                                      acc leaves))
                            {} leaf-sets)
        ;; leaf-id → {draft-id → slice}
        leaf-slices (into {}
                          (for [leaf all-leaves
                                :let [ds     (referrers (:msg-id leaf))
                                      slices (tok/split-tokens (:tokens leaf {})
                                                               (count ds))]]
                            [(:msg-id leaf) (zipmap ds slices)]))
        tokens-of   (fn [draft]
                      (reduce tok/merge-tokens {}
                              (for [leaf (leaf-sets (:msg-id draft))]
                                (get-in leaf-slices [(:msg-id leaf) (:msg-id draft)]))))
        msgs-per-port (reduce
                        (fn [acc [port d]]
                          (update acc port (fnil conj []) (materialize d (tokens-of d))))
                        {} pairs)
        events        (mapv (fn [[_ d]]
                              (if (> (count (::parents d)) 1)
                                (merge-event step-id d)
                                (split-event step-id d)))
                            pairs)]
    [msgs-per-port events]))
