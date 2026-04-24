(ns toolkit.datapotamus.msg
  "Message envelopes + the free algebra for building them + pure synthesis.

   A **message envelope** is a plain map. Three shapes distinguished by
   key absence:

     data:    has :data, has :tokens
     signal:  no  :data, has :tokens
     done:    no  :data, no  :tokens

   `nil` is a valid data value; envelope kind is structural, not a
   sentinel. Token conservation laws are XOR-based (see `token.clj`).

   Handlers return outputs in a per-port map. Each output is either a
   msg built by the free-algebra constructors below (`child`,
   `children`, `pass`, `signal`, `merge`) or a bare data value that gets
   auto-wrapped as a `child` of the input message. Pending msgs carry
   an in-memory `::parents` ref to their direct parents (concrete
   messages or other pending msgs), forming a DAG whose leaves are the
   handler's input messages.

   `synthesize` is a single pure fold over that DAG. For each leaf,
   it counts how many pending msgs reference it, XOR-splits the leaf's
   tokens K-ways, and XOR-merges the slices into each referrer. Then
   it applies any stamped `::assoc-tokens` / `::dissoc-tokens` metadata
   and strips the ref graph. Output is concrete messages partitioned
   by port, plus a list of :split/:merge trace events."
  (:refer-clojure :exclude [merge])
  (:require [toolkit.datapotamus.token :as tok]))

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
;; Free-algebra constructors over input messages
;; ============================================================================

(defn pending?
  "True iff `x` is a pending msg (pre-synthesis, carries `::parents`)."
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
  "XOR-merge `token-map` into the msg's final tokens during synthesis."
  [m token-map]
  (update m ::assoc-tokens #(tok/merge-tokens (or % {}) token-map)))

(defn dissoc-tokens
  "Remove `group-keys` from the msg's final tokens during synthesis."
  [m group-keys]
  (update m ::dissoc-tokens (fnil into #{}) group-keys))

(def drain
  "Sentinel return value from a handler, in place of the port-map. Tells
   the interpreter: do NOT auto-propagate a signal for this invocation,
   even though no outputs were produced. Two use cases:

     * Batching / deferred propagation — you've stashed the input msg in
       state and will emit a merge of several inputs in a later handler
       call. The tokens ride along on the stashed ref; auto-signalling
       now would double-count them once the merge fires.

     * Genuine terminal drop — you really want the input's tokens to die
       here (rare).

   Return either `drain` or `[state' drain]`. Any other empty port-map
   (including `{}` and `{:port []}`) triggers the default auto-signal on
   every declared output port."
  ::drain)

;; ============================================================================
;; Synthesis — one pure fold from pending msgs to concrete messages
;; ============================================================================

(defn- leaves-of
  "The set of ultimate (non-pending) ancestors reachable from `m`."
  [m]
  (if (pending? m)
    (into #{} (mapcat leaves-of) (::parents m))
    #{m}))

(defn- coerce-data
  "Replace bare data values with `child` msgs of `parent-msg`."
  [outputs parent-msg]
  (mapv (fn [[port v]]
          (if (pending? v) [port v] [port (child nil parent-msg v)]))
        outputs))

(defn- materialize
  "Strip refs + stamped meta; assoc final :tokens."
  [m tokens]
  (let [added   (::assoc-tokens m)
        dropped (::dissoc-tokens m)
        t1      (if added   (tok/merge-tokens tokens added) tokens)
        t2      (if (seq dropped) (apply dissoc t1 dropped) t1)]
    (-> m
        (dissoc ::parents ::assoc-tokens ::dissoc-tokens)
        (assoc :tokens t2))))

(defn- split-event [step-id m]
  {:kind           :split
   :step-id        step-id
   :msg-id         (:msg-id m)
   :data-id        (:data-id m)
   :parent-msg-ids (:parent-msg-ids m)})

(defn- merge-event [step-id m]
  {:kind           :merge
   :step-id        step-id
   :msg-id         (:msg-id m)
   :parent-msg-ids (:parent-msg-ids m)})

(defn- flatten-outputs
  "Map {port [m ...]} → seq [[port m] ...]. Within-port order preserved;
   across-port order is arbitrary (map iteration)."
  [outputs]
  (vec (for [[port ms] outputs, m ms] [port m])))

(defn synthesize
  "Post-handler fold. Pure.

   `outputs`    : {port [msg-or-data ...]}
   `input-msg`  : the message that triggered the handler (parent for data values)
   `step-id`    : trace subject for generated events

   Returns [msgs-per-port events]
     msgs-per-port : {port [concrete-msg ...]}
     events        : [split-or-merge-event ...]"
  [outputs input-msg step-id]
  (let [pairs       (coerce-data (flatten-outputs outputs) input-msg)
        leaf-sets   (into {}
                          (map (fn [[_ m]] [(:msg-id m) (leaves-of m)]))
                          pairs)
        all-leaves  (into #{} (mapcat val) leaf-sets)
        ;; leaf-id → vector of msg-ids that reach it
        referrers   (reduce (fn [acc [mid leaves]]
                              (reduce (fn [a l]
                                        (update a (:msg-id l) (fnil conj []) mid))
                                      acc leaves))
                            {} leaf-sets)
        ;; leaf-id → {msg-id → slice}
        leaf-slices (into {}
                          (for [leaf all-leaves
                                :let [mids   (referrers (:msg-id leaf))
                                      slices (tok/split-tokens (:tokens leaf {})
                                                               (count mids))]]
                            [(:msg-id leaf) (zipmap mids slices)]))
        tokens-of   (fn [m]
                      (reduce tok/merge-tokens {}
                              (for [leaf (leaf-sets (:msg-id m))]
                                (get-in leaf-slices [(:msg-id leaf) (:msg-id m)]))))]
    (reduce
     (fn [[pm evs] [port pending]]
       (let [t        (tokens-of pending)
             m        (materialize pending t)
             mk-event (if (> (count (::parents pending)) 1) merge-event split-event)
             ev       (assoc (mk-event step-id pending) :tokens t)]
         [(update pm port (fnil conj []) m) (conj evs ev)]))
     [{} []]
     pairs)))
