(ns toolkit.datapotamus.msg
  "Message envelopes + the free algebra for building them + pure synthesis.

   A **message envelope** is a plain map with a `:msg-kind` stamp:

     :data    has :data, has :tokens
     :signal  no  :data, has :tokens
     :done    no  :data, no  :tokens

   The absence of :data / :tokens is preserved for structural clarity,
   but `:msg-kind` is the canonical source — dispatch reads the stamp
   rather than re-deriving the shape. `nil` is a valid `:data` value.
   Token conservation laws are XOR-based (see `token.clj`).

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
  {:msg-id (random-uuid) :data-id (random-uuid) :msg-kind :data
   :data data :tokens {} :parent-msg-ids []})

(defn new-signal
  "Root signal msg: no parent, supplied tokens."
  [tokens]
  {:msg-id (random-uuid) :data-id (random-uuid) :msg-kind :signal
   :tokens tokens :parent-msg-ids []})

(defn new-done
  "Done marker: no :data, no :tokens."
  []
  {:msg-id (random-uuid) :msg-kind :done :parent-msg-ids []})

(defn from-opts
  "Build a root envelope from {:data ... :tokens ...} opts.
   Presence rules:
     :data given        → data envelope (with :tokens if also given)
     only :tokens given → signal envelope
     neither            → done marker"
  [{:keys [data tokens] :as opts}]
  (case [(contains? opts :data) (contains? opts :tokens)]
    [true  true]  (assoc (new-msg data) :tokens tokens)
    [true  false] (new-msg data)
    [false true]  (new-signal tokens)
    [false false] (new-done)))

(defn envelope-kind [m] (:msg-kind m))
(defn data?         [m] (= (:msg-kind m) :data))
(defn signal?       [m] (= (:msg-kind m) :signal))
(defn done?         [m] (= (:msg-kind m) :done))

;; ============================================================================
;; Free-algebra constructors over input messages
;; ============================================================================

(defn pending?
  "True iff `x` is a pending msg (pre-synthesis, carries `::parents`)."
  [x]
  (and (map? x) (contains? x ::parents)))

(defn- derive-skeleton
  [kind parent-msg-ids]
  {:msg-id         (random-uuid)
   :data-id        (random-uuid)
   :msg-kind       kind
   :parent-msg-ids (vec parent-msg-ids)})

(defn child
  "1-to-1 derive. 2-arity uses `(:msg ctx)` as parent; 3-arity takes explicit."
  ([ctx data]              (child ctx (:msg ctx) data))
  ([_ctx parent data]
   (-> (derive-skeleton :data [(:msg-id parent)])
       (assoc :data data ::parents [parent]))))

(defn children
  "N-way derive — one child per element of `datas`."
  ([ctx datas]         (children ctx (:msg ctx) datas))
  ([ctx parent datas]  (mapv #(child ctx parent %) datas)))

(defn pass
  "Preserve parent's :data and :data-id in a new child envelope."
  [ctx]
  (let [parent (:msg ctx)]
    (-> (derive-skeleton :data [(:msg-id parent)])
        (assoc :data-id (:data-id parent)
               :data    (:data parent)
               ::parents [parent]))))

(defn signal
  "Signal child of `(:msg ctx)` — carries lineage but no :data."
  [ctx]
  (let [parent (:msg ctx)]
    (-> (derive-skeleton :signal [(:msg-id parent)])
        (assoc ::parents [parent]))))

(defn merge
  "N-parent derive. Synthesis XOR-merges all parents' tokens."
  [_ctx parents data]
  (-> (derive-skeleton :data (mapv :msg-id parents))
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
        ;; msg-id → #{leaf-msgs}
        leaf-sets   (into {}
                          (map (fn [[_ m]] [(:msg-id m) (leaves-of m)]))
                          pairs)
        ;; leaf-msg → [mid ...] — every msg that reaches this leaf
        referrers   (reduce (fn [acc [mid leaves]]
                              (reduce (fn [a l] (update a l (fnil conj []) mid))
                                      acc leaves))
                            {} leaf-sets)
        ;; leaf-id → {mid → slice}
        leaf-slices (into {}
                          (map (fn [[leaf mids]]
                                 [(:msg-id leaf)
                                  (zipmap mids (tok/split-tokens (:tokens leaf {})
                                                                 (count mids)))]))
                          referrers)
        tokens-of   (fn [m]
                      (reduce tok/merge-tokens {}
                              (for [leaf (leaf-sets (:msg-id m))]
                                (get-in leaf-slices [(:msg-id leaf) (:msg-id m)]))))]
    (reduce
     (fn [[pm evs] [port pending]]
       (let [t  (tokens-of pending)
             m  (materialize pending t)
             ev {:kind           (if (> (count (::parents pending)) 1) :merge :split)
                 :step-id        step-id
                 :msg-id         (:msg-id pending)
                 :data-id        (:data-id pending)
                 :parent-msg-ids (:parent-msg-ids pending)
                 :tokens         t}]
         [(update pm port (fnil conj []) m) (conj evs ev)]))
     [{} []]
     pairs)))
