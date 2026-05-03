(ns toolkit.datapotamus.shape
  "Shape decomposition over a stepmap topology.

   `(decompose topology)` walks `(step/topology stepmap)` into a tree
   that mirrors the combinator hierarchy. Every container carries a
   `:shape` describing how its conns wire its children:

     :empty            no conns at this level
     :chain            linear sequence; `:order` is the source→sink path
     :scatter-gather   one source → K disjoint branches → one sink;
                       `:source`, `:sink`, `:branches` (vec of node-vecs)
     :cycle            non-trivial SCC; `:order` is an Eades–Lin–Smyth
                       member ordering that minimizes backward edges,
                       `:internal-edges` is every edge with both
                       endpoints inside the SCC
     :prime            irreducible residue (non-series-parallel);
                       `:order` is a topo sort of this level's
                       sub-DAG (with SCCs already condensed),
                       `:internal-edges` is every edge in that sub-DAG

   Within `:order` and `:branches`, an element is either a single
   full-path or a nested `{:kind :cycle ...}` record. `:internal-edges`
   endpoints match `:order` elements one-to-one. This recursion lets
   the same vocabulary describe any directed graph (acyclic, cyclic,
   or mixed) at any combinator level.

   The renderer derives spine vs off-spine on the fly: spine = the
   consecutive pairs along `:order` that exist in `:internal-edges`,
   off-spine = the rest.

   Pure data in/out. No runtime dependency."
  (:require [clojure.set :as set]))

;; ============================================================================
;; Tarjan's SCC
;; ============================================================================

(defn- tarjan
  "Strongly-connected components that contain a cycle (size > 1, or
   single-node with self-loop) of the directed graph (`nodes` set,
   `succ` map node→succ-set). Each returned SCC is a vector."
  [nodes succ]
  (let [idx*  (volatile! 0)
        idx   (volatile! {})
        low   (volatile! {})
        stk   (volatile! [])
        on    (volatile! #{})
        out   (volatile! [])]
    (letfn [(visit [v]
              (let [i @idx*]
                (vswap! idx* inc)
                (vswap! idx assoc v i)
                (vswap! low assoc v i))
              (vswap! stk conj v)
              (vswap! on conj v)
              (doseq [w (succ v)]
                (cond
                  (not (@idx w)) (do (visit w)
                                     (vswap! low update v min (@low w)))
                  (@on w)        (vswap! low update v min (@idx w))))
              (when (= (@low v) (@idx v))
                (let [c (loop [acc []]
                          (let [w (peek @stk)]
                            (vswap! stk pop)
                            (vswap! on disj w)
                            (let [acc' (conj acc w)]
                              (if (= w v) acc' (recur acc')))))]
                  (when (or (> (count c) 1)
                            (contains? (succ (first c)) (first c)))
                    (vswap! out conj c)))))]
      (doseq [v nodes :when (not (@idx v))] (visit v))
      @out)))

;; ============================================================================
;; DAG shape detection
;; ============================================================================

(defn- chain-order
  "Topological order of a chain (single source → single sink, every
   middle node in/out 1). nil if the graph isn't a chain."
  [nodes succ pred]
  (let [n    (count nodes)
        srcs (filter #(empty? (get pred %)) nodes)]
    (when (= 1 (count srcs))
      (loop [acc [(first srcs)] cur (first srcs)]
        (let [nxt (get succ cur)]
          (cond
            (and (empty? nxt) (= (count acc) n))
            acc

            (and (= 1 (count nxt)) (< (count acc) n))
            (recur (conj acc (first nxt)) (first nxt))

            :else
            nil))))))

(defn- weak-components
  "Weakly-connected components of `vs` under `edges` (each edge `[u v]`)."
  [vs edges]
  (let [adj (reduce (fn [m [u v]]
                      (cond-> m
                        (and (vs u) (vs v))
                        (-> (update u (fnil conj #{}) v)
                            (update v (fnil conj #{}) u))))
                    {} edges)]
    (loop [seen #{} acc []]
      (let [start (first (set/difference vs seen))]
        (if (nil? start)
          acc
          (let [comp (loop [stk [start] cs #{}]
                       (if-let [v (peek stk)]
                         (if (cs v)
                           (recur (pop stk) cs)
                           (recur (into (pop stk) (get adj v)) (conj cs v)))
                         cs))]
            (recur (into seen comp) (conj acc comp))))))))

(defn- node-key
  "Sortable key over either a path-vector or an SCC marker. Cycles
   sort distinctly from atoms; ties within each break by structure."
  [n]
  (if (and (map? n) (::scc n))
    [1 (vec (sort (::scc n)))]
    [0 n]))

(defn- kahn-order
  "Kahn's topological sort with deterministic lex tie-breaking via
   `node-key`. Caller guarantees the input is a DAG (no cycles)."
  [nodes succ pred]
  (let [pred-cnt (volatile! (into {} (for [n nodes] [n (count (get pred n))])))]
    (loop [out []
           remaining (set nodes)]
      (if (empty? remaining)
        out
        (let [u (->> remaining
                     (filter #(zero? (get @pred-cnt %)))
                     (sort-by node-key)
                     first)]
          (when-not u
            (throw (ex-info "kahn-order: input contains a cycle"
                            {:remaining remaining})))
          (doseq [v (get succ u)]
            (vswap! pred-cnt update v dec))
          (recur (conj out u) (disj remaining u)))))))

(defn- build-succ [edges]
  (reduce (fn [m [u v]] (update m u (fnil conj #{}) v)) {} edges))

(defn- build-pred [edges]
  (reduce (fn [m [u v]] (update m v (fnil conj #{}) u)) {} edges))

(defn- topo-positions
  "Map node → topological position (deterministic via `node-key` tie-break)."
  [paths succ pred]
  (let [pred-cnt (volatile! (into {} (for [n paths] [n (count (get pred n))])))]
    (loop [out {}
           remaining (set paths)]
      (if (empty? remaining)
        out
        (let [u (->> remaining
                     (filter #(zero? (get @pred-cnt %)))
                     (sort-by node-key)
                     first)]
          (when-not u
            (throw (ex-info "topo-positions: input contains a cycle"
                            {:remaining remaining})))
          (doseq [v (get succ u)]
            (vswap! pred-cnt update v dec))
          (recur (assoc out u (count out))
                 (disj remaining u)))))))

(defn- find-cut-node
  "First node X (in topo order, between S and T exclusive) that lies on
   every S→T path. nil if none exists. X is a cut iff no edge u→v skips
   it in topological order — i.e., no edge has topo(u) < topo(X) <
   topo(v) with u ≠ X and v ≠ X."
  [paths edges S T succ pred]
  (let [pos (topo-positions paths succ pred)
        ps  (pos S)
        pt  (pos T)]
    (->> paths
         (remove #{S T})
         (filter #(< ps (pos %) pt))
         (sort-by pos)
         (filter (fn [X]
                   (let [px (pos X)]
                     (not-any? (fn [[u v]]
                                 (and (not= u X) (not= v X)
                                      (< (pos u) px)
                                      (> (pos v) px)))
                               edges))))
         first)))

(defn- reachable-set
  "Set of nodes reachable from `start` via a succ-map (directed walk)."
  [start succ-map]
  (loop [stk [start] seen #{}]
    (if (empty? stk)
      seen
      (let [v (peek stk)]
        (if (seen v)
          (recur (pop stk) seen)
          (recur (into (pop stk) (get succ-map v)) (conj seen v)))))))

(defn- subgraph-between
  "The sub-DAG of nodes that lie on some directed S→X path (S, X
   inclusive). Returns `[paths edges]`."
  [edges S X]
  (let [succ   (build-succ edges)
        pred   (build-pred edges)
        from-S (reachable-set S succ)
        to-X   (reachable-set X pred)
        sub    (set/intersection from-S to-X)
        sub-e  (filterv (fn [[u v]] (and (sub u) (sub v))) edges)]
    [(vec sub) sub-e]))

(declare classify-dag)

(defn- clean-parallel-component?
  "Is `comp` a clean two-terminal sub-DAG: single source connected from
   S, single sink connected to T, and no S/T edges crossing the
   component except through that one source/sink?"
  [comp edges S T]
  (let [comp-set  (set comp)
        int-edges (filter (fn [[u v]] (and (comp-set u) (comp-set v))) edges)
        cs (build-succ int-edges)
        cp (build-pred int-edges)
        comp-srcs (filter #(empty? (get cp %)) comp-set)
        comp-snks (filter #(empty? (get cs %)) comp-set)]
    (when (and (= 1 (count comp-srcs))
               (= 1 (count comp-snks)))
      (let [src-of (first comp-srcs)
            snk-of (first comp-snks)]
        (and (some (fn [[u v]] (and (= u S) (= v src-of))) edges)
             (some (fn [[u v]] (and (= u snk-of) (= v T))) edges)
             (every? (fn [[u v]]
                       (or (not= u S)
                           (not (comp-set v))
                           (= v src-of)))
                     edges)
             (every? (fn [[u v]]
                       (or (not= v T)
                           (not (comp-set u))
                           (= u snk-of)))
                     edges))))))

(defn- try-parallel-decompose
  "If the level is a strict parallel composition — every middle weak
   component is a clean two-terminal sub-DAG with a single source
   connected to S and a single sink connected to T — return the
   scatter-gather record (each branch recursively decomposed). nil
   otherwise."
  [paths edges S T]
  (let [middle (disj (set paths) S T)
        middle-edges (filter (fn [[u v]] (and (middle u) (middle v))) edges)
        components (weak-components middle middle-edges)
        has-direct? (boolean (some (fn [[u v]] (and (= u S) (= v T))) edges))]
    (when (and (or (seq components) has-direct?)
               (every? #(clean-parallel-component? % edges S T) components))
      (let [branch-shapes
            (mapv (fn [comp]
                    (let [comp-set (set comp)
                          int-edges (filterv (fn [[u v]] (and (comp-set u) (comp-set v)))
                                             edges)]
                      (classify-dag (vec comp-set) int-edges)))
                  components)]
        {:kind :scatter-gather
         :source S
         :sink T
         :branches (cond-> branch-shapes
                     has-direct? (conj {:kind :empty}))}))))

(defn- chain-pre-elements
  "Order-elements representing shape `s` in a chain context where s
   ends at node X. Drops the trailing X for a chain; for a non-chain
   shape, returns `[s.source s]` so the chain reads
   `<source>, <sg-record>, X, ...`."
  [s]
  (case (:kind s)
    :chain (vec (butlast (:order s)))
    :empty []
    :scatter-gather [(:source s) s]
    [s]))

(defn- chain-post-elements
  "Order-elements representing shape `s` in a chain context where s
   starts at node X. Drops the leading X for a chain; for a non-chain
   shape, returns `[s s.sink]` so the chain reads
   `..., X, <sg-record>, <sink>`."
  [s]
  (case (:kind s)
    :chain (vec (rest (:order s)))
    :empty []
    :scatter-gather [s (:sink s)]
    [s]))

(defn- classify-dag
  "Recursively decompose a DAG into a series-parallel shape tree.
   Returns one of:
     {:kind :empty}
     {:kind :chain :order [...]}              ; :order may include scatter-gather records
     {:kind :scatter-gather :source S :sink T :branches [shape...]}
     {:kind :prime :order [...] :internal-edges [...]}

   Algorithm (Valdes–Tarjan–Lawler-style two-terminal recognition):
     1. Trivial: empty / single-node / no edges.
     2. Multi-source or multi-sink → :prime.
     3. Chain — every internal node has in/out-degree 1.
     4. Series cut — find a node X (between S and T) that lies on every
        S→T path; recurse on the two halves and compose as a chain.
     5. Parallel — middle splits into clean two-terminal components,
        each connected once-to-S and once-to-T; recurse on each branch.
     6. Otherwise → :prime (e.g., the Wheatstone bridge)."
  [paths edges]
  (let [path-set (set paths)]
    (cond
      (empty? path-set) {:kind :empty}

      :else
      (let [succ    (build-succ edges)
            pred    (build-pred edges)
            srcs    (filter #(empty? (get pred %)) path-set)
            snks    (filter #(empty? (get succ %)) path-set)
            ->prime (fn [] {:kind :prime
                            :order (kahn-order path-set succ pred)
                            :internal-edges (vec edges)})]
        (cond
          (= 1 (count path-set))
          {:kind :chain :order (vec path-set)}

          (empty? edges)
          (->prime)

          (or (not= 1 (count srcs)) (not= 1 (count snks)))
          (->prime)

          :else
          (let [S (first srcs) T (first snks)]
            (or
              (when-let [order (chain-order path-set succ pred)]
                {:kind :chain :order (vec order)})
              (when-let [X (find-cut-node path-set edges S T succ pred)]
                (let [[g1-paths g1-edges] (subgraph-between edges S X)
                      [g2-paths g2-edges] (subgraph-between edges X T)
                      s1 (classify-dag g1-paths g1-edges)
                      s2 (classify-dag g2-paths g2-edges)]
                  {:kind :chain
                   :order (vec (concat (chain-pre-elements s1)
                                       [X]
                                       (chain-post-elements s2)))}))
              (try-parallel-decompose path-set edges S T)
              (->prime))))))))

;; ============================================================================
;; Per-level: SCC condensation + recursive labeling
;; ============================================================================

(defn- eades-order
  "Greedy Eades–Lin–Smyth FAS heuristic over an SCC: peel sources
   (append to s1), peel sinks (prepend to s2), then pick the remaining
   node with max (out-deg − in-deg). Ties broken by lex order on the
   node key. Returns a vec of members; consecutive-pair edges in the
   result form a near-maximum spanning chain, leaving the FAS as the
   off-spine remainder."
  [members internal-edges]
  (let [members (set members)]
    (loop [remaining members
           edges    (filter (fn [[u v]] (and (members u) (members v)))
                            internal-edges)
           s1       []
           s2       ()]
      (if (empty? remaining)
        (vec (concat s1 s2))
        (let [succ (reduce (fn [m [u v]] (update m u (fnil conj #{}) v)) {} edges)
              pred (reduce (fn [m [u v]] (update m v (fnil conj #{}) u)) {} edges)
              srcs (->> remaining
                        (filter #(empty? (get pred %)))
                        (sort-by node-key))
              snks (->> remaining
                        (filter #(empty? (get succ %)))
                        (sort-by node-key))
              drop-node (fn [u edges]
                          (filterv (fn [[a b]] (and (not= a u) (not= b u)))
                                   edges))]
          (cond
            (seq srcs)
            (let [u (first srcs)]
              (recur (disj remaining u) (drop-node u edges) (conj s1 u) s2))

            (seq snks)
            (let [u (first snks)]
              (recur (disj remaining u) (drop-node u edges) s1 (cons u s2)))

            :else
            (let [score   (fn [n] (- (count (get succ n)) (count (get pred n))))
                  ranked  (sort-by (juxt (comp - score) node-key) remaining)
                  u       (first ranked)]
              (recur (disj remaining u) (drop-node u edges) (conj s1 u) s2))))))))

(declare unmark-shape-tree)

(defn- unmark-elt
  "Replace SCC markers with :cycle records inside an :order element.
   For nested shape records (scatter-gather, etc.), recurse via
   `unmark-shape-tree`."
  [unmark elt]
  (cond
    (and (map? elt) (#{:chain :scatter-gather :cycle :prime :empty} (:kind elt)))
    (unmark-shape-tree elt unmark)
    :else
    (unmark elt)))

(defn- unmark-shape-tree
  "Walk a shape tree and replace SCC markers (`{::scc members}`) with
   :cycle records via `unmark`. Recurses through nested shape records
   in :order (scatter-gather inside chain) and :branches
   (sub-shapes inside scatter-gather)."
  [shape unmark]
  (let [unmark-edge (fn [[u v]] [(unmark u) (unmark v)])
        unmark-order (fn [order] (mapv #(unmark-elt unmark %) order))]
    (case (:kind shape)
      :empty shape
      :chain (update shape :order unmark-order)
      :scatter-gather (-> shape
                          (update :source unmark)
                          (update :sink unmark)
                          (update :branches
                                  (fn [bs] (mapv #(unmark-shape-tree % unmark) bs))))
      :cycle (-> shape
                 (update :order unmark-order)
                 (cond->
                  (:internal-edges shape)
                   (update :internal-edges (fn [es] (mapv unmark-edge es)))))
      :prime (-> shape
                 (update :order unmark-order)
                 (update :internal-edges (fn [es] (mapv unmark-edge es)))))))

(defn- ->cycle [members internal-edges]
  (let [int-edges (vec internal-edges)]
    {:kind :cycle
     :order (eades-order members int-edges)
     :internal-edges int-edges}))

(defn- classify-level
  "Classify the conn-subgraph at one combinator level. Condense any
   non-trivial SCCs, classify the resulting DAG, then re-inflate
   markers as `{:kind :cycle ...}` records inside `:order`/`:branches`/
   `:source`/`:sink`/`:internal-edges`. This makes the function total
   over arbitrary directed graphs while keeping a uniform output
   vocabulary."
  [paths edges]
  (let [path-set (set paths)
        succ     (reduce (fn [m [u v]] (update m u (fnil conj #{}) v)) {} edges)
        sccs     (tarjan path-set succ)]
    (cond
      (empty? sccs)
      (classify-dag path-set edges)

      ;; Whole level is one SCC: emit cycle directly
      (and (= 1 (count sccs)) (= path-set (set (first sccs))))
      (->cycle (first sccs) edges)

      :else
      (let [scc-members (mapv set sccs)
            scc-marker  (mapv #(do {::scc %}) scc-members)
            marker-of   (into {} (for [[members marker] (map vector scc-members scc-marker)
                                       v members]
                                   [v marker]))
            scc-eds-of  (zipmap scc-marker
                                (mapv (fn [members]
                                        (vec (filter (fn [[u v]]
                                                       (and (members u) (members v)))
                                                     edges)))
                                      scc-members))
            super-nodes (mapv #(or (marker-of %) %) paths)
            super-set   (set super-nodes)
            super-edges (->> edges
                             (mapv (fn [[u v]] [(or (marker-of u) u)
                                                (or (marker-of v) v)]))
                             (filter (fn [[u v]] (not= u v)))
                             distinct
                             vec)
            dag-shape   (classify-dag super-set super-edges)
            unmark      (fn [x]
                          (if-let [members (::scc x)]
                            (->cycle members (get scc-eds-of x))
                            x))]
        (unmark-shape-tree dag-shape unmark)))))

;; ============================================================================
;; Tree assembly
;; ============================================================================

(defn decompose
  "Build the shape tree from a `step/topology` output. Each container
   gets a `:shape`; leaves are leaves. The returned tree mirrors the
   combinator hierarchy exactly."
  [topology]
  (let [nodes  (:nodes topology)
        edges  (mapv (juxt :from-path :to-path) (:edges topology))
        nodes-by-parent (group-by #(vec (butlast (:path %))) nodes)
        edges-by-parent (group-by #(vec (butlast (first %))) edges)]
    (letfn [(walk [parent-path]
              (let [children (sort-by :path (get nodes-by-parent parent-path []))
                    paths    (mapv :path children)
                    eds      (get edges-by-parent parent-path [])]
                {:shape    (classify-level paths eds)
                 :children (mapv (fn [n]
                                   (let [base (select-keys n [:path :name :kind :combinator])]
                                     (if (= :container (:kind n))
                                       (let [sub (walk (:path n))]
                                         (-> base
                                             (assoc :shape (:shape sub))
                                             (assoc :children (:children sub))))
                                       base)))
                                 children)}))]
      (-> (walk [])
          (assoc :path [] :name "root" :kind :container)))))
