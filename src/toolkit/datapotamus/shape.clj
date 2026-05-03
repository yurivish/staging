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

(defn- classify-dag
  "Classify an acyclic graph (`paths` set, `edges` vec of `[u v]`)."
  [paths edges]
  (if (empty? paths)
    {:kind :empty}
    (let [succ    (reduce (fn [m [u v]] (update m u (fnil conj #{}) v)) {} edges)
          pred    (reduce (fn [m [u v]] (update m v (fnil conj #{}) u)) {} edges)
          srcs    (filter #(empty? (get pred %)) paths)
          snks    (filter #(empty? (get succ %)) paths)
          ->prime (fn [] {:kind :prime
                          :order (kahn-order paths succ pred)
                          :internal-edges (vec edges)})]
      (cond
        (empty? edges)
        (if (<= (count paths) 1)
          {:kind :chain :order (vec paths)}
          (->prime))

        (or (not= 1 (count srcs)) (not= 1 (count snks)))
        (->prime)

        :else
        (let [src   (first srcs)
              snk   (first snks)
              order (chain-order paths succ pred)]
          (if order
            {:kind :chain :order (vec order)}
            (let [middle      (disj paths src snk)
                  comps       (weak-components middle edges)
                  comp-of     (reduce (fn [m c] (reduce #(assoc %1 %2 c) m c))
                                      {} comps)
                  edges-ok?   (every? (fn [[u v]]
                                        (or (and (= u src) (= v snk))
                                            (and (= u src) (middle v))
                                            (and (middle u) (= v snk))
                                            (and (middle u) (middle v)
                                                 (= (comp-of u) (comp-of v)))))
                                      edges)
                  has-direct? (boolean (some (fn [[u v]] (and (= u src) (= v snk))) edges))]
              (if (and edges-ok? (or (seq comps) has-direct?))
                (let [order-branch
                      (fn [comp]
                        (let [comp-set (set comp)
                              within   (filter (fn [[u v]]
                                                 (and (comp-set u) (comp-set v)))
                                               edges)
                              succ' (reduce (fn [m [u v]] (update m u (fnil conj #{}) v))
                                            {} within)
                              pred' (reduce (fn [m [u v]] (update m v (fnil conj #{}) u))
                                            {} within)]
                          (or (chain-order comp-set succ' pred')
                              ;; Fallback: branch isn't a clean chain
                              ;; (multiple sources/sinks). Preserve some
                              ;; deterministic order.
                              (vec (sort comp-set)))))]
                  {:kind     :scatter-gather
                   :source   src
                   :sink     snk
                   :branches (cond-> (mapv (fn [comp] (vec (order-branch comp))) comps)
                               has-direct? (conj []))})
                (->prime)))))))))

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
                            x))
            unmark-edge (fn [[u v]] [(unmark u) (unmark v)])]
        (cond-> dag-shape
          (:order dag-shape)
          (update :order #(mapv unmark %))

          (:branches dag-shape)
          (update :branches (fn [bs] (mapv #(mapv unmark %) bs)))

          (:source dag-shape)
          (update :source unmark)

          (:sink dag-shape)
          (update :sink unmark)

          (:internal-edges dag-shape)
          (update :internal-edges #(mapv unmark-edge %)))))))

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
