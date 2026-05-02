(ns toolkit.datapotamus.shape
  "Shape decomposition over a stepmap topology.

   `(decompose topology)` walks `(step/topology stepmap)` into a tree
   that mirrors the combinator hierarchy. Every container carries a
   `:shape` describing how its conns wire its children:

     :empty            no conns at this level
     :chain            linear sequence; `:order` is the source→sink path
     :scatter-gather   one source → K disjoint branches → one sink;
                       `:source`, `:sink`, `:branches` (vec of node-vecs)
     :cycle            non-trivial SCC; `:members`, `:back-edges`
     :prime            irreducible (honest fallback); `:sccs` lists
                       any cycles found at this level

   Within `:order` and `:branches`, an element is either a single
   full-path or a nested `{:kind :cycle ...}` record. This recursion
   lets the same vocabulary describe any directed graph (acyclic,
   cyclic, or mixed) at any combinator level.

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

(defn- classify-dag
  "Classify an acyclic graph (`paths` set, `edges` vec of `[u v]`)."
  [paths edges]
  (cond
    (empty? paths)
    {:kind :empty}

    (empty? edges)
    (if (<= (count paths) 1)
      {:kind :chain :order (vec paths)}
      {:kind :prime :reason :no-edges-multi-node})

    :else
    (let [succ (reduce (fn [m [u v]] (update m u (fnil conj #{}) v)) {} edges)
          pred (reduce (fn [m [u v]] (update m v (fnil conj #{}) u)) {} edges)
          srcs (filter #(empty? (get pred %)) paths)
          snks (filter #(empty? (get succ %)) paths)]
      (cond
        (or (not= 1 (count srcs)) (not= 1 (count snks)))
        {:kind :prime :reason [:multi-endpoint (count srcs) (count snks)]}

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
                {:kind     :scatter-gather
                 :source   src
                 :sink     snk
                 :branches (cond-> (mapv vec comps) has-direct? (conj []))}
                {:kind :prime :reason :unclassified}))))))))

;; ============================================================================
;; Per-level: SCC condensation + recursive labeling
;; ============================================================================

(defn- ->cycle [members back-edges]
  {:kind :cycle :members (vec members) :back-edges (vec back-edges)})

(defn- classify-level
  "Classify the conn-subgraph at one combinator level. Condense any
   non-trivial SCCs, classify the resulting DAG, then re-inflate
   markers as `{:kind :cycle ...}` records inside `:order`/`:branches`.
   This makes the function total over arbitrary directed graphs while
   keeping a uniform output vocabulary."
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
        (cond-> dag-shape
          (:order dag-shape)
          (update :order #(mapv unmark %))

          (:branches dag-shape)
          (update :branches (fn [bs] (mapv #(mapv unmark %) bs)))

          (= :prime (:kind dag-shape))
          (assoc :sccs (mapv (fn [m e] (->cycle m e))
                             scc-members
                             (mapv #(get scc-eds-of %) scc-marker))))))))

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
                                   (let [base (select-keys n [:path :name :kind])]
                                     (if (= :container (:kind n))
                                       (let [sub (walk (:path n))]
                                         (-> base
                                             (assoc :shape (:shape sub))
                                             (assoc :children (:children sub))))
                                       base)))
                                 children)}))]
      (-> (walk [])
          (assoc :path [] :name "root" :kind :container)))))
