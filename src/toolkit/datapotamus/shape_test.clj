(ns toolkit.datapotamus.shape-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.datapotamus.combinators.core :as cc]
            [toolkit.datapotamus.shape :as shape]
            [toolkit.datapotamus.step :as step]))

(defn- count-leaves
  "Walk a shape tree, return the number of leaf children."
  [tree]
  (let [walk (fn walk [n]
               (if (= :leaf (:kind n))
                 1
                 (apply + 0 (map walk (:children n)))))]
    (walk tree)))

(defn- topology-leaves [topology]
  (count (filter #(= :leaf (:kind %)) (:nodes topology))))

;; ============================================================================
;; :chain
;; ============================================================================

(deftest chain-basic
  (testing "three-step serial → top-level :chain"
    (let [sm (step/serial (step/step :a inc)
                          (step/step :b inc)
                          (step/step :c inc))
          t  (shape/decompose (step/topology sm))]
      (is (= :chain (-> t :shape :kind)))
      (is (= [[:a] [:b] [:c]] (-> t :shape :order))))))

(deftest chain-of-one
  (testing "single-step is a 1-node chain"
    (let [sm (step/step :only inc)
          t  (shape/decompose (step/topology sm))]
      (is (= :chain (-> t :shape :kind))))))

;; ============================================================================
;; :scatter-gather
;; ============================================================================

(deftest scatter-gather-basic
  (testing "c/parallel inside a serial yields scatter-gather inside the parallel scope"
    (let [sm (step/serial
              (cc/parallel :ann
                           {:a (step/step :sa inc)
                            :b (step/step :sb inc)
                            :c (step/step :sc inc)})
              (step/sink))
          t  (shape/decompose (step/topology sm))
          ;; descend into the :ann container's shape
          ann (->> (:children t)
                   (filter #(= [:ann] (:path %)))
                   first)]
      (is (= :scatter-gather (-> ann :shape :kind)))
      (is (= 3 (count (-> ann :shape :branches)))))))

;; ============================================================================
;; :cycle
;; ============================================================================

(deftest cycle-two-node
  (testing "controller↔pipeline-style 2-cycle is :cycle"
    (let [agent (step/step :agent
                           {:ins  {:in "" :loop ""}
                            :outs {:to "" :final ""}}
                           (fn [_ _ _] {}))
          worker (step/step :worker {:ins {:in ""} :outs {:out ""}}
                            (fn [_ _ _] {}))
          sm (-> (step/beside agent worker)
                 (step/connect [:agent :to]   [:worker :in])
                 (step/connect [:worker :out] [:agent :loop])
                 (step/input-at  [:agent :in])
                 (step/output-at [:agent :final]))
          t  (shape/decompose (step/topology sm))]
      (is (= :cycle (-> t :shape :kind)))
      (is (= #{[:agent] [:worker]} (set (-> t :shape :order))))
      (is (= 2 (count (-> t :shape :internal-edges)))))))

;; ============================================================================
;; :prime fallback
;; ============================================================================

(deftest prime-fallback
  (testing "two disjoint nodes at one level → :prime"
    (let [sm (step/beside (step/step :a inc) (step/step :b inc))
          t  (shape/decompose (step/topology sm))]
      (is (= :prime (-> t :shape :kind))))))

;; ============================================================================
;; Property: leaves preserved
;; ============================================================================

;; ============================================================================
;; :series-parallel recursion — non-trivial sub-shapes inside a chain
;; ============================================================================

(deftest y-shape-decomposes-as-chain-with-nested-parallel
  (testing "src → {A, B} → C → sink: chain[src, parallel(A, B), C, sink]"
    (let [src (step/step :src {:ins {:in ""} :outs {:to-a "" :to-b ""}} (fn [_ _ _] {}))
          a   (step/step :a   {:ins {:in ""} :outs {:out ""}}            (fn [_ _ _] {}))
          b   (step/step :b   {:ins {:in ""} :outs {:out ""}}            (fn [_ _ _] {}))
          c   (step/step :c   {:ins {:from-a "" :from-b ""} :outs {:out ""}} (fn [_ _ _] {}))
          snk (step/sink)
          sm (-> (step/beside src a b c snk)
                 (step/connect [:src :to-a] [:a :in])
                 (step/connect [:src :to-b] [:b :in])
                 (step/connect [:a :out]    [:c :from-a])
                 (step/connect [:b :out]    [:c :from-b])
                 (step/connect [:c :out]    [:sink :in]))
          t (shape/decompose (step/topology sm))
          shape (:shape t)]
      (is (= :chain (:kind shape))
          "top-level shape is a chain (not scatter-gather with one weird branch)")
      (is (= 4 (count (:order shape)))
          "chain has 4 elements: src, parallel-sub-shape, c, sink")
      (is (= [:src] (first (:order shape))))
      (is (= [:c]   (nth (:order shape) 2)))
      (is (= [:sink] (last (:order shape))))
      (let [sg (second (:order shape))]
        (is (and (map? sg) (= :scatter-gather (:kind sg)))
            "second order element is a nested scatter-gather record")
        (is (= 2 (count (:branches sg))))
        (is (= #{{:kind :chain :order [[:a]]}
                 {:kind :chain :order [[:b]]}}
               (set (:branches sg)))
            "branches are chain shape records, not bare path vecs")))))

(deftest series-cut-recurses-cleanly
  (testing "src → A → {B, C} → D → sink: chain[src, A, parallel(B, C), D, sink]"
    (let [src (step/step :src {:ins {:in ""} :outs {:out ""}}    (fn [_ _ _] {}))
          a   (step/step :a   {:ins {:in ""} :outs {:to-b "" :to-c ""}} (fn [_ _ _] {}))
          b   (step/step :b   {:ins {:in ""} :outs {:out ""}}    (fn [_ _ _] {}))
          c   (step/step :c   {:ins {:in ""} :outs {:out ""}}    (fn [_ _ _] {}))
          d   (step/step :d   {:ins {:from-b "" :from-c ""} :outs {:out ""}} (fn [_ _ _] {}))
          snk (step/sink)
          sm (-> (step/beside src a b c d snk)
                 (step/connect [:src :out]  [:a :in])
                 (step/connect [:a :to-b]   [:b :in])
                 (step/connect [:a :to-c]   [:c :in])
                 (step/connect [:b :out]    [:d :from-b])
                 (step/connect [:c :out]    [:d :from-c])
                 (step/connect [:d :out]    [:sink :in]))
          shape (:shape (shape/decompose (step/topology sm)))]
      (is (= :chain (:kind shape)))
      (is (= 5 (count (:order shape))))
      (is (= [:src] (first (:order shape))))
      (is (= [:a] (nth (:order shape) 1)))
      (let [sg (nth (:order shape) 2)]
        (is (and (map? sg) (= :scatter-gather (:kind sg))))
        (is (= 2 (count (:branches sg)))))
      (is (= [:d] (nth (:order shape) 3)))
      (is (= [:sink] (nth (:order shape) 4))))))

(deftest mixed-cycles-and-sp-recurse-via-unmark
  (testing "Two cycles in series at one level: src → (A↔B) → join → (C↔D) → sink"
    (let [src   (step/step :src  {:ins {:in ""} :outs {:out ""}}                       (fn [_ _ _] {}))
          a     (step/step :a    {:ins {:in "" :loop ""} :outs {:fwd "" :back ""}}     (fn [_ _ _] {}))
          b     (step/step :b    {:ins {:in ""} :outs {:back ""}}                      (fn [_ _ _] {}))
          join  (step/step :join {:ins {:in ""} :outs {:out ""}}                       (fn [_ _ _] {}))
          c     (step/step :c    {:ins {:in "" :loop ""} :outs {:fwd "" :back ""}}     (fn [_ _ _] {}))
          d     (step/step :d    {:ins {:in ""} :outs {:back ""}}                      (fn [_ _ _] {}))
          snk   (step/sink)
          sm (-> (step/beside src a b join c d snk)
                 (step/connect [:src :out]   [:a :in])
                 (step/connect [:a :back]    [:b :in])
                 (step/connect [:b :back]    [:a :loop])
                 (step/connect [:a :fwd]     [:join :in])
                 (step/connect [:join :out]  [:c :in])
                 (step/connect [:c :back]    [:d :in])
                 (step/connect [:d :back]    [:c :loop])
                 (step/connect [:c :fwd]     [:sink :in]))
          shape (:shape (shape/decompose (step/topology sm)))]
      (is (= :chain (:kind shape))
          "outer level decomposes as chain (cycles condense to opaque markers, then chain across them)")
      (is (= 5 (count (:order shape)))
          ":order = [src, cycle1, join, cycle2, sink]")
      (is (= [:src] (first (:order shape))))
      (is (= [:join] (nth (:order shape) 2)))
      (is (= [:sink] (last (:order shape))))
      (is (= :cycle (:kind (nth (:order shape) 1)))
          "first cycle properly unmarked")
      (is (= :cycle (:kind (nth (:order shape) 3)))
          "second cycle properly unmarked"))))

(deftest wheatstone-bridge-stays-prime
  (testing "src → {A, B}, A → {C, T}, B → C, C → T (T = sink): non-SP, falls to :prime"
    (let [src (step/step :src {:ins {:in ""} :outs {:to-a "" :to-b ""}} (fn [_ _ _] {}))
          a   (step/step :a   {:ins {:in ""} :outs {:to-c "" :to-t ""}} (fn [_ _ _] {}))
          b   (step/step :b   {:ins {:in ""} :outs {:to-c ""}}          (fn [_ _ _] {}))
          c   (step/step :c   {:ins {:from-a "" :from-b ""} :outs {:to-t ""}} (fn [_ _ _] {}))
          t   (step/step :t   {:ins {:from-a "" :from-c ""} :outs {}}   (fn [_ _ _] {}))
          sm (-> (step/beside src a b c t)
                 (step/connect [:src :to-a] [:a :in])
                 (step/connect [:src :to-b] [:b :in])
                 (step/connect [:a :to-c]   [:c :from-a])
                 (step/connect [:a :to-t]   [:t :from-a])
                 (step/connect [:b :to-c]   [:c :from-b])
                 (step/connect [:c :to-t]   [:t :from-c]))
          shape (:shape (shape/decompose (step/topology sm)))]
      (is (= :prime (:kind shape))
          "Wheatstone bridge is the canonical non-SP DAG; correctly classified as :prime"))))

(deftest leaves-preserved
  (testing "decompose preserves leaf count for various shapes"
    (doseq [[label sm] [[:chain     (step/serial (step/step :a inc)
                                                  (step/step :b inc)
                                                  (step/step :c inc))]
                        [:parallel  (cc/parallel :p
                                                 {:x (step/step :sx inc)
                                                  :y (step/step :sy inc)})]
                        [:nested    (step/serial
                                     (cc/parallel :p1
                                                  {:a (step/step :a1 inc)
                                                   :b (step/step :b1 inc)})
                                     (cc/parallel :p2
                                                  {:c (step/step :c1 inc)
                                                   :d (step/step :d1 inc)}))]]]
      (let [topo (step/topology sm)
            tree (shape/decompose topo)]
        (is (= (topology-leaves topo) (count-leaves tree))
            (str "leaves mismatch for " label))))))

;; ============================================================================
;; Generative property tests over arbitrary SP-DAGs
;; ============================================================================
;;
;; Hand-written tests prove the algorithm works for the cases we
;; thought of. To check it works for ALL series-parallel DAGs (not
;; just the shapes our combinators produce), we generate random
;; SP-trees by recursive composition and verify the decomposition
;; matches.

(def ^:private gen-sp-tree
  "Random SP composition tree:
     :leaf | [:series children] | [:parallel children]
   `gen/recursive-gen` shrinks towards :leaf, so failing cases
   minimize automatically."
  (gen/recursive-gen
   (fn [g]
     (gen/one-of
      [(gen/let [n (gen/choose 2 4)
                 children (gen/vector g n)]
         [:series children])
       (gen/let [n (gen/choose 2 3)
                 children (gen/vector g n)]
         [:parallel children])]))
   (gen/return :leaf)))

(defn- materialize-sp
  "Convert an SP-tree to a flat `{:nodes :edges}` graph between `src`
   and `snk`. `:leaf` becomes a single edge; `:series` chains children
   via fresh intermediate nodes; `:parallel` overlays children sharing
   `src`/`snk`."
  [tree src snk fresh-id]
  (cond
    (= tree :leaf)
    {:nodes #{src snk} :edges #{[src snk]}}

    (= :series (first tree))
    (let [children (second tree)
          n (count children)
          intermediates (vec (repeatedly (dec n) fresh-id))
          endpoints (vec (concat [src] intermediates [snk]))
          subs (mapv #(materialize-sp %1 %2 %3 fresh-id)
                     children
                     (butlast endpoints)
                     (rest endpoints))]
      {:nodes (reduce set/union #{} (map :nodes subs))
       :edges (reduce set/union #{} (map :edges subs))})

    (= :parallel (first tree))
    (let [children (second tree)
          subs (mapv #(materialize-sp % src snk fresh-id) children)]
      {:nodes (reduce set/union #{} (map :nodes subs))
       :edges (reduce set/union #{} (map :edges subs))})))

(defn- sp-tree->topology
  "Materialize an SP-tree into a `step/topology`-compatible dict —
   flat (no containers), every node a leaf."
  [tree]
  (let [counter (atom 0)
        fresh-id (fn []
                   (let [id (swap! counter inc)]
                     [(keyword (str "n" id))]))
        src (fresh-id)
        snk (fresh-id)
        graph (materialize-sp tree src snk fresh-id)]
    {:nodes (mapv (fn [path] {:path path :kind :leaf})
                  (sort (:nodes graph)))
     :edges (mapv (fn [[u v]] {:from-path u :to-path v})
                  (:edges graph))}))

(defn- walk-shapes
  "Lazy seq of every shape record in the tree, including those nested
   inside chain :order and scatter-gather :branches."
  [shape]
  (cons shape
        (lazy-seq
         (case (:kind shape)
           :chain (mapcat (fn [elt]
                            (when (and (map? elt) (:kind elt))
                              (walk-shapes elt)))
                          (:order shape))
           :scatter-gather (mapcat walk-shapes (:branches shape))
           []))))

(defn- contains-prime? [shape]
  (boolean (some #(= :prime (:kind %)) (walk-shapes shape))))

(defspec sp-dag-decomposes-without-prime
  50
  (prop/for-all [tree gen-sp-tree]
    (let [topology (sp-tree->topology tree)
          shape-tree (shape/decompose topology)]
      (not (contains-prime? (:shape shape-tree))))))

(defspec sp-dag-leaves-preserved
  50
  (prop/for-all [tree gen-sp-tree]
    (let [topology (sp-tree->topology tree)
          shape-tree (shape/decompose topology)]
      (= (topology-leaves topology) (count-leaves shape-tree)))))

(defspec sp-dag-decomposition-is-idempotent
  50
  (prop/for-all [tree gen-sp-tree]
    (let [topology (sp-tree->topology tree)]
      (= (shape/decompose topology) (shape/decompose topology)))))

;; ----------------------------------------------------------------------------
;; Wheatstone insertion: take an SP-DAG, replace one of its edges with a
;; wheatstone bridge sub-graph. The result is non-SP, so the
;; decomposition must include a :prime somewhere in the tree.

(defn- insert-wheatstone
  "Pick the first edge in `topology` and replace it with a wheatstone
   bridge: instead of `u → v`, we get `u → A, u → B, A → C, A → v,
   B → C, C → v` with three fresh intermediate nodes."
  [topology]
  (let [edges (:edges topology)]
    (when-let [{:keys [from-path to-path]} (first edges)]
      (let [counter (atom (count (:nodes topology)))
            fresh #(let [id (swap! counter inc)]
                     [(keyword (str "w" id))])
            a (fresh) b (fresh) c (fresh)
            new-nodes [{:path a :kind :leaf}
                       {:path b :kind :leaf}
                       {:path c :kind :leaf}]
            new-edges [{:from-path from-path :to-path a}
                       {:from-path from-path :to-path b}
                       {:from-path a :to-path c}
                       {:from-path a :to-path to-path}
                       {:from-path b :to-path c}
                       {:from-path c :to-path to-path}]]
        {:nodes (into (vec (:nodes topology)) new-nodes)
         :edges (into (vec (rest edges)) new-edges)}))))

(defspec wheatstone-insertion-yields-prime
  50
  (prop/for-all [tree gen-sp-tree]
    (let [topology (sp-tree->topology tree)
          perturbed (insert-wheatstone topology)]
      (or (nil? perturbed)
          (let [shape-tree (shape/decompose perturbed)]
            (contains-prime? (:shape shape-tree)))))))
