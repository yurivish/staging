(ns toolkit.datapotamus.menagerie-test
  "Menagerie of pipeline shapes — gallery + regression fixtures.

   Twenty examples covering every glyph and shape kind in the rendering
   vocabulary. Each entry is either a real datapotamus combinator
   pipeline (preferred when a combinator naturally produces the shape)
   or a hand-built `{:nodes :edges}` topology (for shapes no combinator
   produces, e.g. wheatstone variants).

   Two purposes share the fixture list:
     - `deftest menagerie-decomposes` asserts the expected top-level
       shape kind and that render is total for every entry.
     - `regenerate-kb!` writes `kb/pipeline-menagerie.md` from the same
       fixtures, so the gallery can never drift from the renderer."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus.combinators.core :as cc]
            [toolkit.datapotamus.combinators.workers :as workers]
            [toolkit.datapotamus.render :as render]
            [toolkit.datapotamus.shape :as shape]
            [toolkit.datapotamus.step :as step]))

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- humanize [sid]
  (-> sid name (str/replace "-" " ")))

(defn- raw
  "Hand-build a topology from `nodes` (vec of keywords) and `edges`
   (vec of [from-kw to-kw] pairs). Each node becomes a single-segment
   leaf path; humanized name matches what `step/topology` would produce."
  [nodes edges]
  {:nodes (mapv (fn [n] {:path [n] :kind :leaf :name (humanize n)}) nodes)
   :edges (mapv (fn [[u v]] {:from-path [u] :to-path [v]}) edges)})

(defn- noop-step
  "Step with explicit port spec and a no-op handler — the topology
   walker only needs ports + structure."
  ([id ins outs]
   (step/step id
              {:ins  (zipmap ins  (repeat ""))
               :outs (zipmap outs (repeat ""))}
              (fn [_ _ _] {}))))

(defn- topo
  "Coerce a build result to a topology. Stepmaps go through
   `step/topology`; raw topology maps pass through."
  [x]
  (if (and (map? x) (contains? x :procs))
    (step/topology x)
    x))

;; ============================================================================
;; Fixture builders
;; ============================================================================

;; --- 1. Singleton ---
(defn- ex-singleton []
  (step/step :only inc))

;; --- 2. Tiny chain ---
(defn- ex-tiny-chain []
  (step/serial (step/step :a inc) (step/step :b inc)))

;; --- 3. Long named chain ---
(defn- ex-long-chain []
  (step/serial (step/step :read inc)
               (step/step :parse inc)
               (step/step :validate inc)
               (step/step :enrich inc)
               (step/step :dedupe inc)
               (step/step :score inc)
               (step/step :rank inc)
               (step/step :sink inc)))

;; --- 4. Symmetric Y, K=3 ---
(defn- ex-symmetric-y []
  (step/serial
   (cc/parallel :ann
                {:a (step/step :sa inc)
                 :b (step/step :sb inc)
                 :c (step/step :sc inc)})
   (step/sink)))

;; --- 5. Asymmetric Y (raw) ---
;; src → {a1, b1→b2, c1→c2→c3} → sink. Three branches of length 1, 2, 3.
(defn- ex-asymmetric-y []
  (raw [:src :a1 :b1 :b2 :c1 :c2 :c3 :sink]
       [[:src :a1] [:a1 :sink]
        [:src :b1] [:b1 :b2] [:b2 :sink]
        [:src :c1] [:c1 :c2] [:c2 :c3] [:c3 :sink]]))

;; --- 6. Y with empty branch (raw) ---
;; src → {a, (direct), b→c} → sink. The empty branch is a direct
;; src→sink edge.
(defn- ex-y-with-empty-branch []
  (raw [:src :a :b :c :sink]
       [[:src :a] [:a :sink]
        [:src :sink]
        [:src :b] [:b :c] [:c :sink]]))

;; --- 7. K× collapsed workers ---
(defn- ex-k-workers []
  (workers/round-robin-workers :pool 5 (step/step :worker inc)))

;; --- 8. K× heterogeneous-but-symmetric (parallel of identical sub-chains) ---
;; Each branch wraps an identical (parse → score) sub-chain in its own
;; scope, so per-branch leaf names are the same and the renderer's
;; branch-signature compression collapses all four into one K× sample.
(defn- ex-k-symmetric-chains []
  (let [mk-branch (fn [id]
                    (step/serial id
                                 (step/step :parse inc)
                                 (step/step :score inc)))]
    (cc/parallel :ensemble
                 {:w0 (mk-branch :w0)
                  :w1 (mk-branch :w1)
                  :w2 (mk-branch :w2)
                  :w3 (mk-branch :w3)})))

;; --- 9. Nested parallel-in-parallel (raw) ---
;; outer src → {chain a, {b1, b2}, chain c} → outer sink, where the
;; middle branch is itself a parallel block.
(defn- ex-nested-parallel []
  (raw [:src :a :bsrc :b1 :b2 :bsink :c :sink]
       [[:src :a]   [:a :sink]
        [:src :bsrc] [:bsrc :b1] [:bsrc :b2]
        [:b1 :bsink] [:b2 :bsink] [:bsink :sink]
        [:src :c]   [:c :sink]]))

;; --- 10. Y inside a chain ---
;; (step/serial src (cc/parallel ...) sink) — series-cut recursion.
(defn- ex-y-in-chain []
  (step/serial
   (step/step :src inc)
   (cc/parallel :branch
                {:a (step/step :sa inc)
                 :b (step/step :sb inc)})
   (step/sink)))

;; --- 11. Two-node feedback cycle ---
(defn- ex-two-node-cycle []
  (let [agent  (noop-step :agent  [:in :loop] [:to :final])
        worker (noop-step :worker [:in]       [:out])]
    (-> (step/beside agent worker)
        (step/connect [:agent :to]   [:worker :in])
        (step/connect [:worker :out] [:agent :loop])
        (step/input-at  [:agent :in])
        (step/output-at [:agent :final]))))

;; --- 12. 3-node ring (raw) ---
(defn- ex-three-ring []
  (raw [:a :b :c]
       [[:a :b] [:b :c] [:c :a]]))

;; --- 13. K× workers around a coordinator (raw cycle) ---
;; coordinator → 6 identical workers → coordinator. K× collapse inside
;; an SCC.
(defn- ex-k-cycle []
  (let [ws (mapv #(keyword (str "worker-" %)) (range 6))]
    (raw (cons :coordinator ws)
         (concat (for [w ws] [:coordinator w])
                 (for [w ws] [w :coordinator])))))

;; --- 14. Asymmetric validation loop (raw) ---
;; Mirrors dev/render_algorithm_demos.clj's asymmetric-cycle. Eades
;; FAS heuristic earns its keep here.
(defn- ex-validation-loop []
  (raw [:intake :format-check :content-check :decide :retry]
       [[:intake :format-check]
        [:intake :content-check]
        [:format-check :decide]
        [:content-check :decide]
        [:decide :retry]
        [:retry :intake]]))

;; --- 15. Cycle inside a chain (raw) ---
;; src → A → (B↔C) → D → sink. SCC with surrounding SP context;
;; exercises unmark-shape-tree.
(defn- ex-cycle-in-chain []
  (raw [:src :a :b :c :d :sink]
       [[:src :a] [:a :b]
        [:b :c] [:c :b]
        [:c :d] [:d :sink]]))

;; --- 16. Wheatstone bridge (raw) ---
;; Canonical 5-node prime: src → {a, b}, a → {c, t}, b → c, c → t.
(defn- ex-wheatstone []
  (raw [:src :a :b :c :t]
       [[:src :a] [:src :b]
        [:a :c]   [:a :t]
        [:b :c]
        [:c :t]]))

;; --- 17. Bowtie (raw) ---
;; {stream-a, stream-b} → merge → {sink-x, sink-y}. Multi-source /
;; multi-sink → :prime fallback.
(defn- ex-bowtie []
  (raw [:stream-a :stream-b :merge :sink-x :sink-y]
       [[:stream-a :merge] [:stream-b :merge]
        [:merge :sink-x]   [:merge :sink-y]]))

;; --- 18. Long-arm wheatstone (raw) ---
;; Wheatstone with the b→c arm expanded into b→b1→b2→c. Larger prime
;; interior makes the missing edge visualization noticeable.
(defn- ex-long-arm-wheatstone []
  (raw [:src :a :b :b1 :b2 :c :t]
       [[:src :a] [:src :b]
        [:a :c]   [:a :t]
        [:b :b1]  [:b1 :b2] [:b2 :c]
        [:c :t]]))

;; --- 19. Prime inside a chain (raw) ---
;; src → A → wheatstone(p,q,r,s,u) → D → sink.
(defn- ex-prime-in-chain []
  (raw [:src :a :p :q :r :s :u :d :sink]
       [[:src :a] [:a :p]
        [:p :q]   [:p :r]
        [:q :s]   [:q :u]
        [:r :s]
        [:s :u]
        [:u :d]   [:d :sink]]))

;; --- 20. Two K× stages in series ---
;; Two round-robin pools chained: K=4 then K=3. Two K× blocks
;; separated by a chain transition.
(defn- ex-two-k-stages []
  (step/serial
   (workers/round-robin-workers :extract 4 (step/step :fetch inc))
   (workers/round-robin-workers :transform 3 (step/step :score inc))))

;; ============================================================================
;; Menagerie definition
;; ============================================================================

(def menagerie
  [{:id 1  :name "singleton"           :title "Singleton"
    :kind :real :build ex-singleton :expected :chain
    :source-form "(step/step :only inc)"
    :caption "A single step. Baseline for everything else."}

   {:id 2  :name "tiny-chain"          :title "Tiny chain"
    :kind :real :build ex-tiny-chain :expected :chain
    :source-form "(step/serial (step/step :a inc)\n             (step/step :b inc))"
    :caption "Two-step chain. Smallest chain with a fall-through ↓."}

   {:id 3  :name "long-chain"          :title "Long named chain"
    :kind :real :build ex-long-chain :expected :chain
    :source-form (str "(step/serial (step/step :read inc)\n"
                      "             (step/step :parse inc)\n"
                      "             ;; ...\n"
                      "             (step/step :sink inc))")
    :caption "Eight-stage ETL. Readability stress test for the chain renderer."}

   {:id 4  :name "symmetric-y"         :title "Symmetric Y, K=3"
    :kind :real :build ex-symmetric-y :expected :chain
    :source-form (str "(step/serial\n"
                      "  (cc/parallel :ann\n"
                      "               {:a (step/step :sa inc)\n"
                      "                :b (step/step :sb inc)\n"
                      "                :c (step/step :sc inc)})\n"
                      "  (step/sink))")
    :caption "Three heterogeneous branches under a single rail `⎢`."}

   {:id 5  :name "asymmetric-y"        :title "Asymmetric Y"
    :kind :raw :build ex-asymmetric-y :expected :scatter-gather
    :source-form (str "{:nodes [:src :a1 :b1 :b2 :c1 :c2 :c3 :sink]\n"
                      " :edges [[:src :a1] [:a1 :sink]\n"
                      "         [:src :b1] [:b1 :b2] [:b2 :sink]\n"
                      "         [:src :c1] [:c1 :c2] [:c2 :c3] [:c3 :sink]]}")
    :caption "Three branches of length 1 / 2 / 3. Each branch's exit row (a1, b2, c3) carries a left-column `↓` showing it feeds `sink` below — distinct from in-rail `⎢↓` which marks in-branch chain flow."}

   {:id 6  :name "y-with-empty-branch" :title "Y with empty branch"
    :kind :raw :build ex-y-with-empty-branch :expected :scatter-gather
    :source-form (str "{:nodes [:src :a :b :c :sink]\n"
                      " :edges [[:src :a] [:a :sink]\n"
                      "         [:src :sink]              ; direct shortcut\n"
                      "         [:src :b] [:b :c] [:c :sink]]}")
    :caption "One branch is a direct src→sink edge with no intermediate steps. The `(direct)` marker keeps the rail count honest, and its row carries a branch-exit `↓` — unambiguously its own branch, not attached to the row above."}

   {:id 7  :name "k-workers"           :title "K× collapsed workers"
    :kind :real :build ex-k-workers :expected :chain
    :source-form "(workers/round-robin-workers :pool 5 (step/step :worker inc))"
    :caption "Five identical workers behind a router/join. 1-WL refinement collapses them to a single `K×` row."}

   {:id 8  :name "k-symmetric-chains"  :title "K× symmetric multi-step branches"
    :kind :real :build ex-k-symmetric-chains :expected :chain
    :source-form (str "(let [mk-branch (fn [id]\n"
                      "                  (step/serial id\n"
                      "                               (step/step :parse inc)\n"
                      "                               (step/step :score inc)))]\n"
                      "  (cc/parallel :ensemble\n"
                      "               {:w0 (mk-branch :w0) :w1 (mk-branch :w1)\n"
                      "                :w2 (mk-branch :w2) :w3 (mk-branch :w3)}))")
    :caption "Four parallel branches, each itself a 2-step chain. Branch-signature compression collapses them to one K× sample, and the `score` row carries the K× block's branch-exit `↓` showing flow into `ensemble fan in`."}

   {:id 9  :name "nested-parallel"     :title "Nested parallel-in-parallel"
    :kind :raw :build ex-nested-parallel :expected :scatter-gather
    :source-form (str "{:nodes [:src :a :bsrc :b1 :b2 :bsink :c :sink]\n"
                      " :edges [[:src :a] [:a :sink]\n"
                      "         [:src :bsrc] [:bsrc :b1] [:bsrc :b2]\n"
                      "         [:b1 :bsink] [:b2 :bsink] [:bsink :sink]\n"
                      "         [:src :c] [:c :sink]]}")
    :caption "Outer Y where the middle branch is itself a Y. Exercises nested `⎢ ⎢` rails."}

   {:id 10 :name "y-in-chain"          :title "Y inside a chain"
    :kind :real :build ex-y-in-chain :expected :chain
    :source-form (str "(step/serial\n"
                      "  (step/step :src inc)\n"
                      "  (cc/parallel :branch\n"
                      "               {:a (step/step :sa inc)\n"
                      "                :b (step/step :sb inc)})\n"
                      "  (step/sink))")
    :caption "Series-cut recursion: chain wraps a parallel block in the middle."}

   {:id 11 :name "two-node-cycle"      :title "Two-node feedback cycle"
    :kind :real :build ex-two-node-cycle :expected :cycle
    :source-form (str "(let [agent  (noop-step :agent  [:in :loop] [:to :final])\n"
                      "      worker (noop-step :worker [:in]       [:out])]\n"
                      "  (-> (step/beside agent worker)\n"
                      "      (step/connect [:agent :to]   [:worker :in])\n"
                      "      (step/connect [:worker :out] [:agent :loop])))")
    :caption "Smallest non-trivial `:cycle` — agent ↔ worker."}

   {:id 12 :name "three-ring"          :title "3-node ring"
    :kind :raw :build ex-three-ring :expected :cycle
    :source-form "{:nodes [:a :b :c] :edges [[:a :b] [:b :c] [:c :a]]}"
    :caption "A→B→C→A. Eades order produces a single back-edge `⮥` closing the loop."}

   {:id 13 :name "k-cycle"             :title "K× workers around a coordinator"
    :kind :raw :build ex-k-cycle :expected :cycle
    :source-form (str "(let [ws (mapv #(keyword (str \"worker-\" %)) (range 6))]\n"
                      "  ;; coordinator → each worker; each worker → coordinator\n"
                      "  ...)")
    :caption "Six structurally identical workers surrounding a coordinator inside one SCC. WL refinement collapses workers to one class; the bipartite pattern (fan-out / fan-in) renders cleanly."}

   {:id 14 :name "validation-loop"     :title "Asymmetric validation loop"
    :kind :raw :build ex-validation-loop :expected :cycle
    :source-form (str "{:nodes [:intake :format-check :content-check :decide :retry]\n"
                      " :edges [[:intake :format-check] [:intake :content-check]\n"
                      "         [:format-check :decide] [:content-check :decide]\n"
                      "         [:decide :retry] [:retry :intake]]}")
    :caption "Five-node retry loop with mixed in/out degrees. The Eades–Lin–Smyth FAS heuristic actually discriminates here (see also `dev/render_algorithm_demos.clj`)."}

   {:id 15 :name "cycle-in-chain"      :title "Cycle inside a chain"
    :kind :raw :build ex-cycle-in-chain :expected :chain
    :source-form (str "{:nodes [:src :a :b :c :d :sink]\n"
                      " :edges [[:src :a] [:a :b]\n"
                      "         [:b :c] [:c :b]   ; cycle\n"
                      "         [:c :d] [:d :sink]]}")
    :caption "SCC condensed into the chain spine via `unmark-shape-tree`. Reads as chain[src, A, cycle(B,C), D, sink]. The `(cycle)` marker is suffixed onto `b` (the first cycle member) — no standalone header row."}

   {:id 16 :name "wheatstone"          :title "Wheatstone bridge"
    :kind :raw :build ex-wheatstone :expected :prime
    :source-form (str "{:nodes [:src :a :b :c :t]\n"
                      " :edges [[:src :a] [:src :b]\n"
                      "         [:a :c]   [:a :t]\n"
                      "         [:b :c]\n"
                      "         [:c :t]]}")
    :caption "The canonical non-SP DAG. `:prime` fallback. Top-level primes use `⮧` annotations to surface every off-spine edge — the structure is fully readable."}

   {:id 17 :name "bowtie"              :title "Bowtie (multi-source / multi-sink)"
    :kind :raw :build ex-bowtie :expected :prime
    :source-form (str "{:nodes [:stream-a :stream-b :merge :sink-x :sink-y]\n"
                      " :edges [[:stream-a :merge] [:stream-b :merge]\n"
                      "         [:merge :sink-x]   [:merge :sink-y]]}")
    :caption "Two sources / two sinks short-circuit the chain and SG classifiers; falls through to `:prime` via Kahn topo sort."}

   {:id 18 :name "long-arm-wheatstone" :title "Long-arm wheatstone"
    :kind :raw :build ex-long-arm-wheatstone :expected :prime
    :source-form (str "{:nodes [:src :a :b :b1 :b2 :c :t]\n"
                      " :edges [[:src :a] [:src :b]\n"
                      "         [:a :c]   [:a :t]\n"
                      "         [:b :b1]  [:b1 :b2] [:b2 :c]   ; long arm\n"
                      "         [:c :t]]}")
    :caption "Wheatstone with one cross-arm expanded into a 3-step chain. The longer arm becomes the spine; cross-edges still surface via `⮧` annotations at top level."}

   {:id 19 :name "prime-in-chain"      :title "Prime inside a chain"
    :kind :raw :build ex-prime-in-chain :expected :chain
    :source-form (str "{:nodes [:src :a :p :q :r :s :u :d :sink]\n"
                      " :edges [[:src :a] [:a :p]\n"
                      "         [:p :q] [:p :r] [:q :s] [:q :u]\n"
                      "         [:r :s] [:s :u]\n"
                      "         [:u :d] [:d :sink]]}")
    :caption "A wheatstone-flavored prime cluster embedded between chain neighbors. The `(prime)` marker is suffixed onto `q` (first interior member); interior edges (q→s, q→u, r→s, s→u) and boundary-crossing edges show via `⮧` annotations."}

   {:id 20 :name "two-k-stages"        :title "Two K× stages in series"
    :kind :real :build ex-two-k-stages :expected :chain
    :source-form (str "(step/serial\n"
                      "  (workers/round-robin-workers :extract 4 (step/step :fetch inc))\n"
                      "  (workers/round-robin-workers :transform 3 (step/step :score inc)))")
    :caption "Two K× blocks chained. Shows how chain `↓` propagates between two parallel stages."}])

;; ============================================================================
;; Tests
;; ============================================================================

(deftest menagerie-decomposes-to-expected-shape
  (testing "every fixture decomposes to its expected top-level shape kind"
    (doseq [{:keys [id name build expected]} menagerie]
      (testing (str id " " name)
        (let [t (topo (build))
              decomp (shape/decompose t)]
          (is (= expected (-> decomp :shape :kind))))))))

(deftest menagerie-renders-without-crashing
  (testing "every fixture renders to a non-empty seq of strings"
    (doseq [{:keys [id name build]} menagerie]
      (testing (str id " " name)
        (let [t (topo (build))
              lines (render/render t)]
          (is (seq lines))
          (is (every? string? lines)))))))

(deftest menagerie-render-mentions-every-leaf
  (testing "render is total — every leaf node name appears in the output"
    (doseq [{:keys [id name build]} menagerie
            ;; K× collapse intentionally hides per-instance names; skip
            ;; entries where 1-WL or pattern compression elides leaves.
            :when (not (#{"k-workers" "k-symmetric-chains"
                          "k-cycle" "two-k-stages"} name))]
      (testing (str id " " name)
        (let [t (topo (build))
              lines (render/render t)
              text (str/join "\n" lines)
              leaf-names (->> (:nodes t)
                              (filter #(= :leaf (:kind %)))
                              (map :name))]
          (doseq [ln leaf-names]
            (is (str/includes? text ln)
                (str "leaf " (pr-str ln) " missing from render"))))))))

;; ============================================================================
;; KB gallery generator
;; ============================================================================

(defn- shape-kind-label [k]
  (case k
    :chain          ":chain"
    :scatter-gather ":scatter-gather"
    :cycle          ":cycle"
    :prime          ":prime"
    :empty          ":empty"
    (pr-str k)))

(defn- entry-section
  "Render one fixture as a markdown section."
  [{:keys [id title kind expected source-form caption build]}]
  (let [t (topo (build))
        lines (render/render t)
        source-lang (case kind :real "clojure" :raw "edn")
        kind-label (case kind :real "real (datapotamus combinators)" :raw "raw topology")]
    (str "## " id ". " title "\n\n"
         caption "\n\n"
         "**Source** (" kind-label ", expected shape: `" (shape-kind-label expected) "`):\n\n"
         "```" source-lang "\n"
         source-form "\n"
         "```\n\n"
         "**Render:**\n\n"
         "```\n"
         (str/join "\n" lines) "\n"
         "```\n")))

(defn build-gallery-md
  "Build the full kb/pipeline-menagerie.md content from `fixtures`."
  [fixtures]
  (str
   "# Pipeline-shape menagerie\n\n"
   "_Auto-generated by `src/toolkit/datapotamus/menagerie_test.clj`._\n"
   "_To regenerate: `(toolkit.datapotamus.menagerie-test/regenerate-kb!)` from a REPL._\n\n"
   "Twenty pipeline shapes covering every glyph and shape kind in\n"
   "`toolkit.datapotamus.shape` + `render`. Real combinator pipelines\n"
   "where they exist; hand-built `{:nodes :edges}` topologies otherwise.\n"
   "Companion to `kb/pipeline-shapes.md` (vocabulary) and\n"
   "`kb/pipeline-diagrams.md` (glyphs).\n\n"
   "Several rendering limits previously documented in `kb/pipeline-diagrams.md` were\n"
   "surfaced and fixed while building this menagerie. Current behavior:\n\n"
   "  - **Branch-exit `↓` marker** (was: rail rows lacked a visual indicator they fed the next stage). Now: each branch's last row gets a left-column `↓`, distinct from in-rail `⎢↓` (in-branch chain flow). See #5, #6, #8.\n"
   "  - **Prime interior edges** (was: inline `(prime)` clusters listed nodes without edge annotations). Now: `render-inline-prime` runs `classified-annotations`, so interior and boundary-crossing edges show via `⮥`/`⮧`. See #19.\n"
   "  - **Inline cluster markers attach to a node** (was: `(cycle)` and `(prime)` floated as standalone rows, ambiguous about boundaries). Now: each marker is suffixed onto its first interior member's line. See #15, #19.\n"
   "  - **`(direct)` is now unambiguous** (was: a `(direct)` row above `sink` could read as attached to the previous branch). Now: branch-exit `↓` on each branch row makes `(direct)` clearly its own branch. See #6.\n\n"
   "## Coverage matrix\n\n"
   "Showcase example(s) per feature.\n\n"
   "| Feature | Examples |\n"
   "|---|---|\n"
   "| `↓` fall-through | 2, 3 |\n"
   "| `⎢` rail | 4, 5, 6, 9 |\n"
   "| Nested `⎢ ⎢` | 9 |\n"
   "| `K×` collapse | 7, 8, 13, 20 |\n"
   "| `⮥` back-edge | 11, 12, 13, 14, 15 |\n"
   "| `⮧` forward off-spine | 14, 16, 17, 18 |\n"
   "| `(direct)` empty branch | 6 |\n"
   "| `(cycle)` inline header | 15 |\n"
   "| `(prime)` inline header | 19 |\n"
   "| `:chain` shape (top level) | 1, 2, 3, 4, 7, 8, 10, 15, 19, 20 |\n"
   "| `:scatter-gather` shape (top level) | 5, 6, 9 |\n"
   "| `:cycle` shape (top level) | 11, 12, 13, 14 |\n"
   "| `:prime` shape (top level) | 16, 17, 18 |\n"
   "| Recursion (shape-in-shape) | 9, 10, 15, 19 |\n\n"
   "---\n\n"
   (str/join "\n" (map entry-section fixtures))))

(defn regenerate-kb!
  "Write `kb/pipeline-menagerie.md` from the current fixtures + renderer.
   Call from a REPL whenever fixtures or renderer output change.
   Returns the path written."
  ([] (regenerate-kb! "/work/kb/pipeline-menagerie.md"))
  ([path]
   (spit path (build-gallery-md menagerie))
   path))
