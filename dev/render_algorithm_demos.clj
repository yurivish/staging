(ns render-algorithm-demos
  "Three handcrafted pipelines that exercise specific algorithmic
   boundaries in `toolkit.datapotamus.shape` / `render`. Each demo
   isolates one algorithm whose necessity isn't visible in the regular
   pipelines (which all happen to be series-parallel with symmetric
   cycles).

   Run:
     clojure -J--add-opens=java.base/java.nio=ALL-UNNAMED \\
             -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \\
             -M -e '(load-file \"/work/dev/render_algorithm_demos.clj\")'

   Writes `/work/dev/data/algorithm_demos.md`."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [toolkit.datapotamus.render :as render]
            [toolkit.datapotamus.step :as step]))

;; ============================================================================
;; Fixture 1 — `:prime` shape (multi-source / multi-sink)
;; ============================================================================
;;
;; Boundary exercised: classify-dag → prime fallback (shape.clj:155).
;;
;; Two independent input streams converge at one merge node, get
;; processed, then fan out to two independent sinks. Sources within the
;; container = {stream-a, stream-b}; sinks = {sink-x, sink-y}. The
;; multi-source/multi-sink check in classify-dag short-circuits the
;; chain and scatter-gather classifiers, so the level falls through to
;; `:prime` and Kahn topo sort produces `:order`.

(defn- prime-bowtie []
  (let [stream-a (step/step :stream-a
                            {:ins {:in ""} :outs {:out ""}}
                            (fn [_ _ _] {}))
        stream-b (step/step :stream-b
                            {:ins {:in ""} :outs {:out ""}}
                            (fn [_ _ _] {}))
        merge    (step/step :merge
                            {:ins {:from-a "" :from-b ""}
                             :outs {:to-x "" :to-y ""}}
                            (fn [_ _ _] {}))
        sink-x   (step/step :sink-x
                            {:ins {:in ""} :outs {}}
                            (fn [_ _ _] {}))
        sink-y   (step/step :sink-y
                            {:ins {:in ""} :outs {}}
                            (fn [_ _ _] {}))]
    (-> (step/beside stream-a stream-b merge sink-x sink-y)
        (step/connect [:stream-a :out] [:merge :from-a])
        (step/connect [:stream-b :out] [:merge :from-b])
        (step/connect [:merge :to-x]   [:sink-x :in])
        (step/connect [:merge :to-y]   [:sink-y :in]))))

;; ============================================================================
;; Fixture 2 — Asymmetric SCC (Eades–Lin–Smyth tiebreaker matters)
;; ============================================================================
;;
;; Boundary exercised: eades-order's `out-deg − in-deg` heuristic
;; (shape.clj:212).
;;
;; A 5-node validation loop. `intake` fans out to two checkers; both
;; feed `decide`, which sends to `retry`, which closes the loop back to
;; `intake`. Members have asymmetric degrees:
;;
;;   intake:        out=2, in=1   ← source-leaning  (score +1)
;;   format-check:  out=1, in=1   ← middle          (score  0)
;;   content-check: out=1, in=1   ← middle          (score  0)
;;   decide:        out=1, in=2   ← sink-leaning    (score −1)
;;   retry:         out=1, in=1   ← back to intake  (score  0)
;;
;; Every cycle in the existing fixtures (round-robin / stealing
;; workers) is symmetric — every member has the same in/out degree —
;; so the heuristic just re-discovers what symmetry already implies.
;; Here the heuristic actually picks the visually correct order:
;; intake first (highest score), then peel sources, then decide and
;; retry trail behind. Reads left-to-right with a single back-edge
;; closing the loop.

(defn- asymmetric-cycle []
  (let [intake        (step/step :intake
                                 {:ins {:loop ""}
                                  :outs {:to-format "" :to-content ""}}
                                 (fn [_ _ _] {}))
        format-check  (step/step :format-check
                                 {:ins {:in ""} :outs {:result ""}}
                                 (fn [_ _ _] {}))
        content-check (step/step :content-check
                                 {:ins {:in ""} :outs {:result ""}}
                                 (fn [_ _ _] {}))
        decide        (step/step :decide
                                 {:ins {:from-format "" :from-content ""}
                                  :outs {:retry ""}}
                                 (fn [_ _ _] {}))
        retry         (step/step :retry
                                 {:ins {:in ""} :outs {:loop ""}}
                                 (fn [_ _ _] {}))]
    (-> (step/beside intake format-check content-check decide retry)
        (step/connect [:intake :to-format]      [:format-check :in])
        (step/connect [:intake :to-content]     [:content-check :in])
        (step/connect [:format-check :result]   [:decide :from-format])
        (step/connect [:content-check :result]  [:decide :from-content])
        (step/connect [:decide :retry]          [:retry :in])
        (step/connect [:retry :loop]            [:intake :loop]))))

;; ============================================================================
;; Fixture 3 — Faithfulness-gate bail (WL classes + partial bipartite)
;; ============================================================================
;;
;; Boundary exercised: bipartite-pattern's `:partial` return path
;; (render.clj:438). When WL refinement converges to equal-sized
;; classes but the inter-class edges fit none of the four faithful
;; patterns (fan-in / fan-out / bijection / complete), block
;; aggregation bails and the renderer falls back to per-member output.
;;
;; Six-node SCC: 3 shifters + 3 targets. Each shifter sends to two
;; consecutive targets in a cyclic shift; each target sends to one
;; shifter (a bijection back-edge). All three shifters have identical
;; structural neighborhoods (1 in, 2 outs to two B-class members), so
;; WL keeps them in one class. Same for targets. But the shifter→target
;; bipartite has 6 edges out of a possible 9: not bijection (|ij|≠|Ci|),
;; not complete (|ij|≠|Ci|·|Cj|). Pattern is `:partial`, so
;; `aggregate-cycle` returns nil and the per-member fallback kicks in.

(defn- partial-bipartite-cycle []
  (let [mk-shifter (fn [id]
                     (step/step id
                                {:ins {:from-target ""}
                                 :outs {:to-a "" :to-b ""}}
                                (fn [_ _ _] {})))
        mk-target  (fn [id]
                     (step/step id
                                {:ins {:from-a "" :from-b ""}
                                 :outs {:to-shifter ""}}
                                (fn [_ _ _] {})))
        s0 (mk-shifter :shifter-0)
        s1 (mk-shifter :shifter-1)
        s2 (mk-shifter :shifter-2)
        t0 (mk-target  :target-0)
        t1 (mk-target  :target-1)
        t2 (mk-target  :target-2)]
    (-> (step/beside s0 s1 s2 t0 t1 t2)
        ;; Forward: each shifter goes to two consecutive targets.
        (step/connect [:shifter-0 :to-a] [:target-0 :from-a])
        (step/connect [:shifter-0 :to-b] [:target-1 :from-a])
        (step/connect [:shifter-1 :to-a] [:target-1 :from-b])
        (step/connect [:shifter-1 :to-b] [:target-2 :from-a])
        (step/connect [:shifter-2 :to-a] [:target-2 :from-b])
        (step/connect [:shifter-2 :to-b] [:target-0 :from-b])
        ;; Back: each target → next shifter (a clean bijection).
        (step/connect [:target-0 :to-shifter] [:shifter-1 :from-target])
        (step/connect [:target-1 :to-shifter] [:shifter-2 :from-target])
        (step/connect [:target-2 :to-shifter] [:shifter-0 :from-target]))))

;; ============================================================================
;; Output
;; ============================================================================

(defn- block [title commentary stepmap]
  (let [lines (render/render stepmap)]
    (str "## " title "\n\n"
         commentary "\n\n"
         "```\n" (str/join "\n" lines) "\n```\n")))

(defn -main
  ([] (-main "/work/dev/data/algorithm_demos.md"))
  ([out-path]
   (let [doc
         (str
          "# Datapotamus rendering — algorithm-boundary demos\n\n"
          "_Auto-generated by `dev/render_algorithm_demos.clj`._\n\n"
          "Three handcrafted pipelines, each isolating one algorithm in\n"
          "the shape/render pass whose necessity isn't visible in the\n"
          "regular pipeline registry (which is series-parallel with\n"
          "symmetric cycles by construction).\n\n"

          (block "prime-bowtie"
                 (str "**Algorithm: Kahn topo sort + `:prime` fallback.**\n\n"
                      "Two independent input streams converge at one merge\n"
                      "node, get processed, then fan out to two independent\n"
                      "sinks. Two sources (`stream-a`, `stream-b`) and two\n"
                      "sinks (`sink-x`, `sink-y`) at one container level\n"
                      "fail the single-source / single-sink check in\n"
                      "`classify-dag`, so it falls through to `:prime`.\n"
                      "`kahn-order` produces `:order` and every edge ends\n"
                      "up in `:internal-edges`; off-spine forward arrows\n"
                      "(`→`) annotate the non-consecutive edges.\n\n"
                      "What `:prime` looks like: notice the `→` annotations\n"
                      "and the absence of bracket rails — there's no\n"
                      "scatter-gather structure to draw.")
                 (prime-bowtie))

          "\n"

          (block "asymmetric-cycle"
                 (str "**Algorithm: Eades–Lin–Smyth FAS heuristic.**\n\n"
                      "A 5-node validation loop where members have\n"
                      "asymmetric in/out degrees:\n\n"
                      "| node | out | in | score |\n"
                      "|---|---|---|---|\n"
                      "| `intake` | 2 | 1 | +1 |\n"
                      "| `format-check` | 1 | 1 | 0 |\n"
                      "| `content-check` | 1 | 1 | 0 |\n"
                      "| `decide` | 1 | 2 | −1 |\n"
                      "| `retry` | 1 | 1 | 0 |\n\n"
                      "Eades runs: no empty-source / empty-sink to peel,\n"
                      "so it picks the highest-score node — `intake`.\n"
                      "After dropping intake's edges, both checkers are\n"
                      "empty sources (lex tiebreak picks `content-check`\n"
                      "first), then `format-check`, `decide`, `retry`.\n"
                      "The single back-edge (`retry → intake`) closes the\n"
                      "loop and shows up as `← intake` on the last line —\n"
                      "exactly the FAS we want.\n\n"
                      "Compare with our other cycles (`stealing-workers`,\n"
                      "`round-robin-workers`): every member there has the\n"
                      "same degree by construction, so the score is a\n"
                      "wash and lex tiebreak does all the work. Here the\n"
                      "score actually discriminates: it puts the\n"
                      "source-leaning node at the start regardless of its\n"
                      "name.")
                 (asymmetric-cycle))

          "\n"

          (block "partial-bipartite-cycle"
                 (str "**Algorithm: faithfulness gate (`bipartite-pattern`\n"
                      "→ `:partial` → bail to per-member rendering).**\n\n"
                      "Six-node SCC: three `shifter-*` and three\n"
                      "`target-*`. Each shifter sends to two consecutive\n"
                      "targets in a cyclic shift; each target sends back\n"
                      "to one shifter. All three shifters have identical\n"
                      "structural neighborhoods (1 in, 2 outs to\n"
                      "target-class members) so 1-WL refinement keeps them\n"
                      "in one class; same for targets.\n\n"
                      "But the shifter→target edge multiset has 6 edges\n"
                      "out of 9 possible: not a bijection (|ij|≠|Ci|), not\n"
                      "complete (|ij|≠|Ci|·|Cj|). The pattern is\n"
                      "`:partial`, so `aggregate-cycle` returns nil and\n"
                      "the renderer falls back to per-member output. (Each\n"
                      "shifter / target id has a digit suffix, so\n"
                      "name-pattern compression *would* still try to\n"
                      "compress runs in `:order` if the Eades order\n"
                      "happened to interleave them; here it doesn't.)\n\n"
                      "Without this demo the gate's `:partial` branch is\n"
                      "untested by the registry — every real cycle in the\n"
                      "registry hits one of the four faithful patterns.")
                 (partial-bipartite-cycle)))]
     (io/make-parents out-path)
     (spit out-path doc)
     (println (format "Wrote %s" out-path)))))

(-main)
