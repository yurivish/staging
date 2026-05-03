(ns toolkit.datapotamus.render-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus.combinators.core :as cc]
            [toolkit.datapotamus.combinators.workers :as cw]
            [toolkit.datapotamus.render :as render]
            [toolkit.datapotamus.step :as step]))

(defn- lines [sm] (render/render sm))

(deftest chain-renders-with-fall-through-rail
  (testing "three-step serial → all-but-last lines start with ↓"
    (let [sm (step/serial (step/step :a inc)
                          (step/step :b inc)
                          (step/step :c inc))]
      (is (= ["↓ a" "↓ b" "  c"] (lines sm))))))

(deftest single-leaf-no-fall-through
  (let [sm (step/step :only inc)]
    (is (= ["  only"] (lines sm)))))

(deftest two-cycle-uses-named-back-arrow
  (testing "agent ↔ worker: spine agent→worker, worker→agent rendered as ⬏ agent"
    (let [agent  (step/step :agent
                            {:ins {:in "" :loop ""}
                             :outs {:to "" :final ""}}
                            (fn [_ _ _] {}))
          worker (step/step :worker {:ins {:in ""} :outs {:out ""}}
                            (fn [_ _ _] {}))
          sm (-> (step/beside agent worker)
                 (step/connect [:agent :to]   [:worker :in])
                 (step/connect [:worker :out] [:agent :loop])
                 (step/input-at  [:agent :in])
                 (step/output-at [:agent :final]))
          out (lines sm)]
      (is (= 2 (count out)))
      (is (str/starts-with? (first out) "↓ ")
          "agent has fall-through to worker (spine edge)")
      ;; Backward edge always names the target — even adjacent.
      (is (str/includes? (second out) "⬏ agent")
          (str "expected '⬏ agent' on worker line; got: " (pr-str (second out))))
      (is (not (str/includes? (second out) "⬎"))))))

(deftest prime-two-disjoint-no-fall-through-no-annotations
  (let [sm (step/beside (step/step :a inc) (step/step :b inc))
        out (lines sm)]
    (is (= 2 (count out)))
    (is (every? #(str/starts-with? % "  ") out))
    (is (every? #(not (re-find #"[←→]" %)) out))))

(deftest scatter-gather-marks-branches-with-bracket-rail
  (testing "heterogeneous branches form a ⎢ rail (one row per branch)"
    (let [sm (step/serial
              (step/step :pre inc)
              (cc/parallel :p
                           {:x (step/step :sx inc)
                            :y (step/step :sy inc)
                            :z (step/step :sz inc)})
              (step/sink))
          out (lines sm)
          rail-lines (filter #(str/includes? % "⎢") out)]
      (is (= 3 (count rail-lines))
          (str "expected 3 ⎢ rail rows (one per branch); got: " (pr-str out))))))

(deftest scatter-gather-source-and-sink-no-fall-through
  (let [sm (cc/parallel :p
                        {:a (step/step :sa inc)
                         :b (step/step :sb inc)})
        out (lines sm)]
    (is (every? #(str/starts-with? % "  ") out)
        "no scatter-gather elements get ↓ — all start with two-space placeholder")))

(deftest identical-branches-compress-to-k-times
  (testing "round-robin-workers: K identical workers → '<K>× <inner>'"
    (let [sm (cw/round-robin-workers :pool 4 (step/step :work inc))
          out (lines sm)
          k-lines (filter #(re-find #"\b4×" %) out)]
      (is (= 1 (count k-lines))
          (str "expected exactly one '4×' compressed line, got: " (pr-str out))))))

(deftest leaf-only-branches-do-not-compress
  (testing "branches that are bare leaves with distinct names stay as-is"
    (let [sm (cc/parallel :p
                          {:a (step/step :sa inc)
                           :b (step/step :sb inc)
                           :c (step/step :sc inc)})
          out (lines sm)]
      (is (every? #(not (re-find #"\bx\b|\b3×" %)) out)
          "no K× compression on heterogeneous leaves"))))

(deftest accepts-stepmap-topology-and-shape-tree
  (let [sm   (step/step :only inc)
        topo (step/topology sm)
        tree (#'render/->tree sm)]
    (is (= ["  only"] (render/render sm)))
    (is (= ["  only"] (render/render topo)))
    (is (= ["  only"] (render/render tree)))))

(deftest empty-pipeline-renders-empty
  (let [topo {:nodes [] :edges []}]
    (is (= [] (render/render topo)))))

(deftest cycle-pattern-compression
  (testing "stealing-workers cycle compresses to K× pattern blocks"
    (let [;; A stealing-workers SCC with k=4 produces a cycle whose
          ;; :order contains repeating (sK, wK, eK) members.
          inner (step/handler-map
                 {:ports     {:ins {:in ""} :outs {:out ""}}
                  :on-data   (fn [_ _ _] {:out []})})
          sm (cw/stealing-workers :pool 4 inner)
          out (lines sm)
          k-lines (filter #(re-find #"\d+×" %) out)]
      (is (seq k-lines)
          (str "expected at least one K× pattern line; got: " (pr-str out))))))

(deftest in-branch-ft-lives-in-bracket-rail-not-left-column
  (testing "Multi-element parallel branches: ↓ for in-branch fall-through lives next to bracket char, not in left column"
    (let [sm (step/serial
              (step/step :pre inc)
              (cc/parallel :p
                           {:a (step/serial (step/step :a1 inc) (step/step :a2 inc))
                            :b (step/serial (step/step :b1 inc) (step/step :b2 inc))})
              (step/sink))
          out (lines sm)
          bracket-lines (filter #(re-find #"[⎡⎢⎣]" %) out)]
      ;; No bracket line should start with `↓ ` in the left column.
      (is (every? #(not (str/starts-with? % "↓ ")) bracket-lines)
          (str "expected no left-column ↓ on bracket lines, got: " (pr-str bracket-lines)))
      ;; At least one line should have a bracket-flush `↓` (in-branch fall-through).
      (is (some #(re-find #"[⎡⎢]↓" %) bracket-lines)
          (str "expected at least one bracket-flush ↓, got: " (pr-str bracket-lines))))))

(deftest scatter-gather-branches-render-in-chain-order
  (testing "branch elements appear in source-chain order, not arbitrary set order"
    (let [;; Each branch is a 3-step chain `pre → mid → post`.
          mk (fn [pre mid post]
               (step/serial (step/step pre inc) (step/step mid inc) (step/step post inc)))
          sm (step/serial
              (step/step :start inc)
              (cc/parallel :p
                           {:a (mk :pre-a :mid-a :post-a)
                            :b (mk :pre-b :mid-b :post-b)})
              (step/sink))
          out (lines sm)
          ;; For each branch, find its three lines in order; pre should
          ;; come before mid which should come before post.
          line-idx (fn [tag] (some (fn [[i ln]] (when (str/includes? ln tag) i))
                                   (map-indexed vector out)))]
      (is (< (line-idx "pre a") (line-idx "mid a") (line-idx "post a"))
          (str "branch a out of chain order, got: " (pr-str out)))
      (is (< (line-idx "pre b") (line-idx "mid b") (line-idx "post b"))
          (str "branch b out of chain order, got: " (pr-str out))))))

(deftest singleton-classes-do-not-kify-step-names
  (testing "When a class in a cycle has size 1, real step ids show through (no K placeholder)"
    (let [;; Build a tiny 2-cycle where one of the members is a container
          ;; with internal steps named `:foo-0`, `:foo-1`, etc. Without
          ;; the singleton guard, the renderer would normalize them to
          ;; `foo K`. With the guard, they keep their digits.
          inner-a (step/serial (step/step :foo-0 inc) (step/step :foo-1 inc))
          inner-b (step/step :other inc)
          sm (-> (step/beside (step/serial :a inner-a)
                              (step/serial :b inner-b))
                 (step/connect [:a :out] [:b :in])
                 (step/connect [:b :out] [:a :in])
                 (step/input-at [:a :in])
                 (step/output-at [:a :out]))
          out (lines sm)]
      ;; foo-0 and foo-1 should show as "foo 0" / "foo 1" (humanized),
      ;; not collapsed to "foo K".
      (is (some #(re-find #"\bfoo 0\b" %) out)
          (str "expected real digit for singleton-class member, got: " (pr-str out)))
      (is (some #(re-find #"\bfoo 1\b" %) out)
          (str "expected real digit for singleton-class member, got: " (pr-str out))))))

(deftest render-is-idempotent
  (testing "rendering the same pipeline twice yields byte-identical output"
    (let [sm (step/serial (step/step :a inc) (step/step :b inc))]
      (is (= (lines sm) (lines sm))))))

;; ============================================================================
;; Block aggregation (Option A): WL refinement + faithfulness gate
;; ============================================================================

(deftest stealing-workers-aggregates-fully
  (testing "k=4 stealing-workers with step inner: clean K× blocks, no 15+1 split"
    (let [sm (cw/stealing-workers :pool 4 (step/step :work inc))
          out (render/render sm)]
      ;; Expect exactly one each of "4× shim K", "4× worker K", "4× exit K".
      (is (= 1 (count (filter #(re-find #"\b4× shim K\b" %) out)))
          (str "expected one '4× shim K' line; got: " (pr-str out)))
      (is (= 1 (count (filter #(re-find #"\b4× worker K\b" %) out))))
      (is (= 1 (count (filter #(re-find #"\b4× exit K\b" %) out))))
      ;; Should NOT have any individual `worker N` lines (no half-block).
      (is (every? #(not (re-find #"\bworker \d\b" %)) out)
          (str "expected no individual worker lines, got: " (pr-str out))))))

(deftest disaggregation-restores-per-member
  (testing ":aggregate? false skips block aggregation; per-member artifact reappears"
    (let [sm (cw/stealing-workers :pool 4 (step/step :work inc))
          aggregated     (render/render sm)
          disaggregated  (render/render sm {:aggregate? false})]
      ;; Disaggregated output should differ from aggregated.
      (is (not= aggregated disaggregated))
      ;; The disaggregated form keeps the trailing-worker artifact —
      ;; specifically, `worker N` appears as an individual line.
      (is (some #(re-find #"\bworker \d\b" %) disaggregated)
          (str "expected at least one individual `worker N` line in disaggregated form, got: "
               (pr-str disaggregated))))))

(deftest heterogeneous-workers-do-not-merge
  (testing "one worker with different inner: homogeneous block + odd one"
    (let [;; Build a manual scatter-gather (cc/parallel) where one
          ;; specialist has different inner content. cc/parallel
          ;; uses port-named branches; the heterogeneous case is
          ;; exactly distinct-name leaves.
          sm (cc/parallel :p
                          {:a (step/step :worker-a inc)
                           :b (step/step :worker-b inc)
                           :c (step/step :worker-c inc)
                           :d (step/step :worker-d inc)})
          out (render/render sm)]
      ;; Distinct-name leaves should NOT collapse: each branch root
      ;; gets its own │ line.
      (is (every? #(not (re-find #"4× " %)) out)
          (str "expected no K× collapse on heterogeneous leaves, got: " (pr-str out))))))

(deftest cycle-pattern-bails-gracefully-on-empty
  (testing "an empty / trivial cycle/prime falls back without error"
    (let [topo {:nodes [{:path [:a] :name "a" :kind :leaf}
                        {:path [:b] :name "b" :kind :leaf}]
                :edges []}
          out (render/render topo)]
      ;; Two disjoint leaves render without crashing.
      (is (= 2 (count out))))))

(deftest aggregation-is-idempotent
  (testing "rendering twice yields byte-identical output (block path)"
    (let [sm (cw/stealing-workers :pool 4 (step/step :work inc))]
      (is (= (render/render sm) (render/render sm))))))
