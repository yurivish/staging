(ns podcast.tree-resolve-test
  "Pure-function and mocked end-to-end tests for the Datapotamus-driven
   `:tree` Stage B. No live LLM calls — `cached-chat!` is rebound to
   stubbed responses keyed on `:tree-leaf` vs `:tree-merge` stages."
  (:require [clojure.test :refer [deftest is testing]]
            [podcast.llm :as llm]
            [podcast.tree-resolve :as tr]
            [toolkit.datapotamus.step :as step]))

(def ^:private fwd-ctx       @#'tr/forward-context-paragraphs)
(def ^:private prefix-ids    @#'tr/prefix-leaf-ids)
(def ^:private compute-aliases @#'tr/compute-aliases)
(def ^:private render-leaf   @#'tr/render-leaf-text)
(def ^:private render-merge  @#'tr/render-merge-text)
(def ^:private build-graph   @#'tr/build-graph)

;; ============================================================================
;; Pure helpers
;; ============================================================================

(deftest forward-context-paragraphs-test
  (let [paras [{:id "p0" :text "zero"}
               {:id "p1" :text "one"}
               {:id "p2" :text "two"}
               {:id "p3" :text "three"}
               {:id "p4" :text "four"}]]
    (testing "k=2 from a chunk ending at p1 returns p2 and p3"
      (is (= [{:id "p2" :text "two"} {:id "p3" :text "three"}]
             (fwd-ctx paras [{:id "p0"} {:id "p1"}] 2))))
    (testing "trims at end of paragraphs"
      (is (= [{:id "p4" :text "four"}]
             (fwd-ctx paras [{:id "p3"}] 5))))
    (testing "k=0 returns nil"
      (is (nil? (fwd-ctx paras [{:id "p0"}] 0))))
    (testing "empty focus returns nil"
      (is (nil? (fwd-ctx paras [] 2))))
    (testing "last focus paragraph at end → empty seq"
      (is (= [] (fwd-ctx paras [{:id "p4"}] 2))))))

(deftest prefix-leaf-ids-test
  (let [reg (sorted-map "e_001" {:entity_id "e_001" :canonical "Andy"}
                        "e_002" {:entity_id "e_002" :canonical "Joe"})
        prefixed (prefix-ids reg :c-3)]
    (is (= #{"c-3:e_001" "c-3:e_002"} (set (keys prefixed))))
    (is (= "c-3:e_001" (:entity_id (get prefixed "c-3:e_001"))))))

(deftest compute-aliases-test
  (let [ms [{:surface_form "Andy"      :mention_text "Andy Stumpf"}
            {:surface_form "He"        :mention_text "Andy Stumpf"}
            {:surface_form "the SEAL"  :mention_text "Andy Stumpf"}]]
    (is (= ["Andy" "Andy Stumpf" "He" "the SEAL"]
           (compute-aliases ms [0 1 2])))))

(deftest render-leaf-text-shows-three-regions
  (let [chunk {:context [{:id "p0" :text "earlier"}]
               :focus   [{:id "p1" :text "main"}]}
        fwd   [{:id "p2" :text "after"}]
        ms    [{:paragraph_id "p1" :surface_form "X" :mention_text "Y"}]
        rendered (render-leaf chunk fwd ms)]
    (is (re-find #"=== CONTEXT \(preceding\) ===" rendered))
    (is (re-find #"=== FOCUS — cluster mentions" rendered))
    (is (re-find #"=== CONTEXT \(following\) ===" rendered))
    (is (re-find #"=== MENTIONS TO CLUSTER" rendered))
    (is (re-find #"1\. \[p1\]" rendered))))

(deftest render-merge-text-shows-both-sides
  (let [left  (sorted-map "L:e_001"
                          {:entity_id "L:e_001" :canonical "Andy"
                           :aliases ["Andy" "He"] :summary "SEAL guest"})
        right (sorted-map "R:e_001"
                          {:entity_id "R:e_001" :canonical "Andy Stumpf"
                           :aliases ["Andy Stumpf"] :summary "Former SEAL"})
        rendered (render-merge left right)]
    (is (re-find #"=== LEFT ===" rendered))
    (is (re-find #"=== RIGHT ===" rendered))
    (is (re-find #"L:e_001" rendered))
    (is (re-find #"R:e_001" rendered))
    (is (re-find #"summary: SEAL guest" rendered))))

;; ============================================================================
;; Tree topology — verify build-merge-tree returns a valid step value
;; with the expected internal structure.
;; ============================================================================

(deftest build-graph-shape
  (testing "static graph has the four expected procs regardless of chunk count"
    (let [config {:task :sentiment :resolve-model {:model "stub"}}
          g (build-graph config [] [] 2 4)]
      (is (step/step? g))
      (let [proc-ids (set (keys (:procs g)))]
        (is (contains? proc-ids :tree-explode))
        (is (contains? proc-ids :tree-leaves))
        (is (contains? proc-ids :tree-gather-all))
        (is (contains? proc-ids :tree-reducer))))))

(deftest reducer-self-loop-edge-present
  (let [config {:task :sentiment :resolve-model {:model "stub"}}
        g (build-graph config [] [] 2 4)
        ;; The recursion lives on this single edge.
        loop-edge [[:tree-reducer :loop] [:tree-reducer :in]]]
    (is (some (fn [c] (= (subvec c 0 2) loop-edge)) (:conns g))
        ":tree-reducer :loop must connect back to :tree-reducer :in")))

;; ============================================================================
;; End-to-end with mocked cached-chat! — fires actual flow.
;; ============================================================================

(defn- mock-chat
  "Stub for podcast.llm/cached-chat!. Dispatches on stage:
     :tree-leaf  — invents one entity per chunk based on focus paragraph id
     :tree-merge — ALWAYS proposes one merge of LEFT's first id + RIGHT's first id"
  [stage _model-cfg _system content-parts _schema]
  (case stage
    :tree-leaf
    (let [text (-> content-parts first :text)
          ;; Find the first FOCUS paragraph id (looks like [p1])
          fid  (or (second (re-find #"=== FOCUS[^\n]*\n\[([^\]\s]+)" text)) "x")]
      {:value {:entities [{:entity_id "e_001"
                           :canonical (str "Person-" fid)
                           :summary (str "A character introduced in " fid)
                           :mention_indices [1]}]}
       :tokens 100
       :cache  :miss})

    :tree-merge
    (let [text (-> content-parts first :text)
          left-id  (or (second (re-find #"=== LEFT ===\n\[([^\]]+)\]" text)) nil)
          right-id (or (second (re-find #"=== RIGHT ===\n\[([^\]]+)\]" text)) nil)
          merges (if (and left-id right-id)
                   [{:ids [left-id right-id]
                     :canonical (str left-id "+" right-id)
                     :summary (str "merged " left-id " and " right-id)}]
                   [])]
      {:value {:merges merges}
       :tokens 50
       :cache  :miss})))

(defn- fixture-paragraphs []
  (vec (for [i (range 8)]
         {:id (str "p" i) :text (str "paragraph " i " content")})))

(defn- fixture-chunk [i]
  (let [paras (fixture-paragraphs)
        focus (subvec paras (* i 2) (* (inc i) 2))]
    {:chunk-id  (keyword (str "c-" i))
     :focus     focus
     :context   (when (pos? i) [{:id (str "p" (dec (* i 2))) :text "ctx"}])
     :focus-ids (set (map :id focus))}))

(defn- fixture-mentions [i]
  [{:chunk-id     (keyword (str "c-" i))
    :paragraph_id (:id (first (:focus (fixture-chunk i))))
    :surface_form (str "S" i)
    :mention_text (str "Person" i)}])

(deftest end-to-end-with-four-chunks
  (let [chunks       (mapv fixture-chunk (range 4))
        all-mentions (vec (mapcat fixture-mentions (range 4)))
        config       {:task           :sentiment
                      :resolve-model  {:model "stub" :max-tokens 64}
                      :resolve-strategy :tree}]
    (with-redefs [llm/cached-chat! mock-chat]
      (let [{:keys [registry tokens cache rejected]}
            (llm/resolve-entities! config all-mentions
                                   (fixture-paragraphs) "desc" chunks)]
        (testing "tree completes and reports :tree cache marker"
          (is (= :tree cache))
          (is (= [] rejected)))
        (testing "leaves fired (4 × 100) + 3 internal merges (3 × 50)"
          (is (= (+ (* 4 100) (* 3 50)) tokens)))
        (testing "all 4 leaf entities collapsed into one root entity by repeated merging"
          (is (= 1 (count registry))))
        (testing "the root entity carries a chained canonical from successive merges"
          (let [[_ e] (first registry)]
            (is (string? (:canonical e)))
            (is (string? (:summary e)))))))))

(deftest end-to-end-with-one-chunk
  (let [chunks       [(fixture-chunk 0)]
        all-mentions (fixture-mentions 0)
        config       {:task           :sentiment
                      :resolve-model  {:model "stub" :max-tokens 64}
                      :resolve-strategy :tree}]
    (with-redefs [llm/cached-chat! mock-chat]
      (let [{:keys [registry tokens cache]}
            (llm/resolve-entities! config all-mentions
                                   (fixture-paragraphs) "desc" chunks)]
        (is (= :tree cache))
        (is (= 100 tokens))
        (is (= 1 (count registry)))))))

(deftest end-to-end-with-empty-chunks
  (let [config {:task :sentiment :resolve-model {} :resolve-strategy :tree}]
    (with-redefs [llm/cached-chat! mock-chat]
      (is (thrown? Exception
                   (llm/resolve-entities! config [] [] "desc" []))))))
