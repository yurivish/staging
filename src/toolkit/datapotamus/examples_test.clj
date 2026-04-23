(ns toolkit.datapotamus.examples-test
  "Tests-as-examples for Datapotamus. Each deftest is a single-vignette
   demonstrating one capability; the suite ordered simplest → most
   sophisticated.

   Most tests use the composition API (`dp/serial`, `dp/step`, `dp/as-step`,
   etc.) since it's the recommended authoring surface. A handful keep
   hand-authored step maps to document that the raw layer also works."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.dependency :as dep]
            [toolkit.datapotamus :as dp]
            [toolkit.datapotamus.token :as tok]
            [toolkit.pubsub :as pubsub]))

(defn- events-of [result kind]
  (filterv #(= kind (:kind %)) (:events result)))

(defn- events->dag
  "Fold events into a stuartsierra dependency graph + a {msg-id → kind}
   map. Each msg depends on its parents (per :parent-msg-ids)."
  [events]
  (reduce (fn [{:keys [g kinds]} e]
            (case (:kind e)
              :seed     {:g (dep/depend g (:msg-id e) ::root)
                         :kinds (assoc kinds (:msg-id e) :seed)}
              (:send-out :merge)
              {:g (reduce (fn [g p] (dep/depend g (:msg-id e) p))
                          g (:parent-msg-ids e))
               :kinds (assoc kinds (:msg-id e) (:kind e))}
              {:g g :kinds kinds}))
          {:g (dep/graph) :kinds {}}
          events))

;; -----------------------------------------------------------------------------
;; Linear chains (serial composition)
;; -----------------------------------------------------------------------------

(deftest linear-chain
  (let [wf     (dp/serial
                (dp/step :inc  inc)
                (dp/step :dbl  #(* 2 %))
                (dp/sink))
        result (dp/run! wf {:data 5})]
    (testing "run completes"
      (is (= :completed (:state result)))
      (is (nil? (:error result))))
    (testing "each step recv'd exactly once"
      (is (= {:inc 1 :dbl 1 :sink 1}
             (frequencies (map :step-id (events-of result :recv))))))
    (testing "each step succeeded exactly once"
      (is (= {:inc 1 :dbl 1 :sink 1}
             (frequencies (map :step-id (events-of result :success))))))
    (testing "exactly two port send-outs (inc and dbl; sink is terminal)"
      (let [sends (filterv :port (events-of result :send-out))]
        (is (= {:inc 1 :dbl 1} (frequencies (map :step-id sends))))))
    (testing "no failures"
      (is (empty? (events-of result :failure))))
    (testing "provenance chain: sink's recv's parent is dbl's send-out"
      (let [sink-recv-id    (:msg-id (first (filterv #(= :sink (:step-id %))
                                                     (events-of result :recv))))
            dbl-send-child  (first (filterv #(and (= :dbl (:step-id %)) (:port %))
                                            (events-of result :send-out)))]
        (is (= sink-recv-id (:msg-id dbl-send-child)))))))

(deftest math-double-and-triple
  ;; Linear math pipeline: double the input, then emit the doubled value three times.
  (let [wf     (dp/serial
                (dp/step :dbl #(* 2 %))
                (dp/fan-out :triple 3)
                (dp/sink))
        result (dp/run! wf {:data 5})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "dbl ran once, triple ran once, sink received thrice"
      (is (= {:dbl 1 :triple 1 :sink 3}
             (frequencies (map :step-id (events-of result :recv))))))
    (testing "sink sees 10 three times"
      (is (= [10 10 10]
             (mapv :data (filterv #(= :sink (:step-id %))
                                  (events-of result :recv))))))))

(deftest static-fan-out
  (let [wf     (dp/serial
                (dp/fan-out :split 3)
                (dp/sink))
        result (dp/run! wf {:data :x})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "three children emitted from fan-out"
      (is (= 3 (count (filterv #(and (= :split (:step-id %)) (:port %))
                               (events-of result :send-out))))))
    (testing "three recvs at the sink, each with a unique msg-id"
      (let [sink-recvs (filterv #(= :sink (:step-id %)) (events-of result :recv))]
        (is (= 3 (count sink-recvs)))
        (is (= 3 (count (distinct (map :msg-id sink-recvs)))))))
    (testing "fan-out's three children XOR to zero on their fresh group token"
      (let [split-sends (filterv #(and (= :split (:step-id %)) (:port %))
                                 (events-of result :send-out))
            group-keys  (->> split-sends
                             (mapcat (comp keys :tokens))
                             distinct)]
        (is (= 1 (count group-keys)) "all three children share one group key")
        (let [gk (first group-keys)
              vs (mapv (fn [e] (get-in e [:tokens gk])) split-sends)]
          (is (= 0 (reduce bit-xor vs))))))))

(deftest token-fan-in
  (let [wf     (dp/serial
                (dp/fan-out :split 3)
                (dp/fan-in :fi :split)
                (dp/sink))
        result (dp/run! wf {:data :x})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "exactly one merge event with three parents"
      (let [merges (events-of result :merge)]
        (is (= 1 (count merges)))
        (is (= 3 (count (:parent-msg-ids (first merges)))))))
    (testing "sink recv'd exactly one msg"
      (is (= 1 (count (filterv #(= :sink (:step-id %))
                               (events-of result :recv))))))
    (testing "fan-in sent exactly one downstream msg on :out"
      (is (= 1 (count (filterv #(and (= :fi (:step-id %)) (:port %))
                               (events-of result :send-out))))))
    (testing "the sink-bound msg's parent is the merge-node id"
      (let [merge-id   (:msg-id (first (events-of result :merge)))
            fi-send    (first (filterv #(and (= :fi (:step-id %)) (:port %))
                                       (events-of result :send-out)))]
        (is (= [merge-id] (:parent-msg-ids fi-send)))))))

(deftest named-port-fan-out-fan-in
  ;; The Go version's `FanOut(ports=[a,b]) → per-port procs → FanIn`
  ;; pattern. The Clojure `fan-out` is count-only (single :out port), so
  ;; to hit different procs on different ports we write an inline custom
  ;; handler that splits tokens manually. Promote to a combinator later
  ;; if this pattern appears in 2+ flows.
  (let [split  (dp/step :split
                 {:ins {:in ""} :outs {:a "" :b ""}}
                 (fn [_ctx s m]
                   ;; Split parent tokens across the two outputs (:a and :b)
                   ;; in addition to adding the new zero-sum group, so this
                   ;; named-port fan-out composes correctly inside a nested
                   ;; topology — mirrors what the built-in `dp/fan-out` does.
                   (let [gid           [:split (:msg-id m)]
                         [va vb]       (tok/split-value 0 2)
                         [pa pb]       (tok/split-tokens (:tokens m) 2)
                         mk            (fn [parent-tokens slice]
                                         (-> (dp/child-same-data m)
                                             (assoc :tokens
                                                    (assoc parent-tokens gid slice))))]
                     [s {:a [(mk pa va)] :b [(mk pb vb)]}])))
        wf     (-> (dp/merge-steps
                    split
                    (dp/step :procA #(+ % 100))
                    (dp/step :procB #(* % 10))
                    (dp/fan-in :merge :split)
                    (dp/sink))
                   (dp/connect [:split :a]   [:procA :in])
                   (dp/connect [:split :b]   [:procB :in])
                   (dp/connect [:procA :out] [:merge :in])
                   (dp/connect [:procB :out] [:merge :in])
                   (dp/connect [:merge :out] [:sink :in])
                   (dp/input-at :split)
                   (dp/output-at :sink))
        result (dp/run! wf {:data 5})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "exactly one merge event with two parents"
      (let [merges (events-of result :merge)]
        (is (= 1 (count merges)))
        (is (= 2 (count (:parent-msg-ids (first merges)))))))
    (testing "sink receives one merged msg carrying procA's +100 and procB's *10"
      (let [sink-recv (first (filterv #(= :sink (:step-id %))
                                      (events-of result :recv)))]
        (is (= #{105 50} (set (:data sink-recv))))))))

;; -----------------------------------------------------------------------------
;; Multi-port routing — needs merge-steps + connect
;; -----------------------------------------------------------------------------

(deftest nested-fan-out-fan-in-composes
  ;; Outer fan-out (N=3) contains an inner fan-out (N=2) whose group is
  ;; closed by an inner fan-in before the outer fan-in. Six leaf
  ;; messages flow through both fan-outs; the inner fan-in fires three
  ;; times (once per outer-group slice, pairing two inner-group leaves
  ;; each); the outer fan-in then fires EXACTLY ONCE with three parents.
  ;;
  ;; Regression: a previous `fan-out` bug duplicated parent tokens across
  ;; children instead of splitting them, which caused the inner fan-in's
  ;; merges to each carry :outer XORed to 0 and the outer fan-in to fire
  ;; prematurely (3 single-msg merges instead of 1 triple-msg merge).
  ;; This test would fail under that bug.
  (let [wf     (dp/serial
                (dp/fan-out :outer 3)
                (dp/fan-out :inner 2)
                (dp/fan-in  :fi-inner :inner)
                (dp/fan-in  :fi-outer :outer)
                (dp/sink))
        result (dp/run! wf {:data :seed})
        merges (events-of result :merge)]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "inner fan-in fires 3 times, each with 2 parents"
      (let [inner (filterv #(= :fi-inner (:step-id %)) merges)]
        (is (= 3 (count inner)))
        (is (every? #(= 2 (count (:parent-msg-ids %))) inner))))
    (testing "outer fan-in fires EXACTLY ONCE with 3 parents"
      (let [outer (filterv #(= :fi-outer (:step-id %)) merges)]
        (is (= 1 (count outer)))
        (is (= 3 (count (:parent-msg-ids (first outer)))))))
    (testing "sink received exactly one merged msg"
      (is (= 1 (count (filterv #(= :sink (:step-id %))
                               (events-of result :recv))))))))

(deftest dynamic-fan-out
  (let [wf     (-> (dp/merge-steps
                    (dp/router :route [:odd :even]
                               (fn [xs]
                                 (for [x xs]
                                   {:data x
                                    :port (if (odd? x) :odd :even)})))
                    (dp/sink :odd-sink)
                    (dp/sink :even-sink))
                   (dp/connect [:route :odd]  [:odd-sink :in])
                   (dp/connect [:route :even] [:even-sink :in])
                   (dp/input-at :route))
        result (dp/run! wf {:data [1 2 3 4 5]})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "odds routed to :odd-sink (3: 1, 3, 5), evens to :even-sink (2: 2, 4)"
      (is (= 3 (count (filterv #(= :odd-sink  (:step-id %)) (events-of result :recv)))))
      (is (= 2 (count (filterv #(= :even-sink (:step-id %)) (events-of result :recv))))))
    (testing "router emitted 5 send-outs total"
      (is (= 5 (count (filterv #(and (= :route (:step-id %)) (:port %))
                               (events-of result :send-out))))))))

(deftest agent-loop
  ;; Agent calls the tool up to N times, then emits :final. The tool's
  ;; output flows back to the agent's :tool-result port — a back-edge in
  ;; the graph.
  (let [calls (atom 0)
        agent (dp/step :agent
                {:ins  {:user-in "" :tool-result ""}
                 :outs {:tool-call "" :final ""}}
                (fn [_ctx s m]
                  (let [n (swap! calls inc)]
                    (if (< n 4)
                      [s (dp/emit m :tool-call :query)]
                      [s (dp/emit m :final     :done)]))))
        wf (-> (dp/merge-steps
                agent
                (dp/step :tool (constantly :tool-response))
                (dp/sink))
               (dp/connect [:agent :tool-call] [:tool  :in])
               (dp/connect [:tool  :out]       [:agent :tool-result])
               (dp/connect [:agent :final]     [:sink  :in])
               (dp/input-at [:agent :user-in])
               (dp/output-at :sink))
        result (dp/run! wf {:data :question})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "agent recv'd 4 times (1 initial + 3 tool results)"
      (is (= 4 (count (filterv #(= :agent (:step-id %)) (events-of result :recv))))))
    (testing "tool recv'd 3 times"
      (is (= 3 (count (filterv #(= :tool (:step-id %)) (events-of result :recv))))))
    (testing "sink recv'd exactly once (the :final branch)"
      (is (= 1 (count (filterv #(= :sink (:step-id %)) (events-of result :recv))))))
    (testing "agent emitted exactly one :final send-out"
      (is (= 1 (count (filterv #(and (= :agent (:step-id %)) (= :final (:port %)))
                               (events-of result :send-out))))))
    (testing "agent emitted exactly three :tool-call send-outs"
      (is (= 3 (count (filterv #(and (= :agent (:step-id %)) (= :tool-call (:port %)))
                               (events-of result :send-out))))))))

;; -----------------------------------------------------------------------------
;; Retries + failures
;; -----------------------------------------------------------------------------

(deftest retry-succeeds-after-transient-failures
  (let [attempts (atom 0)
        flaky   (fn [x]
                  (if (< @attempts 2)
                    (do (swap! attempts inc)
                        (throw (ex-info "transient" {:attempt @attempts})))
                    x))
        wf     (dp/serial
                (dp/retry :try flaky 3)
                (dp/sink))
        result (dp/run! wf {:data 42})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "retries are invisible in the trace (exactly one recv per step)"
      (is (= {:try 1 :sink 1}
             (frequencies (map :step-id (events-of result :recv))))))
    (testing "no failure events (retries succeeded before exhaustion)"
      (is (empty? (events-of result :failure))))))

(deftest failure-surfaces-as-event-not-run-level
  (let [permafail (fn [_] (throw (ex-info "boom" {:reason :permafail})))
        wf     (dp/serial
                (dp/retry :try permafail 2)
                (dp/sink))
        result (dp/run! wf {:data 42})]
    (testing "run completes (failure is message-level, not run-level)"
      (is (= :completed (:state result)))
      (is (nil? (:error result))))
    (testing "exactly one :failure event for :try"
      (let [fs (events-of result :failure)]
        (is (= 1 (count fs)))
        (is (= :try (:step-id (first fs))))
        (is (= "boom" (get-in (first fs) [:error :message])))))
    (testing "failed msg does not propagate to sink"
      (is (empty? (filterv #(= :sink (:step-id %)) (events-of result :recv)))))))

(deftest uncaught-failure-continues-run
  (let [wf     (dp/serial
                (dp/step :boom (fn [_] (throw (ex-info "oops" {}))))
                (dp/sink))
        result (dp/run! wf {:data 1})]
    (testing "run completes"
      (is (= :completed (:state result)))
      (is (nil? (:error result))))
    (testing "exactly one :failure event for :boom"
      (let [fs (events-of result :failure)]
        (is (= 1 (count fs)))
        (is (= :boom (:step-id (first fs))))))
    (testing ":recv was emitted for :boom (counter balance relies on this)"
      (is (= 1 (count (filterv #(= :boom (:step-id %)) (events-of result :recv))))))
    (testing "failed msg does not propagate to sink"
      (is (empty? (filterv #(= :sink (:step-id %)) (events-of result :recv)))))))

;; -----------------------------------------------------------------------------
;; Provenance DAG + multiplicity assertions
;; -----------------------------------------------------------------------------

(deftest provenance-dag-is-well-formed
  (let [wf     (dp/serial
                (dp/fan-out :split 3)
                (dp/fan-in :fi :split)
                (dp/sink))
        result (dp/run! wf {:data :x})
        {:keys [g kinds]} (events->dag (:events result))]
    (testing "graph is acyclic (topo-sort succeeds)"
      (is (some? (dep/topo-sort g))))
    (testing "exactly one real root (the seed) attached to the ::root sentinel"
      (let [roots (dep/immediate-dependents g ::root)]
        (is (= 1 (count roots)))))
    (testing "exactly one merge node, with three immediate parents"
      (let [merge-ids (for [[mid k] kinds :when (= :merge k)] mid)]
        (is (= 1 (count merge-ids)))
        (is (= 3 (count (dep/immediate-dependencies g (first merge-ids)))))))))

(deftest multiplicity-frequencies
  (let [wf     (dp/serial
                (dp/fan-out :split 3)
                (dp/sink))
        result (dp/run! wf {:data :x})]
    (testing "each step's :recv, :success, :send-out frequencies"
      (is (= {:split 1 :sink 3}
             (frequencies (map :step-id (events-of result :recv)))))
      (is (= {:split 1 :sink 3}
             (frequencies (map :step-id (events-of result :success)))))
      (is (= {:split 3}
             (frequencies (map :step-id
                               (filterv :port (events-of result :send-out)))))))))

;; -----------------------------------------------------------------------------
;; Pubsub-based tracing examples
;; -----------------------------------------------------------------------------

(deftest watcher-custom-subscriber
  (let [ps (pubsub/make)
        recv-count (atom 0)
        u (pubsub/sub ps "recv.flow.run-A.>"
                      (fn [_ _ _] (swap! recv-count inc)))
        wf (dp/serial
             (dp/fan-out :split 3)
             (dp/sink))
        result (dp/run! wf {:pubsub ps :flow-id "run-A" :data :x})]
    (u)
    (is (= :completed (:state result)))
    (is (= 4 @recv-count))))      ; split + 3 sinks = 4 :recv events

(deftest multi-flow-shared-pubsub
  (let [ps (pubsub/make)
        tallies (atom {})
        u (pubsub/sub ps "recv.flow.*.>"
                      (fn [_ ev _]
                        (swap! tallies update (first (:flow-path ev)) (fnil inc 0))))
        wf-A (dp/serial (dp/step :a  inc)
                        (dp/sink :sa))
        wf-B (dp/serial (dp/step :b  dec)
                        (dp/sink :sb))
        ra (dp/run! wf-A {:pubsub ps :flow-id "A" :data 1})
        rb (dp/run! wf-B {:pubsub ps :flow-id "B" :data 2})]
    (u)
    (is (= :completed (:state ra)))
    (is (= :completed (:state rb)))
    (is (= {"A" 2 "B" 2} @tallies))))      ; 2 :recv events per flow

;; -----------------------------------------------------------------------------
;; Trace spans: observability for sequential work inside a step
;; -----------------------------------------------------------------------------

(deftest trace-span-flat
  (let [wf     (dp/serial
                (dp/step :work nil
                  (fn [ctx s m]
                    (let [a (dp/with-span ctx :step-a {:kind :first}
                                   (fn [_] (+ (:data m) 1)))
                          b (dp/with-span ctx :step-b {:kind :second}
                                   (fn [_] (* a 10)))]
                      [s (dp/emit m :out b)])))
                (dp/sink))
        result (dp/run! wf {:data 4})]
    (testing "run completes and sink received the final value"
      (is (= :completed (:state result)))
      (is (= 50 (:data (first (filterv #(= :sink (:step-id %))
                                       (events-of result :recv)))))))
    (testing "two span pairs emitted, named as requested"
      (is (= {:step-a 1 :step-b 1}
             (frequencies (map :span-name (events-of result :span-start)))))
      (is (= {:step-a 1 :step-b 1}
             (frequencies (map :span-name (events-of result :span-success))))))
    (testing "span metadata is carried on :span-start events"
      (is (= #{{:kind :first} {:kind :second}}
             (into #{} (map :metadata (events-of result :span-start))))))
    (testing "span scope extends the enclosing step's scope"
      (let [a-start (first (filterv #(= :step-a (:span-name %))
                                    (events-of result :span-start)))
            fid     (second (first (:scope a-start)))]
        (is (= [[:flow fid] [:step :work] [:span :step-a]]
               (:scope a-start)))))
    (testing "completion counters are unaffected by spans"
      (is (= {:sent 2 :recv 2 :completed 2} (:counters result))))))

(deftest trace-span-nested
  (let [wf     (dp/serial
                (dp/step :work nil
                  (fn [ctx s m]
                    (let [v (dp/with-span ctx :outer {}
                                   (fn [ctx']
                                     (dp/with-span ctx' :inner {}
                                            (fn [_] (+ (:data m) 100)))))]
                      [s (dp/emit m :out v)])))
                (dp/sink))
        result (dp/run! wf {:data 1})]
    (testing "inner span's scope contains the outer span"
      (let [inner-start (first (filterv #(= :inner (:span-name %))
                                        (events-of result :span-start)))
            kinds       (mapv first (:scope inner-start))]
        (is (= [:flow :step :span :span] kinds))
        (is (= [:span :outer] (nth (:scope inner-start) 2)))
        (is (= [:span :inner] (nth (:scope inner-start) 3)))))
    (testing "outer span succeeds exactly once, inner succeeds exactly once"
      (is (= {:outer 1 :inner 1}
             (frequencies (map :span-name (events-of result :span-success))))))))

(deftest trace-span-failure-propagates-to-step
  (let [wf     (dp/serial
                (dp/step :work nil
                  (fn [ctx s m]
                    (let [v (dp/with-span ctx :risky {}
                                   (fn [_]
                                     (throw (ex-info "nope" {:d (:data m)}))))]
                      [s (dp/emit m :out v)])))
                (dp/sink))
        result (dp/run! wf {:data 7})]
    (testing "run completes (message-level failure, not run-level)"
      (is (= :completed (:state result))))
    (testing "exactly one :span-failure and one step :failure"
      (is (= 1 (count (events-of result :span-failure))))
      (is (= 1 (count (filterv #(= :work (:step-id %))
                               (events-of result :failure))))))
    (testing ":span-failure carries the exception message"
      (is (= "nope"
             (get-in (first (events-of result :span-failure)) [:error :message]))))
    (testing "sink does not receive a message (span failure short-circuited the step)"
      (is (empty? (filterv #(= :sink (:step-id %)) (events-of result :recv)))))))

(deftest trace-span-subjects-are-dotted
  (let [ps (pubsub/make)
        subjects (atom [])
        u (pubsub/sub ps "span-start.flow.run-T.>"
                      (fn [subj _ _] (swap! subjects conj subj)))
        wf     (dp/step :work nil
                 (fn [ctx s _m]
                   (dp/with-span ctx :outer {}
                          (fn [ctx']
                            (dp/with-span ctx' :inner {} (fn [_] :ok))))
                   [s {}]))
        result (dp/run! wf {:pubsub ps :flow-id "run-T" :data :x})]
    (u)
    (is (= :completed (:state result)))
    (is (= #{"span-start.flow.run-T.step.work.span.outer"
             "span-start.flow.run-T.step.work.span.outer.span.inner"}
           (set @subjects)))))

(deftest nested-flow-namespaced
  ;; Inner step embedded via `as-step`. Step-ids can collide between
  ;; outer and inner; `as-step` namespaces inner procs (e.g. :sub.inc)
  ;; and scope composition gives inner events a distinct subject path.
  (let [ps (pubsub/make)
        inner (dp/step :inc #(+ % 100))
        outer (dp/serial
               (dp/step :inc inc)       ; SAME step-id as inner's :inc
               (dp/as-step :sub inner)
               (dp/sink))
        outer-recvs (atom [])
        inner-recvs (atom [])
        u1 (pubsub/sub ps "recv.flow.outer.step.inc"
                       (fn [_ ev _] (swap! outer-recvs conj ev)))
        u2 (pubsub/sub ps "recv.flow.outer.flow.sub.step.inc"
                       (fn [_ ev _] (swap! inner-recvs conj ev)))
        result (dp/run! outer {:pubsub ps :flow-id "outer" :data 5})]
    (u1) (u2)
    (is (= :completed (:state result)))
    (is (= 1 (count @outer-recvs)))
    (is (= 1 (count @inner-recvs)))
    (is (= ["outer"]       (:flow-path (first @outer-recvs))))
    (is (= ["outer" "sub"] (:flow-path (first @inner-recvs))))
    (is (= 5 (:data (first @outer-recvs))))
    (is (= 6 (:data (first @inner-recvs))))))

;; -----------------------------------------------------------------------------
;; Span coverage (regression tests for subtle invariants)
;; -----------------------------------------------------------------------------

(deftest span-step-id-attribution
  (let [wf     (dp/serial
                (dp/step :only nil
                  (fn [ctx s m]
                    (dp/with-span ctx :outer {}
                           (fn [c] (dp/with-span c :inner {} (fn [_] :ok))))
                    [s (dp/emit m :out :ok)]))
                (dp/sink))
        result (dp/run! wf {:data :x})
        spans  (concat (events-of result :span-start) (events-of result :span-success))]
    (is (seq spans))
    (is (every? #(= :only (:step-id %)) spans))
    (is (= #{:outer :inner} (set (map :span-name spans))))))

(deftest span-in-multi-port-step
  (let [work (dp/step :work
               {:outs {:a "" :b ""}}
               (fn [ctx s m]
                 (let [x (dp/with-span ctx :compute-a {} (fn [_] (+ (:data m) 1)))
                       y (dp/with-span ctx :compute-b {} (fn [_] (* (:data m) 10)))]
                   [s (dp/emit m :a x :b y)])))
        wf     (-> (dp/merge-steps
                    work
                    (dp/sink :sink-a)
                    (dp/sink :sink-b))
                   (dp/connect [:work :a] [:sink-a :in])
                   (dp/connect [:work :b] [:sink-b :in])
                   (dp/input-at :work))
        result (dp/run! wf {:data 5})]
    (is (= 6  (:data (first (filterv #(= :sink-a (:step-id %)) (events-of result :recv))))))
    (is (= 50 (:data (first (filterv #(= :sink-b (:step-id %)) (events-of result :recv))))))
    (is (= {:compute-a 1 :compute-b 1}
           (frequencies (map :span-name (events-of result :span-start)))))))

(deftest span-in-nested-flow
  (let [inner  (dp/step :work nil
                 (fn [ctx s m]
                   (let [v (dp/with-span ctx :nested-op {:src :inner}
                                  (fn [_] (* (:data m) 2)))]
                     [s (dp/emit m :out v)])))
        outer  (dp/serial
                (dp/as-step :sub inner)
                (dp/sink))
        result (dp/run! outer {:flow-id "F" :data 5})
        span   (first (events-of result :span-start))]
    (is (= :completed (:state result)))
    (is (= :nested-op (:span-name span)))
    (is (= [[:flow "F"] [:flow "sub"] [:step :work] [:span :nested-op]]
           (:scope span)))
    (is (= ["F" "sub"] (:flow-path span)))))

(deftest span-event-ordering
  (let [wf     (dp/serial
                (dp/step :step nil
                  (fn [ctx s m]
                    (let [v (dp/with-span ctx :op {} (fn [_] (inc (:data m))))]
                      [s (dp/emit m :out v)])))
                (dp/sink))
        result (dp/run! wf {:data 1})
        events (:events result)
        idx-of (fn [pred] (first (keep-indexed (fn [i e] (when (pred e) i)) events)))
        recv-i (idx-of #(and (= :recv (:kind %)) (= :step (:step-id %))))
        ss-i   (idx-of #(= :span-start (:kind %)))
        sx-i   (idx-of #(= :span-success (:kind %)))
        succ-i (idx-of #(and (= :success (:kind %)) (= :step (:step-id %))))]
    (is (< recv-i ss-i sx-i succ-i))))

(deftest factory-ctx-has-scope-and-step-id
  ;; Raw factory form via `dp/proc` — the only form that exposes the
  ;; factory-time ctx capture. Used rarely, for advanced hooks.
  (let [captured (atom nil)
        factory  (fn [ctx]
                   (reset! captured {:step-id (:step-id ctx)
                                     :scope   (:prefix (:pubsub ctx))})
                   (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                       ([_] {}) ([s _] s)
                       ([s _ _] [s {}])))
        wf       (dp/proc :my-step factory)
        _        (dp/run! wf {:flow-id "F" :data :x})]
    (is (= {:step-id :my-step
            :scope   [[:flow "F"] [:step :my-step]]}
           @captured))))

(deftest multiple-step-styles
  (let [wf     (dp/serial
                (dp/step :a inc)
                (dp/step :b nil (fn [_ctx s m] [s (dp/emit m :out (* (:data m) 10))]))
                (dp/step :c nil (fn [_ctx s m] [s (dp/emit m :out (dec (:data m)))]))
                (dp/sink))
        result (dp/run! wf {:data 5})]
    (is (= :completed (:state result)))
    ;; 5 → a(inc) → 6 → b(*10) → 60 → c(dec) → 59
    (is (= 59 (:data (first (filterv #(= :sink (:step-id %))
                                     (events-of result :recv))))))))

;; -----------------------------------------------------------------------------
;; Hand-authored step map (documents the raw layer below the DSL)
;; -----------------------------------------------------------------------------

(deftest hand-authored-step-map
  ;; The DSL is optional sugar; a step is just a map. This test uses a
  ;; raw step map directly to show the underlying shape that dp/serial
  ;; produces.
  (let [wf     (-> (dp/merge-steps
                    (dp/step :inc inc)
                    (dp/sink))
                   (dp/connect [:inc :out] [:sink :in])
                   (dp/input-at :inc))
        result (dp/run! wf {:data 3})]
    (is (= :completed (:state result)))
    (is (= 4 (:data (first (filterv #(= :sink (:step-id %))
                                    (events-of result :recv))))))))

;; -----------------------------------------------------------------------------
;; Long-running handle API: start! / inject! / await-quiescent! / stop!
;; -----------------------------------------------------------------------------

(deftest long-running-multiple-injects
  (let [wf (dp/serial
            (dp/step :inc  inc)
            (dp/sink))
        h (dp/start! wf)]
    (dp/inject! h {:data 1})
    (dp/await-quiescent! h)
    (is (= {:sent 2 :recv 2 :completed 2} (dp/counters h)))

    (dp/inject! h {:data 10})
    (dp/inject! h {:data 100})
    (dp/await-quiescent! h)
    (is (= {:sent 6 :recv 6 :completed 6} (dp/counters h)))

    (let [result (dp/stop! h)]
      (is (= :completed (:state result)))
      (is (= [2 11 101]
             (mapv :data (filterv #(= :sink (:step-id %))
                                  (events-of result :recv))))))))

(deftest cancellation-via-cancel-promise
  (let [observed (atom nil)
        wf (dp/serial
             (dp/step :step nil
               (fn [{:keys [cancel]} s m]
                 (reset! observed (realized? cancel))
                 [s (dp/emit m :out (:data m))]))
             (dp/sink))
        h    (dp/start! wf)
        {:keys [::dp/cancel]} h]
    (dp/inject! h {:data :x})
    (dp/await-quiescent! h)
    (is (false? @observed) "cancel promise not delivered while running")
    (is (false? (realized? cancel)) "cancel promise unrealized during run")

    (dp/stop! h)
    (is (true? (realized? cancel)) "cancel promise delivered on stop!")
    (is (= :stopped @cancel))))

;; -----------------------------------------------------------------------------
;; Podcast-domain examples (E1–E5)
;;
;; Tests-as-examples for realistic pipelines. All IO is stubbed with pure
;; functions; the structure and span topology are what matters.
;; -----------------------------------------------------------------------------

(defn- fake-fetch-rss [url]
  {:url url
   :items [{:id :e1 :title "A" :enclosure "a.mp3"}
           {:id :e2 :title "B" :enclosure "b.mp3"}
           {:id :e3 :title "C" :enclosure "c.mp3"}]})

(defn- fake-parse-feed [rss] (:items rss))
(defn- fake-download   [ep]  (assoc ep :audio-bytes 42))
(defn- fake-transcribe [ep]
  (assoc ep :transcript (str (name (:id ep)) " alpha beta gamma delta epsilon")))
(defn- fake-chunk [ep]
  (assoc ep :chunks (vec (partition-all 2 (str/split (:transcript ep) #"\s+")))))
(defn- fake-summarize [ep] (assoc ep :summary (str "summary-" (name (:id ep)))))
(defn- fake-analyze-part [part]
  {:tokens (count part) :weight (reduce + 0 (map #(count (str %)) part))})

(defn- vt-map
  "Run f across items on virtual threads, preserving input order."
  [f items]
  (with-open [exec (java.util.concurrent.Executors/newVirtualThreadPerTaskExecutor)]
    (let [futures (mapv (fn [x] (.submit exec ^Callable (fn [] (f x)))) items)]
      (mapv (fn [^java.util.concurrent.Future fut] (.get fut)) futures))))

(defn- dyn-fan-out
  "Emit one child per input item (data is a vector). Fresh zero-sum token
   group keyed by `[:dyn <parent-msg-id>]`; a downstream `(dp/fan-in :dyn)`
   will merge them. Splits the parent's existing tokens across the N
   children so nested fan-out/fan-in topologies compose correctly
   (mirroring what the built-in `dp/fan-out` does)."
  [id]
  (dp/step id {:ins {:in ""} :outs {:out ""}}
    (fn [_ctx s m]
      (let [items         (:data m)
            n             (count items)
            gid           [:dyn (:msg-id m)]
            new-slices    (tok/split-value 0 n)
            parent-splits (tok/split-tokens (:tokens m) n)
            kids          (mapv (fn [i item v]
                                  (-> (dp/child-with-data m item)
                                      (assoc :tokens
                                             (assoc (nth parent-splits i) gid v))))
                                (range n) items new-slices)]
        [s {:out kids}]))))

;; --- E1. Single-podcast sequential pipeline --------------------------------

(deftest example-podcast-sequential-pipeline
  (let [wf     (dp/serial
                (dp/step :process nil
                  (fn [ctx s m]
                    (let [url      (:url (:data m))
                          rss      (dp/with-span ctx :fetch-rss {:url url}
                                          (fn [_] (fake-fetch-rss url)))
                          episodes (dp/with-span ctx :parse-feed
                                          {:count (count (:items rss))}
                                          (fn [_] (fake-parse-feed rss)))
                          done     (mapv (fn [ep]
                                           (dp/with-span ctx :process-episode
                                                  {:id (:id ep)}
                                                  (fn [_]
                                                    (-> ep fake-download
                                                        fake-transcribe
                                                        fake-chunk
                                                        fake-summarize))))
                                         episodes)]
                      [s (dp/emit m :out (mapv :summary done))])))
                (dp/sink))
        result (dp/run! wf {:data {:url "https://example.com/feed.xml"}})]
    (testing "run completes, sink received the summaries"
      (is (= :completed (:state result)))
      (is (= ["summary-e1" "summary-e2" "summary-e3"]
             (:data (first (filterv #(= :sink (:step-id %))
                                    (events-of result :recv)))))))
    (testing "top-level spans emitted in source order"
      (is (= [:fetch-rss :parse-feed
              :process-episode :process-episode :process-episode]
             (mapv :span-name (events-of result :span-start)))))
    (testing "completion counters only reflect graph-level msgs"
      (is (= {:sent 2 :recv 2 :completed 2} (:counters result))))))

;; --- E2. Graph fan-out over podcasts ---------------------------------------

(deftest example-podcast-graph-fanout
  (let [wf       (dp/serial
                  (dyn-fan-out :split)
                  (dp/step :process nil
                    (fn [ctx s m]
                      (let [url      (:url (:data m))
                            episodes (dp/with-span ctx :fetch-and-parse {:url url}
                                            (fn [_] (fake-parse-feed (fake-fetch-rss url))))
                            sums     (mapv (fn [ep]
                                             (dp/with-span ctx :episode {:id (:id ep)}
                                                    (fn [_] (:summary (fake-summarize
                                                                       (fake-transcribe ep))))))
                                           episodes)]
                        [s (dp/emit m :out {:podcast url :summaries sums})])))
                  (dp/fan-in :merge :dyn)
                  (dp/sink))
        podcasts [{:url "https://a"} {:url "https://b"} {:url "https://c"}]
        result   (dp/run! wf {:data podcasts})]
    (testing "run completes; sink receives one merged msg"
      (is (= :completed (:state result)))
      (is (= 1 (count (filterv #(= :sink (:step-id %)) (events-of result :recv))))))
    (testing "exactly three per-podcast :fetch-and-parse spans"
      (is (= 3 (count (filterv #(= :fetch-and-parse (:span-name %))
                               (events-of result :span-start))))))
    (testing "3 podcasts × 3 episodes = 9 :episode spans"
      (is (= 9 (count (filterv #(= :episode (:span-name %))
                               (events-of result :span-start))))))
    (testing "fan-in produced exactly one merge node"
      (is (= 1 (count (events-of result :merge)))))))

;; --- E3. Parallel chunk analysis via virtual threads -----------------------

(deftest example-parallel-chunk-analysis-vt
  (let [wf     (dp/serial
                (dp/step :episode nil
                  (fn [ctx s m]
                    (let [ep      (:data m)
                          chunks  (:chunks (fake-chunk (fake-transcribe ep)))
                          analyze (fn [[i c]]
                                    (dp/with-span ctx :analyze-chunk {:idx i :size (count c)}
                                           (fn [_] (fake-analyze-part c))))
                          results (vt-map analyze (map-indexed vector chunks))]
                      [s (dp/emit m :out results)])))
                (dp/sink))
        result (dp/run! wf {:data {:id :e1}})
        out    (:data (first (filterv #(= :sink (:step-id %)) (events-of result :recv))))]
    (testing "run completes; sink received a vector of analysis maps"
      (is (= :completed (:state result)))
      (is (vector? out))
      (is (every? :tokens out))
      (is (pos? (count out))))
    (testing "one :analyze-chunk span per chunk, all attributed to :episode"
      (let [spans (filterv #(= :analyze-chunk (:span-name %))
                           (events-of result :span-start))]
        (is (= (count out) (count spans)))
        (is (every? #(= :episode (:step-id %)) spans))))
    (testing "every :analyze-chunk span's scope lives under [:step :episode]"
      (let [spans (filterv #(= :analyze-chunk (:span-name %))
                           (events-of result :span-start))]
        (is (every? (fn [ev] (= [:step :episode] (nth (:scope ev) 1))) spans))))))

;; --- E4. Cross-podcast aggregation after fan-in ----------------------------

(deftest example-cross-podcast-aggregation
  (let [wf     (dp/serial
                (dp/step :agg nil
                  (fn [ctx s m]
                    (let [vs     (:data m)
                          topics (dp/with-span ctx :detect-topics {:n (count vs)}
                                        (fn [_] (set (mapcat :keywords vs))))
                          drift  (dp/with-span ctx :spectrum-drift {}
                                        (fn [_] (reduce + 0 (map :stance vs))))
                          ranked (dp/with-span ctx :rank-stories {}
                                        (fn [_] (vec (sort-by :score > vs))))]
                      [s (dp/emit m :out {:topics topics
                                           :drift  drift
                                           :top    (first ranked)})])))
                (dp/sink))
        feed [{:keywords [:climate :election] :stance  1 :score 0.8}
              {:keywords [:tech :election]    :stance -1 :score 0.9}
              {:keywords [:climate :economy]  :stance  0 :score 0.5}]
        result (dp/run! wf {:data feed})
        out    (:data (first (filterv #(= :sink (:step-id %)) (events-of result :recv))))]
    (testing "run completes; aggregation results are correct"
      (is (= :completed (:state result)))
      (is (= #{:climate :election :tech :economy} (:topics out)))
      (is (= 0 (:drift out)))
      (is (= 0.9 (:score (:top out)))))
    (testing "three analysis passes emitted as spans in source order"
      (is (= [:detect-topics :spectrum-drift :rank-stories]
             (mapv :span-name (events-of result :span-start)))))))

;; --- E5. Nested fan-out/fan-in: graph + in-step virtual threads ------------

(deftest example-nested-fanout-fanin
  (let [wf       (dp/serial
                  (dyn-fan-out :split)
                  (dp/step :process nil
                    (fn [ctx s m]
                      (let [url      (:url (:data m))
                            episodes (dp/with-span ctx :fetch {:url url}
                                            (fn [_] (fake-parse-feed (fake-fetch-rss url))))
                            per-ep   (fn [ep]
                                       (dp/with-span ctx :episode {:id (:id ep)}
                                              (fn [c]
                                                (let [chunks (:chunks (fake-chunk (fake-transcribe ep)))
                                                      analyze-part (fn [[i p]]
                                                                     (dp/with-span c :analyze-part
                                                                            {:idx i}
                                                                            (fn [_] (fake-analyze-part p))))]
                                                  {:id (:id ep)
                                                   :parts (vt-map analyze-part
                                                                  (map-indexed vector chunks))}))))
                            results  (vt-map per-ep episodes)]
                        [s (dp/emit m :out {:podcast url :episodes results})])))
                  (dp/fan-in :merge :dyn)
                  (dp/sink))
        podcasts [{:url "https://a"} {:url "https://b"}]
        result   (dp/run! wf {:data podcasts})]
    (testing "run completes; sink received one merged msg"
      (is (= :completed (:state result)))
      (is (= 1 (count (filterv #(= :sink (:step-id %)) (events-of result :recv))))))
    (testing "two :fetch spans (one per podcast); six :episode spans (3 episodes × 2 podcasts)"
      (is (= 2 (count (filterv #(= :fetch (:span-name %))
                               (events-of result :span-start)))))
      (is (= 6 (count (filterv #(= :episode (:span-name %))
                               (events-of result :span-start))))))
    (testing "every :analyze-part span attributes to the :process step"
      (let [parts (filterv #(= :analyze-part (:span-name %))
                           (events-of result :span-start))]
        (is (pos? (count parts)))
        (is (every? #(= :process (:step-id %)) parts))
        (is (every? (fn [ev] (= [:step :process] (nth (:scope ev) 1))) parts))))
    (testing "innermost span scope is [... [:step :process] [:span :episode] [:span :analyze-part]]"
      (let [part (first (filterv #(= :analyze-part (:span-name %))
                                 (events-of result :span-start)))]
        (is (= [:span :episode]      (nth (:scope part) 2)))
        (is (= [:span :analyze-part] (nth (:scope part) 3)))))
    (testing "fan-in collapsed both podcasts into one merge"
      (is (= 1 (count (events-of result :merge)))))))
