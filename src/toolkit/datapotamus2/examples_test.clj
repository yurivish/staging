(ns toolkit.datapotamus2.examples-test
  "Tests-as-examples for Datapotamus2. Each deftest is a single-vignette
   demonstrating one capability; the suite ordered simplest → most
   sophisticated.

   Most tests use the DSL (`dsl/serial`, `dsl/step`, `dsl/as-step`, etc.)
   since it's the recommended authoring surface. A handful keep hand-
   authored flow maps to document that the lower layer also works."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.dependency :as dep]
            [toolkit.datapotamus2.combinators :as c]
            [toolkit.datapotamus2.core :as dp2]
            [toolkit.datapotamus2.dsl :as dsl]
            [toolkit.datapotamus2.token :as tok]
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
  (let [flow   (dsl/serial
                (dsl/step :inc  (c/wrap inc))
                (dsl/step :dbl  (c/wrap #(* 2 %)))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data 5})]
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

(deftest static-fan-out
  (let [flow   (dsl/serial
                (dsl/step :split (c/fan-out :split 3))
                (dsl/step :sink  (c/absorb-sink)))
        result (dp2/run! flow {:data :x})]
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
  (let [flow   (dsl/serial
                (dsl/step :split (c/fan-out :split 3))
                (dsl/step :fi    (c/fan-in  :split))
                (dsl/step :sink  (c/absorb-sink)))
        result (dp2/run! flow {:data :x})]
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

;; -----------------------------------------------------------------------------
;; Multi-port routing — needs merge-flows + connect
;; -----------------------------------------------------------------------------

(deftest dynamic-fan-out
  (let [flow   (-> (dsl/merge-flows
                    (dsl/step :route     (c/router [:odd :even]
                                                   (fn [xs]
                                                     (for [x xs]
                                                       {:data x
                                                        :port (if (odd? x) :odd :even)}))))
                    (dsl/step :odd-sink  (c/absorb-sink))
                    (dsl/step :even-sink (c/absorb-sink)))
                   (dsl/connect [:route :odd]  [:odd-sink :in])
                   (dsl/connect [:route :even] [:even-sink :in])
                   (dsl/input-at :route))
        result (dp2/run! flow {:data [1 2 3 4 5]})]
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
  ;; the graph. Declared boundary uses [:agent :user-in] so no :port
  ;; override is needed at inject time.
  (let [calls (atom 0)
        agent (dsl/from-handler :agent
                {:ins  {:user-in "" :tool-result ""}
                 :outs {:tool-call "" :final ""}}
                (fn [_ctx s m]
                  (let [n (swap! calls inc)]
                    (if (< n 4)
                      [s (dsl/emit m :tool-call :query)]
                      [s (dsl/emit m :final     :done)]))))
        flow (-> (dsl/merge-flows
                  agent
                  (dsl/step :tool (c/wrap (fn [_] :tool-response)))
                  (dsl/step :sink (c/absorb-sink)))
                 (dsl/connect [:agent :tool-call] [:tool  :in])
                 (dsl/connect [:tool  :out]       [:agent :tool-result])
                 (dsl/connect [:agent :final]     [:sink  :in])
                 (dsl/input-at [:agent :user-in])
                 (dsl/output-at :sink))
        result (dp2/run! flow {:data :question})]
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
        flow   (dsl/serial
                (dsl/step :try  (c/retry flaky 3))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data 42})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "retries are invisible in the trace (exactly one recv per step)"
      (is (= {:try 1 :sink 1}
             (frequencies (map :step-id (events-of result :recv))))))
    (testing "no failure events (retries succeeded before exhaustion)"
      (is (empty? (events-of result :failure))))))

(deftest failure-surfaces-as-event-not-run-level
  ;; A throw in a step-fn is a message-level failure: it surfaces as a
  ;; :failure event, the failed msg does not propagate downstream, and
  ;; the run as a whole still completes. retry exhausts its attempts
  ;; and the final throw reaches wrap-proc's catch.
  (let [permafail (fn [_] (throw (ex-info "boom" {:reason :permafail})))
        flow   (dsl/serial
                (dsl/step :try  (c/retry permafail 2))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data 42})]
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
  ;; Isolates wrap-proc's swallow from retry. A plain throw in step-fn
  ;; yields :failure + run completion, with no downstream emission.
  (let [flow   (dsl/serial
                (dsl/step :boom (c/wrap (fn [_] (throw (ex-info "oops" {})))))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data 1})]
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
  (let [flow   (dsl/serial
                (dsl/step :split (c/fan-out :split 3))
                (dsl/step :fi    (c/fan-in  :split))
                (dsl/step :sink  (c/absorb-sink)))
        result (dp2/run! flow {:data :x})
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
  ;; Demonstrates the frequencies-based assertion style (per user memory).
  (let [flow   (dsl/serial
                (dsl/step :split (c/fan-out :split 3))
                (dsl/step :sink  (c/absorb-sink)))
        result (dp2/run! flow {:data :x})]
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
  ;; External observer: subscribes a handler against the pubsub; it fires
  ;; synchronously as events are published by wrap-procs.
  (let [ps (pubsub/make)
        recv-count (atom 0)
        u (pubsub/sub ps "recv.flow.run-A.>"
                      (fn [_ _ _] (swap! recv-count inc)))
        flow (dsl/serial
              (dsl/step :split (c/fan-out :split 3))
              (dsl/step :sink  (c/absorb-sink)))
        result (dp2/run! flow {:pubsub ps :flow-id "run-A" :data :x})]
    (u)
    (is (= :completed (:state result)))
    (is (= 4 @recv-count))))      ; split + 3 sinks = 4 :recv events

(deftest multi-flow-shared-pubsub
  ;; One pubsub, two concurrent flows. Events distinguishable by flow-id.
  (let [ps (pubsub/make)
        tallies (atom {})
        u (pubsub/sub ps "recv.flow.*.>"
                      (fn [_ ev _]
                        (swap! tallies update (first (:flow-path ev)) (fnil inc 0))))
        flow-A (dsl/serial (dsl/step :a  (c/wrap inc))
                           (dsl/step :sa (c/absorb-sink)))
        flow-B (dsl/serial (dsl/step :b  (c/wrap dec))
                           (dsl/step :sb (c/absorb-sink)))
        ra (dp2/run! flow-A {:pubsub ps :flow-id "A" :data 1})
        rb (dp2/run! flow-B {:pubsub ps :flow-id "B" :data 2})]
    (u)
    (is (= :completed (:state ra)))
    (is (= :completed (:state rb)))
    (is (= {"A" 2 "B" 2} @tallies))))      ; 2 :recv events per flow

;; -----------------------------------------------------------------------------
;; Trace spans: observability for sequential work inside a step
;; -----------------------------------------------------------------------------

(deftest trace-span-flat
  ;; A step uses `trace` to observe two sibling operations.
  ;; Spans emit :span-start / :span-success; run completion is unaffected.
  (let [flow   (dsl/serial
                (dsl/from-handler :work
                  (fn [ctx s m]
                    (let [a (dp2/with-span ctx :step-a {:kind :first}
                                   (fn [_] (+ (:data m) 1)))
                          b (dp2/with-span ctx :step-b {:kind :second}
                                   (fn [_] (* a 10)))]
                      [s (dsl/emit m :out b)])))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data 4})]
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
  ;; A span inside a span: inner scope nests under outer.
  (let [flow   (dsl/serial
                (dsl/from-handler :work
                  (fn [ctx s m]
                    (let [v (dp2/with-span ctx :outer {}
                                   (fn [ctx']
                                     (dp2/with-span ctx' :inner {}
                                            (fn [_] (+ (:data m) 100)))))]
                      [s (dsl/emit m :out v)])))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data 1})]
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
  ;; A throw inside a span emits :span-failure, rethrows, and the step's
  ;; wrap-proc converts it into a :failure event. Run still completes.
  (let [flow   (dsl/serial
                (dsl/from-handler :work
                  (fn [ctx s m]
                    (let [v (dp2/with-span ctx :risky {}
                                   (fn [_]
                                     (throw (ex-info "nope" {:d (:data m)}))))]
                      [s (dsl/emit m :out v)])))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data 7})]
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
  ;; Confirms the pubsub subject shape for span events — useful for
  ;; external observers that subscribe by subject.
  (let [ps (pubsub/make)
        subjects (atom [])
        u (pubsub/sub ps "span-start.flow.run-T.>"
                      (fn [subj _ _] (swap! subjects conj subj)))
        flow   (dsl/from-handler :work
                 (fn [ctx s _m]
                   (dp2/with-span ctx :outer {}
                          (fn [ctx']
                            (dp2/with-span ctx' :inner {} (fn [_] :ok))))
                   [s {}]))
        result (dp2/run! flow {:pubsub ps :flow-id "run-T" :data :x})]
    (u)
    (is (= :completed (:state result)))
    (is (= #{"span-start.flow.run-T.step.work.span.outer"
             "span-start.flow.run-T.step.work.span.outer.span.inner"}
           (set @subjects)))))

(deftest nested-flow-namespaced
  ;; Inner flow embedded via `as-step`. Step-ids can collide between
  ;; outer and inner; `as-step` namespaces inner procs (e.g. :sub.inc)
  ;; and scope composition gives inner events a distinct subject path.
  (let [ps (pubsub/make)
        inner (dsl/step :inc (c/wrap #(+ % 100)))
        outer (dsl/serial
               (dsl/step :inc (c/wrap inc))       ; SAME step-id as inner's :inc
               (dsl/as-step :sub inner)
               (dsl/step :sink (c/absorb-sink)))
        outer-recvs (atom [])
        inner-recvs (atom [])
        u1 (pubsub/sub ps "recv.flow.outer.step.inc"
                       (fn [_ ev _] (swap! outer-recvs conj ev)))
        u2 (pubsub/sub ps "recv.flow.outer.flow.sub.step.inc"
                       (fn [_ ev _] (swap! inner-recvs conj ev)))
        result (dp2/run! outer {:pubsub ps :flow-id "outer" :data 5})]
    (u1) (u2)
    (is (= :completed (:state result)))
    (is (= 1 (count @outer-recvs)))
    (is (= 1 (count @inner-recvs)))
    (is (= ["outer"]       (:flow-path (first @outer-recvs))))
    (is (= ["outer" "sub"] (:flow-path (first @inner-recvs))))
    (is (= 5 (:data (first @outer-recvs))))            ; outer's :inc received seed 5
    (is (= 6 (:data (first @inner-recvs))))))          ; inner's :inc received 6 (5+1 from outer's inc)

;; -----------------------------------------------------------------------------
;; Span coverage (regression tests for subtle invariants)
;; -----------------------------------------------------------------------------

(deftest span-step-id-attribution
  ;; Every span event's :step-id is the ENCLOSING step, never the span's own
  ;; name — regardless of nesting depth.
  (let [flow   (dsl/serial
                (dsl/from-handler :only
                  (fn [ctx s m]
                    (dp2/with-span ctx :outer {}
                           (fn [c] (dp2/with-span c :inner {} (fn [_] :ok))))
                    [s (dsl/emit m :out :ok)]))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data :x})
        spans  (concat (events-of result :span-start) (events-of result :span-success))]
    (is (seq spans))
    (is (every? #(= :only (:step-id %)) spans))
    (is (= #{:outer :inner} (set (map :span-name spans))))))

(deftest span-in-multi-port-step
  ;; Spans don't perturb multi-port output. Uses merge-flows + connect
  ;; because the step emits to :a and :b.
  (let [work (dsl/from-handler :work
               {:outs {:a "" :b ""}}
               (fn [ctx s m]
                 (let [x (dp2/with-span ctx :compute-a {} (fn [_] (+ (:data m) 1)))
                       y (dp2/with-span ctx :compute-b {} (fn [_] (* (:data m) 10)))]
                   [s (dsl/emit m :a x :b y)])))
        flow   (-> (dsl/merge-flows
                    work
                    (dsl/step :sink-a (c/absorb-sink))
                    (dsl/step :sink-b (c/absorb-sink)))
                   (dsl/connect [:work :a] [:sink-a :in])
                   (dsl/connect [:work :b] [:sink-b :in])
                   (dsl/input-at :work))
        result (dp2/run! flow {:data 5})]
    (is (= 6  (:data (first (filterv #(= :sink-a (:step-id %)) (events-of result :recv))))))
    (is (= 50 (:data (first (filterv #(= :sink-b (:step-id %)) (events-of result :recv))))))
    (is (= {:compute-a 1 :compute-b 1}
           (frequencies (map :span-name (events-of result :span-start)))))))

(deftest span-in-nested-flow
  ;; Full scope composition: inner flow's step emits a span. Scope must be
  ;; [[:flow outer-fid] [:flow sub-sid] [:step inner-sid] [:span name]].
  (let [inner  (dsl/from-handler :work
                 (fn [ctx s m]
                   (let [v (dp2/with-span ctx :nested-op {:src :inner}
                                  (fn [_] (* (:data m) 2)))]
                     [s (dsl/emit m :out v)])))
        outer  (dsl/serial
                (dsl/as-step :sub inner)
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! outer {:flow-id "F" :data 5})
        span   (first (events-of result :span-start))]
    (is (= :completed (:state result)))
    (is (= :nested-op (:span-name span)))
    (is (= [[:flow "F"] [:flow "sub"] [:step :work] [:span :nested-op]]
           (:scope span)))
    (is (= ["F" "sub"] (:flow-path span)))))

(deftest span-event-ordering
  ;; Within one step invocation: :recv < :span-start < :span-success < :success.
  (let [flow   (dsl/serial
                (dsl/from-handler :step
                  (fn [ctx s m]
                    (let [v (dp2/with-span ctx :op {} (fn [_] (inc (:data m))))]
                      [s (dsl/emit m :out v)])))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data 1})
        events (:events result)
        idx-of (fn [pred] (first (keep-indexed (fn [i e] (when (pred e) i)) events)))
        recv-i (idx-of #(and (= :recv (:kind %)) (= :step (:step-id %))))
        ss-i   (idx-of #(= :span-start (:kind %)))
        sx-i   (idx-of #(= :span-success (:kind %)))
        succ-i (idx-of #(and (= :success (:kind %)) (= :step (:step-id %))))]
    (is (< recv-i ss-i sx-i succ-i))))

(deftest factory-ctx-has-scope-and-step-id
  ;; The step's factory is called once per proc with a ctx carrying the
  ;; proc's identity. Scope lives on the pubsub's prefix. This test uses
  ;; the raw factory form (dsl/step) to capture ctx at factory time —
  ;; from-handler doesn't expose that moment.
  (let [captured (atom nil)
        factory  (fn [ctx]
                   (reset! captured {:step-id (:step-id ctx)
                                     :scope   (:prefix (:pubsub ctx))})
                   (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                       ([_] {}) ([s _] s)
                       ([s _ _] [s {}])))
        flow     (dsl/step :my-step factory)
        _        (dp2/run! flow {:flow-id "F" :data :x})]
    (is (= {:step-id :my-step
            :scope   [[:flow "F"] [:step :my-step]]}
           @captured))))

(deftest multiple-step-styles
  ;; Combinator factory + two handler-based steps compose in one flow.
  (let [flow   (dsl/serial
                (dsl/step :a (c/wrap inc))
                (dsl/from-handler :b (fn [_ctx s m] [s (dsl/emit m :out (* (:data m) 10))]))
                (dsl/from-handler :c (fn [_ctx s m] [s (dsl/emit m :out (dec (:data m)))]))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data 5})]
    (is (= :completed (:state result)))
    ;; 5 → a(inc) → 6 → b(*10) → 60 → c(dec) → 59
    (is (= 59 (:data (first (filterv #(= :sink (:step-id %))
                                     (events-of result :recv))))))))

;; -----------------------------------------------------------------------------
;; Hand-authored flow map (documents the raw layer below the DSL)
;; -----------------------------------------------------------------------------

(deftest hand-authored-flow-map
  ;; The DSL is optional sugar; a flow is just a map. This test uses a
  ;; raw flow map directly to show the underlying shape that dsl/serial
  ;; produces.
  (let [flow   {:procs {:inc  (c/wrap inc)
                        :sink (c/absorb-sink)}
                :conns [[[:inc :out] [:sink :in]]]
                :in    :inc}
        result (dp2/run! flow {:data 3})]
    (is (= :completed (:state result)))
    (is (= 4 (:data (first (filterv #(= :sink (:step-id %))
                                    (events-of result :recv))))))))

;; -----------------------------------------------------------------------------
;; Long-running handle API: start! / inject! / await-quiescent! / stop!
;; -----------------------------------------------------------------------------

(deftest long-running-multiple-injects
  ;; Start a flow, inject several messages over time, await quiescence
  ;; between batches, then stop cleanly.
  (let [flow (dsl/serial
              (dsl/step :inc  (c/wrap inc))
              (dsl/step :sink (c/absorb-sink)))
        h (dp2/start! flow)]
    (dp2/inject! h {:data 1})
    (dp2/await-quiescent! h)
    (is (= {:sent 2 :recv 2 :completed 2} (dp2/counters h)))

    (dp2/inject! h {:data 10})
    (dp2/inject! h {:data 100})
    (dp2/await-quiescent! h)
    (is (= {:sent 6 :recv 6 :completed 6} (dp2/counters h)))

    (let [result (dp2/stop! h)]
      (is (= :completed (:state result)))
      (is (= [2 11 101]
             (mapv :data (filterv #(= :sink (:step-id %))
                                  (events-of result :recv))))))))

(deftest cancellation-via-cancel-promise
  ;; A step can poll (:cancel ctx); when stop! is called, the promise
  ;; is delivered. The step sees it and can exit early.
  (let [observed (atom nil)
        flow (dsl/serial
              (dsl/from-handler :step
                (fn [{:keys [cancel]} s m]
                  (reset! observed (realized? cancel))
                  [s (dsl/emit m :out (:data m))]))
              (dsl/step :sink (c/absorb-sink)))
        h    (dp2/start! flow)
        {:keys [::dp2/cancel]} h]
    (dp2/inject! h {:data :x})
    (dp2/await-quiescent! h)
    (is (false? @observed) "cancel promise not delivered while running")
    (is (false? (realized? cancel)) "cancel promise unrealized during run")

    (dp2/stop! h)
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
  "Run f across items on virtual threads, preserving input order.
   Used in podcast examples to parallelize blocking work inside a step."
  [f items]
  (with-open [exec (java.util.concurrent.Executors/newVirtualThreadPerTaskExecutor)]
    (let [futures (mapv (fn [x] (.submit exec ^Callable (fn [] (f x)))) items)]
      (mapv (fn [^java.util.concurrent.Future fut] (.get fut)) futures))))

(defn- dyn-fan-out-factory
  "Emit one child per input item (data is a vector). Fresh zero-sum token
   group keyed by \"dyn-<parent-msg-id>\"; a downstream `(c/fan-in :dyn)`
   will merge them."
  []
  (fn [_ctx]
    (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
        ([_] {}) ([s _] s)
        ([s _ m]
         (let [items  (:data m)
               gid    (str "dyn-" (:msg-id m))
               values (tok/split-value 0 (count items))
               kids   (mapv (fn [item v]
                              (-> (dp2/child-with-data m item)
                                  (assoc :tokens (assoc (:tokens m) gid v))))
                            items values)]
           [s {:out kids}])))))

;; --- E1. Single-podcast sequential pipeline --------------------------------

(deftest example-podcast-sequential-pipeline
  ;; One coarse step does the whole per-podcast sequence. Each operation is
  ;; a span with useful metadata.
  (let [flow   (dsl/serial
                (dsl/from-handler :process
                  (fn [ctx s m]
                    (let [url      (:url (:data m))
                          rss      (dp2/with-span ctx :fetch-rss {:url url}
                                          (fn [_] (fake-fetch-rss url)))
                          episodes (dp2/with-span ctx :parse-feed
                                          {:count (count (:items rss))}
                                          (fn [_] (fake-parse-feed rss)))
                          done     (mapv (fn [ep]
                                           (dp2/with-span ctx :process-episode
                                                  {:id (:id ep)}
                                                  (fn [_]
                                                    (-> ep fake-download
                                                        fake-transcribe
                                                        fake-chunk
                                                        fake-summarize))))
                                         episodes)]
                      [s (dsl/emit m :out (mapv :summary done))])))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data {:url "https://example.com/feed.xml"}})]
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
  ;; N podcasts fanned out as separate msgs. A coarse :process step runs
  ;; per podcast with spans; fan-in collects into one aggregate.
  (let [flow     (dsl/serial
                  (dsl/step :split (dyn-fan-out-factory))
                  (dsl/from-handler :process
                    (fn [ctx s m]
                      (let [url      (:url (:data m))
                            episodes (dp2/with-span ctx :fetch-and-parse {:url url}
                                            (fn [_] (fake-parse-feed (fake-fetch-rss url))))
                            sums     (mapv (fn [ep]
                                             (dp2/with-span ctx :episode {:id (:id ep)}
                                                    (fn [_] (:summary (fake-summarize
                                                                       (fake-transcribe ep))))))
                                           episodes)]
                        [s (dsl/emit m :out {:podcast url :summaries sums})])))
                  (dsl/step :merge (c/fan-in :dyn))
                  (dsl/step :sink  (c/absorb-sink)))
        podcasts [{:url "https://a"} {:url "https://b"} {:url "https://c"}]
        result   (dp2/run! flow {:data podcasts})]
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
  ;; Inside one step: transcribe, chunk, analyze chunks in parallel via
  ;; virtual threads. Spans emitted from worker threads still slot into
  ;; the caller's step scope because ctx is captured lexically.
  (let [flow   (dsl/serial
                (dsl/from-handler :episode
                  (fn [ctx s m]
                    (let [ep      (:data m)
                          chunks  (:chunks (fake-chunk (fake-transcribe ep)))
                          analyze (fn [[i c]]
                                    (dp2/with-span ctx :analyze-chunk {:idx i :size (count c)}
                                           (fn [_] (fake-analyze-part c))))
                          results (vt-map analyze (map-indexed vector chunks))]
                      [s (dsl/emit m :out results)])))
                (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data {:id :e1}})
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
  ;; After fan-in collects per-podcast summaries, one aggregation step does
  ;; multi-pass analysis as distinct spans. This is the "pulse" phase.
  (let [flow   (dsl/serial
                (dsl/from-handler :agg
                  (fn [ctx s m]
                    (let [vs     (:data m)
                          topics (dp2/with-span ctx :detect-topics {:n (count vs)}
                                        (fn [_] (set (mapcat :keywords vs))))
                          drift  (dp2/with-span ctx :spectrum-drift {}
                                        (fn [_] (reduce + 0 (map :stance vs))))
                          ranked (dp2/with-span ctx :rank-stories {}
                                        (fn [_] (vec (sort-by :score > vs))))]
                      [s (dsl/emit m :out {:topics topics
                                           :drift  drift
                                           :top    (first ranked)})])))
                (dsl/step :sink (c/absorb-sink)))
        feed [{:keywords [:climate :election] :stance  1 :score 0.8}
              {:keywords [:tech :election]    :stance -1 :score 0.9}
              {:keywords [:climate :economy]  :stance  0 :score 0.5}]
        result (dp2/run! flow {:data feed})
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
  ;; Three-level hierarchy:
  ;;   outer  — graph fan-out/fan-in across podcasts
  ;;   middle — per-podcast step fans out episodes on virtual threads
  ;;   inner  — per-episode span fans out analysis parts on virtual threads
  ;; Spans emitted from deep nested virtual threads still attribute to the
  ;; outer :process step; scopes compose correctly.
  (let [flow     (dsl/serial
                  (dsl/step :split (dyn-fan-out-factory))
                  (dsl/from-handler :process
                    (fn [ctx s m]
                      (let [url      (:url (:data m))
                            episodes (dp2/with-span ctx :fetch {:url url}
                                            (fn [_] (fake-parse-feed (fake-fetch-rss url))))
                            per-ep   (fn [ep]
                                       (dp2/with-span ctx :episode {:id (:id ep)}
                                              (fn [c]
                                                (let [chunks (:chunks (fake-chunk (fake-transcribe ep)))
                                                      analyze-part (fn [[i p]]
                                                                     (dp2/with-span c :analyze-part
                                                                            {:idx i}
                                                                            (fn [_] (fake-analyze-part p))))]
                                                  {:id (:id ep)
                                                   :parts (vt-map analyze-part
                                                                  (map-indexed vector chunks))}))))
                            results  (vt-map per-ep episodes)]
                        [s (dsl/emit m :out {:podcast url :episodes results})])))
                  (dsl/step :merge (c/fan-in :dyn))
                  (dsl/step :sink  (c/absorb-sink)))
        podcasts [{:url "https://a"} {:url "https://b"}]
        result   (dp2/run! flow {:data podcasts})]
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
