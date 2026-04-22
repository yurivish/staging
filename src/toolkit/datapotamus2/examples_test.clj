(ns toolkit.datapotamus2.examples-test
  "Tests-as-examples for Datapotamus2. Each deftest is a single-vignette
   demonstrating one capability; the suite ordered simplest → most
   sophisticated."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.dependency :as dep]
            [toolkit.datapotamus2.combinators :as c]
            [toolkit.datapotamus2.core :as dp2]
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

(deftest linear-chain
  (let [spec {:procs {:inc  (c/wrap :inc inc)
                      :dbl  (c/wrap :dbl #(* 2 %))
                      :sink (c/absorb-sink :sink)}
              :conns [[[:inc :out] [:dbl :in]]
                      [[:dbl :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :inc :data 5})]
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
  (let [spec {:procs {:split (c/fan-out :split 3)
                      :sink  (c/absorb-sink :sink)}
              :conns [[[:split :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :split :data :x})]
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

(deftest dynamic-fan-out
  (let [spec {:procs {:route     (c/router :route [:odd :even]
                                           (fn [xs]
                                             (for [x xs]
                                               {:data x
                                                :port (if (odd? x) :odd :even)})))
                      :odd-sink  (c/absorb-sink :odd-sink)
                      :even-sink (c/absorb-sink :even-sink)}
              :conns [[[:route :odd]  [:odd-sink  :in]]
                      [[:route :even] [:even-sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :route :data [1 2 3 4 5]})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "odds routed to :odd-sink (3: 1, 3, 5), evens to :even-sink (2: 2, 4)"
      (is (= 3 (count (filterv #(= :odd-sink  (:step-id %)) (events-of result :recv)))))
      (is (= 2 (count (filterv #(= :even-sink (:step-id %)) (events-of result :recv))))))
    (testing "router emitted 5 send-outs total"
      (is (= 5 (count (filterv #(and (= :route (:step-id %)) (:port %))
                               (events-of result :send-out))))))))

(deftest token-fan-in
  (let [spec {:procs {:split (c/fan-out :split 3)
                      :fi    (c/fan-in  :fi "split-")
                      :sink  (c/absorb-sink :sink)}
              :conns [[[:split :out] [:fi :in]]
                      [[:fi :out]    [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :split :data :x})]
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

(deftest agent-loop
  ;; Agent calls the tool up to N times, then emits :final. The tool's
  ;; output flows back to the agent's :tool-result port — a back-edge in
  ;; the graph. No new framework concept — just another conn.
  (let [calls      (atom 0)
        agent-step (fn ([] {:params {}
                            :ins  {:user-in "" :tool-result ""}
                            :outs {:tool-call "" :final ""}})
                       ([_] {}) ([s _] s)
                       ([s _ m]
                        (let [n (swap! calls inc)]
                          (if (< n 4)
                            [s {:tool-call [(dp2/child-with-data m :query)]}]
                            [s {:final     [(dp2/child-with-data m :done)]}]))))
        spec {:procs {:agent agent-step
                      :tool  (c/wrap :tool (fn [_] :tool-response))
                      :sink  (c/absorb-sink :sink)}
              :conns [[[:agent :tool-call] [:tool  :in]]
                      [[:tool  :out]       [:agent :tool-result]]
                      [[:agent :final]     [:sink  :in]]]}
        result (dp2/run! (dp2/instrument spec)
                         {:entry :agent :port :user-in :data :question})]
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

(deftest retry-succeeds-after-transient-failures
  (let [attempts (atom 0)
        flaky   (fn [x]
                  (if (< @attempts 2)
                    (do (swap! attempts inc)
                        (throw (ex-info "transient" {:attempt @attempts})))
                    x))
        spec {:procs {:try  (c/retry :try flaky 3)
                      :sink (c/absorb-sink :sink)}
              :conns [[[:try :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :try :data 42})]
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
        spec {:procs {:try  (c/retry :try permafail 2)
                      :sink (c/absorb-sink :sink)}
              :conns [[[:try :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :try :data 42})]
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
  (let [spec {:procs {:boom (c/wrap :boom (fn [_] (throw (ex-info "oops" {}))))
                      :sink (c/absorb-sink :sink)}
              :conns [[[:boom :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :boom :data 1})]
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

(deftest provenance-dag-is-well-formed
  (let [spec {:procs {:split (c/fan-out :split 3)
                      :fi    (c/fan-in  :fi "split-")
                      :sink  (c/absorb-sink :sink)}
              :conns [[[:split :out] [:fi :in]]
                      [[:fi :out]    [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :split :data :x})
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
  (let [spec {:procs {:split (c/fan-out :split 3)
                      :sink  (c/absorb-sink :sink)}
              :conns [[[:split :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :split :data :x})]
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
        spec (dp2/instrument
              {:procs {:split (c/fan-out :split 3)
                       :sink  (c/absorb-sink :sink)}
               :conns [[[:split :out] [:sink :in]]]
               :entry :split}
              {:pubsub ps :flow-id "run-A"})
        result (dp2/run! spec {:data :x})]
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
        spec-A (dp2/instrument
                {:procs {:a (c/wrap :a inc) :sa (c/absorb-sink :sa)}
                 :conns [[[:a :out] [:sa :in]]]
                 :entry :a}
                {:pubsub ps :flow-id "A"})
        spec-B (dp2/instrument
                {:procs {:b (c/wrap :b dec) :sb (c/absorb-sink :sb)}
                 :conns [[[:b :out] [:sb :in]]]
                 :entry :b}
                {:pubsub ps :flow-id "B"})
        ra (dp2/run! spec-A {:data 1})
        rb (dp2/run! spec-B {:data 2})]
    (u)
    (is (= :completed (:state ra)))
    (is (= :completed (:state rb)))
    (is (= {"A" 2 "B" 2} @tallies))))      ; 2 :recv events per flow

;; -----------------------------------------------------------------------------
;; Trace spans: observability for sequential work inside a step
;; -----------------------------------------------------------------------------

(deftest trace-span-flat
  ;; A factory step-fn uses `trace` to observe two sibling operations.
  ;; Spans emit :span-start / :span-success; run completion is unaffected.
  (let [factory (fn [{:keys [trace] :as ctx}]
                  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                      ([_] {}) ([s _] s)
                      ([s _ m]
                       (let [a (trace ctx :step-a {:kind :first}
                                      (fn [_] (+ (:data m) 1)))
                             b (trace ctx :step-b {:kind :second}
                                      (fn [_] (* a 10)))]
                         [s {:out [(dp2/child-with-data m b)]}]))))
        spec {:procs {:work factory
                      :sink (c/absorb-sink :sink)}
              :conns [[[:work :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :work :data 4})]
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
  (let [factory (fn [{:keys [trace] :as ctx}]
                  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                      ([_] {}) ([s _] s)
                      ([s _ m]
                       (let [v (trace ctx :outer {}
                                      (fn [ctx']
                                        (trace ctx' :inner {}
                                               (fn [_] (+ (:data m) 100)))))]
                         [s {:out [(dp2/child-with-data m v)]}]))))
        spec {:procs {:work factory
                      :sink (c/absorb-sink :sink)}
              :conns [[[:work :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :work :data 1})]
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
  (let [factory (fn [{:keys [trace] :as ctx}]
                  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                      ([_] {}) ([s _] s)
                      ([s _ m]
                       (let [v (trace ctx :risky {}
                                      (fn [_]
                                        (throw (ex-info "nope" {:d (:data m)}))))]
                         [s {:out [(dp2/child-with-data m v)]}]))))
        spec {:procs {:work factory
                      :sink (c/absorb-sink :sink)}
              :conns [[[:work :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :work :data 7})]
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
        factory (fn [{:keys [trace] :as ctx}]
                  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                      ([_] {}) ([s _] s)
                      ([s _ _]
                       (trace ctx :outer {}
                              (fn [ctx']
                                (trace ctx' :inner {} (fn [_] :ok))))
                       [s {}])))
        spec (dp2/instrument
              {:procs {:work factory} :conns [] :entry :work}
              {:pubsub ps :flow-id "run-T"})
        result (dp2/run! spec {:data :x})]
    (u)
    (is (= :completed (:state result)))
    (is (= #{"span-start.flow.run-T.step.work.span.outer"
             "span-start.flow.run-T.step.work.span.outer.span.inner"}
           (set @subjects)))))

(deftest nested-flow-namespaced
  ;; Nested flow as data — no subflow combinator. Step-ids can collide
  ;; between outer and inner; the flow-path in subjects + event bodies
  ;; namespaces them.
  (let [ps (pubsub/make)
        inner {:procs {:inc  (c/wrap :inc #(+ % 100))
                       :done (c/absorb-sink :done)}
               :conns [[[:inc :out] [:done :in]]]
               :entry :inc :output :done}
        outer {:procs {:inc  (c/wrap :inc inc)        ; SAME step-id as inner's :inc
                       :sub  inner                     ; <-- nested flow is data
                       :sink (c/absorb-sink :sink)}
               :conns [[[:inc :out] [:sub :in]]
                       [[:sub :out] [:sink :in]]]
               :entry :inc}
        outer-recvs (atom [])
        inner-recvs (atom [])
        u1 (pubsub/sub ps "recv.flow.outer.step.inc"
                       (fn [_ ev _] (swap! outer-recvs conj ev)))
        u2 (pubsub/sub ps "recv.flow.outer.flow.sub.step.inc"
                       (fn [_ ev _] (swap! inner-recvs conj ev)))
        result (dp2/run! (dp2/instrument outer {:pubsub ps :flow-id "outer"})
                         {:data 5})]
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
  (let [factory (fn [{:keys [trace] :as ctx}]
                  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                      ([_] {}) ([s _] s)
                      ([s _ m]
                       (trace ctx :outer {}
                              (fn [c] (trace c :inner {} (fn [_] :ok))))
                       [s {:out [(dp2/child-with-data m :ok)]}])))
        spec {:procs {:only factory :sink (c/absorb-sink :sink)}
              :conns [[[:only :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :only :data :x})
        spans  (concat (events-of result :span-start) (events-of result :span-success))]
    (is (seq spans))
    (is (every? #(= :only (:step-id %)) spans))
    (is (= #{:outer :inner} (set (map :span-name spans))))))

(deftest span-in-multi-port-step
  ;; Spans don't perturb multi-port output.
  (let [factory (fn [{:keys [trace] :as ctx}]
                  (fn ([] {:params {} :ins {:in ""} :outs {:a "" :b ""}})
                      ([_] {}) ([s _] s)
                      ([s _ m]
                       (let [x (trace ctx :compute-a {} (fn [_] (+ (:data m) 1)))
                             y (trace ctx :compute-b {} (fn [_] (* (:data m) 10)))]
                         [s {:a [(dp2/child-with-data m x)]
                             :b [(dp2/child-with-data m y)]}]))))
        spec {:procs {:work   factory
                      :sink-a (c/absorb-sink :sink-a)
                      :sink-b (c/absorb-sink :sink-b)}
              :conns [[[:work :a] [:sink-a :in]]
                      [[:work :b] [:sink-b :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :work :data 5})]
    (is (= 6  (:data (first (filterv #(= :sink-a (:step-id %)) (events-of result :recv))))))
    (is (= 50 (:data (first (filterv #(= :sink-b (:step-id %)) (events-of result :recv))))))
    (is (= {:compute-a 1 :compute-b 1}
           (frequencies (map :span-name (events-of result :span-start)))))))

(deftest span-in-nested-flow
  ;; Full scope composition: inner flow's step emits a span. Scope must be
  ;; [[:flow outer-fid] [:flow sub-sid] [:step inner-sid] [:span name]].
  (let [inner {:procs {:work (fn [{:keys [trace] :as ctx}]
                               (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                                   ([_] {}) ([s _] s)
                                   ([s _ m]
                                    (let [v (trace ctx :nested-op {:src :inner}
                                                   (fn [_] (* (:data m) 2)))]
                                      [s {:out [(dp2/child-with-data m v)]}]))))
                      :done (c/absorb-sink :done)}
               :conns [[[:work :out] [:done :in]]]
               :entry :work :output :done}
        outer {:procs {:sub  inner
                       :sink (c/absorb-sink :sink)}
               :conns [[[:sub :out] [:sink :in]]]
               :entry :sub}
        result (dp2/run! (dp2/instrument outer {:flow-id "F"}) {:data 5})
        span   (first (events-of result :span-start))]
    (is (= :completed (:state result)))
    (is (= :nested-op (:span-name span)))
    (is (= [[:flow "F"] [:flow "sub"] [:step :work] [:span :nested-op]]
           (:scope span)))
    (is (= ["F" "sub"] (:flow-path span)))))

(deftest span-event-ordering
  ;; Within one step invocation: :recv < :span-start < :span-success < :success.
  (let [factory (fn [{:keys [trace] :as ctx}]
                  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                      ([_] {}) ([s _] s)
                      ([s _ m]
                       (let [v (trace ctx :op {} (fn [_] (inc (:data m))))]
                         [s {:out [(dp2/child-with-data m v)]}]))))
        spec {:procs {:step factory :sink (c/absorb-sink :sink)}
              :conns [[[:step :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :step :data 1})
        events (:events result)
        idx-of (fn [pred] (first (keep-indexed (fn [i e] (when (pred e) i)) events)))
        recv-i (idx-of #(and (= :recv (:kind %)) (= :step (:step-id %))))
        ss-i   (idx-of #(= :span-start (:kind %)))
        sx-i   (idx-of #(= :span-success (:kind %)))
        succ-i (idx-of #(and (= :success (:kind %)) (= :step (:step-id %))))]
    (is (< recv-i ss-i sx-i succ-i))))

(deftest factory-ctx-has-scope-and-step-id
  ;; Factory is called once per proc with a ctx carrying the proc's
  ;; identity. :scope includes the step entry.
  (let [captured (atom nil)
        factory  (fn [ctx]
                   (reset! captured (select-keys ctx [:step-id :scope]))
                   (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                       ([_] {}) ([s _] s)
                       ([s _ _] [s {}])))
        spec  {:procs {:my-step factory}
               :conns []
               :entry :my-step}
        _     (dp2/run! (dp2/instrument spec {:flow-id "F"}) {:data :x})]
    (is (= {:step-id :my-step
            :scope   [[:flow "F"] [:step :my-step]]}
           @captured))))

(deftest mixed-proc-kinds
  ;; Combinator factory, user factory, and bare step-fn in one spec;
  ;; auto-detection handles all three.
  (let [combinator-factory (c/wrap :a inc)
        user-factory       (fn [_ctx]
                             (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                                 ([_] {}) ([s _] s)
                                 ([s _ m]
                                  [s {:out [(dp2/child-with-data m (* (:data m) 10))]}])))
        bare-stepfn        (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                               ([_] {}) ([s _] s)
                               ([s _ m]
                                [s {:out [(dp2/child-with-data m (dec (:data m)))]}]))
        spec {:procs {:a    combinator-factory
                      :b    user-factory
                      :c    bare-stepfn
                      :sink (c/absorb-sink :sink)}
              :conns [[[:a :out] [:b :in]]
                      [[:b :out] [:c :in]]
                      [[:c :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :a :data 5})]
    (is (= :completed (:state result)))
    ;; 5 → a(inc) → 6 → b(*10) → 60 → c(dec) → 59
    (is (= 59 (:data (first (filterv #(= :sink (:step-id %))
                                     (events-of result :recv))))))))

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
   group keyed by \"group-<parent-msg-id>\"; a downstream (c/fan-in _ \"group-\")
   will merge them."
  []
  (fn [_ctx]
    (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
        ([_] {}) ([s _] s)
        ([s _ m]
         (let [items  (:data m)
               gid    (str "group-" (:msg-id m))
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
  (let [process (fn [{:keys [trace] :as ctx}]
                  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                      ([_] {}) ([s _] s)
                      ([s _ m]
                       (let [url      (:url (:data m))
                             rss      (trace ctx :fetch-rss {:url url}
                                             (fn [_] (fake-fetch-rss url)))
                             episodes (trace ctx :parse-feed
                                             {:count (count (:items rss))}
                                             (fn [_] (fake-parse-feed rss)))
                             done     (mapv (fn [ep]
                                              (trace ctx :process-episode
                                                     {:id (:id ep)}
                                                     (fn [_]
                                                       (-> ep fake-download
                                                           fake-transcribe
                                                           fake-chunk
                                                           fake-summarize))))
                                            episodes)]
                         [s {:out [(dp2/child-with-data m (mapv :summary done))]}]))))
        spec {:procs {:process process :sink (c/absorb-sink :sink)}
              :conns [[[:process :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec)
                         {:entry :process :data {:url "https://example.com/feed.xml"}})]
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
  (let [process-one (fn [{:keys [trace] :as ctx}]
                      (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                          ([_] {}) ([s _] s)
                          ([s _ m]
                           (let [url      (:url (:data m))
                                 episodes (trace ctx :fetch-and-parse {:url url}
                                                 (fn [_] (fake-parse-feed (fake-fetch-rss url))))
                                 sums     (mapv (fn [ep]
                                                  (trace ctx :episode {:id (:id ep)}
                                                         (fn [_] (:summary (fake-summarize
                                                                            (fake-transcribe ep))))))
                                                episodes)]
                             [s {:out [(dp2/child-with-data m {:podcast url :summaries sums})]}]))))
        spec {:procs {:split   (dyn-fan-out-factory)
                      :process process-one
                      :merge   (c/fan-in :merge "group-")
                      :sink    (c/absorb-sink :sink)}
              :conns [[[:split   :out] [:process :in]]
                      [[:process :out] [:merge   :in]]
                      [[:merge   :out] [:sink    :in]]]}
        podcasts [{:url "https://a"} {:url "https://b"} {:url "https://c"}]
        result   (dp2/run! (dp2/instrument spec) {:entry :split :data podcasts})]
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
  (let [factory (fn [{:keys [trace] :as ctx}]
                  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                      ([_] {}) ([s _] s)
                      ([s _ m]
                       (let [ep      (:data m)
                             chunks  (:chunks (fake-chunk (fake-transcribe ep)))
                             analyze (fn [[i c]]
                                       (trace ctx :analyze-chunk {:idx i :size (count c)}
                                              (fn [_] (fake-analyze-part c))))
                             results (vt-map analyze (map-indexed vector chunks))]
                         [s {:out [(dp2/child-with-data m results)]}]))))
        spec {:procs {:episode factory :sink (c/absorb-sink :sink)}
              :conns [[[:episode :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :episode :data {:id :e1}})
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
  (let [aggregate (fn [{:keys [trace] :as ctx}]
                    (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                        ([_] {}) ([s _] s)
                        ([s _ m]
                         (let [vs     (:data m)
                               topics (trace ctx :detect-topics {:n (count vs)}
                                             (fn [_] (set (mapcat :keywords vs))))
                               drift  (trace ctx :spectrum-drift {}
                                             (fn [_] (reduce + 0 (map :stance vs))))
                               ranked (trace ctx :rank-stories {}
                                             (fn [_] (vec (sort-by :score > vs))))]
                           [s {:out [(dp2/child-with-data m
                                                          {:topics topics
                                                           :drift  drift
                                                           :top    (first ranked)})]}]))))
        spec {:procs {:agg aggregate :sink (c/absorb-sink :sink)}
              :conns [[[:agg :out] [:sink :in]]]}
        feed [{:keywords [:climate :election] :stance  1 :score 0.8}
              {:keywords [:tech :election]    :stance -1 :score 0.9}
              {:keywords [:climate :economy]  :stance  0 :score 0.5}]
        result (dp2/run! (dp2/instrument spec) {:entry :agg :data feed})
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
  (let [process (fn [{:keys [trace] :as ctx}]
                  (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                      ([_] {}) ([s _] s)
                      ([s _ m]
                       (let [url      (:url (:data m))
                             episodes (trace ctx :fetch {:url url}
                                             (fn [_] (fake-parse-feed (fake-fetch-rss url))))
                             per-ep   (fn [ep]
                                        (trace ctx :episode {:id (:id ep)}
                                               (fn [c]
                                                 (let [chunks (:chunks (fake-chunk (fake-transcribe ep)))
                                                       analyze-part (fn [[i p]]
                                                                      (trace c :analyze-part
                                                                             {:idx i}
                                                                             (fn [_] (fake-analyze-part p))))]
                                                   {:id (:id ep)
                                                    :parts (vt-map analyze-part
                                                                   (map-indexed vector chunks))}))))
                             results  (vt-map per-ep episodes)]
                         [s {:out [(dp2/child-with-data m {:podcast url :episodes results})]}]))))
        spec {:procs {:split   (dyn-fan-out-factory)
                      :process process
                      :merge   (c/fan-in :merge "group-")
                      :sink    (c/absorb-sink :sink)}
              :conns [[[:split   :out] [:process :in]]
                      [[:process :out] [:merge   :in]]
                      [[:merge   :out] [:sink    :in]]]}
        podcasts [{:url "https://a"} {:url "https://b"}]
        result   (dp2/run! (dp2/instrument spec) {:entry :split :data podcasts})]
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
