(ns toolkit.datapotamus2.examples-test
  "Tests-as-examples for Datapotamus2. Each deftest is a single-vignette
   demonstrating one capability; the suite ordered simplest → most
   sophisticated."
  (:require [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.dependency :as dep]
            [toolkit.datapotamus2.core :as dp2]
            [toolkit.datapotamus2.combinators :as c]
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

(deftest failure-propagates-to-failed-state
  (let [permafail (fn [_] (throw (ex-info "boom" {:reason :permafail})))
        spec {:procs {:try  (c/retry :try permafail 2)
                      :sink (c/absorb-sink :sink)}
              :conns [[[:try :out] [:sink :in]]]}
        result (dp2/run! (dp2/instrument spec) {:entry :try :data 42})]
    (testing "run fails"
      (is (= :failed (:state result))))
    (testing "exactly one :failure event for :try"
      (let [fs (events-of result :failure)]
        (is (= 1 (count fs)))
        (is (= :try (:step-id (first fs))))
        (is (= "boom" (get-in (first fs) [:error :message])))))
    (testing "sink never received"
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
