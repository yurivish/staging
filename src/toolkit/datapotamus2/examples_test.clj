(ns toolkit.datapotamus2.examples-test
  "Tests-as-examples for Datapotamus2. Each deftest is a single-vignette
   demonstrating one capability; the suite ordered simplest → most
   sophisticated."
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus2.core :as dp2]
            [toolkit.datapotamus2.combinators :as c]))

(defn- events-of [result kind]
  (filterv #(= kind (:kind %)) (:events result)))

(defn- build-dag
  "Fold events into {msg-id → {:parents [...] :kind ...}}. Roots have
   empty :parents; every other node lists its parents."
  [events]
  (reduce (fn [g e]
            (case (:kind e)
              :seed     (assoc g (:msg-id e) {:parents [] :kind :seed})
              :send-out (assoc g (:msg-id e) {:parents (vec (:parent-msg-ids e))
                                              :kind :send-out})
              :merge    (assoc g (:msg-id e) {:parents (vec (:parent-msg-ids e))
                                              :kind :merge})
              g))
          {} events))

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
        dag    (build-dag (:events result))]
    (testing "every non-root msg's parents exist in the DAG"
      (doseq [[mid {:keys [parents]}] dag]
        (doseq [p parents]
          (is (contains? dag p) (str "parent " p " of " mid " missing")))))
    (testing "exactly one root node (the seed)"
      (is (= 1 (count (filterv (fn [[_ v]] (empty? (:parents v))) dag)))))
    (testing "exactly one merge node, with three parents"
      (let [merges-in-dag (filterv (fn [[_ v]] (= :merge (:kind v))) dag)]
        (is (= 1 (count merges-in-dag)))
        (is (= 3 (count (:parents (second (first merges-in-dag)))))))) ))

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
