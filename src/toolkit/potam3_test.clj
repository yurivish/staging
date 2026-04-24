(ns toolkit.potam3-test
  (:require [clojure.test :refer [deftest testing is]]
            [toolkit.potam3.combinators :as c]
            [toolkit.potam3.flow :as flow]
            [toolkit.potam3.msg :as msg]
            [toolkit.potam3.step :as step]
            [toolkit.potam3.token :as tok]))

;; ----------------------------------------------------------------------------
;; Helpers
;; ----------------------------------------------------------------------------

(defn- events-of
  ([result kind]          (filterv #(= kind (:kind %)) (:events result)))
  ([result kind msg-kind] (filterv #(and (= kind (:kind %)) (= msg-kind (:msg-kind %)))
                                   (:events result))))

(defn- recvs-at [result sid]
  (filterv #(and (= sid (:step-id %)) (= :recv (:kind %)))
           (:events result)))

(defn- sends-at [result sid]
  (filterv #(and (= sid (:step-id %)) (= :send-out (:kind %)))
           (:events result)))

;; ----------------------------------------------------------------------------
;; Act I — Linear pipelines
;; ----------------------------------------------------------------------------

(deftest single-step-pipeline
  (let [wf     (step/serial (step/step :inc inc) (step/sink))
        result (flow/run! wf {:data 1})]
    (is (= :completed (:state result)))
    (is (= 2 (:data (first (recvs-at result :sink)))))))

(deftest chained-pipeline-threads-data
  (let [wf     (step/serial
                (step/step :inc inc)
                (step/step :dbl #(* 2 %))
                (step/sink))
        result (flow/run! wf {:data 5})]
    (is (= :completed (:state result)))
    (is (= 12 (:data (first (recvs-at result :sink)))))))

(deftest handler-state-threads-across-invocations
  (let [wf     (step/step :acc nil
                          (fn [_ctx s d]
                            (let [s' (update s :sum (fnil + 0) d)]
                              [s' {:out [(:sum s')]}])))
        result (flow/run-seq wf [1 2 3])]
    (is (= :completed (:state result)))
    (is (= [[1] [3] [6]] (:outputs result)))))

;; ----------------------------------------------------------------------------
;; Act II — Fan-out / fan-in
;; ----------------------------------------------------------------------------

(deftest fan-out-mints-zero-sum-group
  (let [wf     (step/serial (c/fan-out :split 3) (step/sink))
        result (flow/run! wf {:data :x})
        sends  (sends-at result :split)]
    (testing "three sends on the fan-out"
      (is (= 3 (count sends))))
    (testing "tokens for the minted group XOR to 0"
      (let [gk (first (filter vector? (mapcat (comp keys :tokens) sends)))]
        (is (some? gk))
        (is (= 0 (reduce bit-xor 0 (mapv #(get-in % [:tokens gk]) sends))))))))

(deftest fan-in-closes-group-when-xor-zero
  (let [wf     (step/serial
                (c/fan-out :split 3)
                (c/fan-in :fi :split)
                (step/sink))
        result (flow/run! wf {:data :x})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "exactly one merge, with three parents"
      (let [merges (events-of result :merge)]
        (is (= 1 (count merges)))
        (is (= 3 (count (:parent-msg-ids (first merges)))))))
    (testing "sink received exactly one merged msg"
      (is (= 1 (count (recvs-at result :sink))))
      (is (= [:x :x :x] (:data (first (recvs-at result :sink))))))))

;; ----------------------------------------------------------------------------
;; Act III — Signals
;; ----------------------------------------------------------------------------

(deftest signal-flows-through-untouched
  (let [source (step/step :source nil
                          (fn [ctx _s _d]
                            {:out [(msg/signal ctx)]}))
        wf     (step/serial
                source
                (step/step :mid inc)
                (step/sink))
        result (flow/run! wf {:data :seed})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "user handler for :mid never ran (no data recv)"
      (is (zero? (count (filterv #(and (= :mid (:step-id %))
                                        (= :recv (:kind %))
                                        (= :data (:msg-kind %)))
                                 (:events result))))))
    (testing "sink received a signal"
      (is (= 1 (count (filterv #(and (= :sink (:step-id %))
                                      (= :recv (:kind %))
                                      (= :signal (:msg-kind %)))
                               (:events result))))))))

(deftest inject-signal-preserves-tokens
  (let [wf     (step/serial (step/step :a inc) (step/sink))
        h      (flow/start! wf)]
    (try
      (flow/inject! h {:tokens {"g" 42}})
      (flow/await-quiescent! h)
      (let [result (flow/stop! h)
            sink-recv (first (filterv #(and (= :sink (:step-id %))
                                            (= :recv (:kind %))
                                            (= :signal (:msg-kind %)))
                                      (:events result)))]
        (is (some? sink-recv))
        (is (= {"g" 42} (:tokens sink-recv))))
      (finally (when-not (realized? (::flow/cancel h)) (flow/stop! h))))))

;; ----------------------------------------------------------------------------
;; Act IV — Done cascade
;; ----------------------------------------------------------------------------

(deftest done-cascades-through-chain
  (let [wf (step/serial (step/step :a inc) (step/step :b inc) (step/sink))
        h  (flow/start! wf)]
    (flow/inject! h {})
    (flow/await-quiescent! h)
    (let [result (flow/stop! h)
          by     (fn [sid]
                   (frequencies (map :msg-kind
                                     (filterv #(and (= sid (:step-id %))
                                                    (= :recv (:kind %)))
                                              (:events result)))))]
      (is (= :completed (:state result)))
      (is (= 1 (get (by :a) :done 0)))
      (is (= 1 (get (by :b) :done 0)))
      (is (= 1 (get (by :sink) :done 0))))))

;; ----------------------------------------------------------------------------
;; Act V — run-seq attribution
;; ----------------------------------------------------------------------------

(deftest run-seq-attributes-outputs-to-inputs
  (let [wf     (step/step :dbl #(* 2 %))
        result (flow/run-seq wf [1 2 3 10])]
    (is (= :completed (:state result)))
    (is (= [[2] [4] [6] [20]] (:outputs result)))))

(deftest run-seq-with-fan-out-fan-in
  (let [wf     (step/serial
                (c/fan-out :split 2)
                (step/step :dbl #(* 2 %))
                (c/fan-in :fi :split))
        result (flow/run-seq wf [1 5])]
    (is (= :completed (:state result)))
    (is (= [[[2 2]] [[10 10]]] (:outputs result)))))

;; ----------------------------------------------------------------------------
;; Act VI — Pure unit tests (msg/synthesize)
;; ----------------------------------------------------------------------------

(deftest synthesize-preserves-tokens-1-to-1
  (let [input   (assoc (msg/new-msg :x) :tokens {"g" 42})
        drafts  {:out [(msg/child nil input :y)]}
        [pm _]  (msg/synthesize drafts input :step)
        out-msg (first (:out pm))]
    (is (= {"g" 42} (:tokens out-msg)))
    (is (= :y (:data out-msg)))))

(deftest synthesize-splits-tokens-k-ways
  (let [input  (assoc (msg/new-msg :x) :tokens {"g" 123})
        drafts {:out [(msg/child nil input :a)
                      (msg/child nil input :b)
                      (msg/child nil input :c)]}
        [pm _] (msg/synthesize drafts input :step)
        outs   (:out pm)
        xor-g  (reduce bit-xor 0 (mapv #(get-in % [:tokens "g"]) outs))]
    (is (= 3 (count outs)))
    (is (= 123 xor-g))))

(deftest synthesize-applies-assoc-tokens
  (let [input  (msg/new-msg :x)
        draft  (-> (msg/child nil input :y) (msg/assoc-tokens {"extra" 99}))
        [pm _] (msg/synthesize {:out [draft]} input :step)
        m      (first (:out pm))]
    (is (= {"extra" 99} (:tokens m)))))
