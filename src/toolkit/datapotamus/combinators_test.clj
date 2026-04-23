(ns toolkit.datapotamus.combinators-test
  "Tests for the draft combinator layer in
   `toolkit.datapotamus.combinators`. Organized by the Parts called out
   in that file."
  (:refer-clojure :exclude [map filter mapcat])
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus :as dp]
            [toolkit.datapotamus.combinators :as c]))

(defn- events-of
  ([result kind]
   (filterv #(= kind (:kind %)) (:events result)))
  ([result kind msg-kind]
   (filterv #(and (= kind (:kind %)) (= msg-kind (:msg-kind %)))
            (:events result))))

(defn- recvs-at [result sid]
  (filterv #(= sid (:step-id %)) (events-of result :recv)))

(defn- sends-at [result sid]
  (filterv #(and (= sid (:step-id %)) (:port %))
           (events-of result :send-out)))

;; ============================================================================
;; Part 1 — Stream-shape (1:1)
;; ============================================================================

(deftest map-is-a-pure-fn-alias
  (let [wf     (dp/serial (c/map :inc inc) (dp/sink))
        result (dp/run! wf {:data 5})]
    (is (= :completed (:state result)))
    (is (= 6 (:data (first (recvs-at result :sink)))))))

(deftest filter-routes-pass-and-fail
  (let [wf (-> (dp/merge-steps
                (c/filter :odd? odd?)
                (dp/sink :pass-sink)
                (dp/sink :fail-sink))
               (dp/connect [:odd? :pass] [:pass-sink :in])
               (dp/connect [:odd? :fail] [:fail-sink :in])
               (dp/input-at :odd?))
        h  (dp/start! wf)]
    (doseq [d [1 2 3 4 5]] (dp/inject! h {:data d}))
    (dp/await-quiescent! h 5000)
    (let [result (dp/stop! h)]
      (testing "run completes"
        (is (= :completed (:state result))))
      (testing "odds routed to :pass-sink, evens to :fail-sink"
        (is (= 3 (count (recvs-at result :pass-sink))))
        (is (= 2 (count (recvs-at result :fail-sink)))))
      (testing "router emitted 5 send-outs"
        (is (= 5 (count (sends-at result :odd?))))))))

(deftest scan-threads-accumulator-across-invocations
  (let [wf     (c/scan :sum 0 +)
        result (dp/run-seq wf [1 2 3 4])]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "each input sees the running sum"
      (is (= [[1] [3] [6] [10]] (:outputs result))))))

(deftest tap-runs-side-effect-forwards-data-unchanged
  (let [seen   (atom [])
        wf     (c/tap :probe #(swap! seen conj %))
        result (dp/run-seq wf [1 2 3])]
    (testing "run completes; downstream sees unmodified values"
      (is (= :completed (:state result)))
      (is (= [[1] [2] [3]] (:outputs result))))
    (testing "tap observed every input exactly once, in order"
      (is (= [1 2 3] @seen)))))

;; ============================================================================
;; Part 2 — Cardinality changers
;; ============================================================================

(deftest mapcat-emits-one-child-per-item-in-collection
  (let [wf     (dp/serial (c/mapcat :expand identity) (dp/sink))
        result (dp/run! wf {:data [:a :b :c]})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "three :send-out events on :expand"
      (is (= 3 (count (sends-at result :expand)))))
    (testing "sink received three msgs, one per collection element"
      (is (= #{:a :b :c} (set (clojure.core/map :data (recvs-at result :sink))))))))

(deftest mapcat-plus-fan-in-closes-the-group
  (let [wf     (dp/serial
                (c/mapcat :expand identity)
                (dp/fan-in :fi :expand)
                (dp/sink))
        result (dp/run! wf {:data [:a :b :c]})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "exactly one merge at the fan-in with three parents"
      (let [merges (events-of result :merge)]
        (is (= 1 (count merges)))
        (is (= 3 (count (:parent-msg-ids (first merges)))))))
    (testing "sink received exactly one merged msg"
      (is (= 1 (count (recvs-at result :sink)))))))

(deftest mapcat-establishes-zero-sum-group-token
  (let [wf     (dp/serial (c/mapcat :expand identity) (dp/sink))
        result (dp/run! wf {:data [:a :b :c]})
        sends  (sends-at result :expand)
        gkey   (first (distinct (clojure.core/mapcat
                                 (fn [e] (clojure.core/filter vector?
                                                              (keys (:tokens e))))
                                 sends)))]
    (is (some? gkey) "mapcat stamped a vector group-key onto the children")
    (when gkey
      (let [values (mapv #(get-in % [:tokens gkey]) sends)]
        (is (= 0 (reduce bit-xor values))
            "the three children's group tokens XOR to 0")))))

(deftest tee-broadcasts-to-every-named-port
  (let [wf (-> (dp/merge-steps
                (c/tee :fork [:a :b :c])
                (dp/sink :sink-a)
                (dp/sink :sink-b)
                (dp/sink :sink-c))
               (dp/connect [:fork :a] [:sink-a :in])
               (dp/connect [:fork :b] [:sink-b :in])
               (dp/connect [:fork :c] [:sink-c :in])
               (dp/input-at :fork))
        result (dp/run! wf {:data 42})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "each named sink received exactly one msg carrying the input"
      (doseq [sid [:sink-a :sink-b :sink-c]]
        (let [rs (recvs-at result sid)]
          (is (= 1 (count rs)))
          (is (= 42 (:data (first rs)))))))))

(deftest tee-establishes-zero-sum-group-token
  (let [wf (-> (dp/merge-steps
                (c/tee :fork [:a :b])
                (dp/sink :sink-a)
                (dp/sink :sink-b))
               (dp/connect [:fork :a] [:sink-a :in])
               (dp/connect [:fork :b] [:sink-b :in])
               (dp/input-at :fork))
        result (dp/run! wf {:data 42})
        sends  (sends-at result :fork)
        gkey   (first (distinct (clojure.core/mapcat
                                 (fn [e] (clojure.core/filter vector?
                                                              (keys (:tokens e))))
                                 sends)))]
    (is (some? gkey) "tee stamped a vector group-key onto its broadcasts")
    (when gkey
      (let [values (mapv #(get-in % [:tokens gkey]) sends)]
        (is (= 0 (reduce bit-xor values))
            "the two broadcasts' group tokens XOR to 0")))))

;; ============================================================================
;; Part 3 — Error routing
;; ============================================================================

(deftest try-catch-routes-success-and-exceptions-to-different-ports
  (let [wf (-> (dp/merge-steps
                (c/try-catch :safe-div #(/ 10 %))
                (dp/sink :ok-sink)
                (dp/sink :err-sink))
               (dp/connect [:safe-div :out] [:ok-sink :in])
               (dp/connect [:safe-div :err] [:err-sink :in])
               (dp/input-at :safe-div))
        happy (dp/run! wf {:data 2})
        sad   (dp/run! wf {:data 0})]
    (testing "happy path: (/ 10 2) → :ok-sink with value 5"
      (is (= :completed (:state happy)))
      (is (= 5 (:data (first (recvs-at happy :ok-sink)))))
      (is (empty? (recvs-at happy :err-sink)))
      (is (empty? (events-of happy :failure))))
    (testing "sad path: (/ 10 0) → :err-sink with {:error <Throwable> :data 0}"
      (is (= :completed (:state sad)))
      (is (empty? (recvs-at sad :ok-sink)))
      (let [err-data (:data (first (recvs-at sad :err-sink)))]
        (is (instance? Throwable (:error err-data)))
        (is (= 0 (:data err-data))))
      (testing "no :failure event — the exception was handled as data"
        (is (empty? (events-of sad :failure)))))))

;; ============================================================================
;; Part 4 — Composition
;; ============================================================================

(deftest loop-until-iterates-body-until-pred-fires
  (let [body     (dp/step :body inc)
        wf       (dp/serial
                  (c/loop-until :spin body #(>= % 3))
                  (dp/sink))
        h        (dp/start! wf)
        _        (dp/inject! h {:data 0})
        signal   (dp/await-quiescent! h 5000)
        result   (dp/stop! h)]
    (testing "loop quiesces within timeout"
      (is (= :quiescent signal)))
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "sink received the exit value exactly once"
      (let [rs (recvs-at result :sink)]
        (is (= 1 (count rs)))
        (is (= 3 (:data (first rs))))))
    (testing "body fired 3 times (0→1, 1→2, 2→3)"
      (is (= 3 (count (recvs-at result :body)))))))

(deftest for-each-applies-inner-step-to-each-element
  (let [inner  (dp/step :double #(* 2 %))
        wf     (dp/serial (c/for-each :fe identity inner) (dp/sink))
        result (dp/run! wf {:data [1 2 3]})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "sink received exactly one merged msg"
      (is (= 1 (count (recvs-at result :sink)))))
    (testing "merged data has doubled values (order not asserted — fan-in uses arrival order)"
      (is (= #{2 4 6}
             (set (:data (first (recvs-at result :sink)))))))))

(deftest into-vec-aliases-fan-in
  (let [wf     (dp/serial
                (dp/fan-out :split 3)
                (c/into-vec :collect :split)
                (dp/sink))
        result (dp/run! wf {:data :x})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "exactly one merge closes the fan-out group"
      (is (= 1 (count (events-of result :merge)))))
    (testing "sink received exactly one merged msg carrying three copies"
      (let [rs (recvs-at result :sink)]
        (is (= 1 (count rs)))
        (is (= 3 (count (:data (first rs)))))
        (is (= #{:x} (set (:data (first rs)))))))))

;; ============================================================================
;; Part 5 — Joins
;; ============================================================================

(deftest join-by-key-pairs-inputs-by-key
  (let [wf (-> (dp/merge-steps
                (dp/step :src-l identity)
                (dp/step :src-r identity)
                (c/join-by-key :j [:left :right] :k)
                (dp/sink))
               (dp/connect [:src-l :out] [:j :left])
               (dp/connect [:src-r :out] [:j :right])
               (dp/connect [:j :out]     [:sink :in])
               (dp/input-at :src-l))
        h  (dp/start! wf)]
    (dp/inject! h {:in :src-l :data {:k :x :v :L1}})
    (dp/inject! h {:in :src-r :data {:k :x :v :R1}})
    (dp/inject! h {:in :src-l :data {:k :y :v :L2}})
    (dp/await-quiescent! h 5000)
    (let [result (dp/stop! h)
          rs     (recvs-at result :sink)]
      (testing "exactly one msg paired at :x; :y stayed buffered"
        (is (= 1 (count rs)))
        (is (= [{:k :x :v :L1} {:k :x :v :R1}] (:data (first rs))))))))

(deftest join-by-key-xor-combines-parents-tokens
  (let [wf (-> (dp/merge-steps
                (dp/step :src-l identity)
                (dp/step :src-r identity)
                (c/join-by-key :j [:left :right] :k)
                (dp/sink))
               (dp/connect [:src-l :out] [:j :left])
               (dp/connect [:src-r :out] [:j :right])
               (dp/connect [:j :out]     [:sink :in])
               (dp/input-at :src-l))
        h  (dp/start! wf)]
    (dp/inject! h {:in :src-l :data {:k :m} :tokens {"g" 7}})
    (dp/inject! h {:in :src-r :data {:k :m} :tokens {"g" 11}})
    (dp/await-quiescent! h 5000)
    (let [result (dp/stop! h)
          sends  (sends-at result :j)]
      (testing "join emitted exactly one msg"
        (is (= 1 (count sends))))
      (testing "emitted tokens carry XOR of the two injected values"
        (is (= (bit-xor 7 11) (get-in (first sends) [:tokens "g"])))))))

;; ============================================================================
;; Part 6 — Stateful accumulators
;; ============================================================================

(deftest batch-emits-every-n-inputs
  (let [wf (dp/serial (c/batch :b 2) (dp/sink))
        h  (dp/start! wf)]
    (doseq [d [1 2 3 4 5 6]] (dp/inject! h {:data d}))
    (dp/await-quiescent! h 5000)
    (let [result (dp/stop! h)
          rs     (recvs-at result :sink)]
      (testing "run completes; sink received 3 batches of 2"
        (is (= :completed (:state result)))
        (is (= 3 (count rs)))
        (is (= [[1 2] [3 4] [5 6]] (mapv :data rs)))))))

(deftest batch-xor-combines-tokens-across-batch-members
  (let [wf (dp/serial (c/batch :b 2) (dp/sink))
        h  (dp/start! wf)]
    (dp/inject! h {:data :a :tokens {"g" 1}})
    (dp/inject! h {:data :b :tokens {"g" 2}})
    (dp/inject! h {:data :c :tokens {"g" 4}})
    (dp/inject! h {:data :d :tokens {"g" 8}})
    (dp/await-quiescent! h 5000)
    (let [result (dp/stop! h)
          sends  (sends-at result :b)]
      (testing "two batches emitted"
        (is (= 2 (count sends))))
      (testing "each batch's token is XOR of its two members"
        (is (= [(bit-xor 1 2) (bit-xor 4 8)]
               (mapv #(get-in % [:tokens "g"]) sends)))))))

(deftest batch-drops-partial-tail-on-quiescence
  ;; Pins the "LOSSY AT STREAM END" contract documented at
  ;; combinators.clj:340 — a partial batch that never reaches N is
  ;; silently dropped, along with its tokens.
  (let [wf (dp/serial (c/batch :b 2) (dp/sink))
        h  (dp/start! wf)]
    (doseq [d [1 2 3]] (dp/inject! h {:data d}))
    (dp/await-quiescent! h 5000)
    (let [result (dp/stop! h)
          rs     (recvs-at result :sink)]
      (testing "only the one full batch reaches the sink"
        (is (= 1 (count rs)))
        (is (= [1 2] (:data (first rs)))))
      (testing "no downstream trace of the 3rd input's msg"
        (is (= 1 (count (sends-at result :b))))))))
