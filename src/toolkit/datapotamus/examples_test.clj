(ns toolkit.datapotamus.examples-test
  "Tests-as-examples for Datapotamus."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.dependency :as dep]
            [toolkit.datapotamus :as dp]
            [toolkit.pubsub :as pubsub]))

(defn- events-of
  ([result kind]
   (filterv #(= kind (:kind %)) (:events result)))
  ([result kind msg-kind]
   (filterv #(and (= kind (:kind %)) (= msg-kind (:msg-kind %)))
            (:events result))))

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
               :kinds (if (= :merge (get kinds (:msg-id e)))
                        kinds
                        (assoc kinds (:msg-id e) (:kind e)))}
              {:g g :kinds kinds}))
          {:g (dep/graph) :kinds {}}
          events))

;; -----------------------------------------------------------------------------
;; Linear chains
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
    (testing "exactly two port send-outs"
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
                             (filter vector?)
                             distinct)]
        (is (= 1 (count group-keys)))
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
    (testing "the sink-bound msg IS the merge node"
      (let [merge-ev (first (events-of result :merge))
            fi-send  (first (filterv #(and (= :fi (:step-id %)) (:port %))
                                     (events-of result :send-out)))]
        (is (= (:msg-id merge-ev) (:msg-id fi-send)))
        (is (= 3 (count (:parent-msg-ids fi-send))))
        (is (= (:parent-msg-ids merge-ev) (:parent-msg-ids fi-send)))))))

(deftest nested-fan-out-fan-in-composes
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
    (testing "odds routed to :odd-sink (1, 3, 5), evens to :even-sink (2, 4)"
      (is (= 3 (count (filterv #(= :odd-sink  (:step-id %)) (events-of result :recv)))))
      (is (= 2 (count (filterv #(= :even-sink (:step-id %)) (events-of result :recv))))))
    (testing "router emitted 5 send-outs total"
      (is (= 5 (count (filterv #(and (= :route (:step-id %)) (:port %))
                               (events-of result :send-out))))))))

(deftest agent-loop
  (let [calls (atom 0)
        agent (dp/step :agent
                {:ins  {:user-in "" :tool-result ""}
                 :outs {:tool-call "" :final ""}}
                (fn [_ctx s _d]
                  (let [n (swap! calls inc)]
                    (if (< n 4)
                      [s [[:tool-call :query]]]
                      [s [[:final     :done]]]))))
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
    (testing "agent recv'd 4 times"
      (is (= 4 (count (filterv #(= :agent (:step-id %)) (events-of result :recv))))))
    (testing "tool recv'd 3 times"
      (is (= 3 (count (filterv #(= :tool (:step-id %)) (events-of result :recv))))))
    (testing "sink recv'd exactly once"
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
    (testing "retries are invisible in the trace"
      (is (= {:try 1 :sink 1}
             (frequencies (map :step-id (events-of result :recv))))))
    (testing "no failure events"
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
    (testing ":recv was emitted for :boom"
      (is (= 1 (count (filterv #(= :boom (:step-id %)) (events-of result :recv))))))
    (testing "failed msg does not propagate to sink"
      (is (empty? (filterv #(= :sink (:step-id %)) (events-of result :recv)))))))

;; -----------------------------------------------------------------------------
;; Provenance DAG + multiplicity
;; -----------------------------------------------------------------------------

(deftest provenance-dag-is-well-formed
  (let [wf     (dp/serial
                (dp/fan-out :split 3)
                (dp/fan-in :fi :split)
                (dp/sink))
        result (dp/run! wf {:data :x})
        {:keys [g kinds]} (events->dag (:events result))]
    (testing "graph is acyclic"
      (is (some? (dep/topo-sort g))))
    (testing "exactly one real root"
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
    (is (= 4 @recv-count))))

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
    (is (= {"A" 2 "B" 2} @tallies))))

(deftest nested-flow-namespaced
  (let [ps (pubsub/make)
        inner (dp/step :inc #(+ % 100))
        outer (dp/serial
               (dp/step :inc inc)
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

(deftest factory-ctx-has-scope-and-step-id
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
                (dp/step :b nil (fn [_ctx s d] [s [[:out (* d 10)]]]))
                (dp/step :c nil (fn [_ctx s d] [s [[:out (dec d)]]]))
                (dp/sink))
        result (dp/run! wf {:data 5})]
    (is (= :completed (:state result)))
    (is (= 59 (:data (first (filterv #(= :sink (:step-id %))
                                     (events-of result :recv))))))))

(deftest hand-authored-step-map
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
;; Long-running handle API
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
               (fn [{:keys [cancel] :as ctx} s _d]
                 (reset! observed (realized? cancel))
                 [s [[:out (dp/pass ctx)]]]))
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
;; Podcast-domain example (simplified — no spans)
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

(deftest example-podcast-sequential-pipeline
  (let [wf     (dp/step :process nil
                 (fn [_ctx s d]
                   (let [url      (:url d)
                         rss      (fake-fetch-rss url)
                         episodes (fake-parse-feed rss)
                         done     (mapv (fn [ep]
                                          (-> ep fake-download
                                              fake-transcribe
                                              fake-chunk
                                              fake-summarize))
                                        episodes)]
                     [s [[:out (mapv :summary done)]]])))
        result (dp/run-seq wf [{:url "https://example.com/feed.xml"}])]
    (testing "run completes; output attributed to the single input"
      (is (= :completed (:state result)))
      (is (= [["summary-e1" "summary-e2" "summary-e3"]]
             (first (:outputs result)))))))
