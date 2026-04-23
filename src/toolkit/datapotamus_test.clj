(ns toolkit.datapotamus-test
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus :as dp]))

(defn- events-of
  ([result kind]
   (filterv #(= kind (:kind %)) (:events result)))
  ([result kind msg-kind]
   (filterv #(and (= kind (:kind %)) (= msg-kind (:msg-kind %)))
            (:events result))))

;; --- step + serial: smallest composition -----------------------------------

(deftest step-is-a-1-in-1-out-step
  (let [f (dp/step :inc inc)]
    (is (= #{:inc} (set (keys (:procs f)))))
    (is (= :inc (:in f)))
    (is (= :inc (:out f)))
    (is (= [] (:conns f)))))

(deftest serial-glues-steps-sequentially
  (let [a (dp/step :inc inc)
        b (dp/step :dbl #(* 2 %))
        composed (dp/serial a b (dp/sink))
        result   (dp/run! composed {:data 5})]
    (testing "run completes and sink received the expected data"
      (is (= :completed (:state result)))
      (is (= 12 (:data (first (filterv #(= :sink (:step-id %))
                                       (events-of result :recv)))))))
    (testing "conns were auto-wired"
      (is (= [[[:inc :out] [:dbl :in]]
              [[:dbl :out] [:sink :in]]]
             (:conns composed))))
    (testing "input/output are first/last"
      (is (= :inc  (:in composed)))
      (is (= :sink (:out composed))))))

(deftest serial-rejects-collisions
  (let [a (dp/step :same inc)
        b (dp/step :same dec)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"colliding"
                          (dp/serial a b)))))

(deftest serial-respects-vector-boundaries
  (let [custom (-> (dp/step :b
                     {:ins  {:custom-in  ""}
                      :outs {:custom-out ""}}
                     (fn [_ctx s d]
                       [s [[:custom-out (* d 10)]]]))
                   (dp/input-at  [:b :custom-in])
                   (dp/output-at [:b :custom-out]))
        composed (dp/serial
                  (dp/step :a inc)
                  custom
                  (dp/sink))
        result   (dp/run! composed {:data 3})]
    (testing "run completes; b received through :custom-in and emitted on :custom-out"
      (is (= :completed (:state result)))
      (is (= 40 (:data (first (filterv #(= :sink (:step-id %))
                                       (events-of result :recv)))))))
    (testing "conns wire through the declared ports"
      (is (= #{[[:a :out]          [:b :custom-in]]
               [[:b :custom-out]   [:sink :in]]}
             (set (:conns composed)))))))

;; --- as-step: black-box composition ---------------------------------------

(deftest deep-nested-as-step
  (let [leaf  (dp/step :leaf inc)
        sub3  (dp/as-step :sub3 leaf)
        sub2  (dp/as-step :sub2 sub3)
        sub1  (dp/as-step :sub1 sub2)
        outer (dp/serial sub1 (dp/sink))
        result (dp/run! outer {:flow-id "F" :data 41})
        leaf-recv (first (filterv #(= :leaf (:step-id %))
                                  (events-of result :recv)))]
    (testing "run completes; sink got (inc 41) = 42"
      (is (= :completed (:state result)))
      (is (= 42 (:data (first (filterv #(= :sink (:step-id %))
                                       (events-of result :recv)))))))
    (testing "leaf step's scope nests one [:flow] per subflow level"
      (is (= [[:flow "F"]
              [:flow "sub1"]
              [:flow "sub2"]
              [:flow "sub3"]
              [:step :leaf]]
             (:scope leaf-recv))))
    (testing ":flow-path is the [:flow] sids in order"
      (is (= ["F" "sub1" "sub2" "sub3"] (:flow-path leaf-recv))))
    (testing ":step-id stays the original :leaf, not the prefixed graph id"
      (is (= :leaf (:step-id leaf-recv))))))

(deftest as-step-namespaces-inner-procs
  (let [inner    (dp/serial
                  (dp/step :a inc)
                  (dp/step :b #(* 10 %)))
        outer    (dp/serial
                  (dp/step :pre  identity)
                  (dp/as-step :sub inner)
                  (dp/step :post identity)
                  (dp/sink))
        result   (dp/run! outer {:flow-id "f" :data 2})
        sink-recv (first (filterv #(= :sink (:step-id %))
                                  (events-of result :recv)))]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "data flowed through pre → sub(a,b) → post → sink"
      (is (= 30 (:data sink-recv))))
    (testing "inner events have the nested [:flow :sub] scope segment"
      (let [inner-a-recv (first (filterv #(and (= :a (:step-id %))
                                                (= :recv (:kind %)))
                                          (:events result)))]
        (is (= [[:flow "f"] [:flow "sub"] [:step :a]]
               (:scope inner-a-recv)))))
    (testing "outer-level step (e.g. :pre) has only the flow-level scope"
      (let [pre-recv (first (filterv #(and (= :pre (:step-id %))
                                            (= :recv (:kind %)))
                                      (:events result)))]
        (is (= [[:flow "f"] [:step :pre]]
               (:scope pre-recv)))))))

;; --- passthrough -----------------------------------------------------------

(deftest passthrough-is-transparent
  (let [wf (dp/serial
             (dp/step :inc inc)
             (dp/passthrough)
             (dp/sink))
        result (dp/run! wf {:data 7})]
    (is (= :completed (:state result)))
    (is (= 8 (:data (first (filterv #(= :sink (:step-id %))
                                    (events-of result :recv))))))))

(deftest passthrough-preserves-data-id
  (let [wf (dp/serial
            (dp/step :inc inc)
            (dp/passthrough :pt)
            (dp/sink))
        result (dp/run! wf {:data 7})
        pt-send (first (filterv #(and (= :pt (:step-id %)) (:port %))
                                (events-of result :send-out)))
        sink-recv (first (filterv #(= :sink (:step-id %))
                                  (events-of result :recv)))]
    (is (= :completed (:state result)))
    (is (= (:data-id pt-send) (:data-id sink-recv))
        "sink receives same data-id as passthrough's send")))

;; --- merge-steps + connect ------------------------------------------------

(deftest agent-style-multi-port-with-connect
  (let [calls (atom 0)
        agent-step
        (dp/step :agent
          {:ins  {:user-in "" :tool-result ""}
           :outs {:tool-call "" :final ""}}
          (fn [_ctx s _d]
            (let [n (swap! calls inc)]
              (if (< n 3)
                [s [[:tool-call :query]]]
                [s [[:final     :done]]]))))
        wf (-> (dp/merge-steps
                agent-step
                (dp/step :tool (constantly :tool-response))
                (dp/sink))
               (dp/connect [:agent :tool-call] [:tool :in])
               (dp/connect [:tool :out]        [:agent :tool-result])
               (dp/connect [:agent :final]     [:sink :in])
               (dp/input-at [:agent :user-in])
               (dp/output-at :sink))
        result (dp/run! wf {:data :question})]
    (is (= :completed (:state result)))
    (is (= 3 (count (filterv #(= :agent (:step-id %))
                             (events-of result :recv)))))
    (is (= 1 (count (filterv #(= :sink (:step-id %))
                             (events-of result :recv)))))))

;; --- fcatch ----------------------------------------------------------------

(deftest fcatch-returns-exceptions-as-values
  (let [safe-div (dp/fcatch /)]
    (is (= 5 (safe-div 10 2)))
    (is (instance? ArithmeticException (safe-div 10 0)))))

(deftest fcatch-composes-with-router-for-try-catch-semantics
  (let [risky  (fn [x] (if (neg? x) (throw (ex-info "negative" {:x x})) (* x 2)))
        wf     (-> (dp/merge-steps
                    (dp/step :work (dp/fcatch risky))
                    (dp/router :split [:ok :err]
                               (fn [v]
                                 [{:data v
                                   :port (if (instance? Throwable v) :err :ok)}]))
                    (dp/sink :ok-sink)
                    (dp/sink :err-sink))
                   (dp/connect [:work :out]    [:split :in])
                   (dp/connect [:split :ok]    [:ok-sink :in])
                   (dp/connect [:split :err]   [:err-sink :in])
                   (dp/input-at :work)
                   (dp/output-at :ok-sink))
        happy  (dp/run! wf {:data 4})
        sad    (dp/run! wf {:data -1})]
    (testing "success path delivers to :ok-sink"
      (is (= :completed (:state happy)))
      (is (= 8 (:data (first (filterv #(= :ok-sink (:step-id %))
                                      (events-of happy :recv))))))
      (is (empty? (filterv #(= :err-sink (:step-id %)) (events-of happy :recv)))))
    (testing "failure path delivers a Throwable to :err-sink; run still completes"
      (is (= :completed (:state sad)))
      (is (instance? Throwable
                     (:data (first (filterv #(= :err-sink (:step-id %))
                                            (events-of sad :recv))))))
      (testing "no :failure trace event — the error was handled as data"
        (is (empty? (events-of sad :failure)))))))

;; --- inject signal / done behavior -----------------------------------------

(deftest inject-signal-via-tokens-opt
  (let [wf (dp/serial
             (dp/step :a inc)
             (dp/step :b #(* 2 %))
             (dp/sink))
        h  (dp/start! wf)
        _  (dp/inject! h {:tokens {"g" 777}})
        _  (dp/await-quiescent! h)
        result (dp/stop! h)
        events (:events result)]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "no data :recv events (pure-fn handlers were bypassed)"
      (is (empty? (events-of result :recv :data))))
    (testing "sink received the signal with injected tokens intact"
      (let [sink-recv (first (filterv #(and (= :sink (:step-id %))
                                             (= :recv (:kind %))
                                             (= :signal (:msg-kind %)))
                                       events))]
        (is (some? sink-recv))
        (is (= {"g" 777} (:tokens sink-recv)))))))

(deftest done-cascades-through-single-input-chain
  (let [wf (dp/serial
             (dp/step :a inc)
             (dp/step :b #(* 2 %))
             (dp/step :c inc)
             (dp/sink))
        h (dp/start! wf)
        _ (dp/inject! h {})
        _ (dp/await-quiescent! h)
        result (dp/stop! h)
        events (:events result)
        by-kind (fn [sid]
                  (frequencies (map (juxt :kind :msg-kind)
                                    (filterv #(= sid (:step-id %)) events))))]
    (testing "run completes and counters balance"
      (is (= :completed (:state result)))
      (is (nil? (:error result)))
      (let [{:keys [sent recv completed]} (:counters result)]
        (is (= sent recv completed))))
    (testing "no user handler was invoked (no data :recv events)"
      (is (empty? (events-of result :recv :data))))
    (testing "each mid-chain step cascaded done"
      (doseq [sid [:a :b :c]]
        (is (= 1 (get (by-kind sid) [:recv :done] 0)))
        (is (= 1 (get (by-kind sid) [:send-out :done] 0)))
        (is (= 1 (get (by-kind sid) [:success :done] 0)))))
    (testing "sink absorbed done"
      (is (= 1 (get (by-kind :sink) [:recv :done] 0)))
      (is (zero? (get (by-kind :sink) [:send-out :done] 0)))
      (is (= 1 (get (by-kind :sink) [:success :done] 0))))))

(deftest done-multi-input-holds-until-all-inputs-closed
  (let [wf (-> (dp/merge-steps
                 (dp/step :merge
                   {:ins  {:left "" :right ""}
                    :outs {:out ""}}
                   (fn [_ctx s d]
                     [s [[:out d]]]))
                 (dp/sink))
               (dp/connect [:merge :out] [:sink :in])
               (dp/input-at :merge))
        h (dp/start! wf)
        events-after (fn [] (dp/events h))
        by-kind (fn [sid]
                  (frequencies (map (juxt :kind :msg-kind)
                                    (filterv #(= sid (:step-id %)) (events-after)))))]
    (testing "before any injection, no events"
      (is (empty? (filterv #(= :merge (:step-id %)) (events-after)))))
    (dp/inject! h {:in :merge :port :left})
    (dp/await-quiescent! h)
    (testing "after first done on :left: :recv done fired, cascade has NOT"
      (is (= 1 (get (by-kind :merge) [:recv :done] 0)))
      (is (zero? (get (by-kind :merge) [:send-out :done] 0)))
      (is (= 1 (get (by-kind :merge) [:success :done] 0)))
      (is (empty? (filterv #(= :sink (:step-id %)) (events-after)))))
    (dp/inject! h {:in :merge :port :right})
    (dp/await-quiescent! h)
    (testing "after second done on :right: cascade fires, sink receives done"
      (is (= 2 (get (by-kind :merge) [:recv :done] 0)))
      (is (= 1 (get (by-kind :merge) [:send-out :done] 0)))
      (is (= 2 (get (by-kind :merge) [:success :done] 0)))
      (is (= 1 (get (by-kind :sink) [:recv :done] 0)))
      (is (= 1 (get (by-kind :sink) [:success :done] 0))))
    (testing "user handler was never invoked"
      (is (empty? (events-of (dp/stop! h) :recv :data))))))

;; --- signal messages -------------------------------------------------------

(deftest signal-flows-through-untouched
  (let [source (dp/step :source nil
                 (fn [ctx s _d]
                   [s [[:out (dp/signal ctx)]]]))
        wf (dp/serial
             source
             (dp/step :mid-a inc)
             (dp/step :mid-b #(* 2 %))
             (dp/step :mid-c inc)
             (dp/sink))
        result (dp/run! wf {:data :seed})
        events (:events result)
        by-kind (fn [sid]
                  (frequencies (map (juxt :kind :msg-kind)
                                    (filterv #(= sid (:step-id %)) events))))]
    (testing "run completes and counters balance"
      (is (= :completed (:state result)))
      (is (nil? (:error result)))
      (let [{:keys [sent recv completed]} (:counters result)]
        (is (= sent recv completed))))
    (testing "source saw data, downstream steps saw signals"
      (is (= 1 (get (by-kind :source) [:recv :data] 0)))
      (is (= 1 (get (by-kind :source) [:success :data] 0)))
      (doseq [sid [:mid-a :mid-b :mid-c :sink]]
        (is (zero? (get (by-kind sid) [:recv :data] 0)))
        (is (= 1 (get (by-kind sid) [:recv :signal] 0)))
        (is (= 1 (get (by-kind sid) [:success :signal] 0)))))
    (testing "no :failure events"
      (is (empty? (events-of result :failure))))
    (testing "the message reaching the sink is a signal (no :data)"
      (let [sink-recv (first (filterv #(and (= :sink (:step-id %))
                                             (= :recv (:kind %))
                                             (= :signal (:msg-kind %)))
                                       events))]
        (is (some? sink-recv))
        (is (= {} (:tokens sink-recv)))))))

(deftest signal-broadcasts-to-all-declared-output-ports
  (let [gid    "xo-group"
        v      424242
        wf     (-> (dp/merge-steps
                    (dp/router :route [:odd :even]
                               (fn [d] [{:data d
                                         :port (if (odd? d) :odd :even)}]))
                    (dp/sink :odd-sink)
                    (dp/sink :even-sink))
                   (dp/connect [:route :odd]  [:odd-sink :in])
                   (dp/connect [:route :even] [:even-sink :in])
                   (dp/input-at :route))
        h      (dp/start! wf)
        _      (dp/inject! h {:tokens {gid v}})
        _      (dp/await-quiescent! h)
        result (dp/stop! h)
        events (:events result)]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "router forwarded signal on BOTH :odd and :even"
      (let [route-sends (filterv #(and (= :route (:step-id %))
                                       (= :send-out (:kind %))
                                       (= :signal (:msg-kind %)))
                                 events)]
        (is (= #{:odd :even} (set (map :port route-sends))))
        (is (= 2 (count route-sends)))))
    (testing "both sinks received exactly one signal; router handler never ran"
      (doseq [sid [:odd-sink :even-sink]]
        (is (= 1 (count (filterv #(and (= sid (:step-id %))
                                       (= :recv (:kind %))
                                       (= :signal (:msg-kind %)))
                                 events)))))
      (is (empty? (filterv #(and (= :route (:step-id %))
                                 (= :recv (:kind %))
                                 (= :data (:msg-kind %)))
                           events))))
    (testing "XOR of tokens across the two branches reconstructs the original"
      (let [sink-recvs (filterv #(and (= :recv (:kind %))
                                      (= :signal (:msg-kind %))
                                      (or (= :odd-sink (:step-id %))
                                          (= :even-sink (:step-id %))))
                                events)
            xor-sum    (reduce bit-xor 0 (map #(get-in % [:tokens gid]) sink-recvs))]
        (is (= 2 (count sink-recvs)))
        (is (= v xor-sum))))))

;; --- ctx :in-port ----------------------------------------------------------

(deftest in-port-on-ctx-for-multi-input-handlers
  (let [tagger (dp/step :tag
                 {:ins  {:left "" :right ""}
                  :outs {:out ""}}
                 (fn [{:keys [in-port]} s d]
                   [s [[:out {:port in-port :data d}]]]))
        wf     (-> (dp/merge-steps
                    (dp/step :src-l identity)
                    (dp/step :src-r identity)
                    tagger
                    (dp/sink))
                   (dp/connect [:src-l :out] [:tag :left])
                   (dp/connect [:src-r :out] [:tag :right])
                   (dp/connect [:tag :out]   [:sink :in])
                   (dp/input-at :src-l)
                   (dp/output-at :sink))
        result (dp/run! wf {:data :from-left})
        sink-recv (first (filterv #(= :sink (:step-id %))
                                  (events-of result :recv)))]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "tagger saw the msg arrive on :left and labeled it accordingly"
      (is (= {:port :left :data :from-left} (:data sink-recv))))))

(deftest in-port-defaults-to-in-for-single-input-handlers
  (let [seen (atom nil)
        wf (dp/serial
             (dp/step :probe nil
               (fn [{:keys [in-port] :as ctx} s _d]
                 (reset! seen in-port)
                 [s [[:out (dp/pass ctx)]]]))
             (dp/sink))
        _  (dp/run! wf {:data 1})]
    (is (= :in @seen))))

;; --- validation ------------------------------------------------------------

(deftest handler-emitting-on-undeclared-port-surfaces-as-failure-event
  (let [wf (dp/serial
             (dp/step :bad {:ins {:in ""} :outs {:out ""}}
               (fn [_ctx s d]
                 [s [[:nope d]]]))
             (dp/sink))
        result (dp/run! wf {:data 1})
        failures (filterv #(and (= :failure (:kind %))
                                (= :bad (:step-id %)))
                          (:events result))]
    (testing "run completes (message-level failure, not run-level)"
      (is (= :completed (:state result)))
      (is (nil? (:error result))))
    (testing "exactly one :failure event for :bad"
      (is (= 1 (count failures))))
    (testing ":failure event mentions the unknown port and declared ports"
      (let [msg (get-in (first failures) [:error :message])]
        (is (re-find #"undeclared port" msg))
        (is (re-find #":nope" msg))
        (is (re-find #":out" msg))))
    (testing "sink never received a message"
      (is (empty? (filterv #(= :sink (:step-id %))
                           (events-of result :recv)))))))

(deftest inject-rejects-unknown-step
  (let [wf (dp/serial (dp/step :a inc) (dp/sink))
        h  (dp/start! wf)]
    (try
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"unknown step :nonexistent"
                            (dp/inject! h {:in :nonexistent :data 1})))
      (testing "ex-info's :known carries the actual sids for helpful debugging"
        (try
          (dp/inject! h {:in :nonexistent :data 1})
          (catch clojure.lang.ExceptionInfo e
            (is (contains? (:known (ex-data e)) :a))
            (is (contains? (:known (ex-data e)) :sink)))))
      (finally (dp/stop! h)))))

(deftest inject-rejects-unknown-port
  (let [wf (dp/serial
             (dp/step :a {:ins {:real-in ""} :outs {:out ""}}
               (fn [_ctx s d] [s [[:out d]]]))
             (dp/sink))
        h  (dp/start! wf)]
    (try
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"does not declare input port :typo"
                            (dp/inject! h {:in :a :port :typo :data 1})))
      (testing "ex-info lists the declared input ports"
        (try
          (dp/inject! h {:in :a :port :typo :data 1})
          (catch clojure.lang.ExceptionInfo e
            (is (= #{:real-in} (:declared (ex-data e)))))))
      (finally (dp/stop! h)))))

;; --- helpers: child / children / pass / signal / merge --------------------

(deftest child-single-and-multi-port-outputs
  (let [wf (-> (dp/merge-steps
                (dp/step :emit {:ins {:in ""} :outs {:a "" :b ""}}
                  (fn [ctx s d]
                    [s [[:a (dp/child ctx (str "a-" d))]
                        [:b (dp/child ctx (str "b-" d))]]]))
                (dp/sink :sink-a)
                (dp/sink :sink-b))
               (dp/connect [:emit :a] [:sink-a :in])
               (dp/connect [:emit :b] [:sink-b :in])
               (dp/input-at :emit))
        result (dp/run! wf {:data 7})]
    (is (= :completed (:state result)))
    (is (= "a-7" (:data (first (filterv #(= :sink-a (:step-id %))
                                        (events-of result :recv))))))
    (is (= "b-7" (:data (first (filterv #(= :sink-b (:step-id %))
                                        (events-of result :recv))))))))

(deftest children-splits-tokens-across-siblings
  (let [seen (atom [])
        wf (-> (dp/merge-steps
                (dp/fan-out :fo 1)
                (dp/step :burst {:ins {:in ""} :outs {:out ""}}
                  (fn [ctx s _d]
                    (let [kids (dp/children ctx [:a :b :c])]
                      [s (mapv (fn [k] [:out k]) kids)])))
                (dp/step :observe {:ins {:in ""} :outs {:out ""}}
                  (fn [ctx s _d]
                    (swap! seen conj (:tokens (:msg ctx)))
                    [s [[:out (dp/pass ctx)]]]))
                (dp/sink))
               (dp/connect [:fo :out] [:burst :in])
               (dp/connect [:burst :out] [:observe :in])
               (dp/connect [:observe :out] [:sink :in])
               (dp/input-at :fo))
        result (dp/run! wf {:data :x})
        tokens @seen]
    (is (= :completed (:state result)))
    (is (= 3 (count tokens)) "three children reached observe")
    (testing "XOR of the one zero-sum group key across all three children = 0"
      (let [gks (distinct (mapcat keys tokens))]
        (is (= 1 (count gks)))
        (let [gk (first gks)]
          (is (zero? (reduce bit-xor 0 (map #(get % gk) tokens)))))))))

(deftest merge-helper-fires-merge-event
  (let [wf (dp/serial
            (dp/step :merge-in-place {:outs {:out ""}}
              (fn [ctx s _d]
                (let [b        (dp/child ctx {:stage :b})
                      c        (dp/child ctx {:stage :c})
                      combined (dp/merge ctx [b c] {:combined true})]
                  [s [[:out combined]]])))
            (dp/sink))
        result (dp/run! wf {:data {:stage :a}})]
    (testing "sink receives the combined msg"
      (let [recvs (filterv #(= :sink (:step-id %)) (events-of result :recv))]
        (is (= :completed (:state result)))
        (is (= 1 (count recvs)))
        (is (= {:combined true} (:data (first recvs))))))
    (testing "two :split events (B, C) and one :merge event"
      (is (= 2 (count (events-of result :split))))
      (is (= 1 (count (events-of result :merge)))))))

(deftest child-chain-records-intermediates-as-trace-events
  (let [wf     (dp/serial
                (dp/step :chain {:ins {:in ""} :outs {:out ""}}
                  (fn [ctx s _d]
                    (let [b (dp/child ctx {:stage :b})
                          c (dp/child ctx b {:stage :c})
                          d (dp/child ctx c {:stage :d})]
                      [s [[:out d]]])))
                (dp/sink))
        result (dp/run! wf {:data {:stage :a}})]
    (testing "run completes; sink receives exactly one msg whose stage is :d"
      (is (= :completed (:state result)))
      (let [recvs (filterv #(= :sink (:step-id %)) (events-of result :recv))]
        (is (= 1 (count recvs)))
        (is (= :d (:stage (:data (first recvs)))))))
    (testing "exactly three :split events in order B, C, D"
      (let [chain-splits (filterv #(= :chain (:step-id %))
                                  (events-of result :split))]
        (is (= 3 (count chain-splits)))
        (is (= [:b :c :d] (mapv (comp :stage :data) chain-splits)))))
    (testing "DAG connects: C's parent is B, D's parent is C"
      (let [chain-splits (filterv #(= :chain (:step-id %))
                                  (events-of result :split))
            b-id (:msg-id (first chain-splits))
            c-ev (second chain-splits)
            d-ev (nth chain-splits 2)]
        (is (= [b-id] (:parent-msg-ids c-ev)))
        (is (= [(:msg-id c-ev)] (:parent-msg-ids d-ev)))))))

;; --- run-seq: collector + per-input attribution ---------------------------

(deftest run-seq-empty-coll
  (let [result (dp/run-seq (dp/step :inc inc) [])]
    (is (= :completed (:state result)))
    (is (= [] (:outputs result)))))

(deftest run-seq-single-input-single-output
  (let [result (dp/run-seq (dp/step :inc inc) [1 2 3])]
    (is (= :completed (:state result)))
    (is (= [[2] [3] [4]] (:outputs result)))))

(deftest run-seq-fan-out-produces-multiple-outputs-per-input
  (let [result (dp/run-seq (dp/serial (dp/step :dbl #(* 2 %))
                                      (dp/fan-out :three 3))
                           [5 7])]
    (is (= :completed (:state result)))
    (testing "each input contributes three copies of its doubled value"
      (is (= [[10 10 10] [14 14 14]] (mapv #(vec (sort %)) (:outputs result)))))))

(deftest start-rejects-unwired-output-port
  (testing "a router whose :drop port is never connected is rejected at start!"
    (let [wf (dp/router :route [:out :drop]
                        (fn [x] [{:data x :port (if (odd? x) :out :drop)}]))]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"unwired output port"
           (dp/run-seq wf [1])))))
  (testing "the same step becomes valid once every out is wired"
    (let [wf (-> (dp/merge-steps
                  (dp/router :route [:out :drop]
                             (fn [x] [{:data x :port (if (odd? x) :out :drop)}]))
                  (dp/sink :drop-sink))
                 (dp/connect [:route :drop] [:drop-sink :in])
                 (dp/input-at  :route)
                 (dp/output-at [:route :out]))
          result (dp/run-seq wf [1 2 3 4])]
      (is (= :completed (:state result)))
      (is (= [[1] [] [3] []] (:outputs result))))))

(deftest run-seq-no-output-gives-empty-slot
  (let [filter-odd (dp/step :filter-odd nil
                            (fn [_ctx s d]
                              [s (if (odd? d) [[:out d]] [])]))
        result (dp/run-seq filter-odd [1 2 3 4])]
    (is (= :completed (:state result)))
    (testing "odd inputs produce output; even inputs produce empty slots"
      (is (= [[1] [] [3] []] (:outputs result))))))

(deftest run-seq-preserves-order-across-interleaved-inputs
  (let [wf (dp/step :sq (fn [x] (* x x)))
        result (dp/run-seq wf [2 3 4 5])]
    (is (= :completed (:state result)))
    (is (= [[4] [9] [16] [25]] (:outputs result)))))

(deftest run-seq-failure-of-one-input-does-not-lose-others
  (let [wf (dp/retry :try
                     (fn [x]
                       (if (= x :bad)
                         (throw (ex-info "boom" {}))
                         x))
                     1)
        result (dp/run-seq wf [:a :bad :c])]
    (is (= :completed (:state result)))
    (testing "good inputs produce outputs, the failing input gets an empty slot"
      (is (= [[:a] [] [:c]] (:outputs result))))
    (testing "exactly one :failure event, attributed to :try"
      (let [fs (events-of result :failure)]
        (is (= 1 (count fs)))
        (is (= :try (:step-id (first fs))))))))

(deftest run-seq-mapcat-style-via-children
  (let [wf (dp/step :expand {:outs {:out ""}}
                    (fn [ctx s d]
                      [s (mapv (fn [k] [:out k]) (dp/children ctx [d (inc d) (inc (inc d))]))]))
        result (dp/run-seq wf [10 20])]
    (is (= :completed (:state result)))
    (is (= [[10 11 12] [20 21 22]]
           (mapv #(vec (sort %)) (:outputs result))))))

(deftest await-quiescent-returns-failed-on-flow-error
  (let [bad (dp/proc :bad
                     (fn [_factory-ctx]
                       (fn
                         ([] {:params {} :ins {:in ""} :outs {:out ""}})
                         ([_s] {})
                         ([_s _transition]
                          (throw (ex-info "boom" {:cause :intentional})))
                         ([s _in-id _m] [s {}]))))
        h   (dp/start! (dp/serial bad (dp/sink)))]
    (try
      (let [signal (dp/await-quiescent! h 5000)]
        (testing "done-promise carries [:failed err] from the error-chan drain"
          (is (vector? signal))
          (is (= :failed (first signal)))
          (is (= "boom" (-> signal second :message)))
          (is (= {:cause :intentional} (-> signal second :data)))))
      (finally (dp/stop! h)))))
