(ns toolkit.datapotamus-test
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus :as dp]))

(defn- events-of [result kind]
  (filterv #(= kind (:kind %)) (:events result)))

;; --- step + serial: smallest composition -----------------------------------

(deftest step-is-a-1-in-1-out-step
  (let [f (dp/step :inc inc)]
    (is (= #{:inc} (set (keys (:procs f))))
        "procs map has a single entry keyed by the id")
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
  ;; A child step whose boundary was set with input-at/output-at [sid port]
  ;; should compose under serial via its declared ports, not :in/:out defaults.
  (let [custom (-> (dp/step :b
                     {:ins  {:custom-in  ""}
                      :outs {:custom-out ""}}
                     (fn [_ctx s m]
                       [s {:custom-out [(dp/child-with-data m (* (:data m) 10))]}]))
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
                                       (events-of result :recv)))))))  ; 3 → 4 → 40
    (testing "conns wire through the declared ports"
      (is (= #{[[:a :out]          [:b :custom-in]]
               [[:b :custom-out]   [:sink :in]]}
             (set (:conns composed)))))))

;; --- as-step: black-box composition ---------------------------------------

(deftest deep-nested-as-step
  ;; Three levels of as-step nesting: F → sub1 → sub2 → sub3 → leaf.
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
      (is (= 30 (:data sink-recv))))        ; 2 → 2 → (inc 2)=3 → (* 10 3)=30 → 30 → 30
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

;; --- passthrough: transparent forwarder ------------------------------------

(deftest passthrough-is-transparent
  (let [wf (dp/serial
             (dp/step :inc inc)
             (dp/passthrough)
             (dp/sink))
        result (dp/run! wf {:data 7})]
    (is (= :completed (:state result)))
    (is (= 8 (:data (first (filterv #(= :sink (:step-id %))
                                    (events-of result :recv)))))
        "passthrough forwards (inc 7) = 8 unchanged")))

;; --- merge-steps + connect: explicit wiring -------------------------------

(deftest agent-style-multi-port-with-connect
  ;; Agent-tool pattern built via merge-steps + explicit connects.
  (let [calls (atom 0)
        agent-step
        (dp/step :agent
          {:ins  {:user-in "" :tool-result ""}
           :outs {:tool-call "" :final ""}}
          (fn [_ctx s m]
            (let [n (swap! calls inc)]
              (if (< n 3)
                [s (dp/emit m :tool-call :query)]
                [s (dp/emit m :final     :done)]))))
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
                             (events-of result :recv))))
        "agent recv'd 1 initial + 2 tool results")
    (is (= 1 (count (filterv #(= :sink (:step-id %))
                             (events-of result :recv)))))))

;; --- fcatch: lift exceptions into the data domain -----------------------

(deftest fcatch-returns-exceptions-as-values
  (let [safe-div (dp/fcatch /)]
    (is (= 5 (safe-div 10 2)))
    (is (instance? ArithmeticException (safe-div 10 0)))))

(deftest fcatch-composes-with-router-for-try-catch-semantics
  ;; End-to-end: a step wraps a throwing fn with fcatch, then a downstream
  ;; router splits the stream by whether the value is a Throwable. This is
  ;; the "try-catch combinator" built out of existing primitives.
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

;; --- done messages: per-port closure signals ----------------------------

(deftest inject-signal-via-tokens-opt
  ;; With no :data but a :tokens map, inject! builds and injects a signal
  ;; (mirrors the envelope: dataless but token-carrying). Downstream
  ;; pure-fn steps are bypassed by wrap-proc's signal path; the sink
  ;; sees a signal with the injected tokens intact.
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
      (is (empty? (filterv #(= :recv (:kind %)) events))))
    (testing "sink received the signal with injected tokens intact"
      (let [sink-recv (first (filterv #(and (= :sink (:step-id %))
                                             (= :recv-signal (:kind %)))
                                       events))]
        (is (some? sink-recv))
        (is (= {"g" 777} (:tokens sink-recv)))))))

(deftest done-cascades-through-single-input-chain
  ;; A done injected at the head of a chain of pure-fn steps must
  ;; propagate to the sink without invoking any user handler. The
  ;; handlers would throw on nil data if called.
  (let [wf (dp/serial
             (dp/step :a inc)
             (dp/step :b #(* 2 %))
             (dp/step :c inc)
             (dp/sink))
        h (dp/start! wf)
        _ (dp/inject! h {})   ; empty opts → done
        _ (dp/await-quiescent! h)
        result (dp/stop! h)
        events (:events result)
        kinds-by-step (fn [sid]
                        (frequencies (map :kind (filterv #(= sid (:step-id %)) events))))]
    (testing "run completes and counters balance"
      (is (= :completed (:state result)))
      (is (nil? (:error result)))
      (let [{:keys [sent recv completed]} (:counters result)]
        (is (= sent recv completed))))
    (testing "no user handler was invoked (zero data :recv events anywhere)"
      (is (empty? (filterv #(= :recv (:kind %)) events))))
    (testing "each mid-chain step cascaded done: one :done-in and one :done-out"
      (doseq [sid [:a :b :c]]
        (is (= 1 (get (kinds-by-step sid) :done-in 0)))
        (is (= 1 (get (kinds-by-step sid) :done-out 0)))
        (is (= 1 (get (kinds-by-step sid) :done-complete 0)))))
    (testing "sink absorbed done: :done-in but no :done-out (no outputs declared)"
      (is (= 1 (get (kinds-by-step :sink) :done-in 0)))
      (is (zero? (get (kinds-by-step :sink) :done-out 0)))
      (is (= 1 (get (kinds-by-step :sink) :done-complete 0))))))

(deftest done-multi-input-holds-until-all-inputs-closed
  ;; A 2-input step holds its done cascade until both input ports have
  ;; received done. First inject → :done-in only; second inject → cascade.
  (let [wf (-> (dp/merge-steps
                 (dp/step :merge
                   {:ins  {:left "" :right ""}
                    :outs {:out ""}}
                   (fn [_ctx s m]
                     [s (dp/emit m :out (:data m))]))
                 (dp/sink))
               (dp/connect [:merge :out] [:sink :in])
               (dp/input-at :merge))
        h (dp/start! wf)
        events-after (fn []
                       (let [snap (dp/events h)]
                         snap))
        kinds-of (fn [sid]
                   (frequencies (map :kind (filterv #(= sid (:step-id %)) (events-after)))))]
    (testing "before any injection, no events"
      (is (empty? (filterv #(= :merge (:step-id %)) (events-after)))))
    (dp/inject! h {:in :merge :port :left})   ; no :data, no :tokens → done
    (dp/await-quiescent! h)
    (testing "after first done on :left: :done-in fired, cascade has NOT"
      (is (= 1 (get (kinds-of :merge) :done-in 0)))
      (is (zero? (get (kinds-of :merge) :done-out 0)))
      (is (= 1 (get (kinds-of :merge) :done-complete 0)))
      (is (empty? (filterv #(= :sink (:step-id %)) (events-after)))
          "sink still idle — done hasn't cascaded"))
    (dp/inject! h {:in :merge :port :right})   ; no :data, no :tokens → done
    (dp/await-quiescent! h)
    (testing "after second done on :right: cascade fires, sink receives done"
      (is (= 2 (get (kinds-of :merge) :done-in 0)))
      (is (= 1 (get (kinds-of :merge) :done-out 0)))
      (is (= 2 (get (kinds-of :merge) :done-complete 0)))
      (is (= 1 (get (kinds-of :sink) :done-in 0)))
      (is (= 1 (get (kinds-of :sink) :done-complete 0))))
    (testing "user handler was never invoked — no data :recv anywhere"
      (is (empty? (filterv #(= :recv (:kind %)) (events-after)))))
    (let [{:keys [sent recv completed]} (dp/counters h)]
      (testing "final counters balance"
        (is (= sent recv completed))))
    (dp/stop! h)))

;; --- signal messages: dataless coordination primitives ------------------

(deftest signal-flows-through-untouched
  ;; A custom source emits a signal (a msg with no :data key). Three
  ;; downstream pure-fn steps would throw if they saw the signal as data
  ;; (inc, * 2, inc on nil). wrap-proc's signal short-circuit bypasses
  ;; their handlers and forwards the signal unchanged to the sink.
  (let [source (dp/step :source nil
                 (fn [_ctx s m]
                   ;; emit one signal carrying parent's tokens unchanged
                   [s {:out [(dp/child-signal m)]}]))
        wf (dp/serial
             source
             (dp/step :mid-a inc)
             (dp/step :mid-b #(* 2 %))
             (dp/step :mid-c inc)
             (dp/sink))
        result (dp/run! wf {:data :seed})
        events (:events result)
        kinds-by-step (fn [step-id]
                        (frequencies (map :kind (filterv #(= step-id (:step-id %)) events))))]
    (testing "run completes and counters balance"
      (is (= :completed (:state result)))
      (is (nil? (:error result)))
      (let [{:keys [sent recv completed]} (:counters result)]
        (is (= sent recv completed))))
    (testing "source saw data (:recv), downstream steps saw signals (:recv-signal)"
      (is (= 1 (get (kinds-by-step :source) :recv)))
      (is (= 1 (get (kinds-by-step :source) :success)))
      (is (zero? (get (kinds-by-step :source) :recv-signal 0)))
      (doseq [sid [:mid-a :mid-b :mid-c :sink]]
        (testing (str sid " never received a data message")
          (is (zero? (get (kinds-by-step sid) :recv 0))))
        (testing (str sid " received exactly one signal")
          (is (= 1 (get (kinds-by-step sid) :recv-signal 0)))
          (is (= 1 (get (kinds-by-step sid) :success-signal 0))))))
    (testing "no :failure events — pure-fn handlers were bypassed, never saw nil"
      (is (empty? (filterv #(= :failure (:kind %)) events))))
    (testing "the message reaching the sink is itself a signal (no :data)"
      ;; pull the signal from the :recv-signal at sink; tokens should be empty
      ;; (no fan-out upstream), parent chain preserved
      (let [sink-recv (first (filterv #(and (= :sink (:step-id %))
                                             (= :recv-signal (:kind %)))
                                       events))]
        (is (some? sink-recv))
        (is (= {} (:tokens sink-recv)))))))

(deftest signal-broadcasts-to-all-declared-output-ports
  ;; A signal arriving at a multi-output step (e.g. router) forwards on
  ;; every declared output port with parent tokens split across them.
  ;; XOR of the split shares reconstructs the original — so any
  ;; downstream fan-in waiting for the group still receives full token
  ;; mass, regardless of which branch its token slice traveled through.
  (let [gid    "xo-group"
        v      424242
        source (dp/step :source nil
                 (fn [_ctx s m]
                   [s {:out [(assoc (dp/child-signal m)
                                    :tokens {gid v})]}]))
        wf     (-> (dp/merge-steps
                    source
                    (dp/router :route [:odd :even]
                               (fn [d] [{:data d
                                         :port (if (odd? d) :odd :even)}]))
                    (dp/sink :odd-sink)
                    (dp/sink :even-sink))
                   (dp/connect [:source :out] [:route :in])
                   (dp/connect [:route :odd]  [:odd-sink :in])
                   (dp/connect [:route :even] [:even-sink :in])
                   (dp/input-at :source))
        result (dp/run! wf {:data :seed})
        events (:events result)]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "router forwarded signal on BOTH :odd and :even"
      (let [route-sends (filterv #(and (= :route (:step-id %))
                                       (= :send-out-signal (:kind %)))
                                 events)]
        (is (= #{:odd :even} (set (map :port route-sends))))
        (is (= 2 (count route-sends)))))
    (testing "both sinks received exactly one signal; router handler never ran"
      (doseq [sid [:odd-sink :even-sink]]
        (is (= 1 (count (filterv #(and (= sid (:step-id %))
                                       (= :recv-signal (:kind %)))
                                 events)))))
      (is (empty? (filterv #(and (= :route (:step-id %)) (= :recv (:kind %)))
                           events))
          ":route's data handler should not have been called on the signal"))
    (testing "XOR of tokens across the two branches reconstructs the original"
      (let [sink-recvs (filterv #(and (= :recv-signal (:kind %))
                                      (or (= :odd-sink (:step-id %))
                                          (= :even-sink (:step-id %))))
                                events)
            xor-sum    (reduce bit-xor 0 (map #(get-in % [:tokens gid]) sink-recvs))]
        (is (= 2 (count sink-recvs)))
        (is (= v xor-sum))))))

(deftest signal-preserves-tokens-through-chain
  ;; A signal emitted with a token group passes through intermediate
  ;; steps with the token unchanged — the invariant that makes signals
  ;; usable as the identity element for fan-in groups.
  (let [gid "test-group"
        token-val 12345
        source (dp/step :source nil
                 (fn [_ctx s m]
                   [s {:out [(assoc (dp/child-signal m)
                                    :tokens {gid token-val})]}]))
        wf (dp/serial
             source
             (dp/step :mid1 inc)     ; would throw if called
             (dp/step :mid2 inc)     ; would throw if called
             (dp/sink))
        result (dp/run! wf {:data :seed})
        sink-recv (first (filterv #(and (= :sink (:step-id %))
                                         (= :recv-signal (:kind %)))
                                   (:events result)))]
    (is (= :completed (:state result)))
    (is (= {gid token-val} (:tokens sink-recv))
        "token survives every signal hop intact")))

;; --- ctx :in-port: multi-input handlers can dispatch on arrival port -----

(deftest in-port-on-ctx-for-multi-input-handlers
  ;; A 2-input step that tags data by the port it arrived on. This is the
  ;; shape `join-by-key` will need, exercised here as a minimal test.
  (let [tagger (dp/step :tag
                 {:ins  {:left "" :right ""}
                  :outs {:out ""}}
                 (fn [{:keys [in-port]} s m]
                   [s (dp/emit m :out {:port in-port :data (:data m)})]))
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
  ;; The single-input case: :in-port is still populated (always reflects
  ;; the actual arrival port, default :in).
  (let [seen (atom nil)
        wf (dp/serial
             (dp/step :probe nil
               (fn [{:keys [in-port]} s m]
                 (reset! seen in-port)
                 [s (dp/emit m :out (:data m))]))
             (dp/sink))
        _  (dp/run! wf {:data 1})]
    (is (= :in @seen))))

;; --- validation: wrap-proc + inject! ------------------------------------

(deftest handler-emitting-on-undeclared-port-surfaces-as-failure-event
  ;; A handler returns a port-map with a key NOT in its declared :outs.
  ;; wrap-proc asserts subset; the thrown ex-info is caught and surfaces
  ;; as a :failure trace event. Run completes (message-level failure).
  (let [wf (dp/serial
             (dp/step :bad {:ins {:in ""} :outs {:out ""}}
               (fn [_ctx s m]
                 ;; typo: :nope is not declared
                 [s {:nope [(dp/child-with-data m (:data m))]}]))
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
    (testing "sink never received a message (failed msg didn't propagate)"
      (is (empty? (filterv #(= :sink (:step-id %))
                           (filterv #(= :recv (:kind %)) (:events result))))))))

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
               (fn [_ctx s m] [s (dp/emit m :out (:data m))]))
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

;; --- emit: output-map helper ----------------------------------------------

(deftest emit-single-and-multi-port
  (let [m {:msg-id :fake-parent :data-id :fake-data :data 42
           :tokens {} :parent-msg-ids []}
        single (dp/emit m :out 7)
        multi  (dp/emit m :a 1 :b 2)
        repeated (dp/emit m :out 1 :out 2 :out 3)
        mixed  (dp/emit m :out :main :audit :a :audit :b)]
    (testing "single port, single msg"
      (is (= [:out] (keys single)))
      (is (= 7 (get-in single [:out 0 :data]))))
    (testing "multiple ports, one msg each"
      (is (= #{:a :b} (set (keys multi))))
      (is (= 1 (get-in multi [:a 0 :data])))
      (is (= 2 (get-in multi [:b 0 :data]))))
    (testing "repeated port key accumulates in argument order"
      (is (= [:out] (keys repeated)))
      (is (= 3 (count (:out repeated))))
      (is (= [1 2 3] (mapv :data (:out repeated)))))
    (testing "mixed single and repeated keys"
      (is (= #{:out :audit} (set (keys mixed))))
      (is (= 1 (count (:out mixed))))
      (is (= 2 (count (:audit mixed))))
      (is (= [:a :b] (mapv :data (:audit mixed)))))))

(deftest emit-splits-tokens-across-children
  ;; Children emitted from one call collectively XOR back to the parent's
  ;; tokens — the property that keeps upstream fan-out / downstream fan-in
  ;; consistent when a middle step branches.
  (let [m {:msg-id :p :data-id :d :data :x
           :tokens {:g1 7 :g2 11}
           :parent-msg-ids []}
        out      (dp/emit m :a 1 :b 2 :out 3)
        children (mapcat val out)
        xor-by-key (fn [k] (reduce bit-xor 0 (map #(get (:tokens %) k) children)))]
    (is (= 3 (count children)))
    (is (= 7  (xor-by-key :g1)))
    (is (= 11 (xor-by-key :g2)))))

(deftest emit-preserves-fan-in-invariant
  ;; Sanity check that a middle step emitting multiple children via
  ;; `emit` doesn't poison the upstream fan-out's zero-sum group — the
  ;; fan-in still fires exactly once, with the expected parent count.
  (let [wf (-> (dp/merge-steps
                 (dp/fan-out :split 2)
                 ;; middle step: each input becomes 3 children on :out
                 (dp/step :explode nil
                   (fn [_ctx s m]
                     [s (dp/emit m :out :a :out :b :out :c)]))
                 (dp/fan-in :merge :split)
                 (dp/sink))
               (dp/connect [:split :out]   [:explode :in])
               (dp/connect [:explode :out] [:merge :in])
               (dp/connect [:merge :out]   [:sink :in])
               (dp/input-at :split)
               (dp/output-at :sink))
        result (dp/run! wf {:data :seed})
        merges (filterv #(= :merge (:kind %)) (:events result))
        sink-recvs (filterv #(and (= :recv (:kind %)) (= :sink (:step-id %)))
                            (:events result))]
    (is (= :completed (:state result)))
    (is (= 1 (count merges))
        "exactly one merge event — the fan-in's zero-sum group closed once")
    (is (= 6 (count (:parent-msg-ids (first merges))))
        "merge names all six leaves as parents (2 fan-out branches × 3 explode children)")
    (is (= 1 (count sink-recvs))
        "exactly one merged msg reaches the sink")))

;; --- derive: handler-internal intermediates as trace events -----------------

(deftest derive-chain-records-intermediates-as-trace-events
  ;; Handler internally derives A → B → C, then emits D (child of C) on :out.
  ;; `dp/derive` creates a child msg and publishes a :derive event via ctx's
  ;; pubsub as a side effect — so B and C show up in the trace without the
  ;; handler ever touching the output map's trace annotations.
  (let [wf     (dp/serial
                (dp/step :chain {:ins {:in ""} :outs {:out ""}}
                  (fn [ctx s _m]
                    (let [b (dp/derive ctx {:stage :b})
                          c (dp/derive ctx b {:stage :c})]
                      [s (dp/emit c :out {:stage :d})])))
                (dp/sink))
        result (dp/run! wf {:data {:stage :a}})]
    (testing "run completes; sink receives exactly one msg whose stage is :d"
      (is (= :completed (:state result)))
      (let [recvs (filterv #(= :sink (:step-id %)) (events-of result :recv))]
        (is (= 1 (count recvs)))
        (is (= :d (:stage (:data (first recvs)))))))
    (testing "exactly two :derive events in order B, C"
      (let [derives (events-of result :derive)]
        (is (= 2 (count derives)))
        (is (= [:b :c] (mapv (comp :stage :data) derives)))))
    (testing "DAG connects: C's parent is B, D's parent is C"
      (let [derives  (events-of result :derive)
            b-id     (:msg-id (first derives))
            c-id     (:msg-id (second derives))
            c-ev     (second derives)
            d-send   (first (filterv #(and (= :chain (:step-id %)) (= :out (:port %)))
                                     (events-of result :send-out)))]
        (is (= [b-id] (:parent-msg-ids c-ev)))
        (is (= [c-id] (:parent-msg-ids d-send)))))))

(deftest derive-multi-parent-fires-merge-event
  ;; Handler derives B and C separately from the input, then merges them
  ;; by calling `derive` with a vector of parents. The merged msg has
  ;; :parent-msg-ids = [b-id c-id] directly (no synthetic intermediate),
  ;; and a :merge trace event names both parents.
  (let [wf     (dp/serial
                (dp/step :merge-in-place {:outs {:out ""}}
                  (fn [ctx s _m]
                    (let [b        (dp/derive ctx {:stage :b})
                          c        (dp/derive ctx {:stage :c})
                          combined (dp/derive ctx [b c] {:combined true})]
                      [s {:out [combined]}])))
                (dp/sink))
        result (dp/run! wf {:data {:stage :a}})]
    (testing "sink receives exactly one combined msg"
      (let [recvs (filterv #(= :sink (:step-id %)) (events-of result :recv))]
        (is (= :completed (:state result)))
        (is (= 1 (count recvs)))
        (is (= {:combined true} (:data (first recvs))))))
    (testing "two :derive events (B and C) and one :merge event"
      (is (= 2 (count (events-of result :derive))))
      (is (= 1 (count (events-of result :merge)))))
    (testing ":merge event's parents are [B-id C-id]"
      (let [derives (events-of result :derive)
            merge-ev (first (events-of result :merge))
            b-id (:msg-id (first derives))
            c-id (:msg-id (second derives))]
        (is (= [b-id c-id] (:parent-msg-ids merge-ev)))))
    (testing "combined's :parent-msg-ids name B and C directly (no intermediate)"
      (let [derives (events-of result :derive)
            b-id (:msg-id (first derives))
            c-id (:msg-id (second derives))
            send-out (first (filterv #(and (= :merge-in-place (:step-id %))
                                           (= :out (:port %)))
                                     (events-of result :send-out)))]
        (is (= [b-id c-id] (:parent-msg-ids send-out)))))))

