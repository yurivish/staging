(ns toolkit.datapotamus-test
  "Narrative test suite for toolkit.datapotamus.

   The file is read top-to-bottom as an introduction: each Act adds one
   concept to what the previous Acts established. No test depends on a
   primitive introduced later than itself.

     I    — A step is the smallest unit
     II   — Linear composition with `serial`
     III  — Beyond linear: explicit graphs
     IV   — Nesting with `as-step`
     V    — Message kinds: data, signal, done
     VI   — Derivation helpers + ctx :in-port
     VII  — Provenance & trace events
     VIII — Token conservation: fan-out & fan-in
     IX   — Escape hatches: assoc-tokens / dissoc-tokens
     X    — Errors & failures
     XI   — Validation (early rejects)
     XII  — Long-running handles
     XIII — Running a collection: run-seq
     XIV  — Observing runs via scoped pubsub
     XV   — A worked example (podcast pipeline)
     XVI  — Parallel workers (workers combinator)"
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.dependency :as dep]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.token :as tok]
            [toolkit.pubsub :as pubsub]))

;; --- helpers ----------------------------------------------------------------

(defn- events-of
  "Filter `result`'s :events to those matching `kind` (and optionally `msg-kind`)."
  ([result kind]
   (filterv #(= kind (:kind %)) (:events result)))
  ([result kind msg-kind]
   (filterv #(and (= kind (:kind %)) (= msg-kind (:msg-kind %)))
            (:events result))))

(defn- events->dag
  "Fold events into a stuartsierra dependency graph + a {msg-id → kind}
   map. Each msg depends on its parents (per :parent-msg-ids). Used by
   Act VII to assert structural shape of provenance."
  [events]
  (reduce (fn [{:keys [g kinds]} e]
            (case (:kind e)
              :inject   {:g (dep/depend g (:msg-id e) ::root)
                         :kinds (assoc kinds (:msg-id e) :inject)}
              (:send-out :merge)
              {:g (reduce (fn [g p] (dep/depend g (:msg-id e) p))
                          g (:parent-msg-ids e))
               :kinds (if (= :merge (get kinds (:msg-id e)))
                        kinds
                        (assoc kinds (:msg-id e) (:kind e)))}
              {:g g :kinds kinds}))
          {:g (dep/graph) :kinds {}}
          events))

;; ============================================================================
;; Act I — A step is the smallest unit
;;
;; Everything composes up from a step. A step is a map with :procs, :conns,
;; :in, :out; `step/step` builds the 1-proc kind. Before wiring anything we
;; inspect one to see what's in the box.
;; ============================================================================

;; A freshly built step has one proc, one conn list (empty), and in=out=id.
(deftest step-is-a-1-in-1-out-step
  (let [f (step/step :inc inc)]
    (is (= #{:inc} (set (keys (:procs f)))))
    (is (= :inc (:in f)))
    (is (= :inc (:out f)))
    (is (= [] (:conns f)))))

;; ============================================================================
;; Act II — Linear composition with `serial`
;;
;; `serial` glues steps end-to-end: the :out of one becomes the :in of the
;; next. Sinks terminate a chain. `flow/run!` drives the graph for a single
;; injected message and returns the collected trace events.
;; ============================================================================

;; serial + run!: data flows inc → dbl → sink.
(deftest serial-glues-steps-sequentially
  (let [a (step/step :inc inc)
        b (step/step :dbl #(* 2 %))
        composed (step/serial a b (step/sink))
        result   (flow/run! composed {:data 5})]
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

;; Each step fires :recv + :success exactly once; send-outs are counted once per port.
(deftest linear-chain
  (let [wf     (step/serial
                (step/step :inc  inc)
                (step/step :dbl  #(* 2 %))
                (step/sink))
        result (flow/run! wf {:data 5})]
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

;; Two step styles: `(step id f)` pure-fn vs `(step id nil handler)` 3-arg form.
(deftest multiple-step-styles
  (let [wf     (step/serial
                (step/step :a inc)
                (step/step :b nil (fn [_ctx s d] [s {:out [(* d 10)]}]))
                (step/step :c nil (fn [_ctx s d] [s {:out [(dec d)]}]))
                (step/sink))
        result (flow/run! wf {:data 5})]
    (is (= :completed (:state result)))
    (is (= 59 (:data (first (filterv #(= :sink (:step-id %))
                                     (events-of result :recv))))))))

;; serial requires unique proc ids across the composed steps.
(deftest serial-rejects-collisions
  (let [a (step/step :same inc)
        b (step/step :same dec)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"collision"
                          (step/serial a b)))))

;; A step can expose custom :in / :out port names via input-at / output-at;
;; serial wires through those vector endpoints rather than the defaults.
(deftest serial-respects-vector-boundaries
  (let [custom (-> (step/step :b
                              {:ins  {:custom-in  ""}
                               :outs {:custom-out ""}}
                              (fn [_ctx s d]
                                [s {:custom-out [(* d 10)]}]))
                   (step/input-at  [:b :custom-in])
                   (step/output-at [:b :custom-out]))
        composed (step/serial
                  (step/step :a inc)
                  custom
                  (step/sink))
        result   (flow/run! composed {:data 3})]
    (testing "run completes; b received through :custom-in and emitted on :custom-out"
      (is (= :completed (:state result)))
      (is (= 40 (:data (first (filterv #(= :sink (:step-id %))
                                       (events-of result :recv)))))))
    (testing "conns wire through the declared ports"
      (is (= #{[[:a :out]          [:b :custom-in]]
               [[:b :custom-out]   [:sink :in]]}
             (set (:conns composed)))))))

;; `passthrough` forwards data unchanged — a no-op in the pipeline.
(deftest passthrough-is-transparent
  (let [wf (step/serial
            (step/step :inc inc)
            (step/passthrough :pt)
            (step/sink))
        result (flow/run! wf {:data 7})]
    (is (= :completed (:state result)))
    (is (= 8 (:data (first (filterv #(= :sink (:step-id %))
                                    (events-of result :recv))))))))

;; passthrough preserves :data-id (useful for dedup/caching keyed on identity).
(deftest passthrough-preserves-data-id
  (let [wf (step/serial
           (step/step :inc inc)
           (step/passthrough :pt)
           (step/sink))
        result (flow/run! wf {:data 7})
        pt-send (first (filterv #(and (= :pt (:step-id %)) (:port %))
                                (events-of result :send-out)))
        sink-recv (first (filterv #(= :sink (:step-id %))
                                  (events-of result :recv)))]
    (is (= :completed (:state result)))
    (is (= (:data-id pt-send) (:data-id sink-recv))
        "sink receives same data-id as passthrough's send")))

;; ============================================================================
;; Act III — Beyond linear: explicit graphs
;;
;; When the topology isn't a line, assemble the graph by hand:
;; `merge-steps` unions procs; `connect` adds conns; `input-at` / `output-at`
;; set the boundary. This unlocks feedback loops and multi-port routing.
;; ============================================================================

;; The minimal hand-wired graph: merge-steps + a single connect + input-at.
(deftest hand-authored-step-map
  (let [wf     (-> (step/merge-steps
                    (step/step :inc inc)
                    (step/sink))
                   (step/connect [:inc :out] [:sink :in])
                   (step/input-at :inc))
        result (flow/run! wf {:data 3})]
    (is (= :completed (:state result)))
    (is (= 4 (:data (first (filterv #(= :sink (:step-id %))
                                    (events-of result :recv))))))))

;; Agent-style feedback loop: agent emits either :tool-call or :final. The
;; tool's output loops back to the agent on :tool-result. After N calls the
;; agent emits :final, which flows on to :sink.
(deftest agent-style-multi-port-with-connect
  (let [calls (atom 0)
        agent-step
        (step/step :agent
                   {:ins  {:user-in "" :tool-result ""}
                    :outs {:tool-call "" :final ""}}
                   (fn [_ctx s _d]
                     (let [n (swap! calls inc)]
                       (if (< n 3)
                         [s {:tool-call [:query]}]
                         [s {:final     [:done]}]))))
        wf (-> (step/merge-steps
                agent-step
                (step/step :tool (constantly :tool-response))
                (step/sink))
               (step/connect [:agent :tool-call] [:tool :in])
               (step/connect [:tool :out]        [:agent :tool-result])
               (step/connect [:agent :final]     [:sink :in])
               (step/input-at [:agent :user-in])
               (step/output-at :sink))
        result (flow/run! wf {:data :question})]
    (is (= :completed (:state result)))
    (is (= 3 (count (filterv #(= :agent (:step-id %))
                             (events-of result :recv)))))
    (is (= 1 (count (filterv #(= :sink (:step-id %))
                             (events-of result :recv)))))))

;; Dynamic fan-out via a multi-port step: one input (a coll) fans out to
;; per-element routes across :odd and :even. Potam3 doesn't ship a
;; `router`; the same idea is one short handler.
(deftest dynamic-fan-out
  (let [wf     (-> (step/merge-steps
                    (step/step :route {:ins {:in ""} :outs {:odd "" :even ""}}
                               (fn [ctx _s xs]
                                 {:odd  (mapv #(msg/child ctx %) (filter odd? xs))
                                  :even (mapv #(msg/child ctx %) (filter even? xs))}))
                    (step/sink :odd-sink)
                    (step/sink :even-sink))
                   (step/connect [:route :odd]  [:odd-sink :in])
                   (step/connect [:route :even] [:even-sink :in])
                   (step/input-at :route))
        result (flow/run! wf {:data [1 2 3 4 5]})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "odds routed to :odd-sink (1, 3, 5), evens to :even-sink (2, 4)"
      (is (= 3 (count (filterv #(= :odd-sink  (:step-id %)) (events-of result :recv)))))
      (is (= 2 (count (filterv #(= :even-sink (:step-id %)) (events-of result :recv))))))
    (testing "route emitted 5 send-outs total"
      (is (= 5 (count (filterv #(and (= :route (:step-id %)) (:port %))
                               (events-of result :send-out))))))))

;; ============================================================================
;; Act IV — Nesting with `as-step`
;;
;; `as-step` treats an inner (composed) step as a single proc under a new id.
;; Scopes nest one [:flow] segment per level; `:step-id` remains the original.
;; ============================================================================

;; Three levels of nesting — each subflow adds one [:flow id] segment to scope.
(deftest deep-nested-as-step
  (let [leaf  (step/step :leaf inc)
        sub3  (step/as-step :sub3 leaf)
        sub2  (step/as-step :sub2 sub3)
        sub1  (step/as-step :sub1 sub2)
        outer (step/serial sub1 (step/sink))
        result (flow/run! outer {:flow-id "F" :data 41})
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

;; Outer steps see only the flow-level scope; steps inside an as-step get the
;; extra [:flow :sub] segment, but ids aren't collided between inner and outer.
(deftest as-step-namespaces-inner-procs
  (let [inner    (step/serial
                  (step/step :a inc)
                  (step/step :b #(* 10 %)))
        outer    (step/serial
                  (step/step :pre  identity)
                  (step/as-step :sub inner)
                  (step/step :post identity)
                  (step/sink))
        result   (flow/run! outer {:flow-id "f" :data 2})
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

;; ============================================================================
;; Act V — Message kinds: data, signal, done
;;
;; Three message kinds flow on the wire. User handlers see only :data —
;; :signal and :done messages bypass user code but still move through the
;; graph and drive per-group accounting and port-closure semantics.
;;
;; Use :signal to ferry tokens / coordination across the graph without a
;; payload — fan-in closing a group, broadcast under a token key. Use :done
;; to say "this input is closed, propagate accordingly": on a single-input
;; step it cascades immediately; on a multi-input step it waits until every
;; input has been closed before cascading downstream.
;; ============================================================================

;; A signal flows through untouched — user handlers aren't invoked, but
;; :recv/:success events still fire and tokens propagate.
(deftest signal-flows-through-untouched
  (let [source (step/step :source nil
                          (fn [ctx _s _d]
                            {:out [(msg/signal ctx)]}))
        wf (step/serial
            source
            (step/step :mid-a inc)
            (step/step :mid-b #(* 2 %))
            (step/step :mid-c inc)
            (step/sink))
        result (flow/run! wf {:data :x})
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

;; A signal fans across all declared output ports automatically — the default
;; :on-signal broadcasts one signal msg to every output. Tokens split K-ways
;; so XOR reconstructs.
(deftest signal-broadcasts-to-all-declared-output-ports
  (let [gid    "xo-group"
        v      424242
        wf     (-> (step/merge-steps
                    (step/step :route
                               {:ins {:in ""} :outs {:odd "" :even ""}}
                               (fn [ctx _s d]
                                 (if (odd? d)
                                   {:odd  [(msg/child ctx d)]}
                                   {:even [(msg/child ctx d)]})))
                    (step/sink :odd-sink)
                    (step/sink :even-sink))
                   (step/connect [:route :odd]  [:odd-sink :in])
                   (step/connect [:route :even] [:even-sink :in])
                   (step/input-at :route))
        h      (flow/start! wf)
        _      (flow/inject! h {:tokens {gid v}})
        _      (flow/await-quiescent! h)
        result (flow/stop! h)
        events (:events result)]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "route forwarded signal on BOTH :odd and :even"
      (let [route-sends (filterv #(and (= :route (:step-id %))
                                       (= :send-out (:kind %))
                                       (= :signal (:msg-kind %)))
                                 events)]
        (is (= #{:odd :even} (set (map :port route-sends))))
        (is (= 2 (count route-sends)))))
    (testing "both sinks received exactly one signal; route handler never ran"
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

;; Inject with :tokens (no :data) routes a signal — user handlers skipped.
(deftest inject-signal-via-tokens-opt
  (let [wf (step/serial
           (step/step :a inc)
           (step/step :b #(* 2 %))
           (step/sink))
        h  (flow/start! wf)
        _  (flow/inject! h {:tokens {"g" 777}})
        _  (flow/await-quiescent! h)
        result (flow/stop! h)
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

;; Inject with neither :data nor :tokens routes a :done marker. On a linear
;; chain, each step sees :done once and cascades it to the next.
(deftest done-cascades-through-single-input-chain
  (let [wf (step/serial
           (step/step :a inc)
           (step/step :b #(* 2 %))
           (step/step :c inc)
           (step/sink))
        h (flow/start! wf)
        _ (flow/inject! h {})
        _ (flow/await-quiescent! h)
        result (flow/stop! h)
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

;; A multi-input step holds its :done cascade until every input port has
;; received its own :done marker — only then does it close downstream.
(deftest done-multi-input-holds-until-all-inputs-closed
  (let [wf (-> (step/merge-steps
                (step/step :merge
                           {:ins  {:left "" :right ""}
                            :outs {:out ""}}
                           (fn [_ctx s d]
                             [s {:out [d]}]))
                (step/sink))
               (step/connect [:merge :out] [:sink :in])
               (step/input-at :merge))
        h (flow/start! wf)
        events-after (fn [] (flow/events h))
        by-kind (fn [sid]
                  (frequencies (map (juxt :kind :msg-kind)
                                    (filterv #(= sid (:step-id %)) (events-after)))))]
    (testing "before any injection, no events"
      (is (empty? (filterv #(= :merge (:step-id %)) (events-after)))))
    (flow/inject! h {:in :merge :port :left})
    (flow/await-quiescent! h)
    (testing "after first done on :left: :recv done fired, cascade has NOT"
      (is (= 1 (get (by-kind :merge) [:recv :done] 0)))
      (is (zero? (get (by-kind :merge) [:send-out :done] 0)))
      (is (= 1 (get (by-kind :merge) [:success :done] 0)))
      (is (empty? (filterv #(= :sink (:step-id %)) (events-after)))))
    (flow/inject! h {:in :merge :port :right})
    (flow/await-quiescent! h)
    (testing "after second done on :right: cascade fires, sink receives done"
      (is (= 2 (get (by-kind :merge) [:recv :done] 0)))
      (is (= 1 (get (by-kind :merge) [:send-out :done] 0)))
      (is (= 2 (get (by-kind :merge) [:success :done] 0)))
      (is (= 1 (get (by-kind :sink) [:recv :done] 0)))
      (is (= 1 (get (by-kind :sink) [:success :done] 0))))
    (testing "user handler was never invoked"
      (is (empty? (events-of (flow/stop! h) :recv :data))))))

;; ============================================================================
;; Act VI — Derivation helpers + ctx :in-port
;;
;; Handlers build outputs via `child`/`children`/`pass`/`signal`/`merge`.
;; These return pending msgs whose parent refs drive synthesis after the
;; handler returns — splits and merges in the trace events, tokens
;; distributed over the in-handler DAG.
;; ============================================================================

;; Single-input handler: ctx :in-port defaults to :in.
(deftest in-port-defaults-to-in-for-single-input-handlers
  (let [seen (atom nil)
        wf (step/serial
            (step/step :probe nil
                       (fn [{:keys [in-port] :as ctx} s _d]
                         (reset! seen in-port)
                         [s {:out [(msg/pass ctx)]}]))
            (step/sink))
        _  (flow/run! wf {:data 1})]
    (is (= :in @seen))))

;; Multi-input handler: ctx :in-port carries which port this invocation arrived on.
(deftest in-port-on-ctx-for-multi-input-handlers
  (let [tagger (step/step :tag
                          {:ins  {:left "" :right ""}
                           :outs {:out ""}}
                          (fn [{:keys [in-port]} s d]
                            [s {:out [{:port in-port :data d}]}]))
        wf     (-> (step/merge-steps
                    (step/step :src-l identity)
                    (step/step :src-r identity)
                    tagger
                    (step/sink))
                   (step/connect [:src-l :out] [:tag :left])
                   (step/connect [:src-r :out] [:tag :right])
                   (step/connect [:tag :out]   [:sink :in])
                   (step/input-at :src-l)
                   (step/output-at :sink))
        result (flow/run! wf {:data :from-left})
        sink-recv (first (filterv #(= :sink (:step-id %))
                                  (events-of result :recv)))]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "tagger saw the msg arrive on :left and labeled it accordingly"
      (is (= {:port :left :data :from-left} (:data sink-recv))))))

;; `child` emits one derived message per call; multi-port handlers call it
;; once per output port.
(deftest child-single-and-multi-port-outputs
  (let [wf (-> (step/merge-steps
                (step/step :emit {:ins {:in ""} :outs {:a "" :b ""}}
                           (fn [ctx s d]
                             [s {:a [(msg/child ctx (str "a-" d))]
                                 :b [(msg/child ctx (str "b-" d))]}]))
                (step/sink :sink-a)
                (step/sink :sink-b))
               (step/connect [:emit :a] [:sink-a :in])
               (step/connect [:emit :b] [:sink-b :in])
               (step/input-at :emit))
        result (flow/run! wf {:data 7})]
    (is (= :completed (:state result)))
    (is (= "a-7" (:data (first (filterv #(= :sink-a (:step-id %))
                                        (events-of result :recv))))))
    (is (= "b-7" (:data (first (filterv #(= :sink-b (:step-id %))
                                        (events-of result :recv))))))))

;; `children` splits the parent's tokens K-ways — XOR of the per-child
;; slices equals the parent's token for each group.
(deftest children-splits-tokens-across-siblings
  (let [seen (atom [])
        wf (-> (step/merge-steps
                (c/fan-out :fo 1)
                (step/step :burst {:ins {:in ""} :outs {:out ""}}
                           (fn [ctx s _d]
                             (let [kids (msg/children ctx [:a :b :c])]
                               [s {:out (vec kids)}])))
                (step/step :observe {:ins {:in ""} :outs {:out ""}}
                           (fn [ctx s _d]
                             (swap! seen conj (:tokens (:msg ctx)))
                             [s {:out [(msg/pass ctx)]}]))
                (step/sink))
               (step/connect [:fo :out] [:burst :in])
               (step/connect [:burst :out] [:observe :in])
               (step/connect [:observe :out] [:sink :in])
               (step/input-at :fo))
        result (flow/run! wf {:data :x})
        tokens @seen]
    (is (= :completed (:state result)))
    (is (= 3 (count tokens)) "three children reached observe")
    (testing "XOR of the one zero-sum group key across all three children = 0"
      (let [gks (distinct (mapcat keys tokens))]
        (is (= 1 (count gks)))
        (let [gk (first gks)]
          (is (zero? (reduce bit-xor 0 (map #(get % gk) tokens)))))))))

;; `merge` produces a multi-parent derivation — the emitted msg has two
;; parents, so synthesis tags it as a :merge event (not a :split).
(deftest merge-helper-fires-merge-event
  (let [wf (step/serial
           (step/step :merge-in-place {:ins {:in ""} :outs {:out ""}}
                      (fn [ctx s _d]
                        (let [b        (msg/child ctx {:stage :b})
                              c        (msg/child ctx {:stage :c})
                              combined (msg/merge ctx [b c] {:combined true})]
                          [s {:out [combined]}])))
           (step/sink))
        result (flow/run! wf {:data {:stage :a}})]
    (testing "sink receives the combined msg"
      (let [recvs (filterv #(= :sink (:step-id %)) (events-of result :recv))]
        (is (= :completed (:state result)))
        (is (= 1 (count recvs)))
        (is (= {:combined true} (:data (first recvs))))))
    (testing "exactly one :merge event with two parents"
      (let [merges (events-of result :merge)]
        (is (= 1 (count merges)))
        (is (= 2 (count (:parent-msg-ids (first merges)))))))))

;; Graph-level diamond: `split` emits two siblings to :to-b and :to-c;
;; they traverse B and C respectively and land on D's :left / :right
;; input ports. D pairs them and uses `msg/merge` to combine lineages —
;; so D's emission carries :parent-msg-ids from both B and C.
(deftest diamond-dag-merges-two-branches
  (let [split  (step/step :split {:ins {:in ""} :outs {:to-b "" :to-c ""}}
                          (fn [ctx s d]
                            (let [[kb kc] (msg/children ctx [d d])]
                              [s {:to-b [kb] :to-c [kc]}])))
        b      (step/step :b #(* 10 %))
        c      (step/step :c #(+ 100 %))
        joiner (step/step :d
                          {:ins {:left "" :right ""} :outs {:out ""}}
                          (fn [{:keys [in-port] :as ctx} s d-val]
                            (if-let [pending (:pending s)]
                              [(dissoc s :pending)
                               {:out [(msg/merge ctx
                                                 [(:msg pending) (:msg ctx)]
                                                 {(:port pending) (:data pending)
                                                  in-port         d-val})]}]
                              [(assoc s :pending {:port in-port :msg (:msg ctx) :data d-val})
                               msg/drain])))
        wf     (-> (step/merge-steps split b c joiner (step/sink))
                   (step/connect [:split :to-b] [:b :in])
                   (step/connect [:split :to-c] [:c :in])
                   (step/connect [:b :out]      [:d :left])
                   (step/connect [:c :out]      [:d :right])
                   (step/connect [:d :out]      [:sink :in])
                   (step/input-at  :split)
                   (step/output-at :sink))
        result   (flow/run! wf {:data 5})
        sink-recv (first (filterv #(= :sink (:step-id %))
                                  (events-of result :recv)))
        d-merges  (filterv #(= :d (:step-id %)) (events-of result :merge))]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "D emits exactly one :merge event carrying two parents"
      (is (= 1 (count d-merges)))
      (is (= 2 (count (:parent-msg-ids (first d-merges))))))
    (testing "sink receives one message combining both branches"
      (is (= {:left 50 :right 105} (:data sink-recv))))))

;; ============================================================================
;; Act VII — Provenance & trace events
;;
;; Events form a DAG: every message's :parent-msg-ids points back to its
;; immediate ancestors. Folded together they reconstruct the full lineage
;; of a run. Handlers receive a per-message ctx carrying :step-id, :pubsub
;; (with scope prefix), and :cancel — useful for building subscribers keyed
;; on those fields.
;; ============================================================================

;; Per-message ctx carries :step-id and the step's scope segments.
(deftest handler-ctx-has-scope-and-step-id
  (let [captured (atom nil)
        wf (step/serial
            (step/step :my-step nil
                       (fn [ctx s d]
                         (reset! captured {:step-id (:step-id ctx)
                                           :scope   (:prefix (:pubsub ctx))})
                         [s {:out [d]}]))
            (step/sink))
        _  (flow/run! wf {:flow-id "F" :data :x})]
    (is (= {:step-id :my-step
            :scope   [[:flow "F"] [:step :my-step]]}
           @captured))))

;; Folded events form an acyclic graph with exactly one root — every
;; downstream message traces back to the single injected item through
;; :split / :merge.
(deftest provenance-dag-is-well-formed
  (let [wf     (step/serial
               (c/fan-out :split 3)
               (c/fan-in :fi :split)
               (step/sink))
        result (flow/run! wf {:data :x})
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

;; Frequency summaries match the topology: one fan-out input → three sink recvs.
(deftest multiplicity-frequencies
  (let [wf     (step/serial
               (c/fan-out :split 3)
               (step/sink))
        result (flow/run! wf {:data :x})]
    (testing "each step's :recv, :success, :send-out frequencies"
      (is (= {:split 1 :sink 3}
             (frequencies (map :step-id (events-of result :recv)))))
      (is (= {:split 1 :sink 3}
             (frequencies (map :step-id (events-of result :success)))))
      (is (= {:split 3}
             (frequencies (map :step-id
                               (filterv :port (events-of result :send-out)))))))))

;; ============================================================================
;; Act VIII — Token conservation: fan-out & fan-in
;;
;; `fan-out` mints a fresh zero-sum token group across N children.
;; `fan-in` waits until a group's XOR returns to 0, then emits one merged
;; message and strips the group key. The two compose: nested fan-out/fan-in
;; pairs fire in the right order without any shared state.
;; ============================================================================

;; fan-out emits N children on :out whose fresh group token XOR-sums to 0.
(deftest static-fan-out
  (let [wf     (step/serial
               (c/fan-out :split 3)
               (step/sink))
        result (flow/run! wf {:data :x})]
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

;; fan-out with ordinary pure-fn steps upstream — each of 3 copies carries
;; the doubled value downstream.
(deftest math-double-and-triple
  (let [wf     (step/serial
               (step/step :dbl #(* 2 %))
               (c/fan-out :triple 3)
               (step/sink))
        result (flow/run! wf {:data 5})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "dbl ran once, triple ran once, sink received thrice"
      (is (= {:dbl 1 :triple 1 :sink 3}
             (frequencies (map :step-id (events-of result :recv))))))
    (testing "sink sees 10 three times"
      (is (= [10 10 10]
             (mapv :data (filterv #(= :sink (:step-id %))
                                  (events-of result :recv))))))))

;; fan-in closes the group: one :merge event carrying all three parents,
;; exactly one downstream send-out, one sink recv.
(deftest token-fan-in
  (let [wf     (step/serial
               (c/fan-out :split 3)
               (c/fan-in :fi :split)
               (step/sink))
        result (flow/run! wf {:data :x})]
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

;; Nesting composes: the inner fan-in closes 3 times (once per outer child),
;; the outer fan-in closes exactly once and the sink gets a single message.
(deftest nested-fan-out-fan-in-composes
  (let [wf     (step/serial
               (c/fan-out :outer 3)
               (c/fan-out :inner 2)
               (c/fan-in  :fi-inner :inner)
               (c/fan-in  :fi-outer :outer)
               (step/sink))
        result (flow/run! wf {:data :x})
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

;; ============================================================================
;; Act IX — Escape hatches: assoc-tokens / dissoc-tokens
;;
;; For patterns the helpers can't express — custom groups, dribbling a
;; source's tokens across multiple invocations — stamp tokens directly on
;; pending msgs. The user is responsible for XOR-balancing the stamps so
;; global conservation still holds. fan-out/fan-in are the in-repo exemplars.
;; ============================================================================

;; Smoke: stamp a token-map onto a msg; the stamp shows up on the emission.
(deftest assoc-tokens-smoke
  (let [stamper (step/step :stamper {:ins {:in ""} :outs {:out ""}}
                           (fn [ctx s _d]
                             [s {:out [(-> (msg/child ctx :payload)
                                           (msg/assoc-tokens {"g" 77}))]}]))
        result  (flow/run! (step/serial stamper (step/sink)) {:data :x})
        sends   (filterv #(and (= :stamper (:step-id %)) (:port %))
                         (events-of result :send-out))]
    (is (= 1 (count sends)))
    (is (= 77 (get-in (first sends) [:tokens "g"])))))

;; A data handler that returns an empty port-map doesn't drop the input's
;; tokens — the framework auto-synthesizes a signal on every output port.
;; Without this, filters/dedups/skips would silently break downstream
;; token-coordinated combinators.
(deftest empty-return-auto-propagates-signal
  (let [drop-all (step/step :filter {:ins {:in ""} :outs {:out ""}}
                            (fn [_ctx s _d] [s {}]))
        result   (flow/run! (step/serial drop-all (step/sink))
                            {:data :x :tokens {"g" 42}})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "filter emitted exactly one signal on :out carrying the input's tokens"
      (let [sends (filterv #(and (= :filter (:step-id %)) (:port %))
                           (events-of result :send-out))]
        (is (= 1 (count sends)))
        (is (= :signal (:msg-kind (first sends))))
        (is (= 42 (get-in (first sends) [:tokens "g"])))))
    (testing "sink received the signal"
      (let [sig-recvs (filterv #(and (= :sink (:step-id %))
                                     (= :recv (:kind %))
                                     (= :signal (:msg-kind %)))
                               (:events result))]
        (is (= 1 (count sig-recvs)))))))

;; Multiple output ports: auto-propagation broadcasts a signal to every
;; declared output port (matches default-on-signal broadcast semantics).
;; Tokens are XOR-split across the ports, so their XOR recomposes the input.
(deftest empty-return-auto-propagates-to-every-output-port
  (let [multi  (step/step :multi {:ins {:in ""} :outs {:a "" :b ""}}
                          (fn [_ctx s _d] [s {}]))
        wf     (-> (step/merge-steps multi (step/sink :sa) (step/sink :sb))
                   (step/connect [:multi :a] [:sa :in])
                   (step/connect [:multi :b] [:sb :in])
                   (step/input-at :multi))
        result (flow/run! wf {:data :x :tokens {"g" 7}})
        sends  (filterv #(and (= :multi (:step-id %)) (:port %))
                        (events-of result :send-out))]
    (testing "one signal per declared output port"
      (is (= #{:a :b} (set (map :port sends))))
      (is (every? #(= :signal (:msg-kind %)) sends)))
    (testing "XOR of emitted token slices recomposes the input's tokens"
      (let [sum (reduce bit-xor 0 (keep #(get-in % [:tokens "g"]) sends))]
        (is (= 7 sum))))))

;; `msg/drain` suppresses the auto-signal — use it when you're holding the
;; input's tokens in state (pair-merger / batcher / dribbler patterns).
(deftest drain-suppresses-auto-signal
  (let [drainer (step/step :drain {:ins {:in ""} :outs {:out ""}}
                           (fn [_ctx s _d] [s msg/drain]))
        result  (flow/run! (step/serial drainer (step/sink)) {:data :x :tokens {"g" 99}})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "drainer emitted nothing on :out"
      (is (empty? (filterv #(and (= :drain (:step-id %)) (:port %))
                           (events-of result :send-out)))))
    (testing "no split/merge events for the drained input"
      (is (empty? (events-of result :split))))))

;; Smoke: strip a group key off a msg before emitting.
(deftest dissoc-tokens-smoke
  (let [stripper (step/step :stripper {:ins {:in ""} :outs {:out ""}}
                            (fn [ctx s _d]
                              [s {:out [(-> (msg/pass ctx)
                                            (msg/dissoc-tokens ["a"]))]}]))
        result   (flow/run! (step/serial stripper (step/sink))
                            {:data :payload :tokens {"a" 1 "b" 2}})
        sends    (filterv #(and (= :stripper (:step-id %)) (:port %))
                          (events-of result :send-out))]
    (is (= 1 (count sends)))
    (is (nil? (get-in (first sends) [:tokens "a"])))
    (is (= 2 (get-in (first sends) [:tokens "b"])))))

;; User-written combinator: mint a fresh zero-sum pair on every input,
;; emit one sibling to each of two ports, downstream fan-in closes the group.
(deftest assoc-tokens-mints-custom-group
  (let [gid-key ::my-pair
        minter
        (step/step :minter {:ins {:in ""} :outs {:a "" :b ""}}
                   (fn [ctx s _d]
                     (let [gid     [gid-key (:msg-id (:msg ctx))]
                           [v1 v2] (tok/split-value 0 2)
                           a (-> (msg/child ctx :a-side) (msg/assoc-tokens {gid v1}))
                           b (-> (msg/child ctx :b-side) (msg/assoc-tokens {gid v2}))]
                       [s {:a [a] :b [b]}])))
        wf (-> (step/merge-steps minter (c/fan-in :fi gid-key) (step/sink))
               (step/connect [:minter :a] [:fi :in])
               (step/connect [:minter :b] [:fi :in])
               (step/connect [:fi :out] [:sink :in])
               (step/input-at :minter)
               (step/output-at :sink))
        result (flow/run! wf {:data :go})]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "fan-in saw both siblings and fired one merge"
      (is (= 1 (count (events-of result :merge)))))
    (testing "sink received exactly one merged message"
      (is (= 1 (count (filterv #(= :sink (:step-id %))
                               (events-of result :recv))))))
    (testing "merged message no longer carries the minted gid key"
      (let [fi-send (first (filterv #(and (= :fi (:step-id %)) (:port %))
                                    (events-of result :send-out)))]
        (is (every? (fn [k] (not (and (vector? k) (= gid-key (first k)))))
                    (keys (:tokens fi-send))))))))

;; Deferred consumption across invocations: a pair-merger stashes the first
;; input and uses both in a single second-invocation emission. XOR of the
;; two injected tokens appears on the merged output.
(deftest pair-merger-single-consumption
  (let [pairer (step/step :pairer {:ins {:in ""} :outs {:out ""}}
                          (fn [ctx s _d]
                            (let [pending (:pending s)]
                              (if (nil? pending)
                                [(assoc s :pending (:msg ctx)) msg/drain]
                                [(dissoc s :pending)
                                 {:out [(msg/merge ctx
                                                   [pending (:msg ctx)]
                                                   [(:data pending) (:data (:msg ctx))])]}]))))
        wf     (step/serial pairer (step/sink))
        h      (flow/start! wf)]
    (flow/inject! h {:data :a :tokens {"g" 7}})
    (flow/await-quiescent! h)
    (flow/inject! h {:data :b :tokens {"g" 11}})
    (flow/await-quiescent! h)
    (let [result       (flow/stop! h)
          pairer-sends (filterv #(and (= :pairer (:step-id %)) (:port %))
                                (events-of result :send-out))
          sink-recvs   (filterv #(= :sink (:step-id %)) (events-of result :recv))]
      (testing "exactly one merged message reaches the sink"
        (is (= 1 (count sink-recvs)))
        (is (= [:a :b] (:data (first sink-recvs)))))
      (testing "merged tokens carry XOR of the two injected values"
        (is (= 1 (count pairer-sends)))
        (is (= (bit-xor 7 11) (get-in (first pairer-sends) [:tokens "g"])))))))

;; Dribble pattern: stash source tokens in state, dribble them out across K
;; invocations. Random chunks on all but the last, which carries the residual
;; to close the XOR equation.
(deftest dribble-closes-conservation
  (let [rng                (java.security.SecureRandom.)
        random-tokens-like (fn [m] (update-vals m (fn [_] (.nextLong rng))))
        K                  3
        dribbler
        (step/step :dribbler {:ins {:in ""} :outs {:out ""}}
                   (fn [ctx s _d]
                     (case (:kind (:data (:msg ctx)))
                       :start
                       [(assoc s :residual (:tokens (:msg ctx)) :left K) msg/drain]

                       :tick
                       (let [{:keys [residual left]} s
                             last? (= 1 left)
                             chunk (if last? residual (random-tokens-like residual))
                             out   (-> (msg/child ctx {:i left})
                                       (msg/assoc-tokens chunk))]
                         [(-> s
                              (assoc :residual (tok/merge-tokens residual chunk))
                              (update :left dec))
                          {:out [out]}]))))
        wf (step/serial dribbler (step/sink))
        h  (flow/start! wf)]
    (flow/inject! h {:data {:kind :start} :tokens {"g" 42}})
    (flow/await-quiescent! h)
    (dotimes [_ K]
      (flow/inject! h {:data {:kind :tick}})
      (flow/await-quiescent! h))
    (let [result          (flow/stop! h)
          dribbler-sends  (filterv #(and (= :dribbler (:step-id %)) (:port %))
                                   (events-of result :send-out))]
      (testing "dribbler emits exactly K messages across invocations"
        (is (= K (count dribbler-sends))))
      (testing "XOR of emitted chunks reconstructs source tokens"
        (is (= 42 (reduce bit-xor 0
                          (map #(get-in % [:tokens "g"] 0) dribbler-sends))))))))

;; ============================================================================
;; Act X — Errors & failures
;;
;; Failures are message-level: a throwing handler surfaces as one :failure
;; event, the run continues, other messages complete normally, and the final
;; :state stays :completed. Potam3 does not ship retry/fcatch helpers — the
;; recipes live in datapotamus.md (they are ≤6-line patterns, not framework
;; features).
;; ============================================================================

;; A throwing handler surfaces as a single :failure event; the run still
;; completes and the failed message does not propagate to sink.
(deftest uncaught-failure-continues-run
  (let [wf     (step/serial
               (step/step :boom (fn [_] (throw (ex-info "oops" {}))))
               (step/sink))
        result (flow/run! wf {:data 1})]
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

;; Emitting on an undeclared port is a handler-level failure — one :failure
;; event naming the unknown port and the declared ports.
(deftest handler-emitting-on-undeclared-port-surfaces-as-failure-event
  (let [wf (step/serial
           (step/step :bad {:ins {:in ""} :outs {:out ""}}
                      (fn [_ctx s d]
                        [s {:nope [d]}]))
           (step/sink))
        result (flow/run! wf {:data 1})
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

;; ============================================================================
;; Act XI — Validation (early rejects)
;;
;; Early validation catches wiring mistakes at start!/inject! time rather
;; than letting them manifest as hangs or silent drops at runtime.
;; ============================================================================

;; A step with an unconsumed output port is rejected at start!.
(deftest start-rejects-unwired-output-port
  (testing "a step whose :drop port is never connected is rejected at start!"
    (let [wf (step/step :route
                        {:ins {:in ""} :outs {:out "" :drop ""}}
                        (fn [ctx _s d]
                          (if (odd? d)
                            {:out  [(msg/child ctx d)]}
                            {:drop [(msg/child ctx d)]})))]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"unwired output port"
           (flow/run-seq wf [1])))))
  (testing "the same step becomes valid once every out is wired"
    (let [wf (-> (step/merge-steps
                  (step/step :route
                             {:ins {:in ""} :outs {:out "" :drop ""}}
                             (fn [ctx _s d]
                               (if (odd? d)
                                 {:out  [(msg/child ctx d)]}
                                 {:drop [(msg/child ctx d)]})))
                  (step/sink :drop-sink))
                 (step/connect [:route :drop] [:drop-sink :in])
                 (step/input-at  :route)
                 (step/output-at [:route :out]))
          result (flow/run-seq wf [1 2 3 4])]
      (is (= :completed (:state result)))
      (is (= [[1] [] [3] []] (:outputs result))))))

;; inject! rejects an unknown step id up-front; the ex-data lists known sids.
(deftest inject-rejects-unknown-step
  (let [wf (step/serial (step/step :a inc) (step/sink))
        h  (flow/start! wf)]
    (try
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"unknown step :nonexistent"
                            (flow/inject! h {:in :nonexistent :data 1})))
      (testing "ex-info's :known carries the actual sids for helpful debugging"
        (try
          (flow/inject! h {:in :nonexistent :data 1})
          (catch clojure.lang.ExceptionInfo e
            (is (contains? (:known (ex-data e)) :a))
            (is (contains? (:known (ex-data e)) :sink)))))
      (finally (flow/stop! h)))))

;; inject! rejects an unknown port on a known step; ex-data lists declared ports.
(deftest inject-rejects-unknown-port
  (let [wf (step/serial
           (step/step :a {:ins {:real-in ""} :outs {:out ""}}
                      (fn [_ctx s d] [s {:out [d]}]))
           (step/sink))
        h  (flow/start! wf)]
    (try
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"does not declare input port :typo"
                            (flow/inject! h {:in :a :port :typo :data 1})))
      (testing "ex-info lists the declared input ports"
        (try
          (flow/inject! h {:in :a :port :typo :data 1})
          (catch clojure.lang.ExceptionInfo e
            (is (= #{:real-in} (:declared (ex-data e)))))))
      (finally (flow/stop! h)))))

;; ============================================================================
;; Act XII — Long-running handles
;;
;; `run!` is sugar for start! + one inject + await + stop!. For longer-lived
;; workflows, drive the handle directly: inject many messages, poll counters,
;; read cancellation state from the cancel promise.
;; ============================================================================

;; One handle, three injects, counters sum correctly across injections.
(deftest long-running-multiple-injects
  (let [wf (step/serial
           (step/step :inc  inc)
           (step/sink))
        h (flow/start! wf)]
    (flow/inject! h {:data 1})
    (flow/await-quiescent! h)
    (is (= {:sent 2 :recv 2 :completed 2} (flow/counters h)))

    (flow/inject! h {:data 10})
    (flow/inject! h {:data 100})
    (flow/await-quiescent! h)
    (is (= {:sent 6 :recv 6 :completed 6} (flow/counters h)))

    (let [result (flow/stop! h)]
      (is (= :completed (:state result)))
      (is (= [2 11 101]
             (mapv :data (filterv #(= :sink (:step-id %))
                                  (events-of result :recv))))))))

;; The handle's ::cancel promise is undelivered until stop!; handlers can
;; poll it from ctx for cooperative cancellation.
(deftest cancellation-via-cancel-promise
  (let [observed (atom nil)
        wf (step/serial
           (step/step :step nil
                      (fn [{:keys [cancel] :as ctx} s _d]
                        (reset! observed (realized? cancel))
                        [s {:out [(msg/pass ctx)]}]))
           (step/sink))
        h    (flow/start! wf)
        {:keys [::flow/cancel]} h]
    (flow/inject! h {:data :x})
    (flow/await-quiescent! h)
    (is (false? @observed) "cancel promise not delivered while running")
    (is (false? (realized? cancel)) "cancel promise unrealized during run")

    (flow/stop! h)
    (is (true? (realized? cancel)) "cancel promise delivered on stop!")
    (is (= :stopped @cancel))))

;; Tier-3: a handler-map can supply :on-init to seed non-trivial initial
;; state — the first :on-data call sees whatever :on-init returned.
(deftest on-init-seeds-initial-state
  (let [seen-state (atom nil)
        hmap (step/handler-map
              {:on-init (fn [] {:counter 42})
               :on-data (fn [_ctx s _d]
                          (reset! seen-state s)
                          [(update s :counter inc) {:out [:ok]}])})
        wf   {:procs {:probe hmap} :conns [] :in :probe :out :probe}
        wf   (step/serial (step/as-step :probe wf) (step/sink))]
    (flow/run! wf {:data :x})
    (is (= {:counter 42} @seen-state))))

;; Tier-3: a handler-map can supply :on-stop for resource cleanup.
;; It fires exactly once per proc on `flow/stop!`. The command is delivered
;; asynchronously via core.async.flow's control channel, so the test awaits
;; a latch promise the hook itself delivers.
(deftest on-stop-fires-once-on-shutdown
  (let [stopped (atom 0)
        fired   (promise)
        hmap (step/handler-map
              {:on-data (fn [_ctx s _d] [s {:out [:ok]}])
               :on-stop (fn [_ctx _s]
                          (swap! stopped inc)
                          (deliver fired true))})
        wf   (step/serial
              (step/as-step :probe
                            {:procs {:probe hmap} :conns []
                             :in :probe :out :probe})
              (step/sink))
        h    (flow/start! wf)]
    (flow/inject! h {:data :x})
    (flow/await-quiescent! h)
    (is (zero? @stopped) ":on-stop has not fired during the run")
    (flow/stop! h)
    (is (= true (deref fired 2000 :timeout)) ":on-stop delivered before timeout")
    (is (= 1 @stopped) ":on-stop fired exactly once at shutdown")))

;; ============================================================================
;; Act XIII — Running a collection: run-seq
;;
;; `run-seq` runs a step against a seq of inputs, attributes each output
;; back to the input whose ancestry reaches it, and returns an :outputs
;; vector aligned with the input collection.
;; ============================================================================

;; Empty input → empty outputs, no events.
(deftest run-seq-empty-coll
  (let [result (flow/run-seq (step/step :inc inc) [])]
    (is (= :completed (:state result)))
    (is (= [] (:outputs result)))))

;; Each input yields exactly one output in the same position.
(deftest run-seq-single-input-single-output
  (let [result (flow/run-seq (step/step :inc inc) [1 2 3])]
    (is (= :completed (:state result)))
    (is (= [[2] [3] [4]] (:outputs result)))))

;; Outputs stay attributed to their originating input even when the pipeline
;; runs them concurrently and event order interleaves.
(deftest run-seq-preserves-order-across-interleaved-inputs
  (let [wf (step/step :sq (fn [x] (* x x)))
        result (flow/run-seq wf [2 3 4 5])]
    (is (= :completed (:state result)))
    (is (= [[4] [9] [16] [25]] (:outputs result)))))

;; An input that produces nothing downstream gets an empty slot in :outputs.
(deftest run-seq-no-output-gives-empty-slot
  (let [filter-odd (step/step :filter-odd nil
                              (fn [_ctx s d]
                                [s (if (odd? d) {:out [d]} {})]))
        result (flow/run-seq filter-odd [1 2 3 4])]
    (is (= :completed (:state result)))
    (testing "odd inputs produce output; even inputs produce empty slots"
      (is (= [[1] [] [3] []] (:outputs result))))))

;; One input can produce many outputs — each attributed to its originating input.
(deftest run-seq-fan-out-produces-multiple-outputs-per-input
  (let [result (flow/run-seq (step/serial (step/step :dbl #(* 2 %))
                                          (c/fan-out :three 3))
                             [5 7])]
    (is (= :completed (:state result)))
    (testing "each input contributes three copies of its doubled value"
      (is (= [[10 10 10] [14 14 14]] (mapv #(vec (sort %)) (:outputs result)))))))

;; mapcat-style expansion via `children` — one input → three outputs.
(deftest run-seq-mapcat-style-via-children
  (let [wf (step/step :expand {:ins {:in ""} :outs {:out ""}}
                      (fn [ctx s d]
                        [s {:out (vec (msg/children ctx [d (inc d) (inc (inc d))]))}]))
        result (flow/run-seq wf [10 20])]
    (is (= :completed (:state result)))
    (is (= [[10 11 12] [20 21 22]]
           (mapv #(vec (sort %)) (:outputs result))))))

;; A single output descended from multiple inputs appears under every
;; matching index in :outputs. Here a pair-merger collects two inputs and
;; emits one merged message whose lineage reaches both.
(deftest run-seq-cross-input-merge-appears-under-every-input
  (let [pairer (step/step :pairer {:ins {:in ""} :outs {:out ""}}
                          (fn [ctx s _d]
                            (if-let [pending (:pending s)]
                              [(dissoc s :pending)
                               {:out [(msg/merge ctx
                                                 [pending (:msg ctx)]
                                                 [(:data pending) (:data (:msg ctx))])]}]
                              [(assoc s :pending (:msg ctx)) msg/drain])))
        result (flow/run-seq pairer [:a :b])]
    (is (= :completed (:state result)))
    (testing "the one merged output is attributed to BOTH originating inputs"
      (is (= [[[:a :b]] [[:a :b]]] (:outputs result))))))

;; A failure on one input doesn't drop the others — the failed index gets an
;; empty slot and exactly one :failure event carries the step attribution.
(deftest run-seq-failure-of-one-input-does-not-lose-others
  (let [wf (step/step :try
                      (fn [x]
                        (if (= x :bad)
                          (throw (ex-info "boom" {}))
                          x)))
        result (flow/run-seq wf [:a :bad :c])]
    (is (= :completed (:state result)))
    (testing "good inputs produce outputs, the failing input gets an empty slot"
      (is (= [[:a] [] [:c]] (:outputs result))))
    (testing "exactly one :failure event, attributed to :try"
      (let [fs (events-of result :failure)]
        (is (= 1 (count fs)))
        (is (= :try (:step-id (first fs))))))))

;; ============================================================================
;; Act XIV — Observing runs via scoped pubsub
;;
;; Every event is published on a subject of the form
;;
;;     [<kind> "flow" <flow-id> ("flow" <sub-id>)* ("step" <sid>)?]
;;
;; where <kind> is one of :recv / :success / :send-out / :failure / :split /
;; :merge / :inject / :run-started. Nesting via `as-step` inserts one extra
;; `"flow" <sub-id>` segment per level. Subscribers use glob patterns like
;; `["recv" "flow" :* :>]` (all recvs across all flows) or
;; `["recv" "flow" "outer" "flow" "sub" "step" "inc"]` (a specific nested step).
;; ============================================================================

;; A user subscriber sees every :recv event under a specific flow-id.
(deftest watcher-custom-subscriber
  (let [ps (pubsub/make)
        recv-count (atom 0)
        u (pubsub/sub ps ["recv" "flow" "run-A" :>]
                      (fn [_ _ _] (swap! recv-count inc)))
        wf (step/serial
           (c/fan-out :split 3)
           (step/sink))
        result (flow/run! wf {:pubsub ps :flow-id "run-A" :data :x})]
    (u)
    (is (= :completed (:state result)))
    (is (= 4 @recv-count))))

;; Two separate runs sharing one pubsub — a wildcard subscriber tallies
;; events per flow-id.
(deftest multi-flow-shared-pubsub
  (let [ps (pubsub/make)
        tallies (atom {})
        u (pubsub/sub ps ["recv" "flow" :* :>]
                      (fn [_ ev _]
                        (swap! tallies update (first (:flow-path ev)) (fnil inc 0))))
        wf-A (step/serial (step/step :a  inc)
                          (step/sink :sa))
        wf-B (step/serial (step/step :b  dec)
                          (step/sink :sb))
        ra (flow/run! wf-A {:pubsub ps :flow-id "A" :data 1})
        rb (flow/run! wf-B {:pubsub ps :flow-id "B" :data 2})]
    (u)
    (is (= :completed (:state ra)))
    (is (= :completed (:state rb)))
    (is (= {"A" 2 "B" 2} @tallies))))

;; Regression: `:recv` must fire on arrival, before the handler runs.
;; Historically the runtime batched every lifecycle event for a single
;; invocation into one vector emitted after the handler returned —
;; so `:recv` and `:success` reached subscribers back-to-back, making
;; `recv - success` useless as a live in-flight gauge.
;;
;; We prove the ordering without a sleep: the handler publishes a marker
;; mid-run. Under the correct ordering, the subscriber's kinds-per-step
;; are `[:recv :marker :success]`. Under the old (batched) behavior the
;; marker would appear *before* `:recv`.
(deftest recv-fires-on-arrival-before-handler-runs
  (let [ps    (pubsub/make)
        seen  (atom [])
        probe (step/step :probe nil
                         (fn [ctx _s d]
                           (pubsub/pub (:raw (:pubsub ctx))
                                       ["marker"]
                                       {:kind    :marker
                                        :step-id :probe
                                        :msg-id  (:msg-id (:msg ctx))})
                           {:out [d]}))
        wf (step/serial probe (step/sink))
        u  (pubsub/sub ps [:>]
                       (fn [_ ev _] (swap! seen conj ev)))
        _  (flow/run! wf {:pubsub ps :data :x})]
    (u)
    (let [probe-evs (filterv #(= :probe (:step-id %)) @seen)
          kinds (mapv :kind
                      (filter #(#{:recv :marker :success} (:kind %))
                              probe-evs))]
      (is (= [:recv :marker :success] kinds)))))

;; Nested flows get distinct subject prefixes, so a subscriber can target
;; outer- vs inner-namespaced events even when both use the same step id.
(deftest nested-flow-namespaced
  (let [ps (pubsub/make)
        inner (step/step :inc #(+ % 100))
        outer (step/serial
              (step/step :inc inc)
              (step/as-step :sub inner)
              (step/sink))
        outer-recvs (atom [])
        inner-recvs (atom [])
        u1 (pubsub/sub ps ["recv" "flow" "outer" "step" "inc"]
                       (fn [_ ev _] (swap! outer-recvs conj ev)))
        u2 (pubsub/sub ps ["recv" "flow" "outer" "flow" "sub" "step" "inc"]
                       (fn [_ ev _] (swap! inner-recvs conj ev)))
        result (flow/run! outer {:pubsub ps :flow-id "outer" :data 5})]
    (u1) (u2)
    (is (= :completed (:state result)))
    (is (= 1 (count @outer-recvs)))
    (is (= 1 (count @inner-recvs)))
    (is (= ["outer"]       (:flow-path (first @outer-recvs))))
    (is (= ["outer" "sub"] (:flow-path (first @inner-recvs))))
    (is (= 5 (:data (first @outer-recvs))))
    (is (= 6 (:data (first @inner-recvs))))))

;; ============================================================================
;; Act XV — A worked example
;;
;; Putting the pieces together: a podcast pipeline fetches a feed, parses
;; episodes, and runs each through download/transcribe/chunk/summarize.
;; run-seq attributes the summary list back to its single input URL.
;; ============================================================================

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
  (let [wf     (step/step :process nil
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
                              [s {:out [(mapv :summary done)]}])))
        result (flow/run-seq wf [{:url "https://example.com/feed.xml"}])]
    (testing "run completes; output attributed to the single input"
      (is (= :completed (:state result)))
      (is (= [["summary-e1" "summary-e2" "summary-e3"]]
             (first (:outputs result)))))))

;; ============================================================================
;; Act XVI — Parallel workers (workers combinator)
;;
;; `workers` runs K copies of an inner step behind a round-robin router and
;; merges their outputs through a K-input join. Each worker is a distinct
;; proc, so core.async.flow gives each its own thread — per-message
;; parallelism is real, not cooperative. The router/join/workers share an
;; outer [:flow id] scope segment, and each worker adds [:flow wN] below —
;; so per-stage aggregation and per-worker distinction are both structural.
;; ============================================================================

;; Round-robin distribution: K=3 against 9 inputs places exactly 3 per worker.
;; Per-worker distinction lives in the scope path (each worker adds a
;; [:flow :wN] segment under the shared [:flow :pool]), so we capture the
;; scope vector rather than the (still-shared) :step-id.
(deftest workers-round-robin-distributes-evenly
  (let [seen   (atom [])
        probe  (step/step :probe {:ins {:in ""} :outs {:out ""}}
                          (fn [ctx _s d]
                            (swap! seen conj [(:prefix (:pubsub ctx)) d])
                            {:out [(msg/pass ctx)]}))
        wf     (c/workers :pool 3 probe)
        result (flow/run-seq wf (range 9))]
    (testing "run completes; every input produces exactly one output"
      (is (= :completed (:state result)))
      (is (= [[0] [1] [2] [3] [4] [5] [6] [7] [8]] (:outputs result))))
    (testing "three distinct worker scopes, each seeing 3 messages"
      (is (= 3 (count (distinct (map first @seen)))))
      (is (= #{3} (set (vals (frequencies (map first @seen)))))))))

;; Per-worker state isolation: with K=2, even- and odd-positioned inputs go
;; to different workers, each accumulating its own running sum.
(deftest workers-isolate-state-per-worker
  (let [acc (step/step :acc nil
                       (fn [_ctx s d]
                         (let [s' (update s :sum (fnil + 0) d)]
                           [s' {:out [(:sum s')]}])))
        wf  (step/serial (c/workers :pool 2 acc))
        res (flow/run-seq wf [10 1 20 2 30 3])]
    (is (= :completed (:state res)))
    (testing "w0 sees 10,20,30 → sums 10,30,60; w1 sees 1,2,3 → sums 1,3,6"
      (is (= [[10] [1] [30] [3] [60] [6]] (:outputs res))))))

;; Token preservation: a data msg carrying tokens traverses the router→worker
;; →join path with tokens intact (synthesis's K-way split collapses to 1-way
;; at each step, so the group value is preserved end-to-end).
(deftest workers-preserve-tokens
  (let [wf (step/serial
           (step/step :inc inc)
           (c/workers :pool 3 (step/step :noop identity))
           (step/sink))
        h  (flow/start! wf)]
    (try
      (flow/inject! h {:data 1 :tokens {"g" 7}})
      (flow/await-quiescent! h)
      (let [result    (flow/stop! h)
            sink-recv (first (filter #(and (= :sink (:step-id %))
                                           (= :recv (:kind %))
                                           (= :data (:msg-kind %)))
                                     (:events result)))]
        (is (some? sink-recv))
        (is (= {"g" 7} (:tokens sink-recv))))
      (finally
        (when-not (realized? (::flow/cancel h)) (flow/stop! h))))))

;; Signal semantics: one injected signal produces one downstream signal,
;; not K. The router's custom :on-signal routes (not broadcasts), so exactly
;; one worker sees it, and the join's default broadcast to one :out port
;; emits one signal.
(deftest workers-route-signals-not-broadcast
  (let [wf (step/serial
           (c/workers :pool 4 (step/passthrough :w))
           (step/sink))
        h  (flow/start! wf)]
    (try
      (flow/inject! h {:tokens {"g" 9}})
      (flow/await-quiescent! h)
      (let [result (flow/stop! h)
            sigs   (filterv #(and (= :sink (:step-id %))
                                  (= :recv (:kind %))
                                  (= :signal (:msg-kind %)))
                            (:events result))]
        (is (= 1 (count sigs)))
        (is (= {"g" 9} (:tokens (first sigs)))))
      (finally
        (when-not (realized? (::flow/cancel h)) (flow/stop! h))))))

;; Done cascade fires exactly once downstream. The join's K distinct input
;; ports ensure on-all-closed fires once, not K times.
(deftest workers-done-cascade-fires-once
  (let [wf (step/serial
           (c/workers :pool 3 (step/passthrough :w))
           (step/sink))
        h  (flow/start! wf)]
    (flow/inject! h {})
    (flow/await-quiescent! h)
    (let [result (flow/stop! h)
          dones  (filterv #(and (= :sink (:step-id %))
                                (= :recv (:kind %))
                                (= :done (:msg-kind %)))
                          (:events result))]
      (is (= :completed (:state result)))
      (is (= 1 (count dones))))))

;; `inner` may be a composite step, not just a single handler-map — internal
;; sids get prefixed per-worker so state is isolated even when inner has
;; multiple procs.
(deftest workers-accepts-composite-step
  (let [inner (step/serial (step/step :a inc) (step/step :b #(* 2 %)))
        wf    (step/serial (c/workers :pool 2 inner))
        res   (flow/run-seq wf [1 2 3 4])]
    (is (= :completed (:state res)))
    (is (= [[4] [6] [8] [10]] (:outputs res)))))
