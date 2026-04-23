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
     XV   — A worked example (podcast pipeline)"
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.dependency :as dep]
            [toolkit.datapotamus :as dp]
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

;; ============================================================================
;; Act I — A step is the smallest unit
;;
;; Everything composes up from a step. A step is a map with :procs, :conns,
;; :in, :out; `dp/step` builds the 1-proc kind. Before wiring anything we
;; inspect one to see what's in the box.
;; ============================================================================

;; A freshly built step has one proc, one conn list (empty), and in=out=id.
(deftest step-is-a-1-in-1-out-step
  (let [f (dp/step :inc inc)]
    (is (= #{:inc} (set (keys (:procs f)))))
    (is (= :inc (:in f)))
    (is (= :inc (:out f)))
    (is (= [] (:conns f)))))

;; ============================================================================
;; Act II — Linear composition with `serial`
;;
;; `serial` glues steps end-to-end: the :out of one becomes the :in of the
;; next. Sinks terminate a chain. `run!` drives the graph for a single
;; injected message and returns the collected trace events.
;; ============================================================================

;; serial + run!: data flows inc → dbl → sink.
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

;; Each step fires :recv + :success exactly once; send-outs are counted once per port.
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

;; Two step styles: `(step id f)` pure-fn vs `(step id nil handler)` 3-arg form.
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

;; serial requires unique proc ids across the composed steps.
(deftest serial-rejects-collisions
  (let [a (dp/step :same inc)
        b (dp/step :same dec)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"colliding"
                          (dp/serial a b)))))

;; A step can expose custom :in / :out port names via input-at / output-at;
;; serial wires through those vector endpoints rather than the defaults.
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

;; `passthrough` forwards data unchanged — a no-op in the pipeline.
(deftest passthrough-is-transparent
  (let [wf (dp/serial
             (dp/step :inc inc)
             (dp/passthrough)
             (dp/sink))
        result (dp/run! wf {:data 7})]
    (is (= :completed (:state result)))
    (is (= 8 (:data (first (filterv #(= :sink (:step-id %))
                                    (events-of result :recv))))))))

;; passthrough preserves :data-id (useful for dedup/caching keyed on identity).
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

;; ============================================================================
;; Act III — Beyond linear: explicit graphs
;;
;; When the topology isn't a line, assemble the graph by hand:
;; `merge-steps` unions procs; `connect` adds conns; `input-at` / `output-at`
;; set the boundary. This unlocks feedback loops and multi-port routing.
;; ============================================================================

;; The minimal hand-wired graph: merge-steps + a single connect + input-at.
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

;; Agent-style feedback loop: agent emits either :tool-call or :final. The
;; tool's output loops back to the agent on :tool-result. After N calls the
;; agent emits :final, which flows on to :sink.
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

;; A `router` picks an output port per result — here one input (a coll) fans
;; out to per-element routes across :odd and :even ports.
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

;; ============================================================================
;; Act IV — Nesting with `as-step`
;;
;; `as-step` treats an inner (composed) step as a single proc under a new id.
;; Scopes nest one [:flow] segment per level; `:step-id` remains the original.
;; ============================================================================

;; Three levels of nesting — each subflow adds one [:flow id] segment to scope.
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

;; Outer steps see only the flow-level scope; steps inside an as-step get the
;; extra [:flow :sub] segment, but ids aren't collided between inner and outer.
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

;; A signal fans across all declared output ports (unlike data, which needs
;; the user handler to target a port). Tokens split K-ways so XOR reconstructs.
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

;; Inject with :tokens (no :data) seeds a signal — user handlers skipped.
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

;; Inject with neither :data nor :tokens seeds a :done marker. On a linear
;; chain, each step sees :done once and cascades it to the next.
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

;; A multi-input step holds its :done cascade until every input port has
;; received its own :done marker — only then does it close downstream.
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

;; ============================================================================
;; Act VI — Derivation helpers + ctx :in-port
;;
;; Handlers build outputs via `child`/`children`/`pass`/`signal`/`merge`.
;; These return drafts whose parent refs drive synthesis after the handler
;; returns — splits and merges in the trace events, tokens distributed over
;; the in-handler DAG.
;; ============================================================================

;; Single-input handler: ctx :in-port defaults to :in.
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

;; Multi-input handler: ctx :in-port carries which port this invocation arrived on.
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

;; `child` emits one derived message per call; multi-port handlers call it
;; once per output port.
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

;; `children` splits the parent's tokens K-ways — XOR of the per-child
;; slices equals the parent's token for each group.
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

;; `merge` produces a multi-parent derivation — visible as one :merge event
;; alongside the normal per-child :split events.
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

;; Chains of `child` calls produce one :split event per intermediate draft,
;; even though the intermediates never leave the handler.
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

;; Graph-level diamond: `split` emits two siblings to :to-b and :to-c;
;; they traverse B and C respectively and land on D's :left / :right
;; input ports. D pairs them and uses `dp/merge` to combine lineages —
;; so D's emission carries :parent-msg-ids from both B and C.
(deftest diamond-dag-merges-two-branches
  (let [split  (dp/step :split {:outs {:to-b "" :to-c ""}}
                 (fn [ctx s d]
                   (let [[kb kc] (dp/children ctx [d d])]
                     [s [[:to-b kb] [:to-c kc]]])))
        b      (dp/step :b #(* 10 %))
        c      (dp/step :c #(+ 100 %))
        joiner (dp/step :d
                 {:ins {:left "" :right ""} :outs {:out ""}}
                 (fn [{:keys [in-port] :as ctx} s d-val]
                   (if-let [pending (:pending s)]
                     [(dissoc s :pending)
                      [[:out (dp/merge ctx
                                       [(:msg pending) (:msg ctx)]
                                       {(:port pending) (:data pending)
                                        in-port        d-val})]]]
                     [(assoc s :pending {:port in-port :msg (:msg ctx) :data d-val})
                      []])))
        wf     (-> (dp/merge-steps split b c joiner (dp/sink))
                   (dp/connect [:split :to-b] [:b :in])
                   (dp/connect [:split :to-c] [:c :in])
                   (dp/connect [:b :out]      [:d :left])
                   (dp/connect [:c :out]      [:d :right])
                   (dp/connect [:d :out]      [:sink :in])
                   (dp/input-at  :split)
                   (dp/output-at :sink))
        result   (dp/run! wf {:data 5})
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
;; of a run. Handlers receive a factory-time ctx carrying :step-id, :scope,
;; and :cancel — useful for building subscribers keyed on those fields.
;; ============================================================================

;; Factory ctx carries :step-id and the step's scope segments.
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

;; Folded events form an acyclic graph with exactly one root — every
;; downstream message traces back to the single seed through :split / :merge.
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

;; Frequency summaries match the topology: one fan-out input → three sink recvs.
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

;; fan-out with ordinary pure-fn steps upstream — each of 3 copies carries
;; the doubled value downstream.
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

;; fan-in closes the group: one :merge event carrying all three parents,
;; exactly one downstream send-out, one sink recv.
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

;; Nesting composes: the inner fan-in closes 3 times (once per outer child),
;; the outer fan-in closes exactly once and the sink gets a single message.
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

;; ============================================================================
;; Act IX — Escape hatches: assoc-tokens / dissoc-tokens
;;
;; For patterns the helpers can't express — custom groups, dribbling a
;; source's tokens across multiple invocations — stamp tokens directly on
;; drafts. The user is responsible for XOR-balancing the stamps so global
;; conservation still holds. fan-out/fan-in are the in-repo exemplars.
;; ============================================================================

;; Smoke: stamp a token-map onto a draft; the stamp shows up on the emission.
(deftest assoc-tokens-smoke
  (let [stamper (dp/step :stamper {:ins {:in ""} :outs {:out ""}}
                  (fn [ctx s _d]
                    [s [[:out (-> (dp/child ctx :payload)
                                  (dp/assoc-tokens {"g" 77}))]]]))
        result  (dp/run! (dp/serial stamper (dp/sink)) {:data :seed})
        sends   (filterv #(and (= :stamper (:step-id %)) (:port %))
                         (events-of result :send-out))]
    (is (= 1 (count sends)))
    (is (= 77 (get-in (first sends) [:tokens "g"])))))

;; Smoke: strip a group key off a draft before emitting.
(deftest dissoc-tokens-smoke
  (let [stripper (dp/step :stripper {:ins {:in ""} :outs {:out ""}}
                   (fn [ctx s _d]
                     [s [[:out (-> (dp/pass ctx)
                                   (dp/dissoc-tokens ["a"]))]]]))
        result   (dp/run! (dp/serial stripper (dp/sink))
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
        (dp/step :minter {:ins {:in ""} :outs {:a "" :b ""}}
          (fn [ctx s _d]
            (let [gid     [gid-key (:msg-id (:msg ctx))]
                  [v1 v2] (tok/split-value 0 2)
                  a (-> (dp/child ctx :a-side) (dp/assoc-tokens {gid v1}))
                  b (-> (dp/child ctx :b-side) (dp/assoc-tokens {gid v2}))]
              [s [[:a a] [:b b]]])))
        wf (-> (dp/merge-steps minter (dp/fan-in :fi gid-key) (dp/sink))
               (dp/connect [:minter :a] [:fi :in])
               (dp/connect [:minter :b] [:fi :in])
               (dp/connect [:fi :out] [:sink :in])
               (dp/input-at :minter)
               (dp/output-at :sink))
        result (dp/run! wf {:data :go})]
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
  (let [pairer (dp/step :pairer {:ins {:in ""} :outs {:out ""}}
                 (fn [ctx s _d]
                   (let [pending (:pending s)]
                     (if (nil? pending)
                       [(assoc s :pending (:msg ctx)) []]
                       [(dissoc s :pending)
                        [[:out (dp/merge ctx
                                         [pending (:msg ctx)]
                                         [(:data pending) (:data (:msg ctx))])]]]))))
        wf     (dp/serial pairer (dp/sink))
        h      (dp/start! wf)]
    (dp/inject! h {:data :a :tokens {"g" 7}})
    (dp/await-quiescent! h)
    (dp/inject! h {:data :b :tokens {"g" 11}})
    (dp/await-quiescent! h)
    (let [result       (dp/stop! h)
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
        (dp/step :dribbler {:ins {:in ""} :outs {:out ""}}
          (fn [ctx s _d]
            (case (:kind (:data (:msg ctx)))
              :start
              [(assoc s :residual (:tokens (:msg ctx)) :left K) []]

              :tick
              (let [{:keys [residual left]} s
                    last? (= 1 left)
                    chunk (if last? residual (random-tokens-like residual))
                    out   (-> (dp/child ctx {:i left})
                              (dp/assoc-tokens chunk))]
                [(-> s
                     (assoc :residual (tok/merge-tokens residual chunk))
                     (update :left dec))
                 [[:out out]]]))))
        wf (dp/serial dribbler (dp/sink))
        h  (dp/start! wf)]
    (dp/inject! h {:data {:kind :start} :tokens {"g" 42}})
    (dp/await-quiescent! h)
    (dotimes [_ K]
      (dp/inject! h {:data {:kind :tick}})
      (dp/await-quiescent! h))
    (let [result          (dp/stop! h)
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
;; Failures are message-level by default: a throwing handler surfaces as one
;; :failure event, the run continues, other messages complete normally, and
;; the final :state stays :completed. `fcatch` converts exceptions to values;
;; `retry` re-runs on exception.
;;
;; :state flips to :failed only when the failure is structural — a
;; factory-level throw (from the 2-arity core.async.flow transition) bubbles
;; through error-chan and latches the run's done-promise to [:failed err].
;; Message-level errors never do that.
;; ============================================================================

;; fcatch: wraps a fn so it returns exceptions rather than throwing them.
(deftest fcatch-returns-exceptions-as-values
  (let [safe-div (dp/fcatch /)]
    (is (= 5 (safe-div 10 2)))
    (is (instance? ArithmeticException (safe-div 10 0)))))

;; Compose fcatch with a router to get try/catch semantics at the graph level:
;; ok path → :ok-sink, thrown path → :err-sink, no :failure event produced.
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

;; retry: retries are invisible in the trace — only one :recv per step.
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

;; When retries are exhausted, the error surfaces as a :failure event and
;; the run still completes; the failed message does NOT reach downstream.
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

;; A bare throwing step (no retry) has the same behavior — one :failure, run
;; continues, downstream skipped.
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

;; Emitting on an undeclared port is a handler-level failure — one :failure
;; event naming the unknown port and the declared ports.
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

;; A factory-level throw (out of core.async.flow's 2-arg transition) DOES
;; escalate: await-quiescent! returns [:failed err] carrying the cause.
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

;; ============================================================================
;; Act XI — Validation (early rejects)
;;
;; Early validation catches wiring mistakes at start!/inject! time rather
;; than letting them manifest as hangs or silent drops at runtime.
;; ============================================================================

;; A router with an unconsumed output port is rejected at start!.
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

;; inject! rejects an unknown step id up-front; the ex-data lists known sids.
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

;; inject! rejects an unknown port on a known step; ex-data lists declared ports.
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

;; ============================================================================
;; Act XII — Long-running handles
;;
;; `run!` is sugar for start! + one inject + await + stop!. For longer-lived
;; workflows, drive the handle directly: inject many messages, poll counters,
;; read cancellation state from the cancel promise.
;; ============================================================================

;; One handle, three injects, counters sum correctly across injections.
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

;; The handle's ::cancel promise is undelivered until stop!; handlers can
;; poll it from ctx for cooperative cancellation.
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

;; ============================================================================
;; Act XIII — Running a collection: run-seq
;;
;; `run-seq` runs a step against a seq of inputs, attributes each output
;; back to the input whose ancestry reaches it, and returns an :outputs
;; vector aligned with the input collection.
;; ============================================================================

;; Empty input → empty outputs, no events.
(deftest run-seq-empty-coll
  (let [result (dp/run-seq (dp/step :inc inc) [])]
    (is (= :completed (:state result)))
    (is (= [] (:outputs result)))))

;; Each input yields exactly one output in the same position.
(deftest run-seq-single-input-single-output
  (let [result (dp/run-seq (dp/step :inc inc) [1 2 3])]
    (is (= :completed (:state result)))
    (is (= [[2] [3] [4]] (:outputs result)))))

;; Outputs stay attributed to their originating input even when the pipeline
;; runs them concurrently and event order interleaves.
(deftest run-seq-preserves-order-across-interleaved-inputs
  (let [wf (dp/step :sq (fn [x] (* x x)))
        result (dp/run-seq wf [2 3 4 5])]
    (is (= :completed (:state result)))
    (is (= [[4] [9] [16] [25]] (:outputs result)))))

;; An input that produces nothing downstream gets an empty slot in :outputs.
(deftest run-seq-no-output-gives-empty-slot
  (let [filter-odd (dp/step :filter-odd nil
                            (fn [_ctx s d]
                              [s (if (odd? d) [[:out d]] [])]))
        result (dp/run-seq filter-odd [1 2 3 4])]
    (is (= :completed (:state result)))
    (testing "odd inputs produce output; even inputs produce empty slots"
      (is (= [[1] [] [3] []] (:outputs result))))))

;; One input can produce many outputs — each attributed to its seed.
(deftest run-seq-fan-out-produces-multiple-outputs-per-input
  (let [result (dp/run-seq (dp/serial (dp/step :dbl #(* 2 %))
                                      (dp/fan-out :three 3))
                           [5 7])]
    (is (= :completed (:state result)))
    (testing "each input contributes three copies of its doubled value"
      (is (= [[10 10 10] [14 14 14]] (mapv #(vec (sort %)) (:outputs result)))))))

;; mapcat-style expansion via `children` — one input → three outputs.
(deftest run-seq-mapcat-style-via-children
  (let [wf (dp/step :expand {:outs {:out ""}}
                    (fn [ctx s d]
                      [s (mapv (fn [k] [:out k]) (dp/children ctx [d (inc d) (inc (inc d))]))]))
        result (dp/run-seq wf [10 20])]
    (is (= :completed (:state result)))
    (is (= [[10 11 12] [20 21 22]]
           (mapv #(vec (sort %)) (:outputs result))))))

;; A single output descended from multiple seeds appears under every
;; matching index in :outputs. Here a pair-merger collects two inputs and
;; emits one merged message whose lineage reaches both seeds.
(deftest run-seq-cross-input-merge-appears-under-every-seed
  (let [pairer (dp/step :pairer {:ins {:in ""} :outs {:out ""}}
                 (fn [ctx s _d]
                   (if-let [pending (:pending s)]
                     [(dissoc s :pending)
                      [[:out (dp/merge ctx
                                       [pending (:msg ctx)]
                                       [(:data pending) (:data (:msg ctx))])]]]
                     [(assoc s :pending (:msg ctx)) []])))
        result (dp/run-seq pairer [:a :b])]
    (is (= :completed (:state result)))
    (testing "the one merged output is attributed to BOTH seeds"
      (is (= [[[:a :b]] [[:a :b]]] (:outputs result))))))

;; A failure on one input doesn't drop the others — the failed index gets an
;; empty slot and exactly one :failure event carries the step attribution.
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

;; ============================================================================
;; Act XIV — Observing runs via scoped pubsub
;;
;; Every event is published on a subject of the form
;;
;;     <kind>.flow.<flow-id>[.flow.<sub-id>]*[.step.<sid>]
;;
;; where <kind> is one of :recv / :success / :send-out / :failure / :split /
;; :merge / :seed / :run-started. Nesting via `as-step` inserts one extra
;; `.flow.<sub-id>` segment per level. Subscribers use globs like
;; `recv.flow.*.>` (all recvs across all flows) or
;; `recv.flow.outer.flow.sub.step.inc` (a specific nested step).
;; ============================================================================

;; A user subscriber sees every :recv event under a specific flow-id.
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

;; Two separate runs sharing one pubsub — a wildcard subscriber tallies
;; events per flow-id.
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

;; Nested flows get distinct subject prefixes, so a subscriber can target
;; outer- vs inner-namespaced events even when both use the same step id.
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
