(ns toolkit.datapotamus.close-input-done-test
  "Tests for the proc-fn nil-msg → input-done synthesis behavior in flow.clj.

   Two invariants under test:

   I-1. Close-equivalence on declared inputs. For any handler-map H with
        declared input port :p, feeding a sequence of envelopes onto :p's
        input channel and then *closing the channel* produces the same trace
        stream / counter deltas / output port-map as feeding the same envelopes
        followed by (msg/new-input-done), modulo :msg-id (fresh) and
        :parent-msg-ids (empty) on the terminating event.

   I-2. Filter on undeclared inputs. Closing a channel that reaches the proc
        via ::flow/in-ports under a name not in the handler's declared
        :ports :ins is a silent no-op at the Datapotamus layer:
        no event emitted, no ::input-dones-seen mutation, no run-step dispatch.

   These tests work by injecting a test-owned channel via ::flow/in-ports.
   We can then write envelopes to that channel directly with `a/>!!` or close
   it with `a/close!`. We do NOT use `flow/inject!` because inject! writes to
   the framework-allocated chan, which ::flow/in-ports has overridden — the
   injected message would land on a chan no one reads from."
  (:refer-clojure :exclude [run!])
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as-alias caf]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.pubsub :as pubsub]))

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- start-with-events
  "Start a flow with a wildcard pubsub subscription that collects every event
   into an atom. Returns the handle, augmented with ::evs (the atom) and
   ::unsub (the unsubscribe fn)."
  [stepmap]
  (let [ps    (pubsub/make)
        evs   (atom [])
        unsub (pubsub/sub ps [:>] (fn [_ ev _] (swap! evs conj ev)))
        h     (flow/start! stepmap {:pubsub ps})]
    (assoc h ::evs evs ::unsub unsub)))

(defn- events [h] @(::evs h))

(defn- stop! [h]
  (let [r (flow/stop! h)]
    ((::unsub h))
    (assoc r :events @(::evs h))))

(defn- wait-for-event
  "Poll until at least `n` events satisfy `pred`, then return. Times out after
   `timeout-ms` (default 1000ms). Throws on timeout with a useful payload."
  ([h pred n] (wait-for-event h pred n 1000))
  ([h pred n timeout-ms]
   (let [deadline (+ (System/nanoTime) (* (long timeout-ms) 1000 1000))]
     (loop []
       (let [matches (count (filterv pred (events h)))]
         (cond
           (>= matches n)                    nil
           (> (System/nanoTime) deadline)
           (throw (ex-info "wait-for-event timeout"
                           {:wanted n :got matches
                            :pred-summary (str pred)
                            :events (events h)}))
           :else (do (Thread/sleep 5) (recur))))))))

(defn- success-of
  "Wait for `n` :success events to fire on the given step-id."
  [h sid n]
  (wait-for-event h #(and (= :success (:kind %)) (= sid (:step-id %))) n))

;; --- shapes ------------------------------------------------------------------

;; All probes wire :out to a sink. core.async.flow's resolver throws on emit
;; to an unconnected output port (it tries to look up a chan and fails), and
;; the proc body's catch swallows the throw without dissocing the closed
;; input from its read set — producing an infinite nil-read loop. Sink absorbs.

(defn- absorbing-sink
  "Drop-everything sink with declared :ins {:in \"\"} and no outputs.
   Returned as a raw handler-map so the caller composes it directly into
   :procs alongside their probe."
  []
  (step/handler-map
   {:ports   {:ins {:in ""} :outs {}}
    :on-data (fn [_ _ _] {})}))

(defn- single-input-probe
  "Probe that owns its only input via ::flow/in-ports {:in test-chan}.
   Forwards each data input to :out via msg/pass. Wires :out to an
   absorbing sink so the cascaded done has somewhere to go."
  [test-chan]
  (let [hmap (step/handler-map
              {:ports   {:ins {:in ""} :outs {:out ""}}
               :on-init (fn [] {::caf/in-ports {:in test-chan}})
               :on-data (fn [ctx _s _d] {:out [(msg/pass ctx)]})})]
    {:procs {:probe hmap :sink (absorbing-sink)}
     :conns [[[:probe :out] [:sink :in]]]
     :in    :probe
     :out   :sink}))

(defn- multi-input-probe
  "Probe with multiple declared inputs, each owned by a chan via
   ::flow/in-ports. Forwards data to :out → sink."
  [chans-by-port]
  (let [hmap (step/handler-map
              {:ports   {:ins  (zipmap (keys chans-by-port) (repeat ""))
                         :outs {:out ""}}
               :on-init (fn [] {::caf/in-ports chans-by-port})
               :on-data (fn [ctx _s _d] {:out [(msg/pass ctx)]})})]
    {:procs {:probe hmap :sink (absorbing-sink)}
     :conns [[[:probe :out] [:sink :in]]]
     :in    [:probe (first (keys chans-by-port))]
     :out   :sink}))

(defn- mixed-probe
  "Probe where the handler declares only `decl-port` in :ports :ins, but
   receives an additional input on `extra-port` via ::flow/in-ports. The
   :on-data dispatches on (:in-port ctx) — only forwards data from declared
   ports to :out (which is wired to a sink)."
  [decl-port decl-chan extra-port extra-chan]
  (let [hmap (step/handler-map
              {:ports   {:ins  {decl-port ""} :outs {:out ""}}
               :on-init (fn []
                          {::caf/in-ports {decl-port  decl-chan
                                           extra-port extra-chan}})
               :on-data (fn [ctx _s _d]
                          (if (= (:in-port ctx) decl-port)
                            {:out [(msg/pass ctx)]}
                            ;; data on undeclared input — drop, don't forward
                            {}))})]
    {:procs {:probe hmap :sink (absorbing-sink)}
     :conns [[[:probe :out] [:sink :in]]]
     :in    :probe
     :out   :sink}))

;; --- event projections (modulo nondeterminism) -------------------------------

(defn- recv-event-shape
  "Project a :recv event down to its kind/msg-kind/in-port — the fields whose
   *equality* matters for close-vs-envelope equivalence. :msg-id and
   :parent-msg-ids are intentionally dropped (intrinsically different)."
  [e]
  (select-keys e [:kind :msg-kind :step-id :in-port]))

(defn- send-out-shape
  [e]
  (select-keys e [:kind :msg-kind :step-id :port]))

(defn- success-shape
  [e]
  (select-keys e [:kind :msg-kind :step-id]))

(defn- of-kinds
  "All events of the given kinds, projected to their shape."
  [evs kinds]
  (filterv #(kinds (:kind %)) evs))

(defn- shapes-for
  "Take an event log and return a map {:recv [...] :success [...] :send-out [...]}
   of projected events for the given step-id."
  [evs sid]
  (let [for-step (filterv #(= sid (:step-id %)) evs)]
    {:recv     (mapv recv-event-shape (of-kinds for-step #{:recv}))
     :send-out (mapv send-out-shape   (of-kinds for-step #{:send-out}))
     :success  (mapv success-shape    (of-kinds for-step #{:success}))}))

;; --- scenario runners --------------------------------------------------------

(defn- run-with-terminator
  "Builds a fresh single-input probe, writes each `envelope` from `pre-envs`
   to its input chan, applies `terminator-fn` (either close or done-envelope),
   waits for the cascade-success event, and returns the collected event log
   plus counters."
  [pre-envs terminator-fn]
  (let [c (a/chan)
        h (start-with-events (single-input-probe c))]
    (try
      (doseq [env pre-envs]
        (a/>!! c env))
      ;; wait for each pre-env's :success before terminating, so the trace
      ;; is sequenced deterministically and we don't race with the proc loop.
      (when (seq pre-envs)
        (success-of h :probe (count pre-envs)))
      (terminator-fn c)
      ;; wait for one more :success (the cascade)
      (success-of h :probe (inc (count pre-envs)))
      (let [r (stop! h)]
        {:events   (:events r)
         :counters (:counters r)})
      (catch Throwable t
        (try (stop! h) (catch Throwable _ nil))
        (throw t)))))

(defn- close-scenario [pre-envs] (run-with-terminator pre-envs a/close!))
(defn- envelope-scenario [pre-envs]
  (run-with-terminator pre-envs (fn [c] (a/>!! c (msg/new-input-done)))))

(defn- equivalent?
  "True iff the close-scenario and envelope-scenario produced equivalent
   event shapes for :probe."
  [pre-envs]
  (let [a (close-scenario    pre-envs)
        b (envelope-scenario pre-envs)
        sa (shapes-for (:events a) :probe)
        sb (shapes-for (:events b) :probe)]
    (= sa sb)))

;; ============================================================================
;; A. Close-equivalence on a single declared input
;; ============================================================================

(deftest a1-empty-input-close-equivalent-to-envelope-done
  (testing "scenario A: close immediately"
    (let [{:keys [events counters]} (close-scenario [])
          probe-shapes (shapes-for events :probe)]
      (testing "probe: exactly one :recv (the synthesized done) on :in"
        (is (= 1 (count (:recv probe-shapes))))
        (is (= {:kind :recv :msg-kind :input-done :step-id :probe :in-port :in}
               (first (:recv probe-shapes)))))
      (testing "probe: exactly one :send-out for the auto-appended done on :out"
        (is (= 1 (count (:send-out probe-shapes))))
        (is (= {:kind :send-out :msg-kind :input-done :step-id :probe :port :out}
               (first (:send-out probe-shapes)))))
      (testing "probe: exactly one :success"
        (is (= 1 (count (:success probe-shapes))))
        (is (= :input-done (-> probe-shapes :success first :msg-kind))))
      (testing "counters: probe (sent +1 send-out, recv +1, completed +1) +
                          sink   (recv +1 done arrival, completed +1, no sent)"
        (is (= {:sent 1 :recv 2 :completed 2} counters)))))
  (testing "scenario B: envelope-done"
    (let [{:keys [events counters]} (envelope-scenario [])
          probe-shapes (shapes-for events :probe)]
      (is (= 1 (count (:recv probe-shapes))))
      (is (= 1 (count (:send-out probe-shapes))))
      (is (= 1 (count (:success probe-shapes))))
      (is (= {:sent 1 :recv 2 :completed 2} counters))))
  (testing "shapes are identical between scenarios (I-1)"
    (is (equivalent? []))))

(deftest a2-n-data-then-close-equivalent-to-envelope-done
  (doseq [n [1 5 25]]
    (testing (str n " data envelopes then terminate")
      (let [pre (mapv #(msg/new-msg %) (range n))]
        (is (equivalent? pre)
            (str "shapes diverge for n=" n))))))

(deftest a3-mixed-data-and-signal-then-close-equivalent
  (let [pre [(msg/new-msg 1)
             (msg/new-signal {"g" 1})
             (msg/new-msg 2)
             (msg/new-signal {"h" 2})
             (msg/new-msg 3)]]
    (is (equivalent? pre))))

;; ============================================================================
;; B. Multi-input cascade scenarios
;; ============================================================================

(defn- run-multi-input
  "Build a multi-input probe with the given port→chan map. Apply each
   `step` in `steps` in order. Each step is a [op port-or-msg ...] vector:
     [:write port envelope]   — write envelope to the chan for `port`
     [:close port]            — close the chan for `port`
     [:done port]             — write a done envelope to the chan for `port`
     [:wait-success n]        — wait until probe has n :success events
   Returns the final event log."
  [chans steps]
  (let [h (start-with-events (multi-input-probe chans))]
    (try
      (doseq [step steps]
        (case (first step)
          :write          (a/>!! (chans (second step)) (nth step 2))
          :close          (a/close! (chans (second step)))
          :done           (a/>!! (chans (second step)) (msg/new-input-done))
          :wait-success   (success-of h :probe (second step))))
      (let [r (stop! h)]
        (:events r))
      (catch Throwable t
        (try (stop! h) (catch Throwable _ nil))
        (throw t)))))

(defn- count-cascades
  "Count the number of cascade-completing :success :done events fired."
  [events]
  (count (filterv #(and (= :success (:kind %))
                        (= :input-done    (:msg-kind %))
                        (= :probe   (:step-id %)))
                  events)))

(defn- count-recv-input-dones
  [events]
  (count (filterv #(and (= :recv (:kind %))
                        (= :input-done (:msg-kind %))
                        (= :probe (:step-id %)))
                  events)))

(deftest b1-two-inputs-both-close-fires-cascade-once
  (testing "close :a then :b"
    (let [chans  {:a (a/chan) :b (a/chan)}
          events (run-multi-input chans
                                  [[:close :a]
                                   [:wait-success 1]
                                   [:close :b]
                                   [:wait-success 2]])]
      ;; First close: :recv done on :a, :success done (no cascade fires yet
      ;; because b is still open; run-done's else branch emits a :success
      ;; event for the recorded close-tracking)
      (is (= 2 (count-recv-input-dones events)))
      (is (= 2 (count-cascades  events)))
      (testing "second close: cascade fires (auto-append done on :out)"
        (let [send-outs (filterv #(and (= :send-out (:kind %))
                                       (= :probe (:step-id %)))
                                 events)]
          (is (= 1 (count send-outs)) "exactly one :send-out — the cascade's done")
          (is (= :input-done (:msg-kind (first send-outs))))
          (is (= :out  (:port     (first send-outs))))))))
  (testing "close :b then :a — same result"
    (let [chans  {:a (a/chan) :b (a/chan)}
          events (run-multi-input chans
                                  [[:close :b]
                                   [:wait-success 1]
                                   [:close :a]
                                   [:wait-success 2]])]
      (is (= 2 (count-recv-input-dones events)))
      (is (= 2 (count-cascades  events)))
      (let [send-outs (filterv #(and (= :send-out (:kind %))
                                     (= :probe (:step-id %)))
                               events)]
        (is (= 1 (count send-outs)))))))

(deftest b2-two-inputs-mixed-close-and-envelope-fires-cascade
  (doseq [[lbl ops] [["close-a then envelope-b"
                       [[:close :a] [:wait-success 1] [:done :b] [:wait-success 2]]]
                     ["envelope-a then close-b"
                       [[:done :a] [:wait-success 1] [:close :b] [:wait-success 2]]]
                     ["envelope-b then close-a"
                       [[:done :b] [:wait-success 1] [:close :a] [:wait-success 2]]]
                     ["close-b then envelope-a"
                       [[:close :b] [:wait-success 1] [:done :a] [:wait-success 2]]]]]
    (testing lbl
      (let [chans  {:a (a/chan) :b (a/chan)}
            events (run-multi-input chans ops)
            send-outs (filterv #(and (= :send-out (:kind %))
                                     (= :probe (:step-id %)))
                               events)]
        (is (= 2 (count-recv-input-dones events)))
        (is (= 1 (count send-outs))
            "exactly one :send-out (the cascade's auto-appended done)")))))

(deftest b3-only-some-closed-no-cascade
  (let [chans  {:a (a/chan) :b (a/chan) :c (a/chan)}
        h      (start-with-events (multi-input-probe chans))]
    (try
      (a/close! (chans :a))
      (success-of h :probe 1)
      (a/close! (chans :b))
      (success-of h :probe 2)
      ;; :c still open — no cascade should fire
      (let [send-outs (filterv #(and (= :send-out (:kind %))
                                     (= :probe (:step-id %)))
                               (events h))]
        (is (= 0 (count send-outs))
            "no :send-out yet — cascade waiting on :c"))
      (a/close! (chans :c))
      (success-of h :probe 3)
      (let [send-outs (filterv #(and (= :send-out (:kind %))
                                     (= :probe (:step-id %)))
                               (events h))]
        (is (= 1 (count send-outs))
            "after closing all three: exactly one cascade :send-out"))
      (finally (stop! h)))))

;; ============================================================================
;; C. ::flow/in-ports interaction
;; ============================================================================

(deftest c1-declared-port-name-matches-flow-in-ports-key-cascade-fires
  (testing "declared :in {:queue}, ::flow/in-ports {:queue test-chan} → close cascades"
    (let [test-chan (a/chan)
          hmap (step/handler-map
                {:ports   {:ins {:queue ""} :outs {:out ""}}
                 :on-init (fn [] {::caf/in-ports {:queue test-chan}})
                 :on-data (fn [ctx _s _d] {:out [(msg/pass ctx)]})})
          wf {:procs {:probe hmap :sink (absorbing-sink)}
              :conns [[[:probe :out] [:sink :in]]]
              :in :probe :out :sink}
          h  (start-with-events wf)]
      (try
        (a/close! test-chan)
        (success-of h :probe 1)
        (let [ev (events h)
              recvs   (filterv #(and (= :recv (:kind %)) (= :probe (:step-id %))) ev)
              sends   (filterv #(and (= :send-out (:kind %)) (= :probe (:step-id %))) ev)]
          (is (= 1 (count recvs)))
          (is (= :queue (:in-port (first recvs))) "in-port matches the declared name")
          (is (= 1 (count sends)) "cascade fired"))
        (finally (stop! h))))))

(deftest c2-undeclared-flow-in-ports-close-is-filtered-no-event-no-cascade
  (testing "declared :control, ::flow/in-ports {:queue extra-chan} — closing :queue is a no-op"
    (let [decl-chan  (a/chan)
          extra-chan (a/chan)
          h (start-with-events
             (mixed-probe :control decl-chan :queue extra-chan))]
      (try
        ;; Close the undeclared (extra) input.
        (a/close! extra-chan)
        ;; Give the framework a beat to process the nil read.
        (Thread/sleep 50)
        (let [ev (events h)
              probe-evs (filterv #(= :probe (:step-id %)) ev)]
          (is (empty? probe-evs)
              "filter path: no event emitted when undeclared port closes"))
        ;; Now close the declared input — cascade should fire.
        (a/close! decl-chan)
        (success-of h :probe 1)
        (let [ev (events h)
              recvs (filterv #(and (= :recv (:kind %)) (= :probe (:step-id %))) ev)
              sends (filterv #(and (= :send-out (:kind %)) (= :probe (:step-id %))) ev)]
          (is (= 1 (count recvs)))
          (is (= :control (:in-port (first recvs))))
          (is (= 1 (count sends)) "cascade fires after declared input closes"))
        (finally (stop! h))))))

(deftest c3-undeclared-close-then-declared-envelope-done-cascades
  (testing "close on undeclared port doesn't poison ::input-dones-seen"
    (let [decl-chan  (a/chan)
          extra-chan (a/chan)
          h (start-with-events
             (mixed-probe :control decl-chan :queue extra-chan))]
      (try
        (a/close! extra-chan)
        (Thread/sleep 50)
        (a/>!! decl-chan (msg/new-input-done))
        (success-of h :probe 1)
        (let [ev (events h)
              sends (filterv #(and (= :send-out (:kind %)) (= :probe (:step-id %))) ev)]
          (is (= 1 (count sends))
              "cascade fired even though extra-chan was closed first"))
        (finally (stop! h))))))

(deftest c4-data-on-undeclared-port-still-routes-to-on-data
  (testing "writing data on undeclared ::flow/in-ports port reaches :on-data"
    (let [decl-chan  (a/chan)
          extra-chan (a/chan)
          ;; Use a custom handler that records every (:in-port ctx) it sees.
          seen  (atom [])
          hmap  (step/handler-map
                 {:ports   {:ins {:control ""} :outs {:out ""}}
                  :on-init (fn []
                             {::caf/in-ports {:control decl-chan :queue extra-chan}})
                  :on-data (fn [ctx _s _d]
                             (swap! seen conj (:in-port ctx))
                             {})})
          h (start-with-events
             {:procs {:probe hmap :sink (absorbing-sink)}
              :conns [[[:probe :out] [:sink :in]]]
              :in :probe :out :sink})]
      (try
        (a/>!! extra-chan (msg/new-msg :hello))
        ;; one :on-data invocation on the probe — no :send-out (handler returns {}),
        ;; so we use a raw event poll instead of success-of (which expects :success).
        (wait-for-event h #(and (= :success (:kind %)) (= :probe (:step-id %))) 1)
        (is (= [:queue] @seen)
            "data on undeclared ::flow/in-ports port is delivered to :on-data")
        (finally (stop! h))))))

;; ============================================================================
;; D. Trace shape
;; ============================================================================

(deftest d1-recv-event-for-close-derived-done-has-correct-fields
  (let [c (a/chan)
        h (start-with-events (single-input-probe c))]
    (try
      (a/close! c)
      (success-of h :probe 1)
      (let [recv (->> (events h)
                      (filterv #(and (= :recv (:kind %)) (= :probe (:step-id %))))
                      first)]
        (is (= :recv (:kind recv)))
        (is (= :input-done (:msg-kind recv)))
        (is (= :in   (:in-port recv)))
        (is (= :probe (:step-id recv)))
        (testing ":msg-id is fresh and well-formed"
          (is (uuid? (:msg-id recv))))
        (testing ":parent-msg-ids is present and empty"
          (is (vector? (:parent-msg-ids recv)))
          (is (empty? (:parent-msg-ids recv)))))
      (finally (stop! h)))))

(deftest d1b-recv-event-shape-equivalent-to-envelope-done
  ;; Compare just the projected recv-event shape across both scenarios.
  (let [a (close-scenario [])
        b (envelope-scenario [])
        ar (-> (shapes-for (:events a) :probe) :recv first)
        br (-> (shapes-for (:events b) :probe) :recv first)]
    (is (= ar br) "recv-event projections match")))

;; ============================================================================
;; E. Counter parity
;; ============================================================================

(deftest e1-counter-final-state-matches-between-close-and-envelope
  (doseq [n [0 1 5 25]]
    (testing (str n " data envelopes")
      (let [pre (mapv #(msg/new-msg %) (range n))
            a (close-scenario    pre)
            b (envelope-scenario pre)]
        (is (= (:counters a) (:counters b))
            (str "counter divergence at n=" n))))))

;; ============================================================================
;; F. Edge cases
;; ============================================================================

(deftest f2-already-closed-chan-injected-via-flow-in-ports
  (testing "a chan that's pre-closed before the proc starts triggers cascade on first read"
    (let [c (a/chan)
          _ (a/close! c)
          h (start-with-events (single-input-probe c))]
      (try
        (success-of h :probe 1)
        (let [ev (events h)
              sends (filterv #(and (= :send-out (:kind %)) (= :probe (:step-id %))) ev)]
          (is (= 1 (count sends))))
        (finally (stop! h))))))

(deftest f3-on-all-closed-throwing-is-recorded-as-failure
  (testing "user handler exception on cascade emits :failure, not :success"
    (let [c (a/chan)
          hmap (step/handler-map
                {:ports         {:ins {:in ""} :outs {:out ""}}
                 :on-init       (fn [] {::caf/in-ports {:in c}})
                 :on-data       (fn [_ s _] [s {}])
                 :on-all-closed (fn [_ _] (throw (ex-info "boom" {})))})
          h (start-with-events
             {:procs {:probe hmap :sink (absorbing-sink)}
              :conns [[[:probe :out] [:sink :in]]]
              :in :probe :out :sink})]
      (try
        (a/close! c)
        (wait-for-event h #(and (= :failure (:kind %)) (= :probe (:step-id %))) 1)
        (let [ev (events h)
              fails (filterv #(and (= :failure (:kind %)) (= :probe (:step-id %))) ev)
              succs (filterv #(and (= :success (:kind %)) (= :probe (:step-id %))) ev)
              sends (filterv #(and (= :send-out (:kind %)) (= :probe (:step-id %))) ev)]
          (is (= 1 (count fails)))
          (is (= :input-done (:msg-kind (first fails))))
          (is (= 0 (count succs)) "no :success on cascade-throw")
          (is (= 0 (count sends)) "no auto-appended done emitted on cascade-throw"))
        (finally (stop! h))))))

;; ============================================================================
;; G. End-to-end pipelines
;; ============================================================================

(deftest g1-three-step-linear-pipeline-cascade-from-flow-in-ports
  (testing "close at the head of a 3-step pipeline cascades to the sink"
    (let [c  (a/chan)
          ;; Head: passthrough that owns its :in via ::flow/in-ports.
          head (step/handler-map
                {:ports   {:ins {:in ""} :outs {:out ""}}
                 :on-init (fn [] {::caf/in-ports {:in c}})
                 :on-data (fn [ctx _s _d] {:out [(msg/pass ctx)]})})
          ;; Mid: pure inc.
          mid  (step/step :mid inc)
          ;; Tail: sink.
          tail (step/sink :sink)
          wf   {:procs {:head head :mid mid :sink tail}
                :conns [[[:head :out] [:mid :in]]
                        [[:mid :out]  [:sink :in]]]
                :in :head :out :sink}
          h    (start-with-events wf)]
      (try
        (a/>!! c (msg/new-msg 1))
        (a/>!! c (msg/new-msg 2))
        (success-of h :sink 2)
        (a/close! c)
        (success-of h :sink 3) ; +1 for the cascade
        (let [ev (events h)
              recv-done-by-sid
              (frequencies (mapv :step-id
                                 (filterv #(and (= :recv (:kind %))
                                                (= :input-done (:msg-kind %)))
                                          ev)))]
          (is (= {:head 1 :mid 1 :sink 1} recv-done-by-sid)
              "each step received the cascading done exactly once"))
        (finally (stop! h))))))

;; ============================================================================
;; H. Existing flows are unaffected
;; ============================================================================

;; (The full datapotamus_test suite serves as the regression test for this
;; — see the test runner output. We add one focused canary here too.)

(deftest h1-non-nil-msg-path-unchanged
  (testing "the cond's :else branch reaches the same code as before for any non-nil msg"
    (let [r (flow/run!
             (step/serial (step/step :a inc) (step/sink))
             {:data 5})]
      (is (= :completed (:state r)))
      (is (nil? (:error r))))))

;; ============================================================================
;; I. Property test (test.check)
;;
;; Generates random sequences of data/signal envelopes. For each, asserts that
;; the close-derived and envelope-derived terminations produce the same probe
;; event shapes.
;; ============================================================================

(def ^:private gen-envelope
  (gen/one-of
   [(gen/fmap msg/new-msg    gen/small-integer)
    (gen/fmap (fn [k] (msg/new-signal {(str "g" k) k})) gen/small-integer)]))

(def ^:private gen-pre-envs
  (gen/vector gen-envelope 0 8))

(defspec close-equivalent-to-envelope-done 50
  (prop/for-all [pre gen-pre-envs]
    (equivalent? pre)))

;; ============================================================================
;; J. msg/drain in :on-all-closed
;;
;; Symmetric extension of msg/drain's existing role in run-data-or-signal:
;; if :on-all-closed returns msg/drain (or [s' msg/drain]), the framework
;; suppresses the auto-append of new-done envelopes onto every declared
;; output port. Combinators that gate their own close (closing an internal
;; channel only after in-flight tokens drain) use this to keep extra dones
;; out of their internal channel.
;; ============================================================================

(defn- drained-on-all-closed-probe
  [test-chan return-shape]
  (let [hmap (step/handler-map
              {:ports         {:ins {:in ""} :outs {:out ""}}
               :on-init       (fn [] {::caf/in-ports {:in test-chan}})
               :on-data       (fn [ctx _s _d] {:out [(msg/pass ctx)]})
               :on-all-closed (case return-shape
                                :bare      (fn [_ _] msg/drain)
                                :as-vector (fn [_ s] [s msg/drain]))})]
    {:procs {:probe hmap :sink (absorbing-sink)}
     :conns [[[:probe :out] [:sink :in]]]
     :in    :probe
     :out   :sink}))

(deftest j1-on-all-closed-msg-drain-suppresses-auto-append-bare
  (let [c (a/chan)
        h (start-with-events (drained-on-all-closed-probe c :bare))]
    (try
      (a/>!! c (msg/new-msg :first))
      (success-of h :probe 1)
      (a/close! c)
      (success-of h :probe 2)
      (let [evs       (filterv #(= :probe (:step-id %)) (events h))
            send-outs (filterv #(= :send-out (:kind %)) evs)
            successes (filterv #(= :success (:kind %)) evs)]
        (testing "handler ran on the cascade"
          (is (= 2 (count successes))))
        (testing "send-out for the data, BUT NO send-out for the cascade done"
          (is (= 1 (count send-outs)))
          (is (= :data (:msg-kind (first send-outs))))
          (is (zero? (count (filterv #(= :input-done (:msg-kind %)) send-outs)))
              "drain should suppress the :done auto-append")))
      (finally (stop! h)))))

(deftest j2-on-all-closed-msg-drain-as-vector-also-suppresses
  (let [c (a/chan)
        h (start-with-events (drained-on-all-closed-probe c :as-vector))]
    (try
      (a/close! c)
      (success-of h :probe 1)
      (let [evs       (filterv #(= :probe (:step-id %)) (events h))
            send-outs (filterv #(= :send-out (:kind %)) evs)]
        (testing "[s' msg/drain] return shape also suppresses auto-append"
          (is (zero? (count send-outs)))))
      (finally (stop! h)))))

(deftest j3-non-drain-return-still-auto-appends
  (testing "regression: returning anything other than msg/drain still
            produces the auto-appended done on every output port"
    (let [c    (a/chan)
          hmap (step/handler-map
                {:ports         {:ins {:in ""} :outs {:out ""}}
                 :on-init       (fn [] {::caf/in-ports {:in c}})
                 :on-data       (fn [ctx _ _] {:out [(msg/pass ctx)]})
                 :on-all-closed (fn [_ s] [s {}])})
          h    (start-with-events
                {:procs {:probe hmap :sink (absorbing-sink)}
                 :conns [[[:probe :out] [:sink :in]]]
                 :in :probe :out :sink})]
      (try
        (a/close! c)
        (success-of h :probe 1)
        (let [evs       (filterv #(= :probe (:step-id %)) (events h))
              send-outs (filterv #(= :send-out (:kind %)) evs)]
          (is (= 1 (count send-outs)) "auto-append still fires for non-drain returns")
          (is (= :input-done (:msg-kind (first send-outs)))))
        (finally (stop! h))))))

;; ============================================================================
;; K. :on-input-done per-port hook
;;
;; Fires once per declared input port when input-done first arrives on that
;; port — strictly before :on-all-closed (which only fires when ALL ports
;; have closed). Lets combinators react to upstream-exhaustion of one
;; specific port without waiting for the full all-closed cascade. This is
;; the structural fix for cyclic combinators that need to know "external
;; input is exhausted" before workers cascade input-done back.
;; ============================================================================

(deftest k1-on-input-done-fires-per-port
  (testing ":on-input-done is called once per port as input-done arrives,
            with the closing port-name as third arg"
    (let [ca   (a/chan)
          cb   (a/chan)
          seen (atom [])
          hmap (step/handler-map
                {:ports         {:ins {:a "" :b ""} :outs {:out ""}}
                 :on-init       (fn [] {::caf/in-ports {:a ca :b cb}})
                 :on-data       (fn [_ s _] [s {}])
                 :on-input-done (fn [_ s port]
                                  (swap! seen conj port)
                                  [s {}])})
          h (start-with-events
             {:procs {:probe hmap :sink (absorbing-sink)}
              :conns [[[:probe :out] [:sink :in]]]
              :in :probe :out :sink})]
      (try
        (a/close! ca)
        (success-of h :probe 1)
        (testing "after closing :a, :on-input-done fired once with :a"
          (is (= [:a] @seen)))
        (a/close! cb)
        (success-of h :probe 2)
        (testing "after closing :b, :on-input-done fired again with :b"
          (is (= [:a :b] @seen)))
        (finally (stop! h))))))

(deftest k2-on-input-done-output-merges-with-on-all-closed
  (testing "outputs from :on-input-done are merged with :on-all-closed
            outputs into the proc's port-map"
    (let [ca (a/chan)
          cb (a/chan)
          hmap (step/handler-map
                 {:ports         {:ins {:a "" :b ""} :outs {:out ""}}
                  :on-init       (fn [] {::caf/in-ports {:a ca :b cb}})
                  :on-data       (fn [_ s _] [s {}])
                  :on-input-done (fn [_ s port]
                                   [s {:out [[:per-port port]]}])
                  :on-all-closed (fn [_ s]
                                   [s {:out [:all-closed]}])})
          h (start-with-events
             {:procs {:probe hmap :sink (absorbing-sink)}
              :conns [[[:probe :out] [:sink :in]]]
              :in :probe :out :sink})]
      (try
        (a/close! ca)
        (success-of h :probe 1)
        (a/close! cb)
        (success-of h :probe 2)
        (let [send-outs (filterv #(and (= :send-out (:kind %))
                                       (= :probe (:step-id %))
                                       (= :data (:msg-kind %)))
                                 (events h))
              data-vals (set (map :data send-outs))]
          (testing "saw both per-port outputs and the all-closed output"
            (is (contains? data-vals [:per-port :a]) (str "data-vals = " data-vals))
            (is (contains? data-vals [:per-port :b]) (str "data-vals = " data-vals))
            (is (contains? data-vals :all-closed) (str "data-vals = " data-vals))))
        (finally (stop! h))))))

(deftest k3-on-input-done-default-is-noop
  (testing "handler-maps without :on-input-done still work — default no-op"
    (let [c (a/chan)
          hmap (step/handler-map
                 {:ports   {:ins {:in ""} :outs {:out ""}}
                  :on-init (fn [] {::caf/in-ports {:in c}})
                  :on-data (fn [ctx _ _] {:out [(msg/pass ctx)]})})
          h (start-with-events
             {:procs {:probe hmap :sink (absorbing-sink)}
              :conns [[[:probe :out] [:sink :in]]]
              :in :probe :out :sink})]
      (try
        (a/>!! c (msg/new-msg :hello))
        (success-of h :probe 1)
        (a/close! c)
        (success-of h :probe 2)
        (let [recvs (filterv #(and (= :recv (:kind %)) (= :probe (:step-id %)))
                             (events h))]
          (is (= 2 (count recvs))))
        (finally (stop! h))))))
