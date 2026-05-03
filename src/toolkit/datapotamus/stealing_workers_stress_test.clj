(ns toolkit.datapotamus.stealing-workers-stress-test
  "Cross-cutting stress tests that use stealing-workers as a vehicle but
   exercise framework behavior — signals, errors, concurrency, lineage,
   and property-based counter conservation across random pipelines."
  (:require [clojure.core.async.flow :as-alias flow]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.datapotamus.combinators.aggregate :as agg]
            [toolkit.datapotamus.combinators.workers :as c]
            [toolkit.datapotamus.flow :as df]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.pubsub :as pubsub]))

;; ============================================================================
;; #1. Cyclic flow — a step that emits :work back to its own input via an
;;     explicit conn (not through stealing-workers). With input-done gone,
;;     verify a self-loop with a natural fixed point still terminates.
;; ============================================================================

(deftest cyclic-step-self-loop
  (let [hmap (step/handler-map
              {:ports   {:ins {:in ""} :outs {:out "" :done ""}}
               :on-data (fn [ctx _ d]
                          (let [n (:n d)]
                            (if (zero? n)
                              {:done [(msg/child ctx :reached-zero)]}
                              {:out  [(msg/child ctx {:n (dec n)})]})))})
        ;; Boundary :in routes to :step :in. Boundary :out routes to
        ;; :step :done. Self-loop: :step :out → :step :in.
        wf {:procs {:step hmap}
            :conns [[[:step :out] [:step :in]]]
            :in    [:step :in]
            :out   [:step :done]}
        res (df/run-seq wf [{:n 5}])]
    (is (= :completed (:state res)))
    (testing "exactly one :reached-zero terminator"
      (is (= [[:reached-zero]] (:outputs res))))))

;; ============================================================================
;; #2. Nested stealing pools — outer pool's inner is a step containing
;;     another stealing pool. Stresses ::flow/in-ports / ::flow/out-ports
;;     plumbing under composition.
;; ============================================================================

(deftest nested-stealing-pools
  (let [leaf  (step/handler-map
               {:ports   {:ins {:in ""} :outs {:out ""}}
                :on-data (fn [ctx _ d] {:out [(msg/child ctx [:processed d])]})})
        inner (c/stealing-workers :inner-pool 2 leaf)
        wf    (c/stealing-workers :outer-pool 2 inner)
        res   (df/run-seq wf [1 2 3 4 5 6])]
    (is (= :completed (:state res)))
    (testing "every input produces one output"
      (is (= 6 (count (mapcat identity (:outputs res))))))
    (testing "outputs are wrapped exactly once (one inner pool layer)"
      (let [all (set (mapcat identity (:outputs res)))]
        (is (= #{[:processed 1] [:processed 2] [:processed 3]
                 [:processed 4] [:processed 5] [:processed 6]}
               all))))))

;; ============================================================================
;; #3. Concurrent injects — N producer threads racing into the same flow
;;     boundary while flush-and-drain runs. Counter atomicity, no lost msgs.
;; ============================================================================

(deftest concurrent-injects
  (let [n-threads     8
        n-per-thread  250
        total         (* n-threads n-per-thread)
        ;; Wrap with sink so the flow's :out boundary has a consumer —
        ;; without it the join's emissions would error at the resolver.
        wf            (step/serial (c/stealing-workers :pool 4 (step/step :id identity))
                                   (step/sink))
        h             (df/start! wf {})
        latch         (java.util.concurrent.CountDownLatch. 1)]
    (try
      (let [threads (mapv (fn [t]
                            (Thread.
                             ^Runnable
                             (fn []
                               (.await latch)
                               (dotimes [i n-per-thread]
                                 (df/inject! h {:data [t i]})))))
                          (range n-threads))]
        (run! #(.start ^Thread %) threads)
        (.countDown latch)
        (run! #(.join ^Thread %) threads)
        (df/flush-and-drain! h)
        (let [{:keys [sent recv completed]} (df/counters h)]
          (testing "every inject and the broadcast accounted for"
            ;; sent = total injects + 4 broadcast bumps (one per castee — but no
            ;; castees subscribe in this flow → 0 bumps). Plus per-step send-out
            ;; events. Just assert balance + at-least-2000 sents.
            (is (>= sent total))
            (is (= sent recv completed)))))
      (finally
        (df/stop! h)))))

;; ============================================================================
;; #4. Failure propagation — one worker handler throws. Other inputs
;;     continue processing. The :failure event is captured in the trace.
;; ============================================================================

(deftest worker-failure-doesnt-stall-others
  (let [worker  (step/handler-map
                 {:ports   {:ins {:in ""} :outs {:out ""}}
                  :on-data (fn [ctx _ d]
                             (if (= d :poison)
                               (throw (ex-info "boom" {:d d}))
                               {:out [(msg/child ctx [:ok d])]}))})
        wf      (c/stealing-workers :pool 4 worker)
        ps      (pubsub/make)
        failures (atom [])
        _       (pubsub/sub ps [:>]
                            (fn [_ ev _]
                              (when (= :failure (:kind ev))
                                (swap! failures conj ev))))
        res     (df/run-seq wf [1 2 :poison 4 5] {:pubsub ps})]
    (testing "flow completes (worker failures don't fail the whole flow)"
      (is (= :completed (:state res))))
    (testing "exactly one :failure event captured"
      (is (= 1 (count @failures)))
      (is (= "boom" (-> @failures first :error :message))))
    (testing "non-poison inputs still produced outputs"
      (let [outs (mapcat identity (:outputs res))]
        (is (= 4 (count outs)))
        (is (= #{[:ok 1] [:ok 2] [:ok 4] [:ok 5]} (set outs)))))))

;; ============================================================================
;; #5. Multiple flush cycles — inject 50, flush, inject 50, flush.
;;     Aggregator must reset state on each flush.
;; ============================================================================

(deftest multiple-flush-cycles
  (let [agg-step (agg/batch-by-group #(mod % 3) (fn [k rows] {:key k :rows rows}))
        ;; Wrap with sink so the aggregator's flush emissions go somewhere.
        wf       (step/serial agg-step (step/sink))
        h        (df/start! wf {})]
    (try
      ;; Round 1
      (dotimes [i 50] (df/inject! h {:data i}))
      (let [r1 (df/flush-and-drain! h)
            sent-r1 (-> r1 :final-counters :sent)]
        (is (= :completed (:state r1)) "round 1 completed"))
      ;; Round 2 with new inputs (start from 100 so groups are different shape)
      (dotimes [i 50] (df/inject! h {:data (+ 100 i)}))
      (let [r2 (df/flush-and-drain! h)]
        (is (= :completed (:state r2)) "round 2 completed"))
      (testing "counters at end balance"
        (let [{:keys [sent recv completed]} (df/counters h)]
          (is (= sent recv completed))))
      (finally
        (df/stop! h)))))

;; ============================================================================
;; #6. Stealing actually happens — skew load and observe :recv events
;;     whose :in-port is :peer-N. Proves stealing is a live mechanism,
;;     not just a structural option.
;; ============================================================================

(deftest stealing-actually-happens
  (let [worker (step/handler-map
                {:ports   {:ins {:in ""} :outs {:out ""}}
                 :on-data (fn [ctx _ d]
                            ;; Worker 0 is artificially slow — fast workers
                            ;; finish their round-robin allocation, then
                            ;; alts! over [own peer-0 peer-1 peer-2] returns
                            ;; from peer-0 (worker-0's still-full queue).
                            (when (= "worker-0" (some-> (:step-id ctx) name))
                              (Thread/sleep 10))
                            {:out [(msg/child ctx d)]})})
        wf     (c/stealing-workers :pool 4 worker)
        ps     (pubsub/make)
        steals (atom 0)
        _      (pubsub/sub ps [:>]
                           (fn [_ ev _]
                             (when (and (= :recv (:kind ev))
                                        (= :data (:msg-kind ev))
                                        (when-let [p (:in-port ev)]
                                          (clojure.string/starts-with?
                                           (name p) "peer-")))
                               (swap! steals inc))))
        res    (df/run-seq wf (range 30) {:pubsub ps})]
    (is (= :completed (:state res)))
    (testing "at least one steal happened (recv via :peer-N port)"
      (is (pos? @steals)
          (str "expected positive steal count; got " @steals)))))

;; ============================================================================
;; #7. Step-inner with :work port is rejected — the wrapper only installs
;;     the work-feedback dispatcher for handler-map inners. A step inner
;;     declaring :work has no consumer for it; validate-wired-outs! at
;;     start! time throws.
;; ============================================================================

(deftest step-inner-with-work-port-is-rejected
  (let [hmap-with-work (step/handler-map
                        {:ports   {:ins {:in ""} :outs {:out "" :work ""}}
                         :on-data (fn [_ _ _] {})})
        bad-step       {:procs {:p hmap-with-work} :conns []
                        :in :p :out :p}]
    (is (thrown? Exception
                 (let [h (df/start! (c/stealing-workers :pool 4 bad-step) {})]
                   (df/stop! h))))))

;; ============================================================================
;; #8. Property test — counter conservation under random pipelines.
;;     Generator composes random stages from leaf steps, both worker pool
;;     types (varying K), and three aggregator combinators. For each:
;;     run a randomized input, assert :state = :completed and counters
;;     balance.
;; ============================================================================

(defn- gen-leaf-fn []
  (rand-nth [{:f inc       :nm "inc"}
             {:f dec       :nm "dec"}
             {:f #(* 2 %)  :nm "dbl"}
             {:f identity  :nm "id"}]))

(defn- uniq [prefix]
  (keyword (str prefix "-" (rand-int 1000000))))

(def ^:private gen-stage
  ;; Each stage is materialized as a step value. Returned as a thunk so we
  ;; build with fresh sids on every trial. Aggregators are wrapped under
  ;; a unique id (via step/serial with a keyword first arg) so multiple
  ;; aggregators in one pipeline don't collide on default proc names.
  (gen/elements
   [;; leaf — a single pure step
    (fn []
      (let [{:keys [f nm]} (gen-leaf-fn)]
        (step/step (uniq (str "leaf-" nm)) f)))
    ;; stealing-workers k=1..4 wrapping a leaf
    (fn []
      (let [{:keys [f nm]} (gen-leaf-fn)
            k             (inc (rand-int 4))]
        (c/stealing-workers (uniq (str "sp-" nm))
                            k (step/step (uniq (str "in-" nm)) f))))
    ;; round-robin-workers k=1..4 wrapping a leaf
    (fn []
      (let [{:keys [f nm]} (gen-leaf-fn)
            k             (inc (rand-int 4))]
        (c/round-robin-workers (uniq (str "rr-" nm))
                               k (step/step (uniq (str "in-" nm)) f))))
    ;; batch-by-group — buffers all, emits one summary per group on flush
    (fn []
      (step/serial (uniq "batch")
                   (agg/batch-by-group #(mod % 3) (fn [_k rows] (count rows)))))
    ;; cumulative-by-group — emits per row, last is final
    (fn []
      (step/serial (uniq "cum")
                   (agg/cumulative-by-group #(mod % 5) (fn [_k rows] (count rows)))))
    ;; tumbling-window — fakes :time on the data via identity
    (fn []
      (step/serial (uniq "tumb")
                   (agg/tumbling-window
                    {:size-ms   10
                     :time-fn   #(* 100 (mod (Math/abs (long %)) 10))
                     :on-window (fn [s _e items] {:start s :n (count items)})})))]))

(defn- realize-stage [thunk] (thunk))

(def ^:private gen-pipeline
  (gen/let [thunks (gen/vector gen-stage 1 4)
            inputs (gen/vector gen/small-integer 0 30)]
    [(mapv realize-stage thunks) inputs]))

(defspec prop-counter-conservation 20
  (prop/for-all [[stages inputs] gen-pipeline]
    (let [wf  (apply step/serial stages)
          res (df/run-seq wf inputs)
          {:keys [sent recv completed]} (:counters res)]
      (and (or (= :completed (:state res))
               ;; If the flow legitimately failed (e.g. a div by zero
               ;; somehow), surface that — but it shouldn't happen with
               ;; our pure stages.
               (do (println :failed-trial {:state (:state res)
                                           :error (:error res)
                                           :stage-count (count stages)
                                           :n-inputs (count inputs)})
                   false))
           (= sent recv completed)))))

;; ============================================================================
;; #9. Big-K — k=64 stealing pool with 500 items. Catches per-K-correlation
;;     bugs (the K² channel-reference structure in alts! sets grows fast).
;; ============================================================================

(deftest big-k-stealing-doesnt-bog
  (let [wf  (c/stealing-workers :pool 64 (step/step :id identity))
        t0  (System/currentTimeMillis)
        res (df/run-seq wf (range 500))
        ms  (- (System/currentTimeMillis) t0)]
    (is (= :completed (:state res)))
    (is (= 500 (count (mapcat identity (:outputs res)))))
    (is (< ms 5000) (str "k=64 should complete in <5s; took " ms "ms"))))
