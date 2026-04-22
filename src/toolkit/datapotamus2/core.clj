(ns toolkit.datapotamus2.core
  "Datapotamus2 core: envelope helpers, the `instrument` transformation,
   and `run!` runner.

   `instrument : FlowSpec → FlowSpec` walks every proc and wraps step-fns
   with synchronous trace emission into a `toolkit.pubsub`. A nested
   flow spec (a `:procs` value that is itself a map with `:procs`/`:conns`)
   is recursed into — `instrument` handles arbitrary depth automatically.

   Step-fns never touch `::flow/report`. They return a plain flow output
   map:

     [new-state {out-port [children]
                 ::dp2/derivations [intermediate-msgs]
                 ::dp2/merges      [{:msg-id ... :parents [...]}]}]

   The wrapper synthesizes :recv / :send-out / :merge / :success /
   :failure events and publishes each one synchronously to the pubsub
   before returning to flow. Synchronous publish gives lossless +
   causal-ordered tracing with no buffering or quiesce windows.

   Subjects are kind-first and flat-symmetric across nesting:

     <kind>.flow.<fid1>.step.<stepid>                                 top-level step
     <kind>.flow.<fid1>.flow.<fid2>.step.<stepid>                     one nested
     <kind>.flow.<fid1>.run                                           run-level"
  (:refer-clojure :exclude [run!])
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow]
            [clojure.string :as str]
            [toolkit.pubsub :as pubsub]))

(set! *warn-on-reflection* true)

;; --- envelope ---------------------------------------------------------------

(defn new-msg
  "Root msg: no parent. Starts with empty tokens."
  [data]
  {:msg-id (random-uuid) :data-id (random-uuid) :data data
   :tokens {} :parent-msg-ids []})

(defn child-with-data
  "Child msg with new ids, given data, parent tokens inherited.
   1-arity: parent left nil — the wrapper resolves it to the current
   input msg's id at step-call boundary."
  ([data] (child-with-data nil data))
  ([parent data]
   {:msg-id (random-uuid) :data-id (random-uuid) :data data
    :tokens (:tokens parent {})
    :parent-msg-ids (when parent [(:msg-id parent)])}))

(defn child-with-parents
  "Child msg with explicit multi-parent lineage (for merge outputs)."
  [parent-msg-ids data]
  {:msg-id (random-uuid) :data-id (random-uuid) :data data
   :tokens {} :parent-msg-ids (vec parent-msg-ids)})

(defn child-same-data
  "Child msg with new ids, same data as parent, tokens inherited."
  ([] (child-same-data nil))
  ([parent]
   {:msg-id (random-uuid) :data-id (random-uuid) :data (:data parent)
    :tokens (:tokens parent {})
    :parent-msg-ids (when parent [(:msg-id parent)])}))

;; --- event constructors (internal) ------------------------------------------

(defn- now [] (System/currentTimeMillis))

(defn- recv-event [step-id m]
  {:kind :recv :step-id step-id :msg-id (:msg-id m) :data-id (:data-id m)
   :data (:data m) :at (now)})

(defn- success-event [step-id m]
  {:kind :success :step-id step-id :msg-id (:msg-id m) :at (now)})

(defn- failure-event [step-id m ^Throwable ex]
  {:kind :failure :step-id step-id :msg-id (:msg-id m)
   :error {:message (ex-message ex) :data (ex-data ex)} :at (now)})

(defn- send-out-event [step-id port child]
  {:kind :send-out :step-id step-id :port port
   :msg-id (:msg-id child) :data-id (:data-id child)
   :parent-msg-ids (vec (:parent-msg-ids child))
   :tokens (:tokens child) :data (:data child) :at (now)})

(defn- merge-event [step-id msg-id parents]
  {:kind :merge :step-id step-id :msg-id msg-id
   :parent-msg-ids (vec parents) :at (now)})

;; --- output-map helpers -----------------------------------------------------

(defn- resolve-msg-nils
  "Replace a nil in :parent-msg-ids with [input-msg-id]."
  [m input-id]
  (cond-> m
    (or (nil? (:parent-msg-ids m))
        (some nil? (:parent-msg-ids m)))
    (update :parent-msg-ids
            (fn [ps]
              (if (or (nil? ps) (empty? ps))
                [input-id]
                (mapv #(or % input-id) ps))))))

(defn- port-output-keys
  "Port keys in the output map — anything that's a simple (unqualified) keyword."
  [out]
  (filter (fn [k] (and (keyword? k) (not (namespace k)))) (keys out)))

(defn- dp2-key? [k]
  (and (keyword? k) (= "toolkit.datapotamus2.core" (namespace k))))

(defn- resolve-output-nils [out input-id]
  (let [resolved-ports
        (reduce (fn [m k]
                  (assoc m k (mapv #(resolve-msg-nils % input-id) (get m k))))
                out (port-output-keys out))
        resolved-derivs
        (update resolved-ports ::derivations
                (fn [ds] (when ds (mapv #(resolve-msg-nils % input-id) ds))))
        resolved-merges
        (update resolved-derivs ::merges
                (fn [ms]
                  (when ms
                    (mapv (fn [{:keys [msg-id parents]}]
                            {:msg-id msg-id
                             :parents (if (empty? parents)
                                        [input-id]
                                        (mapv #(or % input-id) parents))})
                          ms))))]
    resolved-merges))

(defn- build-middle-events
  "Events between :recv and :success: merges, nil-port derivations, and
   port send-outs. :recv and :success are emitted by wrap-proc directly
   so that :recv can be published unconditionally before user-fn runs."
  [step-id output]
  (let [ports     (port-output-keys output)
        port-ids  (into #{} (mapcat (fn [p] (map :msg-id (get output p)))) ports)
        derivs    (->> (get output ::derivations)
                       ;; dedup: a msg that appears in a port output is not
                       ;; also emitted as a nil-port derivation.
                       (remove #(port-ids (:msg-id %))))
        merges    (get output ::merges)
        ev-merges (mapv (fn [{:keys [msg-id parents]}]
                          (merge-event step-id msg-id parents))
                        merges)
        ev-derivs (mapv (fn [c] (send-out-event step-id nil c)) derivs)
        ev-sends  (into []
                        (mapcat
                         (fn [p]
                           (map (fn [c] (send-out-event step-id p c)) (get output p))))
                        ports)]
    (into [] cat [ev-merges ev-derivs ev-sends])))

(defn- strip-dp2-keys [out]
  (reduce-kv (fn [m k v] (if (dp2-key? k) m (assoc m k v))) {} out))

;; --- subject + emit ---------------------------------------------------------

(defn- subject-for [{:keys [flow-path]} step-id kind]
  (->> (concat [(name kind)]
               (mapcat (fn [fid] ["flow" fid]) flow-path)
               (if step-id ["step" (name step-id)] ["run"]))
       (str/join ".")))

(defn- make-emit [{:keys [pubsub flow-path] :as ctx}]
  (fn [ev]
    (let [ev' (assoc ev :flow-path flow-path)]
      (pubsub/pub pubsub (subject-for ctx (:step-id ev) (:kind ev)) ev'))))

;; --- wrap-proc --------------------------------------------------------------

(defn- wrap-proc [step-id user-step-fn emit]
  (fn
    ([] (user-step-fn))
    ([arg] (user-step-fn arg))
    ([s arg] (user-step-fn s arg))
    ([s in-id m]
     ;; :recv is emitted unconditionally before user-fn runs so that a
     ;; throw balances against :failure in the completion counter.
     (emit (recv-event step-id m))
     (try
       (let [[s' raw]      (user-step-fn s in-id m)
             resolved      (resolve-output-nils (or raw {}) (:msg-id m))
             middle-events (build-middle-events step-id resolved)
             port-map      (strip-dp2-keys resolved)]
         (doseq [ev middle-events] (emit ev))
         (emit (success-event step-id m))
         [s' port-map])
       (catch Throwable ex
         (emit (failure-event step-id m ex))
         [s {}])))))

;; --- counter logic ----------------------------------------------------------

(defn- update-counters [counters ev]
  (case (:kind ev)
    :recv     (update counters :recv inc)
    :success  (update counters :completed inc)
    :failure  (update counters :completed inc)
    :send-out (if (:port ev)
                (update counters :sent inc)
                counters)                ; :port nil = internal derivation
    counters))

(defn- done? [{:keys [sent recv completed]}]
  (and (pos? sent) (= sent recv) (= recv completed)))

;; --- instrument -------------------------------------------------------------

(declare instrument-with-ctx run-inner)

(defn- nested-spec? [v]
  (and (map? v) (contains? v :procs) (contains? v :conns)))

(defn- expand-nested [step-id nested-spec outer-ctx]
  (let [inner-ctx  (update outer-ctx :flow-path conj (name step-id))
        inner-inst (instrument-with-ctx nested-spec inner-ctx)
        outer-emit (make-emit outer-ctx)]
    (wrap-proc step-id
               (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                   ([_] {}) ([s _] s)
                   ([s _ m]
                    (let [outs (run-inner inner-inst inner-ctx (:data m))]
                      [s {:out (mapv #(child-with-data m %) outs)}])))
               outer-emit)))

(defn- instrument-with-ctx [spec ctx]
  (let [emit   (make-emit ctx)
        procs' (into {}
                 (for [[sid p] (:procs spec)]
                   [sid (if (nested-spec? p)
                          (expand-nested sid p ctx)
                          (wrap-proc sid p emit))]))]
    (assoc spec :procs procs' ::ctx ctx)))

(defn instrument
  "Wrap every proc's step-fn for synchronous trace emission. Nested flow
   specs (maps in :procs with :procs/:conns) are expanded recursively.

   Options:
     :pubsub    existing pubsub instance (default: fresh via pubsub/make)
     :flow-id   this flow's id, a string (default: fresh uuid)"
  ([spec] (instrument spec {}))
  ([spec opts]
   (let [fid (or (:flow-id opts) (str (random-uuid)))
         ctx {:pubsub (or (:pubsub opts) (pubsub/make))
              :flow-path [fid]}]
     (instrument-with-ctx spec ctx))))

;; --- run! -------------------------------------------------------------------

(defn- build-graph [spec]
  (flow/create-flow
   {:procs (into {} (map (fn [[sid pfn]] [sid {:proc (flow/process pfn)}])) (:procs spec))
    :conns (:conns spec)}))

(defn- run-inner
  "Drive an already-instrumented nested spec to completion with a fresh
   seed, return a vector of data values that the inner's :output step
   received (via :recv events). The inner spec's :output should be a
   terminal sink that absorbs the final msgs."
  [inner-spec inner-ctx seed-data]
  (let [{:keys [pubsub flow-path]} inner-ctx
        entry    (:entry inner-spec)
        output   (:output inner-spec)
        counters (atom {:sent 1 :recv 0 :completed 0})
        done-p   (promise)
        outputs  (atom [])
        scope    (str "*." (str/join "." (mapcat (fn [fid] ["flow" fid]) flow-path)) ".>")
        main-sub (pubsub/sub pubsub scope
                             (fn [_ ev _]
                               (when (and (= :recv (:kind ev))
                                          (= output (:step-id ev))
                                          (= flow-path (:flow-path ev)))
                                 (swap! outputs conj (:data ev)))
                               (let [c' (swap! counters update-counters ev)]
                                 (when (done? c')
                                   (deliver done-p :completed)))))
        g (build-graph inner-spec)
        {:keys [error-chan]} (flow/start g)
        seed (new-msg seed-data)]
    (flow/resume g)
    @(flow/inject g [entry :in] [seed])
    (a/thread (when-let [ex (a/<!! error-chan)]
                (deliver done-p [:failed ex])))
    (let [signal @done-p]
      (flow/stop g)
      (main-sub)
      (when (not= :completed signal)
        (throw (or (second signal)
                   (ex-info "inner flow failed" {}))))
      @outputs)))

(defn run!
  "Run an instrumented spec to completion. Seeds a single msg into
   [entry :in] where entry comes from opts or the spec. Returns:

     {:state :completed | :failed
      :events [event-map ...]
      :counters {:sent :recv :completed}
      :error  Throwable or nil}

   Completion is purely counter-driven: when sent == recv == completed,
   the run is done. No timeouts. Because pubsub delivery is synchronous
   on the wrap-proc's thread, every step's events are fully processed
   by subscribers before flow routes its output to the next step — which
   means counter balance genuinely means quiescence, not a timing artifact.

   Opts:
     :entry        entry step-id (required if not in spec's :entry)
     :data         seed data
     :subscribers  {pattern handler-fn} — extra pubsub subscriptions set up
                   for the duration of the run"
  [spec {:keys [entry port data subscribers]
         :or {port :in subscribers {}}}]
  (let [{:keys [pubsub flow-path]} (::ctx spec)
        entry (or entry (:entry spec))
        fid   (first flow-path)
        events   (atom [])
        counters (atom {:sent 1 :recv 0 :completed 0})
        done-p   (promise)
        scope    (str "*.flow." fid ".>")
        main-sub (pubsub/sub pubsub scope
                             (fn [_ ev _]
                               (swap! events conj ev)
                               (let [c' (swap! counters update-counters ev)]
                                 (when (done? c')
                                   (deliver done-p :completed)))))
        user-unsubs (mapv (fn [[pat h]] (pubsub/sub pubsub pat h)) subscribers)
        g (build-graph spec)
        {:keys [error-chan]} (flow/start g)
        seed (new-msg data)]
    (pubsub/pub pubsub (str "run-started.flow." fid ".run")
                {:kind :run-started :flow-path [fid] :at (now)})
    (pubsub/pub pubsub (str "seed.flow." fid ".run")
                {:kind :seed :flow-path [fid]
                 :msg-id (:msg-id seed) :data-id (:data-id seed)
                 :entry entry :port port :at (now)})
    (flow/resume g)
    @(flow/inject g [entry port] [seed])
    (a/thread (when-let [ex (a/<!! error-chan)]
                (deliver done-p [:failed ex])))
    (let [signal @done-p]
      (flow/stop g)
      (main-sub)
      (doseq [u user-unsubs] (u))
      (if (= :completed signal)
        {:state :completed :events @events :counters @counters :error nil}
        {:state :failed :events @events :counters @counters :error (second signal)}))))
