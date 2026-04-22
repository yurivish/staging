(ns toolkit.datapotamus2.core
  "Datapotamus2 core: envelope helpers, the `instrument` transformation,
   and `run!` runner.

   Step-fns are **factories** that take a ctx and return the flow step-fn:

     (defn my-step [ctx]
       (fn ([] {:params {} :ins {:in \"\"} :outs {:out \"\"}})
           ([_] {}) ([s _] s)
           ([s _ m] ...)))

   `instrument` materializes each proc by calling `(p ctx)` before handing
   the resulting step-fn to `clojure.core.async.flow`. The ctx carries only
   what can't be obtained lexically:

     :pubsub   — the shared event bus
     :scope    — tagged path of containers, e.g. [[:flow fid] [:step sid]]
     :step-id  — this proc's step-id
     :emit     — raw publisher, (fn [scope ev])
     :trace    — span helper, (fn [ctx span-name metadata body-fn])

   A nested flow spec (a `:procs` value that is itself a map with
   `:procs`/`:conns`) is recursed into — `instrument` handles arbitrary
   depth automatically.

   Subjects are kind-first and built from the tagged scope:

     <kind>.flow.<fid1>.step.<sid>                   top-level step
     <kind>.flow.<fid1>.flow.<sub>.step.<sid>        nested flow
     <kind>.flow.<fid1>.step.<sid>.span.<name>       span inside a step
     <kind>.flow.<fid1>.run                          run-level

   `wrap-proc` synthesizes :recv / :send-out / :merge / :success / :failure
   events and publishes each one synchronously. `trace` synthesizes
   :span-start / :span-success / :span-failure events around user code
   inside a step; these are inert to the completion counter."
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

;; --- scope + subject --------------------------------------------------------

(defn- scope->segments [scope]
  (mapcat (fn [[k id]]
            [(name k) (if (keyword? id) (name id) (str id))])
          scope))

(defn- subject-for [scope kind]
  (->> (cons (name kind) (scope->segments scope))
       (str/join ".")))

(defn- run-subject-for [scope kind]
  (->> (cons (name kind) (concat (scope->segments scope) ["run"]))
       (str/join ".")))

(defn- flow-path-of
  "Extract the list of flow-ids from a scope (preserves outer-to-inner order).
   Provided on every event body for consumers that just want flow ancestry."
  [scope]
  (mapv (fn [[_ id]] (if (keyword? id) (name id) id))
        (filter (fn [[k _]] (= k :flow)) scope)))

;; --- emit -------------------------------------------------------------------

(defn- make-emit [pubsub]
  (fn [scope ev]
    (pubsub/pub pubsub
                (subject-for scope (:kind ev))
                (assoc ev :scope scope :flow-path (flow-path-of scope)))))

;; --- trace (spans) ----------------------------------------------------------

(defn trace-span
  "Run body-fn within a span scope. Emits :span-start, then either
   :span-success or :span-failure, and rethrows on failure.

   body-fn receives an inner ctx whose :scope is extended with
   [:span span-name]; nested `trace-span` calls made from inside body-fn
   therefore nest correctly.

   Span events are observability only — they fall through the completion
   counter's `case` and do not affect run completion."
  [{:keys [emit scope step-id] :as ctx} span-name metadata body-fn]
  (let [inner-scope (conj (vec scope) [:span span-name])
        inner-ctx   (assoc ctx :scope inner-scope)]
    (emit inner-scope
          {:kind :span-start :step-id step-id :span-name span-name
           :metadata metadata :at (now)})
    (try
      (let [result (body-fn inner-ctx)]
        (emit inner-scope
              {:kind :span-success :step-id step-id :span-name span-name
               :result result :at (now)})
        result)
      (catch Throwable ex
        (emit inner-scope
              {:kind :span-failure :step-id step-id :span-name span-name
               :error {:message (ex-message ex) :data (ex-data ex)}
               :at (now)})
        (throw ex)))))

;; --- wrap-proc --------------------------------------------------------------

(defn- wrap-proc [step-id step-scope emit user-step-fn]
  (fn
    ([] (user-step-fn))
    ([arg] (user-step-fn arg))
    ([s arg] (user-step-fn s arg))
    ([s in-id m]
     (emit step-scope (recv-event step-id m))
     (try
       (let [[s' raw]      (user-step-fn s in-id m)
             resolved      (resolve-output-nils (or raw {}) (:msg-id m))
             middle-events (build-middle-events step-id resolved)
             port-map      (strip-dp2-keys resolved)]
         (doseq [ev middle-events] (emit step-scope ev))
         (emit step-scope (success-event step-id m))
         [s' port-map])
       (catch Throwable ex
         (emit step-scope (failure-event step-id m ex))
         [s {}])))))

;; --- counter logic ----------------------------------------------------------

(defn- update-counters [counters ev]
  (case (:kind ev)
    :recv     (update counters :recv inc)
    :success  (update counters :completed inc)
    :failure  (update counters :completed inc)
    :send-out (if (:port ev)
                (update counters :sent inc)
                counters)
    counters))

(defn- done? [{:keys [sent recv completed]}]
  (and (pos? sent) (= sent recv) (= recv completed)))

;; --- instrument -------------------------------------------------------------

(declare instrument-with-ctx run-inner)

(defn- nested-spec? [v]
  (and (map? v) (contains? v :procs) (contains? v :conns)))

(defn- arities
  "Set of arities a fn responds to (via any invoke-shaped Java method).
   Pattern borrowed from Jepsen (EPL), which handles invoke/invokeStatic/
   doInvoke uniformly across AOT and primitive-typed compilations."
  [f]
  (into #{}
        (keep (fn [^java.lang.reflect.Method m]
                (when (re-find #"invoke" (.getName m))
                  (alength (.getParameterTypes m)))))
        (-> f class .getDeclaredMethods)))

(defn- factory?
  "A factory responds to exactly one arity (ctx)."
  [p]
  (= #{1} (arities p)))

(defn- instrument-proc [sid p outer-ctx emit]
  (let [step-scope (conj (vec (:scope outer-ctx)) [:step sid])]
    (cond
      (nested-spec? p)
      (let [inner-scope (conj (vec (:scope outer-ctx)) [:flow (name sid)])
            inner-ctx   (assoc outer-ctx :scope inner-scope)
            inner-inst  (instrument-with-ctx p inner-ctx)
            step-fn     (fn ([] {:params {} :ins {:in ""} :outs {:out ""}})
                            ([_] {}) ([s _] s)
                            ([s _ m]
                             (let [outs (run-inner inner-inst inner-ctx (:data m))]
                               [s {:out (mapv #(child-with-data m %) outs)}])))]
        (wrap-proc sid step-scope emit step-fn))

      (factory? p)
      (let [proc-ctx {:pubsub  (:pubsub outer-ctx)
                      :scope   step-scope
                      :step-id sid
                      :emit    emit
                      :trace   trace-span}
            step-fn  (p proc-ctx)]
        (wrap-proc sid step-scope emit step-fn))

      :else
      (wrap-proc sid step-scope emit p))))

(defn- instrument-with-ctx [spec outer-ctx]
  (let [emit   (make-emit (:pubsub outer-ctx))
        procs' (into {}
                 (for [[sid p] (:procs spec)]
                   [sid (instrument-proc sid p outer-ctx emit)]))]
    (assoc spec :procs procs' ::ctx outer-ctx)))

(defn instrument
  "Wrap every proc's factory and materialize its step-fn for synchronous
   trace emission. Nested flow specs are expanded recursively.

   Options:
     :pubsub    existing pubsub instance (default: fresh via pubsub/make)
     :flow-id   this flow's id, a string (default: fresh uuid)"
  ([spec] (instrument spec {}))
  ([spec opts]
   (let [fid (or (:flow-id opts) (str (random-uuid)))
         ctx {:pubsub (or (:pubsub opts) (pubsub/make))
              :scope  [[:flow fid]]}]
     (instrument-with-ctx spec ctx))))

;; --- run! -------------------------------------------------------------------

(defn- build-graph [spec]
  (flow/create-flow
   {:procs (into {} (map (fn [[sid pfn]] [sid {:proc (flow/process pfn)}])) (:procs spec))
    :conns (:conns spec)}))

(defn- scope->glob [scope]
  (str "*." (str/join "." (scope->segments scope)) ".>"))

(defn- run-inner
  "Drive an already-instrumented nested spec to completion with a fresh
   seed, return a vector of data values that the inner's :output step
   received (via :recv events). The inner spec's :output should be a
   terminal sink that absorbs the final msgs."
  [inner-spec inner-ctx seed-data]
  (let [{:keys [pubsub scope]} inner-ctx
        entry      (:entry inner-spec)
        output     (:output inner-spec)
        counters   (atom {:sent 1 :recv 0 :completed 0})
        done-p     (promise)
        outputs    (atom [])
        inner-path (flow-path-of scope)
        main-sub   (pubsub/sub pubsub (scope->glob scope)
                               (fn [_ ev _]
                                 (when (and (= :recv (:kind ev))
                                            (= output (:step-id ev))
                                            (= inner-path (:flow-path ev)))
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
  (let [{:keys [pubsub scope]} (::ctx spec)
        entry       (or entry (:entry spec))
        fid         (first (flow-path-of scope))
        events      (atom [])
        counters    (atom {:sent 1 :recv 0 :completed 0})
        done-p      (promise)
        main-sub    (pubsub/sub pubsub (scope->glob scope)
                                (fn [_ ev _]
                                  (swap! events conj ev)
                                  (let [c' (swap! counters update-counters ev)]
                                    (when (done? c')
                                      (deliver done-p :completed)))))
        user-unsubs (mapv (fn [[pat h]] (pubsub/sub pubsub pat h)) subscribers)
        g (build-graph spec)
        {:keys [error-chan]} (flow/start g)
        seed (new-msg data)]
    (pubsub/pub pubsub (run-subject-for scope :run-started)
                {:kind :run-started :flow-path [fid] :scope scope :at (now)})
    (pubsub/pub pubsub (run-subject-for scope :seed)
                {:kind :seed :flow-path [fid] :scope scope
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
