(ns toolkit.datapotamus2.dsl
  "Ergonomic composition of dp2 flows. A flow is any map of the shape
   {:procs :conns :in :out}. This namespace provides thin builders
   that produce those maps via composition.

   Every flow has a single :in and a single :out boundary. `step` makes
   a 1-in 1-out primitive; `serial` glues :out → :in sequentially;
   `as-step` black-boxes an arbitrary flow as a single :in/:out node
   inside a larger flow.

   For independent parallel streams (e.g. two pipelines that share trace
   infrastructure but don't exchange data), run multiple flows on a
   shared pubsub rather than trying to express them as one flow with
   multi-wire boundaries — dp2 intentionally stays 1-in 1-out.

   For multi-port cases inside a single flow (agents with tool-ports,
   routers, back-edges, etc.), drop down to the lower level: `merge-flows`
   unions procs/conns, `connect` adds explicit wires, `input-at` /
   `output-at` override the boundary step (and can take `[sid port]`
   vectors to point at specific ports)."
  (:require [clojure.set :as set]
            [toolkit.datapotamus2.core :as dp2]))

;; --- auto-id counters -------------------------------------------------------

(def ^:private counters (atom {}))

(defn- gen-id [kind]
  (let [n (-> (swap! counters update kind (fnil inc 0)) (get kind))]
    (keyword (str (name kind) "-" n))))

;; --- primitives -------------------------------------------------------------

(defn step
  "A 1-in 1-out flow containing a single proc. `factory` is a dp2 factory
   `(fn [ctx] step-fn)`. Omitting `id` auto-generates one.

   For the common case of writing a step from scratch (custom handler,
   span usage, state threading), prefer `from-handler` which drops the
   vestigial 4-arity ceremony of the raw factory form."
  ([factory]    (step (gen-id :step) factory))
  ([id factory] {:procs {id factory} :conns [] :in id :out id}))

(defn from-handler
  "Build a 1-proc flow around a message handler.

   `handler` is `(fn [ctx s m] -> [s' port-map])` — the same contract as
   the 4-arity step-fn's message handler, without the three vestigial
   arities.

   2-arg form is 1-in 1-out (ports `:in` / `:out`). 3-arg form takes a
   `ports` map `{:ins {...} :outs {...}}` for multi-port steps.

     (from-handler :work
       (fn [{:keys [trace] :as ctx} s m]
         (let [v (trace ctx :op {} (fn [_] (inc (:data m))))]
           [s (emit m :out v)])))"
  ([id handler]        (from-handler id {} handler))
  ([id ports handler]
   (let [ins  (:ins  ports {:in  "Datapotamus flow step input port"})
         outs (:outs ports {:out "Datapotamus flow step output port"})]
     (step id
           (fn [ctx]
             (fn ([]      {:params {} :ins ins :outs outs})
                 ([_]     {})
                 ([s _]   s)
                 ([s _ m] (handler ctx s m))))))))

(defn id-flow
  "Identity / pass-through flow: receives on :in, emits the same data on
   :out (with fresh msg-id + inherited lineage). Useful as a placeholder
   in `serial` or when you need a sentinel node."
  []
  (step (gen-id :id)
        (fn [_ctx]
          (fn ([]      {:params {} :ins {:in ""} :outs {:out ""}})
              ([_]     {})
              ([s _]   s)
              ([s _ m] [s {:out [(dp2/child-same-data m)]}])))))

(defn as-step
  "Black-box `inner-flow` as a single proc under `id` in the containing
   flow. Inner proc ids get namespaced by `id` at instrument time, and
   inner trace events nest under a [:flow id] scope segment."
  [id inner-flow]
  {:procs {id inner-flow} :conns [] :in id :out id})

;; --- composition ------------------------------------------------------------

(defn- assert-no-collision! [a b context]
  (let [coll (set/intersection (set (keys (:procs a))) (set (keys (:procs b))))]
    (when (seq coll)
      (throw (ex-info (str context ": colliding proc ids")
                      {:colliding coll})))))

(defn- conn-glue [from-flow to-flow]
  [[(:out from-flow) :out] [(:in to-flow) :in]])

(defn serial
  "Compose flows sequentially. Glues each flow's :out to the next
   flow's :in (via :out → :in). Input and output of the composite are
   the first flow's :in and the last flow's :out."
  [& flows]
  (when (empty? flows)
    (throw (ex-info "serial needs at least one flow" {})))
  (reduce (fn [acc f]
            (assert-no-collision! acc f "serial")
            {:procs  (merge (:procs acc) (:procs f))
             :conns  (conj (into (vec (:conns acc)) (:conns f))
                           (conn-glue acc f))
             :in  (:in acc)
             :out (:out f)})
          (first flows)
          (rest flows)))

(defn merge-flows
  "Union procs and conns of several flows without auto-wiring. :in /
   :out of the result come from the first flow. Use with `connect`
   for explicit multi-port wiring."
  [& flows]
  (when (empty? flows)
    (throw (ex-info "merge-flows needs at least one flow" {})))
  (reduce (fn [acc f]
            (assert-no-collision! acc f "merge-flows")
            (-> acc
                (update :procs merge (:procs f))
                (update :conns (fnil into []) (:conns f))))
          (first flows)
          (rest flows)))

;; --- explicit wiring --------------------------------------------------------

(defn- resolve-ref [ref default-port]
  (cond
    (keyword? ref) [ref default-port]
    (vector? ref)  ref
    :else (throw (ex-info "connect: invalid ref" {:ref ref}))))

(defn connect
  "Add an explicit conn to the flow. `from` and `to` are refs: a keyword
   (uses the default port) or a [sid port] vector."
  [flow from to]
  (update flow :conns (fnil conj [])
          [(resolve-ref from :out) (resolve-ref to :in)]))

(defn input-at
  "Set the flow's :in boundary. `ref` is either a step-id (default `:in`
   port) or `[step-id port]` for an explicit port."
  [flow ref]
  (assoc flow :in ref))

(defn output-at
  "Set the flow's :out boundary. `ref` is either a step-id (default `:out`
   port) or `[step-id port]` for an explicit port."
  [flow ref]
  (assoc flow :out ref))

;; --- inside a step-fn: emission helper -------------------------------------

(defn emit
  "Build an output port-map from port/data pairs. Each `data` becomes one
   child msg carrying `m`'s lineage.

     (dsl/emit m :out result)            ;; one port, one msg
     (dsl/emit m :a x :b y)              ;; multi-port, one msg each"
  [m & port-data-pairs]
  (reduce (fn [acc [port data]]
            (assoc acc port [(dp2/child-with-data m data)]))
          {}
          (partition 2 port-data-pairs)))
