(ns toolkit.datapotamus.trace
  "Trace events + scoped pubsub.

   Events are plain maps keyed on two axes: :kind (lifecycle role —
   :recv :success :failure :send-out :split :merge :inject :flow-error
   :run-started) × :msg-kind (envelope type — :data :signal :done).
   Subjects use :kind only. Consumers filter on :msg-kind in the handler.

   Every event that references a message carries that message's complete
   envelope (via `msg-envelope`), plus kind-specific extras (e.g. :in-port
   on a :done recv, :port on a :send-out, :error on a :failure).

   A scoped pubsub is a plain map `{:raw raw-ps :prefix [[:flow fid] ...]}`.
   `sp-pub` prepends the scope to the subject and stamps `:scope`,
   `:flow-path`, and `:at` onto every event. To extend a scope with a
   child segment (e.g. a nested step or subflow), `update :prefix conj
   <segment>`."
  (:require [toolkit.pubsub :as pubsub]))

(defn now
  "Monotonic nanosecond clock for event stamps. Use only for measuring
   durations between `:at` values — not for wall-clock display."
  [] (System/nanoTime))

;; ============================================================================
;; Envelope extraction — one place
;; ============================================================================

(defn msg-envelope
  "On-wire envelope fields of `m` as a flat map. Absent keys are skipped so
   envelope shapes stay distinct (done has no tokens; signal has no data)."
  [m]
  (cond-> {:msg-id (:msg-id m) :msg-kind (:msg-kind m)}
    (contains? m :data-id)        (assoc :data-id (:data-id m))
    (contains? m :data)           (assoc :data (:data m))
    (contains? m :tokens)         (assoc :tokens (:tokens m))
    (contains? m :parent-msg-ids) (assoc :parent-msg-ids (vec (:parent-msg-ids m)))))

;; ============================================================================
;; Event constructors — pure
;; ============================================================================

(defn recv-event
  "For :done messages, pass `in-port`."
  ([step-id m] (recv-event step-id m nil))
  ([step-id m in-port]
   (cond-> (assoc (msg-envelope m) :kind :recv :step-id step-id)
     (= :done (:msg-kind m)) (assoc :in-port in-port))))

(defn success-event [step-id m]
  (assoc (msg-envelope m) :kind :success :step-id step-id))

(defn failure-event [step-id m ^Throwable ex]
  (assoc (msg-envelope m)
         :kind :failure :step-id step-id
         :error {:message (ex-message ex) :data (ex-data ex)}))

(defn send-out-event [step-id port child]
  (assoc (msg-envelope child) :kind :send-out :port port :step-id step-id))

;; ============================================================================
;; Scope helpers
;; ============================================================================

(defn- scope->tokens
  "Flatten a scope (`[[:flow fid] [:step sid] ...]`) to the vector of
   name-strings, alternating kind + id: `[\"flow\" fid \"step\" sid]`."
  [scope]
  (vec (mapcat (fn [[k id]]
                 [(name k) (if (keyword? id) (name id) (str id))])
               scope)))

(defn run-subject-for [scope kind]
  (-> [(name kind)] (into (scope->tokens scope)) (conj "run")))

(defn- flow-path-of [scope]
  (mapv (fn [[_ id]] (if (keyword? id) (name id) id))
        (filter (fn [[k _]] (= k :flow)) scope)))

;; ============================================================================
;; Scoped pubsub — publishing helper
;; ============================================================================

(defn push-scope
  "Extend a scoped pubsub with a new scope segment, precomputing the
   flattened tokens and flow-path so `sp-pub` doesn't allocate them on
   every event. A step-sp built once at `start!` gets reused for every
   publish from that proc, so the precompute pays for itself immediately
   and is freed naturally when the flow's handle is gc'd."
  [sp segment]
  (let [prefix' (conj (:prefix sp) segment)]
    (assoc sp
           :prefix    prefix'
           :tokens    (scope->tokens prefix')
           :flow-path (flow-path-of prefix'))))

(defn sp-pub
  "Publish event `ev` on the scoped pubsub. Stamps :at, :scope,
   :flow-path at publish time. Reads the precomputed :tokens and
   :flow-path built by `push-scope`."
  [{:keys [raw prefix tokens flow-path]} ev]
  (pubsub/pub raw
              (into [(name (:kind ev))] tokens)
              (assoc ev :scope prefix :flow-path flow-path :at (now))))
