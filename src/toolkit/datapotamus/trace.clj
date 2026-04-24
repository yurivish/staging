(ns toolkit.datapotamus.trace
  "Trace events + scoped pubsub + counters.

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
  (cond-> {:msg-id (:msg-id m)}
    (contains? m :data-id)        (assoc :data-id (:data-id m))
    (contains? m :data)           (assoc :data (:data m))
    (contains? m :tokens)         (assoc :tokens (:tokens m))
    (contains? m :parent-msg-ids) (assoc :parent-msg-ids (vec (:parent-msg-ids m)))))

;; ============================================================================
;; Event constructors — pure
;; ============================================================================

(defn recv-event
  "Built for any msg-kind. For :done, pass `in-port`."
  ([step-id msg-kind m] (recv-event step-id msg-kind m nil))
  ([step-id msg-kind m in-port]
   (cond-> (assoc (msg-envelope m)
                  :kind :recv :msg-kind msg-kind :step-id step-id)
     (= :done msg-kind) (assoc :in-port in-port))))

(defn success-event [step-id msg-kind m]
  (assoc (msg-envelope m)
         :kind :success :msg-kind msg-kind :step-id step-id))

(defn failure-event [step-id m ^Throwable ex]
  (assoc (msg-envelope m)
         :kind :failure :msg-kind :data :step-id step-id
         :error {:message (ex-message ex) :data (ex-data ex)}))

(defn send-out-event [step-id msg-kind port child]
  (assoc (msg-envelope child)
         :kind :send-out :msg-kind msg-kind :port port :step-id step-id))

;; ============================================================================
;; Scope helpers
;; ============================================================================

(defn scope->tokens
  "Flatten a scope (`[[:flow fid] [:step sid] ...]`) to the vector of
   name-strings, alternating kind + id: `[\"flow\" fid \"step\" sid]`."
  [scope]
  (vec (mapcat (fn [[k id]]
                 [(name k) (if (keyword? id) (name id) (str id))])
               scope)))

(defn subject-for [scope kind]
  (into [(name kind)] (scope->tokens scope)))

(defn run-subject-for [scope kind]
  (-> (subject-for scope kind) (conj "run")))

(defn flow-path-of [scope]
  (mapv (fn [[_ id]] (if (keyword? id) (name id) id))
        (filter (fn [[k _]] (= k :flow)) scope)))

(defn scope->glob [scope]
  (-> [:*] (into (scope->tokens scope)) (conj :>)))

;; ============================================================================
;; Scoped pubsub — publishing helper
;; ============================================================================

(defn sp-pub
  "Publish event `ev` with subject derived from the scope. Stamps :at,
   :scope, :flow-path at publish time."
  [{:keys [raw prefix]} ev]
  (pubsub/pub raw
              (subject-for prefix (:kind ev))
              (assoc ev :scope prefix :flow-path (flow-path-of prefix) :at (now))))
