(ns toolkit.potam3.trace
  "Trace events + scoped pubsub + counters.

   Events are plain maps keyed on two axes: :kind (lifecycle role —
   :recv :success :failure :send-out :split :merge :seed :flow-error
   :run-started) × :msg-kind (envelope type — :data :signal :done).
   Subjects use :kind only. Consumers filter on :msg-kind in the handler.

   A scoped pubsub is a plain map `{:raw raw-ps :prefix [[:flow fid] ...]}`.
   `sp-pub` prepends the scope to the subject and stamps `:scope`,
   `:flow-path`, and `:at` onto every event. To extend a scope with a
   child segment (e.g. a nested step or subflow), `update :prefix conj
   <segment>`."
  (:require [toolkit.pubsub :as pubsub]))

(defn- now [] (System/currentTimeMillis))

;; ============================================================================
;; Event constructors — pure
;; ============================================================================

(defn recv-event
  "Built for any msg-kind. For :done, pass `in-port`."
  ([step-id msg-kind m] (recv-event step-id msg-kind m nil))
  ([step-id msg-kind m in-port]
   (cond-> {:kind :recv :msg-kind msg-kind :step-id step-id
            :msg-id (:msg-id m)}
     (= :data   msg-kind) (assoc :data-id (:data-id m) :data (:data m))
     (= :signal msg-kind) (assoc :data-id (:data-id m) :tokens (:tokens m))
     (= :done   msg-kind) (assoc :in-port in-port))))

(defn success-event [step-id msg-kind m]
  {:kind :success :msg-kind msg-kind :step-id step-id :msg-id (:msg-id m)})

(defn failure-event [step-id m ^Throwable ex]
  {:kind :failure :msg-kind :data :step-id step-id :msg-id (:msg-id m)
   :error {:message (ex-message ex) :data (ex-data ex)}})

(defn send-out-event [step-id msg-kind port child]
  (cond-> {:kind :send-out :msg-kind msg-kind :port port :step-id step-id
           :msg-id (:msg-id child)}
    (not= :done msg-kind)
    (assoc :data-id (:data-id child)
           :parent-msg-ids (vec (:parent-msg-ids child))
           :tokens (:tokens child))

    (= :data msg-kind)
    (assoc :data (:data child))))

(defn seed-event [step-id port msg-kind m]
  (cond-> {:kind :seed :msg-kind msg-kind :step-id step-id :port port
           :msg-id (:msg-id m)}
    (= :data msg-kind)   (assoc :data (:data m) :tokens (:tokens m))
    (= :signal msg-kind) (assoc :tokens (:tokens m))))

(defn flow-error-event [err]
  {:kind :flow-error :error {:message (ex-message err) :data (ex-data err)}})

(defn run-started-event []
  {:kind :run-started})

;; ============================================================================
;; Counters
;; ============================================================================

(defn update-counters [counters ev]
  (case (:kind ev)
    :recv     (update counters :recv inc)
    :success  (update counters :completed inc)
    :failure  (update counters :completed inc)
    :send-out (if (:port ev) (update counters :sent inc) counters)
    counters))

(defn balanced?
  "True iff all work so far has resolved: send count == recv count == completed."
  [{:keys [sent recv completed]}]
  (and (pos? sent) (= sent recv) (= recv completed)))

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
