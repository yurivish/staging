(ns toolkit.potam3.step
  "Step arrows + composition. Pure, no runtime.

   An **arrow** is a plain map with three callable slots:

     {:on-data       (fn [ctx state data] → return)
      :on-signal     (fn [ctx state]      → return)   ; default: broadcast
      :on-all-closed (fn [ctx state]      → return)   ; default: emit nothing
      :ports         {:ins {port-kw doc} :outs {port-kw doc}}}

   Handler return is one of:
     • {port [draft-or-bare ...]}                — no state change
     • [state' {port [draft-or-bare ...]}]        — state updated

   Drafts come from `toolkit.potam3.msg`. Bare values are automatically
   wrapped as `child` drafts of the input message during synthesis.

   A **step** is a wiring container:

     {:procs {sid arrow-or-step}   ; subflows compose recursively
      :conns [[[from-sid port] [to-sid port]] ...]
      :in    sid-or-[sid port]
      :out   sid-or-[sid port]}

   A single-arrow step is still a step. All composition operators return
   steps. Nothing here touches pubsub or core.async."
  (:require [toolkit.potam3.msg :as msg]))

;; ============================================================================
;; Arrow defaults
;; ============================================================================
;;
;; The interpreter calls these when the arrow doesn't supply a custom version.
;; They read the current port spec from ctx (:ins / :outs), which the
;; interpreter injects before every call.

(defn- default-on-signal
  "Broadcast one signal-draft to each output port."
  [ctx _state]
  (into {} (map (fn [p] [p [(msg/signal ctx)]])) (keys (:outs ctx))))

(defn- default-on-all-closed
  "Nothing to drain. Interpreter additionally cascades (new-done) on all outs."
  [_ctx _state]
  {})

(defn- ensure-defaults [arrow]
  (cond-> arrow
    (nil? (:on-signal arrow))     (assoc :on-signal default-on-signal)
    (nil? (:on-all-closed arrow)) (assoc :on-all-closed default-on-all-closed)))

;; ============================================================================
;; Predicates
;; ============================================================================

(defn arrow? [v] (and (map? v) (contains? v :on-data) (contains? v :ports)))
(defn step?  [v] (and (map? v) (contains? v :procs)   (contains? v :conns)))

;; ============================================================================
;; Arrow + step constructors
;; ============================================================================

(defn arrow
  "Build an arrow from a partial spec, filling in defaults."
  [m]
  (ensure-defaults
   (cond-> m
     (nil? (:ports m)) (assoc :ports {:ins {:in ""} :outs {:out ""}}))))

(defn- mk-step [id proc-value]
  {:procs {id proc-value} :conns [] :in id :out id})

(defn step
  "Build a 1-proc step.

   2-arity — pure-fn form:
     (step id f)      ; f :: data → data; ports default to :in / :out

   3-arity — handler form:
     (step id ports handler)
       ports   = {:ins {port-kw doc} :outs {port-kw doc}}  ; nil ⇒ defaults
       handler = (fn [ctx state data] → return)
       return  = {port [draft-or-bare ...]}        ; no state change
               | [state' {port [draft-or-bare ...]}] ; state updated"
  ([id f]
   (step id nil (fn [_ctx _s d] {:out [(f d)]})))
  ([id ports handler]
   (mk-step id (arrow (cond-> {:on-data handler}
                        ports (assoc :ports ports))))))

(defn sink
  "Terminal step — consumes input, emits nothing."
  ([] (sink :sink))
  ([id]
   (mk-step id (arrow {:on-data (fn [_ctx _s _d] {})
                       :ports   {:ins {:in ""} :outs {}}}))))

(defn passthrough
  "Forward data unchanged, preserving :data-id through lineage."
  [id]
  (mk-step id (arrow {:on-data (fn [ctx _s _d] {:out [(msg/pass ctx)]})})))

(defn as-step
  "Wrap any step into a single-entry step under `id`. Use to give a subflow
   a stable outer name (scope prefixing derives from it)."
  [id inner]
  (mk-step id inner))

;; ============================================================================
;; Composition
;; ============================================================================

(defn- endpoint [ref default-port]
  (if (vector? ref) ref [ref default-port]))

(defn- assert-no-collision! [a b]
  (when-let [shared (seq (filter (set (keys (:procs a))) (keys (:procs b))))]
    (throw (ex-info (str "Step composition: proc-id collision " (vec shared))
                    {:colliding-ids (vec shared)}))))

(defn merge-steps
  "Union procs + conns. Composable via explicit `connect`."
  [& steps]
  (reduce (fn [acc s]
            (assert-no-collision! acc s)
            (-> acc
                (update :procs #(clojure.core/merge % (:procs s)))
                (update :conns into (:conns s))))
          {:procs {} :conns []}
          steps))

(defn connect
  "Add one [from → to] conn to a step."
  [s from to]
  (update s :conns conj
          [(endpoint from :out) (endpoint to :in)]))

(defn serial
  "Chain steps sequentially, auto-wiring each :out to next :in."
  [& steps]
  (let [merged (apply merge-steps steps)
        glues  (partition 2 1 steps)
        wired  (reduce (fn [acc [a b]]
                         (connect acc
                                  (endpoint (:out a) :out)
                                  (endpoint (:in  b) :in)))
                       merged glues)]
    (cond-> wired
      (seq steps) (assoc :in  (:in  (first steps))
                         :out (:out (last  steps))))))

(defn input-at  [s ref] (assoc s :in ref))
(defn output-at [s ref] (assoc s :out ref))
