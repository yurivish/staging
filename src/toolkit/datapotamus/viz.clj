(ns toolkit.datapotamus.viz
  "Event-sourced live visualizer for Datapotamus flows.

   Subscribes to a flow's pubsub, reduces events into an in-memory store,
   and pushes debounced snapshots to a Datastar client over SSE. The store
   holds many flows keyed by run id, so many flow runs — running or
   finished — can be observed on the same page.

   Optionally, `attach-handle!` takes a live core.async.flow graph and
   starts a background ping loop that merges input-channel buffer counts
   into the flow state — exposing queue depth on each card in addition
   to the handler-busy count derived from the event stream.

   Public API:
     (from-step stepmap)                        → topology tree, pure
     (make-store)                               → empty multi-flow store atom
     (register-flow! store fid topo title)      → add a flow entry
     (attach-handle! store fid graph ticker)    → stash handle, start ping loop
     (mark-complete! store fid status)          → :running → :completed|:failed
     (apply-event state ev)                     → pure reducer, by run id
     (make-ticker window-ms)                    → trailing-edge debouncer
     (attach! pubsub store ticker)              → zero-arg unsubscribe
     (all-flows state)                          → Hiccup for #viz-grid
     (viz-handler store)                        → bare page handler
     (viz-stream-handler store ticker)          → SSE handler"
  (:require [clojure.core.async.flow :as caf]
            [clojure.datafy :as dat]
            [clojure.string :as str]
            [toolkit.datapotamus.step :as step]
            [toolkit.datastar.core :as d]
            [toolkit.pubsub :as pubsub]
            [toolkit.web :as web])
  (:import (java.util.concurrent CompletableFuture TimeUnit TimeoutException)))

;; ============================================================================
;; Topology — walk a step def into a tree of logical nodes
;; ============================================================================

(defn- humanize [sid]
  (-> (if (keyword? sid) (name sid) (str sid))
      (str/replace "-" " ")))

(declare build-node)

(defn- build-children [procs path]
  (mapv (fn [[sid proc]] (build-node sid proc (conj path sid)))
        procs))

(defn- build-node [sid proc path]
  (cond
    (step/step? proc)
    {:path     path
     :name     (humanize sid)
     :kind     :container
     :children (build-children (:procs proc) path)}

    (step/handler-map? proc)
    {:path path :name (humanize sid) :kind :leaf}

    :else
    (throw (ex-info (str "Unknown proc shape at " sid) {:sid sid}))))

(defn from-step
  "Walk a step def into a topology tree rooted at a synthetic :container."
  [stepmap]
  {:path     []
   :name     "root"
   :kind     :container
   :children (build-children (:procs stepmap) [])})

;; ============================================================================
;; Scope decoding
;;
;; Events carry :scope like [[:scope fid] [:scope|:step id] …]. The outer
;; [:scope fid] identifies the run; everything after maps onto a topology
;; path. `inline-subflow` uses strings for :scope segments and keywords for
;; :step segments — we keywordize everything so paths match step def sids.
;; ============================================================================

(defn- scope->fid [scope]
  (when-let [[_ id] (first scope)]
    (if (keyword? id) (name id) (str id))))

(defn- scope->path [scope]
  (mapv (fn [[_ id]] (if (keyword? id) id (keyword (str id))))
        (rest scope)))

;; ============================================================================
;; Store
;; ============================================================================

(defn make-store
  "Empty multi-flow store. Flows are added via `register-flow!` on start."
  []
  (atom {:flows {}}))

(defn register-flow!
  "Add a new flow entry. Call this just before starting the flow, with
   the `fid` you intend to pass as `:flow-id`."
  [store fid topology title]
  (swap! store assoc-in [:flows fid]
         {:id           fid
          :title        title
          :topology     topology
          :by-path      {}
          :status       :running
          :started-at   (System/currentTimeMillis)
          :completed-at nil})
  nil)

(defn mark-complete!
  "Mark a flow as completed or failed. `status` is :completed or :failed."
  [store fid status]
  (swap! store update-in [:flows fid]
         (fn [f] (when f (-> f
                             (assoc :status status)
                             (assoc :completed-at (System/currentTimeMillis))))))
  nil)

;; ============================================================================
;; Ping-based queue depth
;;
;; Events tell us what handlers are doing; they don't show messages sitting
;; in core.async channels between procs. `flow/ping` does — each proc
;; reports `:ins` with buffer counts. We periodically ping and stash the
;; sum of input buffer counts per topology path under `:queues`. Runtime
;; proc ids ARE topology paths (vectors of keyword sids).
;; ============================================================================

(defn- sum-in-buffers [proc-info]
  (reduce-kv
   (fn [acc _port port-info]
     (+ acc (long (or (get-in port-info [:buffer :count]) 0))))
   0
   (:clojure.core.async.flow/ins proc-info)))

(defn apply-ping
  "Rebuild :queues (path → queued count) from one core.async.flow/ping
   result. Only paths with non-zero queue are stored."
  [flow-state ping]
  (let [queues (reduce-kv
                (fn [acc path proc]
                  (let [q (sum-in-buffers (dat/datafy proc))]
                    (if (pos? q)
                      (assoc acc path q)
                      acc)))
                {}
                ping)]
    (assoc flow-state :queues queues)))

(defn attach-handle!
  "Stash the core.async.flow graph on the flow entry and start a virtual
   thread that pings every `period-ms` (default 500 ms), merging buffer
   counts into state. The loop exits when the flow is marked complete."
  ([store fid graph ticker]
   (attach-handle! store fid graph ticker 500))
  ([store fid graph ticker period-ms]
   (swap! store assoc-in [:flows fid :graph] graph)
   (Thread/startVirtualThread
    (fn []
      (loop []
        (when (= :running (get-in @store [:flows fid :status]))
          (try
            (let [p (caf/ping graph)]
              (swap! store update-in [:flows fid] apply-ping p)
              ((:tick! ticker)))
            (catch Throwable _))
          (Thread/sleep (long period-ms))
          (recur)))))
   nil))

;; ============================================================================
;; Reducers — pure. Events for unknown fids are dropped.
;; ============================================================================

(defn- bump [state fid path k]
  (if (contains? (:flows state) fid)
    (update-in state [:flows fid :by-path path k] (fnil inc 0))
    state))

(defn apply-event [state ev]
  (let [fid  (scope->fid (:scope ev))
        path (scope->path (:scope ev))]
    (case (:kind ev)
      :recv     (bump state fid path :recv)
      :success  (bump state fid path :success)
      :failure  (bump state fid path :failure)
      :send-out (cond-> state (:port ev) (bump fid path :sent))
      state)))

;; ============================================================================
;; Ticker — trailing-edge debounce
;; ============================================================================

(defn make-ticker
  [window-ms]
  (let [!latch (atom (CompletableFuture.))
        !armed (atom false)]
    {:!latch !latch
     :tick!
     (fn tick! []
       (when (compare-and-set! !armed false true)
         (Thread/startVirtualThread
          (fn []
            (try (Thread/sleep (long window-ms))
                 (finally
                   (reset! !armed false)
                   (let [old (first (swap-vals! !latch
                                                (fn [_] (CompletableFuture.))))]
                     (.complete ^CompletableFuture old true))))))))}))

(defn- await-latch
  [^CompletableFuture latch timeout-ms]
  (try (.get latch (long timeout-ms) TimeUnit/MILLISECONDS)
       (catch TimeoutException _ nil)))

;; ============================================================================
;; Subscription wiring
;; ============================================================================

(defn attach!
  "Subscribe to every event on `pubsub`; reduce into `store` and arm the
   ticker on each event. Returns a zero-arg unsubscribe fn."
  [pubsub store ticker]
  (pubsub/sub pubsub
              [:>]
              (fn [_subj ev _match]
                (swap! store apply-event ev)
                ((:tick! ticker)))))

;; ============================================================================
;; Rendering
;; ============================================================================

(def ^:private zero-stats {:recv 0 :success 0 :failure 0 :sent 0})

(defn- leaf-stats [by-path path]
  (merge zero-stats (get by-path path)))

(defn- aggregate [node by-path]
  (if (= :leaf (:kind node))
    (leaf-stats by-path (:path node))
    (reduce (fn [acc c] (merge-with + acc (aggregate c by-path)))
            zero-stats
            (:children node))))

(defn- path->dom-id [fid path]
  (str "card-" fid "-"
       (if (empty? path)
         "root"
         (str/join "-" (map #(if (keyword? %) (name %) (str %)) path)))))

(defn- aggregate-queue [node queues]
  (if (= :leaf (:kind node))
    (long (or (get queues (:path node)) 0))
    (reduce + (map #(aggregate-queue % queues) (:children node)))))

(defn- card [fid node by-path queues]
  (let [{:keys [recv success failure]} (aggregate node by-path)
        inflight (- recv (+ success failure))
        queued   (aggregate-queue node queues)]
    [:div.viz-card
     {:id        (path->dom-id fid (:path node))
      :data-kind (name (:kind node))}
     [:div.viz-card__name (:name node)]
     [:div.viz-card__inflight inflight]
     [:div.viz-card__queue
      {:data-empty (when (zero? queued) "true")}
      (if (pos? queued) (str "queued " queued) "queue empty")]
     [:div.viz-card__meta
      (str recv " recv · " success " ok"
           (when (pos? failure) (str " · " failure " err")))]]))

(defn- fid-short [fid] (subs fid 0 (min 8 (count fid))))

(defn- elapsed-ms [{:keys [started-at completed-at]}]
  (when started-at
    (- (or completed-at (System/currentTimeMillis)) started-at)))

(defn- fmt-duration [ms]
  (cond
    (nil? ms) "—"
    (< ms 1000) (str ms " ms")
    (< ms 60000) (format "%.1f s" (double (/ ms 1000)))
    :else (let [s (long (/ ms 1000))
                m (quot s 60)
                r (rem s 60)]
            (format "%dm %02ds" m r))))

(defn- flow-section [flow]
  (let [fid (:id flow)
        status (:status flow)]
    [:section.viz-flow
     {:id          (str "flow-" fid)
      :data-status (name status)}
     [:header.viz-flow__header
      [:span.viz-flow__title (or (:title flow) "flow")]
      [:span.viz-flow__id (fid-short fid)]
      [:span.viz-flow__status (name status)]
      [:span.viz-flow__elapsed (fmt-duration (elapsed-ms flow))]]
     [:div.viz-flow__cards
      (for [child (:children (:topology flow))]
        (card fid child (:by-path flow) (:queues flow)))]]))

(defn all-flows
  "Hiccup for the current store snapshot — the full #viz-grid element,
   with one section per registered flow, oldest first."
  [{:keys [flows]}]
  (let [ordered (sort-by (fn [[_ f]] (:started-at f)) flows)]
    [:div#viz-grid.viz-grid
     (if (empty? ordered)
       [:div.viz-empty "No flows yet. Click Start flow to begin."]
       (for [[_ flow] ordered]
         (flow-section flow)))]))

;; ============================================================================
;; Ring handlers (convention: `-page` / `-handler` suffixes)
;;
;; These are offered for minimal integrations. Larger apps typically embed
;; `all-flows` directly in their own page — the SSE handler only cares
;; about the store + ticker.
;; ============================================================================

(defn viz-page [store]
  (web/html
   [:html
    [:head
     [:meta {:charset "UTF-8"}]
     [:title "datapotamus viz"]
     [:link {:rel "stylesheet" :href "/static/viz.css"}]
     [:script {:type :module :defer true :src "/static/datastar-pro.js"}]]
    [:body {:data-init "@get('/viz/stream')"}
     [:h1.viz-h1 "datapotamus"]
     (all-flows @store)]]))

(defn viz-handler [store]
  (fn [_req] (viz-page store)))

(defn viz-stream-handler
  "SSE handler: on open, a virtual thread renders the current snapshot,
   parks on the ticker, and re-renders. Exits within ~2 s of SSE close."
  [store ticker]
  (fn [req]
    (d/sse-stream
     req
     {:on-open
      (fn [sse]
        (Thread/startVirtualThread
         (fn []
           (try
             (loop []
               (when (d/sse-open? sse)
                 (let [latch @(:!latch ticker)]
                   (d/patch-elements sse (all-flows @store))
                   (await-latch latch 2000))
                 (recur)))
             (finally (d/sse-close! sse))))))
      :on-close (fn [_sse _status] nil)})))
