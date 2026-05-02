(ns toolkit.datapotamus.combinators.aggregate
  "Group-keyed and time-windowed aggregators.

     `batch-by-group`      — buffer all rows; group on input-done; emit
                             one summary per group.
     `cumulative-by-group` — emit a cumulative summary per group as
                             rows arrive (use when upstream eager-
                             propagates input-done; pair with quiescence).
     `join-by-key`         — N-input batch join on a per-port key; emit
                             one match per key on input-done.
     `tumbling-window`     — non-overlapping fixed-size event-time
                             windows; close on watermark.
     `sliding-window`      — overlapping fixed-size event-time windows;
                             close on watermark."
  (:require [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

(defn batch-by-group
  "Aggregator step. Buffers every input row until input is exhausted,
   then groups by `key-fn` and emits one summary per group as a
   `msg/merge` over all parent msgs in that group.

   Returns `msg/drain` from :on-data to suppress the framework's
   auto-signal on :out — required because we stash the parent ref
   and derive a single `msg/merge` child later in
   `:on-all-input-done`. Without drain, the auto-signal would
   double-count parent tokens (see the deferred-derivation pattern
   in `msg.clj`).

   - `key-fn`         : (row → group-key)
   - `summarize-rows` : (group-key, rows → summary-data) — called
                        once per group on input-done."
  [key-fn summarize-rows]
  {:procs
   {:agg
    (step/handler-map
     {:ports   {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:rows []})
      :on-data
      (fn [ctx s row]
        [(update s :rows conj {:msg (:msg ctx) :row row}) msg/drain])
      :on-all-input-done
      (fn [ctx s]
        (let [grouped (group-by (comp key-fn :row) (:rows s))
              out-msgs (mapv (fn [[k entries]]
                               (let [parents (mapv :msg entries)
                                     rows    (mapv :row entries)]
                                 (msg/merge ctx parents
                                            (summarize-rows k rows))))
                             grouped)]
          {:out out-msgs}))})}
   :conns [] :in :agg :out :agg})

(defn cumulative-by-group
  "Aggregator step. On each input, accumulates the row in its
   per-group state and emits a cumulative summary for the row's
   group as a `msg/merge` over all parent msgs seen for that group
   so far.

   Use this when upstream eager-propagates input-done while
   in-flight work is still arriving — i.e. downstream of a
   `stealing-workers` whose inner declares `:work` (recursive
   feedback mode). The last emission per group is the final
   summary, and quiescence (counter balance, surfaced by
   `flow/await-quiescent!`) is the signal that no more emissions
   are coming. Tests should take the last per group, e.g.
   `(->> outputs (group-by k) vals (mapv last))`.

   For the common case (downstream of plain
   `round-robin-workers` / `stealing-workers` chains, where
   input-done is a reliable barrier), prefer `batch-by-group`:
   it calls `summarize-rows` once per group instead of once per
   input row.

   - `key-fn`         : (row → group-key) — how to group rows.
   - `summarize-rows` : (group-key, rows-so-far → summary-data) — the
                        data emitted via `msg/merge` of all parent
                        msgs seen for that group."
  [key-fn summarize-rows]
  {:procs
   {:agg
    (step/handler-map
     {:ports   {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:groups {}})
      :on-data
      (fn [ctx s row]
        (let [k       (key-fn row)
              s'      (update-in s [:groups k] (fnil conj [])
                                 {:msg (:msg ctx) :row row})
              entries (get-in s' [:groups k])
              parents (mapv :msg entries)
              rows    (mapv :row entries)]
          [s' {:out [(msg/merge ctx parents (summarize-rows k rows))]}]))})}
   :conns [] :in :agg :out :agg})

(defn join-by-key
  "Step that joins N input ports by a per-port key extractor. Each
   incoming msg is buffered under (port, key); on `:on-all-input-done`
   (after every declared input port has reported done), `on-match` is
   called once per key with the per-port items.

   Config:
     :ports        — vector of input port keywords (required, ≥2)
     :key-fns      — `{port-kw key-fn}` per declared port (required)
     :on-match     — `(key port→items → summary)` where `port→items` is
                     a map `{port-kw [item ...]}` (required)
     :require-all? — when true, only emit keys whose `port→items` has a
                     non-empty entry under EVERY declared port. Default
                     false: emit any key seen on any port.
     :id           — sid for the step (default :join-by-key)

   Note: this is a *batch* join — emissions happen on input-done, not
   per-arrival. Use `cumulative-by-group` for incremental aggregation
   over a single input."
  [{:keys [ports key-fns on-match require-all? id]
    :or   {require-all? false id :join-by-key}}]
  (assert (and (sequential? ports) (>= (count ports) 2))
          "join-by-key: :ports must be a vector of ≥2 keywords")
  (assert (every? (set ports) (keys key-fns))
          "join-by-key: :key-fns keys must match :ports")
  (assert on-match "join-by-key: :on-match is required")
  {:in id :out id :conns []
   :procs
   {id
    (step/handler-map
     {:ports   {:ins  (zipmap ports (repeat ""))
                :outs {:out ""}}
      :on-init (fn []
                 {;; {key {port [{:msg :row} ...]}}
                  :buf {}
                  ;; Set of ports we've seen :on-input-done on.
                  :done-ports #{}})
      :on-data
      (fn [ctx s row]
        (let [port  (:in-port ctx)
              k     ((get key-fns port) row)]
          [(update-in s [:buf k port] (fnil conj [])
                      {:msg (:msg ctx) :row row})
           msg/drain]))
      :on-input-done
      (fn [_ctx s port]
        (let [done' (conj (:done-ports s) port)
              s'    (assoc s :done-ports done')]
          [s' {}]))
      :on-all-input-done
      (fn [ctx s]
        (let [emissions
              (vec
               (for [[k port-map] (sort-by key (:buf s))
                     :let [items   (into {}
                                         (for [p ports]
                                           [p (mapv :row (get port-map p []))]))
                           parents (mapv :msg
                                         (mapcat val port-map))]
                     :when (or (not require-all?)
                               (every? #(seq (get items %)) ports))]
                 (msg/merge ctx parents (on-match k items))))]
          {:out emissions}))})}})

;; ============================================================================
;; Time windows — tumbling and sliding, watermark-driven
;; ============================================================================

(defn- window-start-of
  "Floor a time-ms to its tumbling-window-start: `(quot t size) * size`."
  [t size-ms]
  (* (long size-ms) (long (quot (long t) (long size-ms)))))

(defn- emit-closed-windows
  "Close every window whose end ≤ watermark. Returns [s' emissions]
   where emissions is a vec of `msg/child` outputs (empty if nothing
   closed)."
  [ctx s size wm on-window]
  (let [size        (long size)
        wm          (long wm)
        closed-keys (sort (filter #(<= (+ ^long % size) wm) (keys (:windows s))))
        emissions   (mapv (fn [start]
                            (msg/child ctx
                                       (on-window start (+ ^long start size)
                                                  (get-in s [:windows start]))))
                          closed-keys)
        s'          (update s :windows
                            (fn [ws] (apply dissoc ws closed-keys)))]
    [s' emissions]))

(defn tumbling-window
  "Step that aggregates messages into non-overlapping fixed-size windows
   keyed by an event-time extracted from the data. A window
   `[start, start+size-ms)` closes when the watermark (max event-time
   seen) reaches `start+size-ms`; on close, `on-window` is called as
   `(on-window start end items)` and its return is emitted as one
   message on `:out`. Empty windows are not emitted.

   Late events (event-time falls into an already-closed window) are
   passed to `on-late` (default: drop silently). On `:on-all-input-done`,
   any remaining open windows are flushed in start-time order.

   Config:
     :size-ms   — window length in ms (required, positive)
     :time-fn   — `(data → ms)` extractor (default: `:time`)
     :on-window — `(start end items → summary)` (required)
     :on-late   — `(item → any)` side-effect on a late drop (default
                  no-op); items are also dropped from the windowed flow
     :id        — sid for the step (default :tumbling-window)"
  [{:keys [size-ms time-fn on-window on-late id]
    :or   {time-fn :time on-late (fn [_]) id :tumbling-window}}]
  (assert (and (number? size-ms) (pos? size-ms))
          "tumbling-window: :size-ms must be positive")
  (assert on-window "tumbling-window: :on-window is required")
  (let [size (long size-ms)]
    {:procs
     {id
      (step/handler-map
       {:ports   {:ins {:in ""} :outs {:out ""}}
        :on-init (fn [] {:windows {} :watermark Long/MIN_VALUE})
        :on-data
        (fn [ctx s d]
          (let [t     (long (time-fn d))
                start (window-start-of t size)
                wm    (max (:watermark s) t)
                ;; Late if the event's window is already closed.
                late? (<= (+ start size) (:watermark s))]
            (if late?
              (do (on-late d) {})
              (let [s1            (-> s
                                      (assoc :watermark wm)
                                      (update-in [:windows start] (fnil conj []) d))
                    [s2 emissions] (emit-closed-windows ctx s1 size wm on-window)]
                [s2 (if (seq emissions) {:out emissions} {})]))))
        :on-all-input-done
        (fn [ctx s]
          (let [open-keys (sort (keys (:windows s)))]
            {:out (mapv (fn [start]
                          (msg/child ctx
                                     (on-window start (+ start size)
                                                (get-in s [:windows start]))))
                        open-keys)}))})}
     :conns [] :in id :out id}))

(defn sliding-window
  "Step that aggregates messages into overlapping fixed-size windows
   keyed by event-time. Window starts step every `:slide-ms`; each
   window covers `[start, start+size-ms)`. An event at time `t`
   belongs to every window containing `t`. A window closes when the
   watermark (max event-time seen) reaches its end; on close,
   `on-window` is called.

   Config (same shape as `tumbling-window`):
     :size-ms   — window length in ms (required, positive)
     :slide-ms  — distance between consecutive window starts (required,
                  positive; for tumbling behavior, set equal to size-ms)
     :time-fn   — `(data → ms)` extractor (default: `:time`)
     :on-window — `(start end items → summary)` (required)
     :on-late   — `(item → any)` side-effect on late events
     :id        — sid for the step (default :sliding-window)"
  [{:keys [size-ms slide-ms time-fn on-window on-late id]
    :or   {time-fn :time on-late (fn [_]) id :sliding-window}}]
  (assert (and (number? size-ms) (pos? size-ms))
          "sliding-window: :size-ms must be positive")
  (assert (and (number? slide-ms) (pos? slide-ms))
          "sliding-window: :slide-ms must be positive")
  (assert on-window "sliding-window: :on-window is required")
  (let [size  (long size-ms)
        slide (long slide-ms)
        ;; All window-starts that contain time t.
        starts-of
        (fn [t]
          ;; First start whose end > t: smallest k*slide such that k*slide+size > t.
          ;; → k > (t - size) / slide → k_min = floor((t - size) / slide) + 1.
          ;; Last start that contains t: k*slide <= t → k_max = floor(t / slide).
          (let [k-max (long (quot (long t) slide))
                k-min (inc (long (quot (- (long t) size) slide)))
                k-min (max 0 k-min)]
            (mapv #(* slide ^long %) (range k-min (inc k-max)))))]
    {:procs
     {id
      (step/handler-map
       {:ports   {:ins {:in ""} :outs {:out ""}}
        :on-init (fn [] {:windows {} :watermark Long/MIN_VALUE})
        :on-data
        (fn [ctx s d]
          (let [t       (long (time-fn d))
                wm      (max (:watermark s) t)
                ;; Each candidate-start: late if its window already closed.
                cands   (starts-of t)
                live    (remove #(<= (+ ^long % size) (:watermark s)) cands)]
            (if (and (empty? live) (seq cands))
              ;; All candidate windows are closed → fully late.
              (do (on-late d) {})
              (let [s1 (reduce (fn [acc start]
                                 (update-in acc [:windows start] (fnil conj []) d))
                               (assoc s :watermark wm)
                               live)
                    [s2 emissions] (emit-closed-windows ctx s1 size wm on-window)]
                [s2 (if (seq emissions) {:out emissions} {})]))))
        :on-all-input-done
        (fn [ctx s]
          (let [open-keys (sort (keys (:windows s)))]
            {:out (mapv (fn [start]
                          (msg/child ctx
                                     (on-window start (+ start size)
                                                (get-in s [:windows start]))))
                        open-keys)}))})}
     :conns [] :in id :out id}))
