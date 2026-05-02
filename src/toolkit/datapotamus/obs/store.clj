(ns toolkit.datapotamus.obs.store
  "DuckDB-backed trace storage for a single Datapotamus run.

   One DuckDB file per run. Four tables:

     runs      — one row, holds wall-clock origin + topology snapshot.
     events    — append-only firehose; one row per emitted trace event.
     spans     — :recv ↔ :success/:failure pairs by msg-id.
     payloads  — content-addressed dedup of envelope :data values.

   Public API:
     (make-run-meta stepmap & extras)   → seed a run-meta map for the recorder
     (finalize-run-meta run-meta)       → stamp finish wall-time + duration
     (init! ds-or-path)                 → idempotent DDL bootstrap
     (flush-run! ds-or-path trace)      → ingest a deref'd recorder trace

   Caller hooks `make-run-meta` into `recorder/start-recorder!` so the
   trace map's `:run` field carries everything `flush-run!` needs.

   Topology shape comes from `toolkit.datapotamus.step/topology` — see
   that fn's docstring."
  (:require [clojure.data.json :as json]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.step :as step]
            [toolkit.llm.cache :as cache])
  (:import (java.sql Connection Timestamp)
           (java.time Instant)))

(defn- sid-str [sid]
  (if (keyword? sid) (name sid) (str sid)))

;; ============================================================================
;; Run-meta seeding
;; ============================================================================

(defn make-run-meta
  "Seed a run-meta map: run-id, wall + monotonic origins, topology snapshot.
   Pass to `recorder/start-recorder!`. Caller may merge :workflow-id and
   :attrs."
  ([stepmap] (make-run-meta stepmap {}))
  ([stepmap extras]
   (merge {:run-id          (random-uuid)
           :started-at-wall (Instant/now)
           :started-at-ns   (System/nanoTime)
           :topology        (step/topology stepmap)}
          extras)))

(defn finalize-run-meta
  "Stamp `:finished-at-wall` and `:duration-ns` on a run-meta. Caller is
   expected to assoc `:status` (\"ok\" | \"error\" | \"open\")."
  [run-meta]
  (let [end-ns (System/nanoTime)]
    (assoc run-meta
           :finished-at-wall (Instant/now)
           :duration-ns      (- end-ns (:started-at-ns run-meta)))))

;; ============================================================================
;; Schema
;; ============================================================================

(def ^:private ddl
  ["CREATE TABLE IF NOT EXISTS runs (
       run_id            UUID,
       workflow_id       VARCHAR,
       started_at_wall   TIMESTAMP,
       started_at_ns     BIGINT,
       finished_at_wall  TIMESTAMP,
       duration_ns       BIGINT,
       status            VARCHAR,
       topology          JSON,
       attrs             JSON
     )"
   "CREATE TABLE IF NOT EXISTS events (
       seq             BIGINT PRIMARY KEY,
       at_ns           BIGINT,
       kind            VARCHAR,
       msg_id          UUID,
       data_id         UUID,
       msg_kind        VARCHAR,
       step_id         VARCHAR,
       scope_path      VARCHAR[],
       port            VARCHAR,
       parent_msg_ids  UUID[],
       span_id         BIGINT,
       payload_id      VARCHAR,
       error           JSON,
       status_data     JSON
     )"
   "CREATE TABLE IF NOT EXISTS spans (
       span_id        BIGINT PRIMARY KEY,
       msg_id         UUID,
       data_id        UUID,
       step_id        VARCHAR,
       scope_path     VARCHAR[],
       in_port        VARCHAR,
       in_payload_id  VARCHAR,
       started_at_ns  BIGINT,
       ended_at_ns    BIGINT,
       duration_ns    BIGINT,
       status         VARCHAR,
       error_message  VARCHAR,
       error_data     JSON
     )"
   "CREATE TABLE IF NOT EXISTS payloads (
       id           VARCHAR PRIMARY KEY,
       size_bytes   BIGINT,
       content_edn  VARCHAR
     )"])

(defn- coerce-ds [ds-or-path]
  (if (string? ds-or-path)
    (jdbc/get-datasource {:jdbcUrl (str "jdbc:duckdb:" ds-or-path)})
    ds-or-path))

(defn init!
  "Idempotently create the four tables. Returns the datasource."
  [ds-or-path]
  (let [ds (coerce-ds ds-or-path)]
    (doseq [stmt ddl]
      (jdbc/execute! ds [stmt]))
    ds))

;; ============================================================================
;; Hash + bind helpers
;; ============================================================================

(defn- bytes->hex ^String [^bytes b]
  (let [sb (StringBuilder. (* 2 (alength b)))]
    (dotimes [i (alength b)]
      (let [byt (bit-and (aget b i) 0xff)]
        (.append sb (format "%02x" byt))))
    (.toString sb)))

(defn- payload-of
  "Hash the event's envelope `:data` if present. Returns
   `{:id :size :content}` or nil."
  [ev]
  (when (contains? ev :data)
    (let [content (pr-str (:data ev))
          digest  (cache/sha256-bytes content)
          bytes   (.getBytes content "UTF-8")]
      {:id      (bytes->hex digest)
       :size    (alength bytes)
       :content content})))

(defn- ^Connection unwrap-conn [ds]
  (jdbc/get-connection ds))

(defn- str-array ^java.sql.Array [^Connection conn coll]
  (.createArrayOf conn "VARCHAR" (into-array String (mapv str coll))))

(defn- uuid-array ^java.sql.Array [^Connection conn coll]
  (.createArrayOf conn "UUID" (into-array java.util.UUID coll)))

(defn- ts-from-instant [v]
  (cond
    (nil? v) nil
    (instance? Timestamp v) v
    (instance? Instant v)   (Timestamp/from ^Instant v)
    :else (throw (ex-info "Unsupported timestamp" {:value v}))))

(defn- ->json [v]
  (when (some? v)
    (json/write-str v)))

;; ============================================================================
;; Span pairing — group :recv ↔ :success/:failure by msg-id
;; ============================================================================

(defn- pair-spans
  "Walk events; pair `:recv` ↔ `:success`/`:failure` by `:msg-id`. Returns
   a vector of span records and a map `{msg-id → span-id}` for
   denormalizing `events.span_id` at insert."
  [events]
  (let [closer? #(#{:success :failure} (:kind %))
        recv?   #(= :recv (:kind %))
        by-msg  (group-by :msg-id (filter #(or (recv? %) (closer? %)) events))
        pairs   (keep (fn [[mid evs]]
                        (when-let [r (some #(when (recv? %) %) evs)]
                          [mid r (some #(when (closer? %) %) evs)]))
                      by-msg)]
    (loop [out      []
           ids      {}
           span-id  1
           pairs    pairs]
      (if-let [[mid r c] (first pairs)]
        (let [ip (payload-of r)
              s  {:span-id       span-id
                  :msg-id        mid
                  :data-id       (:data-id r)
                  :step-id       (:step-id r)
                  :scope-path    (:scope-path r)
                  :in-port       (:in-port r)
                  :in-payload-id (:id ip)
                  :started-at-ns (:at r)
                  :ended-at-ns   (:at c)
                  :duration-ns   (when c (- (:at c) (:at r)))
                  :status        (case (:kind c)
                                   :success "ok"
                                   :failure "error"
                                   "open")
                  :error-message (get-in c [:error :message])
                  :error-data    (get-in c [:error :data])}]
          (recur (conj out s) (assoc ids mid span-id) (inc span-id) (rest pairs)))
        [out ids]))))

;; ============================================================================
;; Inserts
;; ============================================================================

(defn- insert-run! [^Connection conn run-meta]
  (jdbc/execute!
   conn
   ["INSERT INTO runs
       (run_id, workflow_id, started_at_wall, started_at_ns,
        finished_at_wall, duration_ns, status, topology, attrs)
       VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON))"
    (:run-id run-meta)
    (:workflow-id run-meta)
    (ts-from-instant (:started-at-wall run-meta))
    (:started-at-ns run-meta)
    (ts-from-instant (:finished-at-wall run-meta))
    (:duration-ns run-meta)
    (or (:status run-meta) "open")
    (->json (:topology run-meta))
    (->json (:attrs run-meta))]))

(defn- insert-payloads! [^Connection conn payloads]
  (when (seq payloads)
    (let [sql "INSERT INTO payloads (id, size_bytes, content_edn) VALUES (?, ?, ?)
                ON CONFLICT (id) DO NOTHING"
          rows (mapv (juxt :id :size :content) payloads)]
      (jdbc/execute-batch! conn sql rows {}))))

(defn- insert-events! [^Connection conn rows]
  (when (seq rows)
    (let [sql "INSERT INTO events
                 (seq, at_ns, kind, msg_id, data_id, msg_kind, step_id,
                  scope_path, port, parent_msg_ids, span_id, payload_id,
                  error, status_data)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                         CAST(? AS JSON), CAST(? AS JSON))"]
      (jdbc/execute-batch! conn sql rows {}))))

(defn- insert-spans! [^Connection conn rows]
  (when (seq rows)
    (let [sql "INSERT INTO spans
                 (span_id, msg_id, data_id, step_id, scope_path,
                  in_port, in_payload_id, started_at_ns, ended_at_ns,
                  duration_ns, status, error_message, error_data)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON))"]
      (jdbc/execute-batch! conn sql rows {}))))

;; ============================================================================
;; Row builders — turn trace events into JDBC parameter vectors
;; ============================================================================

(defn- event-row [^Connection conn seq-n ev span-id payload-id]
  [seq-n
   (:at ev)
   (name (:kind ev))
   (:msg-id ev)
   (:data-id ev)
   (some-> (:msg-kind ev) name)
   (some-> (:step-id ev) sid-str)
   (when (:scope-path ev) (str-array conn (:scope-path ev)))
   (some-> (or (:port ev) (:in-port ev)) sid-str)
   (when (seq (:parent-msg-ids ev))
     (uuid-array conn (:parent-msg-ids ev)))
   span-id
   payload-id
   (->json (:error ev))
   (when (= :status (:kind ev)) (->json (:data ev)))])

(defn- span-row [^Connection conn s]
  [(:span-id s)
   (:msg-id s)
   (:data-id s)
   (some-> (:step-id s) sid-str)
   (when (:scope-path s) (str-array conn (:scope-path s)))
   (some-> (:in-port s) sid-str)
   (:in-payload-id s)
   (:started-at-ns s)
   (:ended-at-ns s)
   (:duration-ns s)
   (:status s)
   (:error-message s)
   (->json (:error-data s))])

;; ============================================================================
;; Public ingest entry point
;; ============================================================================

(defn flush-run!
  "Ingest a deref'd recorder trace into a DuckDB file (path or datasource).
   Schema is bootstrapped if missing.

   `trace` is the map produced by `(:stop recorder-handle)` (or
   `@(:trace recorder-handle)`). Reads:
     :run     — the run-meta (see `make-run-meta` / `finalize-run-meta`)
     :events  — vector of trace events in arrival order

   One DuckDB transaction. Idempotent on payloads (ON CONFLICT DO NOTHING)
   but not on runs/events/spans — call against a fresh DB per run."
  [ds-or-path trace]
  (let [ds (init! ds-or-path)
        events (:events trace)
        run    (:run trace)
        [spans span-id-by-msg] (pair-spans events)
        payloads (into {} (keep (fn [ev]
                                  (when-let [p (payload-of ev)]
                                    [(:id p) p]))
                                events))]
    (with-open [conn (unwrap-conn ds)]
      (.setAutoCommit conn false)
      (try
        (insert-run! conn run)
        (insert-payloads! conn (vals payloads))
        (let [event-rows (map-indexed
                          (fn [i ev]
                            (let [span-id    (when (#{:recv :success :failure} (:kind ev))
                                               (span-id-by-msg (:msg-id ev)))
                                  payload    (payload-of ev)
                                  payload-id (:id payload)]
                              (event-row conn i ev span-id payload-id)))
                          events)]
          (insert-events! conn (vec event-rows)))
        (insert-spans! conn (mapv #(span-row conn %) spans))
        (.commit conn)
        (catch Throwable t
          (.rollback conn)
          (throw t))))))
