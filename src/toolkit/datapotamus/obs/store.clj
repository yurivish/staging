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
           (java.time Instant)
           (java.util Collection UUID)
           (org.duckdb DuckDBAppender DuckDBConnection)))

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
  ;; No PRIMARY KEY constraints on the bulk tables: the DuckDB Appender
  ;; (used by `flush-run!`) bypasses constraint checks for performance,
  ;; and the would-be keys are unique by construction anyway —
  ;; `events.seq` and `spans.span_id` are monotonic counters we mint
  ;; here, and `payloads.id` is a sha256 content hash that we dedup
  ;; client-side before inserting.
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
       seq             BIGINT,
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
       span_id        BIGINT,
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
       id           VARCHAR,
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

(defn- ^UUID coerce-uuid [v]
  (cond
    (nil? v)               nil
    (instance? UUID v)     v
    :else                  (UUID/fromString (str v))))

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

;; The `runs` table has exactly one row per flush; a parameterised INSERT
;; via JDBC is fine (and lets us keep the `CAST(? AS JSON)` for the
;; topology/attrs columns without coding around the appender).

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

;; The bulk tables (payloads/events/spans) use the DuckDB Appender API
;; instead of JDBC parameterised INSERT. The Appender writes directly
;; into the columnar buffers — no SQL parser, no per-row plan, no
;; constraint checks. For the trace volumes a real run produces
;; (~125k events for an n=5 hn-sota run) this is ~33× faster end-to-end
;; than `execute-batch!` (770× on payloads alone, where the dropped PK
;; uniqueness check accounts for most of the win).

(defn- append-payloads! [^DuckDBConnection ddb payloads]
  (when (seq payloads)
    (with-open [^DuckDBAppender app
                (.createAppender ddb DuckDBConnection/DEFAULT_SCHEMA "payloads")]
      (doseq [{:keys [id size content]} payloads]
        (.beginRow app)
        (.append app ^String id)
        (.append app (long size))
        (.append app ^String content)
        (.endRow app)))))

(defn- append-event-row!
  [^DuckDBAppender app seq-n ev span-id payload-id]
  (.beginRow app)
  (.append app (long seq-n))
  (if-let [v (:at ev)]      (.append app (long v)) (.appendNull app))
  (.append app ^String (name (:kind ev)))
  (if-let [u (:msg-id ev)]  (.append app ^UUID (coerce-uuid u))   (.appendNull app))
  (if-let [u (:data-id ev)] (.append app ^UUID (coerce-uuid u))   (.appendNull app))
  (if-let [k (:msg-kind ev)] (.append app ^String (name k))       (.appendNull app))
  (if-let [s (:step-id ev)]  (.append app ^String (sid-str s))    (.appendNull app))
  (if-let [sp (:scope-path ev)]
    (.append app ^Collection (mapv str sp))
    (.appendNull app))
  (if-let [p (or (:port ev) (:in-port ev))]
    (.append app ^String (sid-str p))
    (.appendNull app))
  (if-let [parents (seq (:parent-msg-ids ev))]
    (.append app ^Collection (mapv coerce-uuid parents))
    (.appendNull app))
  (if span-id    (.append app (long span-id))     (.appendNull app))
  (if payload-id (.append app ^String payload-id) (.appendNull app))
  (if-let [e (:error ev)]
    (.append app ^String (->json e))
    (.appendNull app))
  (if (and (= :status (:kind ev)) (contains? ev :data))
    (.append app ^String (->json (:data ev)))
    (.appendNull app))
  (.endRow app))

(defn- append-events! [^DuckDBConnection ddb events span-id-by-msg]
  (when (seq events)
    (with-open [^DuckDBAppender app
                (.createAppender ddb DuckDBConnection/DEFAULT_SCHEMA "events")]
      (loop [i 0 evs events]
        (when-let [ev (first evs)]
          (let [span-id    (when (#{:recv :success :failure} (:kind ev))
                             (span-id-by-msg (:msg-id ev)))
                payload    (payload-of ev)
                payload-id (:id payload)]
            (append-event-row! app i ev span-id payload-id))
          (recur (inc i) (next evs)))))))

(defn- append-span-row! [^DuckDBAppender app s]
  (.beginRow app)
  (.append app (long (:span-id s)))
  (if-let [u (:msg-id s)]  (.append app ^UUID (coerce-uuid u)) (.appendNull app))
  (if-let [u (:data-id s)] (.append app ^UUID (coerce-uuid u)) (.appendNull app))
  (if-let [v (:step-id s)] (.append app ^String (sid-str v))   (.appendNull app))
  (if-let [sp (:scope-path s)]
    (.append app ^Collection (mapv str sp))
    (.appendNull app))
  (if-let [v (:in-port s)]       (.append app ^String (sid-str v)) (.appendNull app))
  (if-let [v (:in-payload-id s)] (.append app ^String v)           (.appendNull app))
  (if-let [v (:started-at-ns s)] (.append app (long v))            (.appendNull app))
  (if-let [v (:ended-at-ns s)]   (.append app (long v))            (.appendNull app))
  (if-let [v (:duration-ns s)]   (.append app (long v))            (.appendNull app))
  (.append app ^String (or (:status s) "open"))
  (if-let [v (:error-message s)] (.append app ^String v)             (.appendNull app))
  (if-let [v (:error-data s)]    (.append app ^String (->json v))   (.appendNull app))
  (.endRow app))

(defn- append-spans! [^DuckDBConnection ddb spans]
  (when (seq spans)
    (with-open [^DuckDBAppender app
                (.createAppender ddb DuckDBConnection/DEFAULT_SCHEMA "spans")]
      (doseq [s spans]
        (append-span-row! app s)))))

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

   Bulk tables are written via the DuckDB Appender (no PK / constraint
   enforcement — payload dedup happens in this fn, and `seq` /
   `span-id` are monotonic counters minted here, so duplicates are
   structurally impossible). Call against a fresh DB per run."
  [ds-or-path trace]
  (let [ds (init! ds-or-path)
        events (:events trace)
        [spans span-id-by-msg] (pair-spans events)
        payloads (into {} (keep (fn [ev]
                                  (when-let [p (payload-of ev)]
                                    [(:id p) p]))
                                events))]
    (with-open [conn (unwrap-conn ds)]
      (let [^DuckDBConnection ddb (.unwrap conn DuckDBConnection)]
        (.setAutoCommit conn false)
        (try
          (insert-run! conn (:run trace))
          (append-payloads! ddb (vals payloads))
          (append-events!   ddb events span-id-by-msg)
          (append-spans!    ddb spans)
          (.commit conn)
          (catch Throwable t
            (.rollback conn)
            (throw t)))))))
