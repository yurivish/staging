# Trace storage schema for Datapotamus

## Context

Datapotamus emits structured events to a scoped pubsub during pipeline execution today (`/work/src/toolkit/datapotamus/trace.clj`, `/work/src/toolkit/datapotamus/flow.clj`). `recorder.clj` accumulates them in an in-memory atom; `viz.clj` shows live counts via SSE. There is no persistence layer.

This plan defines a DuckDB-backed schema for trace events, designed for both efficient writes (batch ingest from the recorder atom) and efficient reads (single-run inspection plus cross-run/cross-workflow analytics). UI is explicitly out of scope; a frontend comes later as a separate effort.

The schema borrows useful OTel concepts (two-level identity via run/span IDs, span-as-paired-events, span events for point-in-time annotations, resource attributes) without committing to OTel as a wire protocol or storage format. Datapotamus-native concepts (DAG lineage via merges, content-addressed payloads, run composition via `parent_run_id`/`cohort_id`, dual `msg_id`/`data_id` identity) are preserved.

Target use cases:
- **Primary:** deep inspection of one complex run, especially runs with many fan-outs/merges.
- **Secondary:** cross-run analytics — same workflow over time, across workflows, or per-step across contexts.

Scale is medium: typical runs ≤ ~10M events, fits comfortably in single-machine DuckDB on one file with no partitioning. Compositional pattern (decompose into per-item runs, merge in a separate run) is supported via `parent_run_id` and `cohort_id` on `runs`.

## Storage layout

One DuckDB database file. All tables in the default schema.

### Tables

```sql
-- Static topology metadata, one row per (workflow, version). Immutable.
topologies (
  workflow_id     VARCHAR,
  version         VARCHAR,
  nodes           JSON,        -- [{step_id, in_ports, out_ports, attrs}, ...]
  edges           JSON,        -- [{from_step, from_port, to_step, to_port}, ...]
  attrs           JSON,
  PRIMARY KEY (workflow_id, version)
)

-- One row per pipeline execution.
runs (
  run_id            UUID PRIMARY KEY,
  workflow_id       VARCHAR,
  workflow_version  VARCHAR,
  parent_run_id     UUID,           -- when launched to merge results of other runs
  cohort_id         VARCHAR,        -- free-form grouping label
  started_at_wall   TIMESTAMP,      -- wall clock for cross-run comparison
  finished_at_wall  TIMESTAMP,
  started_at_ns     BIGINT,         -- monotonic origin (rebase event at_ns from this)
  duration_ns       BIGINT,
  status            VARCHAR,        -- 'running' | 'ok' | 'error'
  attrs             JSON            -- top-level inputs, env, model versions, etc.
)

-- Raw event firehose. Immutable, append-only. Storage sorted by (run_id, event_id).
events (
  run_id       UUID,
  event_id     BIGINT,             -- monotonic per run; gives stable ordering
  at_ns        BIGINT,              -- monotonic clock; comparable within run only
  kind         VARCHAR,             -- :recv :success :failure :send-out :split :merge
                                    --  :status :flow-error :inject :run-started
  msg_id       UUID,
  data_id      UUID,                -- content identity; preserved across :pass
  msg_kind     VARCHAR,             -- :data :signal :input-done (envelope kind)
  step_id      VARCHAR,
  scope_path   LIST<VARCHAR>,
  span_id      UUID,                -- denormalized FK; which span this event belongs to
  port         VARCHAR,             -- in-port for :recv, out-port for :send-out
  payload_id   VARCHAR,             -- BLAKE3 hex; references payloads.payload_id
  error        JSON,                -- failure / flow-error: {message, data}
  status_data  JSON,                -- :status user-defined payload
  PRIMARY KEY (run_id, event_id)
)

-- Materialized spans. Pair (:recv, :success/:failure) per (run_id, msg_id, step_id).
spans (
  run_id         UUID,
  span_id        UUID,
  msg_id         UUID,
  data_id        UUID,
  step_id        VARCHAR,
  scope_path     LIST<VARCHAR>,
  started_at_ns  BIGINT,
  ended_at_ns    BIGINT,
  duration_ns    BIGINT,
  status         VARCHAR,            -- 'ok' | 'error'
  error_message  VARCHAR,
  error_data     JSON,
  in_port        VARCHAR,
  in_payload_id  VARCHAR,
  PRIMARY KEY (run_id, span_id)
)

-- Lineage DAG between message envelopes (derived from :split / :merge / :pass events).
lineage_edges (
  run_id         UUID,
  child_msg_id   UUID,
  parent_msg_id  UUID,
  child_data_id  UUID,                -- enables data-level lineage queries
  parent_data_id UUID,
  edge_kind      VARCHAR,              -- :split :merge :pass
  at_ns          BIGINT,
  PRIMARY KEY (run_id, child_msg_id, parent_msg_id)
);
CREATE INDEX lineage_edges_by_parent ON lineage_edges (run_id, parent_msg_id);

-- Content-addressed payload store. Dedup by hash; shared content stored once.
payloads (
  payload_id    VARCHAR PRIMARY KEY,  -- BLAKE3 hex
  content_type  VARCHAR,              -- :edn :json :text :bytes
  size_bytes    BIGINT,
  content_text  VARCHAR,              -- populated for :edn :json :text
  content_bytes BLOB                  -- populated for :bytes
)
```

### Notable design choices

- **`tokens` column omitted.** Token conservation is internal to runtime quiescence detection; not analytically meaningful in trace context. Stopping emission of tokens in `trace.clj` is a possible follow-up but is a separate change.
- **Dual identity (`msg_id`, `data_id`).** `msg_id` is envelope identity (new on every transformation); `data_id` is content identity (preserved across `:pass`). Both are columns on events, spans, and lineage edges so queries can target either level.
- **Spans materialized, not view.** ~30% extra storage for 10–100× faster queries on the hottest path. Cheap at this scale.
- **`span_id` denormalized onto `events`.** Set during ingest derivation. "All events for span X" becomes a simple filter, not a self-join.
- **Reverse index on `lineage_edges`.** Primary key covers child→parent (upstream); the index covers parent→child (downstream descendants).
- **Payloads split into `content_text` / `content_bytes`.** DuckDB compresses text well; `LIKE`/regex/`json_extract` run directly on payloads when text-typed.
- **No partitioning.** One DB file holds all runs. Re-evaluate if/when storage gets uncomfortable.

## Write path

`recorder.clj` is unchanged on the hot path: events accumulate in the in-memory atom during the run, zero DB cost. Persistence happens in one DuckDB transaction at quiescence / failure / explicit flush:

1. INSERT/UPSERT into `runs`.
2. Bulk-INSERT all events as one batch (DuckDB handles >10M rows/sec for this workload).
3. INSERT payload blobs into `payloads` with `ON CONFLICT DO NOTHING` (dedup by content hash).
4. Derive `lineage_edges` via `INSERT ... SELECT` filtering events of kind `:split`/`:merge`/`:pass`.
5. Derive `spans` via `INSERT ... SELECT` self-joining events on `(run_id, msg_id, step_id)` pairing `:recv` with `:success`/`:failure`. Same step also UPDATEs `events.span_id` to populate the denormalized FK.

All derivation lives in one ingest namespace, trivially re-runnable if the schema evolves. Streaming ingest (live writes during the run) is possible later but unnecessary to start: the events table is append-only and derivation can run at end either way.

## Read path

Sort order `(run_id, event_id)` on `events` gives DuckDB zone-map pruning for "everything in run X" — the hottest query — without explicit indexes. Composite primary keys cover most other point lookups. Two queries needing extra structure:

- **"All events for span X"** — `events.span_id` (denormalized at ingest); plain filter.
- **"Downstream descendants of msg X"** — reverse index on `lineage_edges (run_id, parent_msg_id)`; recursive CTE.

Analytical queries (p95 latency by step within a cohort, error rates by kind across workflows, span counts across runs of the same workflow) are column scans DuckDB handles natively without further tuning.

## Critical files

- `/work/src/toolkit/datapotamus/recorder.clj` — add `flush-to-store!` ingest hook; existing in-memory accumulation untouched.
- `/work/src/toolkit/datapotamus/trace.clj` — read-only reference for event shapes/constructors.
- `/work/src/toolkit/datapotamus/flow.clj` — read-only reference for emit harness.
- **New:** `/work/src/toolkit/datapotamus/store.clj` — DDL bootstrap (`init!`), ingest function (`flush-run!`), query helpers. Split into `store/schema.clj` + `store/ingest.clj` only if it actually grows.
- `deps.edn` — add DuckDB JDBC driver dependency.

## Open verifications before/during implementation

- Confirm `:pass` is an actual emitted event kind (the Datapotamus exploration noted it only in the `data_id` docstring). If not, drop it from the `edge_kind` enum and from the lineage-edges derivation filter.
- Confirm tokens are always `string → integer` if we ever decide to keep them; the schema currently drops them entirely.

## Verification (end-to-end)

1. Bootstrap: `(store/init! db-path)` creates tables and indexes; verify with `\d` in `duckdb` CLI.
2. Run a small Datapotamus pipeline (e.g., the `inc → dbl → sink` pattern from `datapotamus_test.clj`); call `(store/flush-run! recorder db-path)`; confirm one row in `runs`, expected events in `events`, derived spans in `spans`.
3. Single-run queries:
   - `SELECT kind, COUNT(*) FROM events WHERE run_id = ? GROUP BY kind`
   - `SELECT step_id, AVG(duration_ns), MAX(duration_ns) FROM spans WHERE run_id = ? GROUP BY step_id`
   - Recursive CTE for downstream descendants of a `msg_id` via `lineage_edges`.
4. Run two more pipelines tagged with the same `cohort_id`; verify cross-run query:
   - `SELECT step_id, percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_ns) FROM spans s JOIN runs r USING (run_id) WHERE r.cohort_id = ? GROUP BY step_id`
5. Payload dedup: emit the same payload across two runs; confirm `payloads` has exactly one row.
6. Storage size sanity check on a representative medium run; confirm comfortably within single-file DuckDB territory.
