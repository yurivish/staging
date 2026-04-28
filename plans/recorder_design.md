# Recorder design considerations

Questions that come up when building a watcher that turns a Datapotamus
run's pubsub stream into a structured artefact for retrospective query.
The current `recorder.clj` is one set of answers; the questions are
what generalise. New pipelines (longer, branchier, multi-task, multi-run,
streaming) will mostly re-pose these and pick different tradeoffs.

## 1. Subscription scope: catch-all vs filtered

Subscribe to `[:>]` (everything) or to specific subject patterns?

**Tradeoff.** Catch-all is simple and lossless — the recorder doesn't
need to know what's flowing. But it pays the cost of every event,
even ones the consumer will never look at, and it conflates events
from sibling runs sharing the pubsub. Filtered is selective but
requires the recorder to know subject conventions, and missing
events are unrecoverable later.

**Current.** Catch-all. Losing data is unrecoverable; filtering
downstream at write/query time is cheap. Cross-run separation is
handled with `scope-path` (see §6), not with subscription patterns.

## 2. Pubsub ownership: caller-supplies vs recorder-creates

Does the recorder allocate its own pubsub or take one the caller
already has?

**Tradeoff.** Recorder-owned is encapsulated and impossible to misuse,
but forces every flow runner to know about the recorder. Caller-supplied
inverts the dependency: the recorder is just another subscriber, and
the caller can attach any number of subscribers (live viz, recorder,
debug tap) to the same pubsub.

**Current.** Caller-supplies, mirroring `viz.clj`'s `attach!`. The
recorder must subscribe **before** any flow starts publishing —
otherwise initial `:run-started`/`:inject` events are missed. The
caller is responsible for the ordering.

## 3. Output shape: flat events vs pre-bucketed

Should the watcher store events as a single ordered vec, or split
them into query-friendly buckets up front?

**Tradeoff.** Flat is trivially correct and preserves all
information; the consumer filters at query time. Pre-bucketing
makes common queries one-line at the cost of (a) duplicating events
across buckets if they fit multiple categories and (b) baking the
bucket taxonomy into the writer.

**Current.** Both. `:events` is the flat fallback; `:lifecycle`,
`:llm-calls`, and `:status` are buckets driven by the consumer's
typical access patterns. New buckets are cheap to add and don't
break existing consumers (they read from the flat list when in doubt).

## 4. Events vs end-of-run enrichment

Some things you want to query aren't in the event stream: input
transcript, chunking parameters, intermediate results assembled
from many events, run timings.

**Tradeoff.** Forcing everything through events keeps the trace
purely event-sourced and reconstructable from a replay. Enrichment
at run end is faster to query and requires no derivation, but the
trace stops being a faithful record of the wire and starts being a
report.

**Current.** Hybrid. Events are the truth; `assoc-final` adds
run-level convenience under `:run` (paragraphs, chunks, registries,
mentions/records by task, totals, timings). The convenience fields
are derivable from the event stream, but pre-deriving them frees
the UI from re-doing it.

## 5. Look-aside (store-by-ID) vs inline duplication

If a big static value (system prompt, registry) is referenced by
many events, do you inline it in each event or store it once and
reference by ID?

**Tradeoff.** Inline keeps each event self-describing — open one
event in a debugger, see everything. Look-aside cuts trace size
roughly proportional to fan-out, but the consumer must follow the
indirection to render an event fully.

**Current.** Look-aside for things that are 1:N: registries
referenced by every Stage C call go under `:registries-by-task`;
each Stage C `:status` event carries `:registry-task :sentiment`,
not the registry. System prompts *should* be look-aside too (they
are static per stage, called many times) but aren't yet, because
they live as private constants in `llm.clj`/`tree_resolve.clj` and
need a small refactor to expose. Per-call content (per-chunk
paragraphs) stays inline because it actually varies per call.

The general rule: look-aside when **the cardinality of references
exceeds the cardinality of distinct values**. One registry, eight
references → look-aside. One prompt, fifty references → look-aside.
Eight prompts, eight references → don't bother.

## 6. Cross-talk between concurrent runs sharing a pubsub

Two flows running in parallel on one pubsub: each emits events the
other doesn't care about. Without filtering, internal subscribers
(like `run-seq`'s provenance recorder) accumulate the wrong run's
events and break.

**Tradeoff.** Per-run pubsubs avoid the question entirely but
fragment subscribers (now the recorder has to merge N pubsubs).
Shared pubsub with `scope-path` filtering is one pubsub, many
filters — but every internal subscriber must remember to filter.

**Current.** Shared pubsub. `run-seq` was patched to scope its
provenance subscription to its own `:flow-id` (`flow.clj:494-528`).
The recorder doesn't filter — it wants everything from every
sub-flow. Consumers downstream filter on `(first scope-path)` to
split per-phase or per-task.

This is a recurring footgun: any new internal subscriber that does
correctness-critical work has to remember to filter by scope. The
unscoped `run-seq` bug only surfaced under multi-task tests.

## 7. Lineage primitives: msg-id, data-id, or domain ID?

What's the right ID to use as the edge between two stages in a
trace UI?

**Tradeoff.** `msg-id` (per-envelope) gives the finest-grained
lineage and is auto-stamped by the framework — but it changes at
every hop and isn't meaningful outside the framework. `data-id`
(per-content) is stable across `pass` calls — useful when the same
datum flows unchanged through multiple steps. Domain IDs
(`chunk-id`, `entity_id`) are meaningful to humans and stable
across the whole pipeline, but the framework has no concept of
them — handlers must put them into payloads explicitly.

**Current.** Domain IDs as the cross-stage primitive (one chunk →
many mentions → one entity → many records); `msg-id` chains via
`parent-msg-ids` for *intra*-stage lineage (e.g. tree-merger's
recursion in Stage B). `data-id` is unused here because no stage
passes data through unchanged. A pipeline that does (e.g. a
side-input shared by many consumers) would benefit from rendering
`data-id` as a "same data" indicator in the UI.

## 8. Status events as the handler→trace integration point

How does a handler tell the recorder "I just did something the trace
should know about" — when the framework only emits structural events
(`recv`/`success`/`send-out`)?

**Tradeoff.** Bake domain semantics into the framework (e.g. a
dedicated `:llm-call-event`) — fast, type-checked, but framework
sprawl. Or use a generic side-band event with a free-form payload
and convention. The latter keeps the framework small but consumers
must agree on payload shape.

**Current.** Generic `:status` event (`trace.clj:128-137`).
Handlers call `(trace/emit ctx {:llm-call {...} :inputs {...}
:outputs {...}})`; the recorder splits `:status` events whose
payload contains `:llm-call` into the `:llm-calls` bucket and
others into `:status`. This convention is documented nowhere
except in the recorder itself, which is fine for one consumer but
won't scale to many. A more complex pipeline should expect to
codify payload shapes — likely as a malli schema on a known
keyword like `:trace.recorder/llm-call`.

## 9. Crash safety: in-memory vs append-on-write

What happens if the run crashes mid-way?

**Tradeoff.** In-memory accumulation is fast and atomic — write
once at the end, no partial files to clean up. Append-on-write
survives crashes but requires durable I/O on a hot path and a
file format that tolerates truncation.

**Current.** In-memory; trace is lost on crash. Acceptable while
runs are minutes-long and re-running is cheap. A long-running
pipeline (hours, mixed concurrency, expensive LLM spend) should
flip to append-on-write — likely transit-msgpack one-event-per-line
into a `.tmsg` file, with run-level enrichment a separate
`run.edn` written at end (or never, if the run died). The
recorder's reducer becomes a writer; the in-memory atom becomes
optional.

## 10. Order preservation for results, not just events

Datapotamus stealing-workers don't preserve input order. If the
trace consumer (or downstream code) needs deterministic output —
e.g. so global indices into a `mentions-vec` are reproducible — who
sorts?

**Tradeoff.** Sort inside the framework (every flow output is
order-preserving; no surprise) — but stealing-workers is
*defined* as not-order-preserving for good reason, and re-sorting
defeats it. Sort at the call site of the flow runner — surfaces
the choice explicitly. Sort at the consumer — flexible but
duplicated everywhere.

**Current.** Call-site sort (`flow.clj`'s `sort-by-input-order`,
in `podcast/flow.clj`). Tests caught this when comparing
single-task vs multi-task runs: `mention_indices` are global
positions in the all-mentions vec, so any reordering of Stage A
outputs changes them. The fix lives at the boundary between the
flow and its caller, not inside the framework.

This will recur for any pipeline that uses stealing-workers and
later builds global indices over their outputs.

## 11. Trace size and how to keep it bounded

Each `:send-out` event carries the full message envelope, which
can include large data values. With 100s of chunks × multiple
stages × multiple tasks, traces grow into tens of MB.

**Tradeoff.** Lossless and verbose, vs. selective and lossy.
Mitigations all add complexity: gzip on write (transparent but
slows tooling); look-aside for big static values (covered above);
strip `:data` from `:send-out` events when the data is also in a
later `:status` (deduplication but breaks message replay);
sample-rate the lifecycle stream (cheap but lossy).

**Current.** Plain EDN, no compression, full envelopes. Fine for
tens of MB; at 100s of MB add gzip. At GBs reconsider whether
each event needs its full envelope — likely not if a `:status`
event already captures the meaningful content.

## 12. Serialization format

EDN, Transit, JSON?

**Tradeoff.** EDN is faithful (keywords, sorted-maps, sets, custom
tags), Clojure-only, slow at scale. Transit is compact and
cross-language but needs a reader on the consumer side. JSON is
universal and lossy (keywords flatten to strings, no sets, key
ordering not preserved).

**Current.** EDN. The trace is for tooling we control;
faithfulness beats portability. A browser UI would project to
JSON (or use Transit) at write time — keep EDN as the source of
truth, derive other formats. The current code does only EDN; JSON
projection is a follow-up.

## 13. Indexing: precompute vs query-time

The trace is mostly a flat list of events. Common queries — "find
the LLM call that produced this entity" — want a `{msg-id → event}`
or `{entity-id → llm-call}` index. Build at write time or at read
time?

**Tradeoff.** Precomputed indexes make the consumer fast and the
trace bigger. Query-time indexes are computed once per UI session
and discarded — fine if the consumer is long-lived (a UI with a
trace loaded), wasteful if it's short-lived (a CLI dump tool).

**Current.** No indexes. The buckets (§3) are a coarse
pre-categorisation but not an index. The consumer (UI or analysis
script) is expected to build whatever indexes it needs on load.
Adding indexes is cheap if a particular query becomes hot.

## 14. Per-run scoping: meaningful `flow-id`s

Multi-stage, multi-task pipelines run many sub-flows. Each
sub-flow's `:flow-id` is the outer scope segment on every event it
emits, so the consumer can group events by phase/task by inspecting
`(first scope-path)`.

**Tradeoff.** Generated UUIDs are uniquely identifying but
opaque — the consumer needs a separate registry to know which UUID
corresponds to which phase/task. Human-readable IDs
(`"stage-b-sentiment"`) are self-describing but require care to
keep unique across runs.

**Current.** Human-readable: `"stage-a"`, `"stage-b-sentiment"`,
`"stage-b-conspiracy"`, `"stage-c"`. Uniqueness within a single
run is sufficient because each `extract!` invocation has its own
pubsub. If multiple runs share a pubsub (e.g. a long-lived
service), prefix with a per-run UUID.
