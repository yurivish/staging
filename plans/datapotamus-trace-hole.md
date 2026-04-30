# Datapotamus trace hole: recursive tree fetch is invisible to the event stream

## Problem statement

The recursive HN tree fetch in five of six existing pipelines is
opaque to the Datapotamus event stream. Each story's tree fetch —
which can span hundreds of HTTP calls executed concurrently on a
shared virtual-thread pool — appears in pubsub as one `:recv` and
one `:send-out` per story. The fan-out width, per-node latency,
per-worker utilization, and per-node failures are all invisible to
`toolkit/datapotamus/viz.clj` and `toolkit/datapotamus/recorder.clj`.

`hn_shape` is the most ironic case: a pipeline whose entire purpose
is to *measure HN tree shape* cannot have its own fetch shape
visualized.

## Where it occurs

Each instance is a recursive function inside one `step/step` body
that uses raw `vt-exec` (a virtual-thread-per-task `ExecutorService`)
to fan out kid fetches in parallel and joins on `Future.get`:

| File | Lines | Function |
|---|---|---|
| `/work/src/hn_density/core.clj` | 32–45 | `fetch-tree` (story tree) |
| `/work/src/hn_density/core.clj` | 151–163 | `fetch-user-history` (per-user submissions) |
| `/work/src/hn_drift/core.clj` | 37–44 | tree fetch |
| `/work/src/hn_shape/core.clj` | 28–39 | tree fetch |
| `/work/src/hn_tempo/core.clj` | 31–38 | tree fetch |
| `/work/src/hn_typing/core.clj` | 38–45 | tree fetch |

`hn/core.clj` is clean (no tree fetch — only top-K direct comments).

The pattern is identical across all six instances:

```clojure
(defonce ^:private vt-exec
  (delay (Executors/newVirtualThreadPerTaskExecutor)))

(defn- fetch-tree [counter id]
  (let [item (get-json (str base "/item/" id ".json"))
        _    (swap! counter inc)
        kids (or (:kids item) [])
        futs (mapv #(.submit ^ExecutorService @vt-exec
                             ^Callable (fn [] (fetch-tree counter %)))
                   kids)]
    (assoc item :kid-trees (mapv #(.get %) futs))))
```

The `counter` atom plus a wrapping `(trace/emit ctx {:event
:fetch-done :n-nodes @counter :ms ...})` at the step boundary gives
one summary event per tree. That is the entire visibility budget
the visualizer has to work with.

## Why it happens — API expressiveness

The HN tree shape doesn't fit the existing combinators cleanly:

- **`c/parallel`**: requires a fixed port set known at
  graph-construction time. Tree shape is data-driven (number of
  kids varies per node).
- **`c/workers` / `c/stealing-workers`**: K parallel copies of one
  step. Doesn't express recursive expansion of work units.
- **`msg/children`**: data-driven N-way split is fine for *one*
  level (we use it freely). Recursive levels would require feeding
  children back into the same step — a cycle in the conn graph.

`core.async.flow` cycles are supported (README §6, "Tool-call
loops"), so the cyclic shape is reachable from existing primitives —
but it's not idiomatic and no example demonstrates it for this
purpose. Pipelines reach for `vt-exec` because it is the path of
least resistance.

## Why it matters

1. **Visualizer/recorder coverage.** The whole point of routing
   work through Datapotamus is comprehensive observability. A
   per-story black box defeats it.

2. **Concurrency picture.** "Are we saturating fetch parallelism
   or starving on long-tail nodes?" is unanswerable from the
   current event stream.

3. **Per-node error attribution.** A node-level fetch failure is
   either swallowed by `Future.get` (depending on how the recursive
   function is written) or surfaces at the step boundary as a
   single failure with no way to know which node failed.

4. **Backpressure.** The shared `vt-exec` has no bounded queue,
   so a slow HN endpoint can spike thread count without any of the
   normal `c/stealing-workers` backpressure.

5. **Drift.** Every new pipeline that needs tree work will copy the
   pattern unless the API offers a clearly better path.

## Fix options

### Option A — Cyclic graph

Wire a `fetch-node` step's `:out` back to its own `:in` via
`step/connect`. Each fetch emits node data on a forward port
(`:emit`) and child-id requests on a feedback port (`:rec`) that
loops back to `:in`. Token conservation handles closure: leaves
emit no children, `:on-data` returns an empty port-map, the
auto-signal carries the tokens forward, and the XOR group balances.

A downstream aggregator step collects all node-data emissions per
root, reassembles the tree from `(:id, :parent)` fields, and emits
one tree msg per story.

- **Pros**: fully traced; real per-node parallelism visible;
  utilization picture is correct; uses only existing primitives.
- **Cons**: the most invasive rewrite; introduces a tree-reassembly
  aggregator that has to deal with node-arrival-order independence;
  the cyclic shape is unfamiliar.

### Option B — Per-node manual `trace/emit`

Add `(trace/emit ctx {:event :fetch-node :id ... :depth ...})`
inside the existing recursive function. The recursion stays
in-step; events appear in pubsub stamped with the parent step's
scope.

- **Pros**: 5-line change per pipeline; preserves the existing
  shape; immediate per-node visibility for debugging.
- **Cons**: doesn't show concurrency *as fan-out* — the events
  appear under one step-id, not one-per-node-as-its-own-step;
  one event per node is verbose for 1000-node trees (consider
  sampling or aggregate-only emit on completion).

### Option C — New `c/expand` (or `c/recur`) combinator

A Datapotamus primitive:

```clojure
(c/expand
  :tree-fetch
  {:fetch       (fn [id] {...node-data...})
   :children-of (fn [node] (:kids node))
   :leaf?       (fn [node] (empty? (:kids node)))
   :workers     8})
```

Internally manages the recursion, exposes per-call trace events,
and emits a tree on output. Built on the cyclic-graph technique
(Option A), but encapsulated.

- **Pros**: cleanest user-facing API for any future tree-shaped
  source; one canonical answer.
- **Cons**: largest engineering cost; design needs validation
  against more than one use case before committing the API surface;
  risks over-fitting to HN's specific tree shape.

## Recommended sequencing

1. **Probe Option A on `hn_shape`** — smallest victim (~140 lines,
   no LLM, no aggregator, output is just shape metrics). Convert
   the recursive `vt-exec` fetcher into a cyclic-graph fetch-node
   step plus a tree-reassembly aggregator. No Datapotamus changes
   required for the probe itself.

2. **Confirm visualization improves** — compare the event count
   and the visualizer rendering for one story before/after.
   Specifically:
   - Per-node timestamps cluster correctly (concurrent fan-out
     rather than serial recursion).
   - Total wall-clock unchanged within noise.
   - Tree output identical (deep-equality test).

3. **Decide on Option C extraction** — based on whether the cyclic
   shape was natural enough to expose directly, or merits hiding
   behind a combinator. If two of {hn_density, hn_drift, hn_tempo,
   hn_typing} would benefit from the same shape, extract.

4. **Roll out** to the remaining four pipelines. Each pipeline
   conversion is mechanical once the pattern is settled.

If Option A turns out to be too invasive (e.g., the
tree-reassembly aggregator is awkward), fall back to Option B
(cheap, partial visibility) as a stopgap.

## Verification

For each converted pipeline:

- **Event count grows** from O(stories) to O(tree-nodes).
  A 30-story run with average tree size 200 should produce ~6000
  per-node events on top of the existing summary events.
- **Output deep-equals pre-conversion** for the same input
  (cache-hit re-run, same trees).
- **Visualizer concurrency view** shows ~`fetch-workers` parallel
  active fetches at peak, not 1.
- **Pubsub subscribe with `[:* "scope" :*  ... "step" :fetch-node]`**
  filters to a per-node firehose.

Property test (suggested):
- Tree-reassembly is associative on partitions of node arrivals
  (out-of-order arrivals must reassemble identically to in-order).

## Implications for new pipelines

Until the fix lands, **new pipelines must not reach for `vt-exec`
inside step bodies**. Acceptable patterns for data-driven
parallelism right now:

- Single-level N-way fan-out: `msg/children` inside a step, then
  `c/stealing-workers` downstream. (The `emotion` plan uses this
  for paginated fetches — it does not introduce a new instance of
  the hole.)
- K-copy parallelism: `c/workers` / `c/stealing-workers`.
- Static-port scatter/gather: `c/parallel`.

If a new pipeline genuinely needs recursive tree fetch, write it
inline with a TODO note pointing at this writeup and accept the
trace hole as known-broken until Option A/B/C lands.

## Out of scope for this writeup

- Whether to also extract a shared `tree-fetch` helper namespace
  for HN specifically (a separable concern from the Datapotamus
  expressiveness question).
- Whether `vt-exec` should be banned globally (no — it has
  legitimate use inside the runtime; the prohibition is on using
  it as a substitute for combinators in pipeline code).
