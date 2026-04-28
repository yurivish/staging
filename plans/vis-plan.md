# Podcast → Datapotamus viz: design notes

## Status: what's already in place

- `toolkit.datapotamus.viz` is a **fully general** multi-flow live visualizer:
  - subscribes to a shared pubsub
  - reduces `:recv/:success/:failure/:send-out` events into per-path counters keyed by flow-id
  - polls `caf/ping` on the live `core.async.flow` graph for input-channel buffer counts (queue depth)
  - renders cards over a topology tree built from any `step/step?` shape via `from-step`
- `demo.server` already wires this up at `/viz` for `hn/build-flow` (one button, one flow).
- The podcast pipeline already publishes everything needed: each stage runs through `flow/run-seq` with a stable `:flow-id` (`stage-a`, `stage-b-<task>`, `stage-c`) and there's a shared `run-pubsub` made inside `extract!`.

## What's specialized vs general

Nothing in `viz` is specialized to hn or any pipeline. The specialization lives in **demo's `viz-start-handler` glue code** — 6 manual steps coupling viz to one flow:

1. mint flow-id
2. build step def (and supply title)
3. `register-flow!` with `from-step` topology
4. `flow/start!` with shared pubsub + flow-id
5. pull `::flow/graph` out of the handle and `attach-handle!`
6. inject, await, stop, `mark-complete!`

For any new flow we want to watch, we'd repeat steps 3, 5, and 7 verbatim. That's the *only* coupling.

## What blocks the podcast integration today

Three concrete gaps, in increasing order of effort:

a. **`extract!` makes its own `run-pubsub`.** Needs a `:pubsub` option so the demo can pass `viz-pubsub`.

b. **Stage step defs are private.** `build-graph` is a private fn inside `podcast.flow` (line 117) and `podcast.tree-resolve` (line 679). Demo can't call `from-step` on them. Fix: lift each as a public `stage-a-graph` / `stage-c-graph` / `tree-resolve-graph` (same pattern as `hn/build-flow`).

c. **`flow/run-seq` doesn't surface its handle.** Without the live graph reference, `attach-handle!` (queue depth via `caf/ping`) can't be called. In-flight counts (`recv − success − failure`) still work without it. Either add an `:on-handle` callback to `run-seq`, or have callers use `start!/inject!/stop!` directly.

## Why three flows, and what would unify them

Stage A → Stage B → Stage C, with one architectural barrier: **Stage C's worker closes over Stage B's registry** (`(get registries-by-task task)` at step-construction time, podcast.flow:81). Once captured, the step is fixed. To unify, the registry must flow as **data**, not as a closed-over value.

Stage B itself is a tree merge-sort barrier — needs all of A's per-task mentions before starting; emits one terminal registry per task. The "all of A is done for task T" moment is not currently a value downstream nodes can wait on as data.

Stage C also needs the original chunks alongside the registry. Today that's free (chunks are an ambient `let`-binding); in a unified flow, chunks must flow to C too.

## Algebraic check: is anything missing from the primitives?

Datapotamus's primitive is `handler-map`: a multi-port stateful **Mealy machine** `(S × ⊔ᵢTᵢ) → S × P(⊔ⱼUⱼ)` with init. `step/step` is sugar over it.

Composition forms a **symmetric monoidal category** with loops (a dataflow PROP):

- `serial` = `∘`
- `beside` = `⊗`
- `connect` adds arbitrary edges (incl. loop-backs, as in tree-resolve)
- `inline-subflow` boxes a sub-DAG as a single scoped object

`handler-map` is **universal** in this category. Mealy + multi-port + nondeterministic emission + recursive composition is expressively complete for any causal stream computation with bounded state and finite port arity.

So the unification is **derived combinators**, not new primitives:

| Combinator | Categorical role | Implementation |
|---|---|---|
| `accumulate-by` | catamorphism (fold) over a per-key stream until end-marker | one `handler-map`: state `Map<K, Buf>`, `:on-data` appends, `:on-done` flushes |
| `join-by-key` | pullback over a key projection (keyed product of two streams) | one `handler-map` with `:side`/`:main` ports; state holds keyed side values + buffered main |
| `subflow-collect` | Kleisli unit for a "stream → value" monad — run sub-flow to quiescence, emit one terminal | thin wrapper over `inline-subflow` + `accumulate-by` on `:final` |

All sit in `toolkit.datapotamus.combinators` next to `workers` / `stealing-workers`, which are the same kind of named-pattern sugar.

### One soft frontier

`:done` is flow-wide. Every keyed combinator has to invent its own in-band sentinel ("end-of-key K") to express per-key completion. The combinators above can hide this internally — convention, not power gap. Lifting per-key-done to a first-class envelope kind alongside `:data`/`:signal`/`:done` would let multiple combinators share one mechanism. Optional polish, doesn't gate the work.

## Unified architecture sketch

```
input {tasks chunks paragraphs}
   │
   ▼
explode→(task,chunk)
   │
   ▼
A-workers ──► mentions per (task,chunk)
   │
   ▼
accumulate-by :task ──► one {task, mentions} per task
   │
   ▼
tree-resolve subflow ──► one {task, registry} per task
   │
   ▼  side feed
   ▼
join-by-key (:task) ◄── (task,chunk) main feed (replayed from input fork)
   │
   ▼
C-workers ──► records per (task,chunk)
   │
   ▼
accumulate-by :task ──► aggregate (D, pure)
```

Stage B becomes either a `subflow-collect` node or a regular `serial` segment that emits `:final` per task. The pipeline returns one step def from a public `build-flow` entry point — same shape as `hn/build-flow`. Demo wires it identically.

## Plan options

In increasing order of scope:

### A. Smallest diff: viz with three flows

- Lift `build-graph` to public `stage-a-graph` / `stage-c-graph` / `tree-resolve-graph` (no behavior change).
- Add `:pubsub` option to `extract!` (default to a fresh one when omitted).
- Demo handler: register topology for each anticipated fid via `from-step`, run `extract!` with `viz-pubsub` in a virtual thread, mark complete after return.
- Optional: `:on-handle` callback on `run-seq` for queue depth via `attach-handle!`. Without it, in-flight counts still work.

Files touched: `podcast/flow.clj`, `podcast/tree_resolve.clj`, `podcast/core.clj`, `demo/server.clj`. Maybe `toolkit/datapotamus/flow.clj` if (c) is included.

### B. Combinators only

- Add `accumulate-by`, `join-by-key`, `subflow-collect` to `toolkit.datapotamus.combinators` with tests.
- Leave `extract!` orchestrating three flows. Viz integration uses option A.
- Unification is a follow-up that can ship independently.

### C. Combinators + unified pipeline + viz

- Add the three combinators (with tests).
- Refactor `podcast.core` so the pipeline is a single graph returned from `build-flow`. Eliminate the cross-stage closure on the registry.
- Demo wires it like `hn/build-flow` (one register, one start, one mark-complete).
- Viz works for free; the whole pipeline appears as one section with nested cards.

Files touched: `toolkit/datapotamus/combinators.clj` (+ tests), `podcast/core.clj`, `podcast/flow.clj`, `podcast/tree_resolve.clj`, `demo/server.clj`. Possibly retire some of the orchestration in `extract!` in favor of pure pre/post around `flow/run-seq`.

## Verification

- `viz` shows live cards for each registered fid as `extract!` runs against a small input (`doublespeak.json` with a tight slice).
- Stage A cards show in-flight that grows then settles to 0.
- Stage B cards (per task) show the merger's loop activity.
- Stage C cards show in-flight and settle to 0.
- For options that include the `:on-handle` plumbing: each card's "queued N" reflects messages sitting in input buffers between procs.
- Existing podcast tests (`podcast/multi_task_test.clj`, `podcast/tree_resolve_test.clj`) still pass.
- For options B/C: new combinator tests cover accumulate-by ordering, join-by-key buffering before side arrival, subflow-collect terminal emission.
