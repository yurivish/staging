# Datapotamus — Rewrite Companion Spec

## Context

You are considering a from-scratch rewrite to deeply understand the code. This doc gives you a compact model of what the system **is** (not how the current code happens to express it) — enough to re-derive the implementation, plus observations on where the current implementation has more moving parts than the model demands.

Current file layout (for reference, not prescription):

| File | LoC | Role |
|------|-----|------|
| `src/toolkit/datapotamus/token.clj` | 32 | XOR u64 algebra — `split-value`, `split-tokens`, `merge-tokens` |
| `src/toolkit/datapotamus.clj` | 1141 | Everything else, in 8 Parts |
| `src/toolkit/datapotamus/combinators.clj` | 407 | Opinionated combinators atop the core |

The `ns` docstring at `datapotamus.clj:1-132` is already a reasonable starting spec. What follows extracts the conceptual skeleton, then flags where the code could be sparer without losing capability.

---

## Use cases to preserve

A rewrite is only legitimate if it keeps these working. Pulled from the tests:

1. Sequential pipelines with state (`serial` of `step`s, including `scan`-style folds).
2. 1-to-N fan-out with token-sealed group, and corresponding fan-in that fires when the XOR of arrivals closes the group.
3. Dynamic cardinality: `mapcat`-style emit-N-where-N-is-runtime (e.g., LLM splits response into chunks; downstream fan-in collects them).
4. Multi-input joins by lineage (`fan-in`) and by data key (`join-by-key`).
5. Conditional routing (`router`, `filter`) and per-branch error routing (`try-catch` to an `:err` port).
6. Retry of a pure function up to N attempts.
7. Loops with predicate exit (`loop-until`).
8. Nested subflows with scope-prefixed tracing (`as-step`).
9. Long-lived flows: `start!` once, `inject!` many times, poll counters, `stop!` gracefully.
10. `run-seq` — run a flow over a collection, attribute outputs back to inputs.
11. Streaming LLM-style: chunks arrive over time, get parsed/batched/summarized, finalize on quiescence. (This is the paradigmatic case — keep it as the design's anchor.)

---

## The four conceptual threads

Datapotamus is *the composition of four threads* that happen to interleave at runtime. A rewrite should keep these conceptually separable.

### Thread A — The algebra *(what flows)*

Three envelope shapes distinguished by key absence:

| Kind   | `:data` | `:tokens` | Meaning |
|--------|---------|-----------|---------|
| data   | yes     | yes       | A value plus its lineage+conservation info |
| signal | no      | yes       | A control pulse that preserves lineage without carrying a value |
| done   | no      | no        | Port-closure marker |

Signals and done are not sentinels inside `:data` — they are structural. (`nil` is a valid data value.)

**Tokens** are a map `{group-id → u64}`. u64s form an abelian group under XOR. Three composition laws hold per handler invocation (see `datapotamus.clj:41-59` — keep this docstring, it's correct):

1. 1-to-1 preserves tokens.
2. N-way split preserves XOR sum.
3. Merge XORs inputs' tokens together.

**Derivation helpers** (`child`, `children`, `pass`, `signal`, `merge`) return *draft* messages carrying `::parents` refs — an in-memory provenance DAG that only lives across one handler return.

**Synthesis** (`datapotamus.clj:447-531`) is a pure pass:
- Walk outputs backward through `::parents` to enumerate ultimate sources.
- For each source, split its tokens K-ways across the K emitted drafts that descend from it (`tok/split-tokens`).
- XOR-merge all slices flowing into each draft into that draft's final `:tokens`.
- Apply any stamped `::assoc-tokens` / `::dissoc-tokens` metadata (the escape hatch for custom group minting).

Output: `{port [final-msg ...]}`. The DAG ref is stripped; what downstream sees is flat immutable values.

> The whole library's novelty lives here. Everything else is glue around this algebra.

### Thread B — The program graph *(what the computation is)*

Pure data. No runtime.

A **step** is a map:

```clojure
{:procs {sid factory, ...}   ; factory :: (fn [ctx] proc-fn)
 :conns [[[from-sid port] [to-sid port]], ...]
 :in    sid-or-[sid port]
 :out   sid-or-[sid port]}
```

A step with one proc is still a step. Composition operators return steps:
- `serial` — chain, auto-wire `:out → :in`.
- `merge-steps` — union procs and conns, no wiring.
- `connect` — add one explicit conn.
- `input-at` / `output-at` — relabel boundary.
- `as-step` — wrap any step into a black box (makes nested scoping well-defined).

Subflows nest: any `:procs` entry may itself be a step, recursively.

### Thread C — The runtime *(what makes it go)*

Built on `clojure.core.async.flow`. The library owns the step→graph translation plus a per-proc wrapper.

**`wrap-proc` (`datapotamus.clj:755-819`)** is the central dispatch. For every incoming message it branches on envelope kind:

- **data** — call user handler; run synthesis; emit `:recv` / `:send-out*` / `:success` or `:failure` trace events; validate declared ports.
- **signal** — bypass handler; split tokens across all output ports (`split-across-children`); emit events.
- **done** — track per-input closure via a `done-ins` atom; forward `done` to outputs only when *all* declared inputs have closed.

**Instrumentation (`datapotamus.clj:825-898`)** recursively inlines subflows by prefixing proc ids with their parent sid (`a.b.c`), resolves endpoint refs through an alias table, and wraps each factory with pubsub scope plus `wrap-proc`.

**Lifecycle (`datapotamus.clj:900-1141`)**: `start!` → `flow/create-flow` + `flow/start`, attach error monitor + quiescence subscriber. `inject!` validates the step/port exists, routes an item, publishes `:inject`. `await-quiescent!` blocks on a promise delivered when counters balance. `stop!` cancels, stops, reads final events. `run!` / `run-seq` are conveniences for the one-shot / finite-collection special cases.

### Thread D — Observability *(cross-cutting)*

- **Trace events** keyed on two orthogonal axes: `:kind` (lifecycle role: `:recv :success :failure :send-out :split :merge :inject :flow-error :run-started`) × `:msg-kind` (`:data :signal :done`). Subjects use `:kind` only.
- **Scoped pubsub** (`datapotamus.clj:405-417`): a `ScopedPubsub` wraps a raw pubsub and prepends a scope prefix (`[:flow fid] [:step sid] ...`) to every subject. Subflows extend the scope; parent subscribers see inner events via glob match.
- **Counters** (`datapotamus.clj:368-381`): track `:recv`, `:sent`, `:completed`. Quiescence = all three nonzero and equal.

Thread D is the only thread most users *read* (via `events` and `counters`). Thread A is the one they have to *understand* to avoid token misuse.

---

## How the threads interleave

```
User handler
   │ returns [s' raw-outputs]
   ▼
Thread A (synthesis)        ← pure, no core.async
   │ returns {port [final-msg ...]}
   ▼
Thread C (wrap-proc)         ← envelope dispatch + error catching
   │ returns [s' {port [msgs]}]
   ▼
core.async.flow              ← message passing, buffers, backpressure
   │
   ▼
Thread D (pubsub events)     ← emitted at every step of A and C
```

**Thread B is consumed once, at `start!`**: it's traversed by instrumentation to build the flow graph, then forgotten.

**Thread A and Thread C are coupled at exactly two lines**: `wrap-proc` calls `user-step-fn` (which is a handler-factory closure), and the factory calls `process-outputs`. You can imagine drawing a clean line between "token algebra" and "envelope dispatch" across `process-outputs` → `handler-factory` → `wrap-proc`.

**Thread D is orthogonal to all three**. It's invoked at every phase boundary, but none of A/B/C change if you remove pubsub (the flow still works, you just can't see it).

---

## Essential surface (minimum viable rewrite)

If you were rewriting and had to keep only the irreducible primitives, I'd argue this is the whole contract:

**Messages**
- `new-msg` — root data msg
- `new-done` — done marker
- `signal?`, `done?` — predicates
- `child`, `children`, `pass`, `signal`, `merge` — draft constructors (the DAG is their only job)
- `assoc-tokens`, `dissoc-tokens` — escape hatch (stamp metadata)

**Tokens** (separate ns)
- `split-tokens`, `merge-tokens` (and `split-value` used by the former)

**Steps**
- `step` — 2-arity (pure fn) and 3-arity (full handler) forms
- `proc` — low-level (raw core.async.flow factory); keep because subflows need it
- `sink` — one-liner but frequently wanted
- `serial`, `merge-steps`, `connect`, `input-at`, `output-at` — composition

**Combinators**
- `fan-out`, `fan-in` — these *are* the algebra's user surface
- `router` — **removal candidate** (reducible to `step` + `emit`)
- `retry`, `fcatch` — **removal candidates** (pure user utilities)

**Runtime**
- `start!`, `inject!`, `stop!`, `await-quiescent!`, `events`, `counters`
- `run!`, `run-seq` — conveniences with real ergonomic value

**Observability**
- Scoped pubsub + event constructors. Counters.

Everything else in the file is glue around these.

---

## The machine, stripped

Forget the code for a moment. Here's the entire machine in one paragraph.

> You have a labeled DAG of **steps**. Each step is a state-ful transformer: it eats a message envelope and spits out (new state, outputs per port). Messages carry two decorations: **lineage** (which messages are my parents?) and **tokens** (an abelian-group element per coordination group — XOR on u64). The token invariant is that every step preserves group-XOR mass from input to output. Outputs aren't built directly; they're **drafts** — terms in a tiny free algebra (`child`, `children`, `pass`, `signal`, `merge`) whose leaves are the step's actual input messages. After the handler runs, a single pure **synthesis** fold walks each draft to its leaves, counts how many drafts reference each leaf, splits that leaf's tokens K-ways, and XOR-merges the slices back into the drafts. The result is a concrete message per draft, ready to flow on its port.

That's it. A state monad over a writer over a free algebra with a fold. Channels, pubsub, core.async are **how**; they aren't **what**.

## Five places where procedural gunk accumulates

Each one is a place the algebra leaks out of the code.

### G1. Envelope dispatch is a `cond` tree, not three arrows.

The algebra says: a step has three arrows, one per envelope kind.

```
on-data   : (ctx, s, data) → (s', drafts, events)
on-signal : (ctx, s)       → (s', drafts, events)
on-done   : (ctx, s, port) → (s', drafts, events)
```

The code says: one 4-arity function with a `cond (done? m) ... (signal? m) ... :else ...` that interleaves dispatch, defaults, tracing, and error handling. This is `wrap-proc` (`datapotamus.clj:755-810`). The three arrows are structurally invisible. Worse, the signal and done "defaults" are hard-coded into the cond branches — users cannot supply their own. That's why "handler reacts to signals" requires reaching for raw `proc`.

### G2. `done-ins` is an atom that doesn't need to be one.

Line 761: `done-ins (atom #{})`. This tracks per-input-port closure state. Algebraically it's ordinary per-step state — no different from the user's `s`. A pure encoding threads it through `s` alongside user state:

```
(on-done ctx {:framework {:closed-ins #{...}} :user ...} port)
  → if all inputs closed, emit done-drafts
  → else return state with closed-ins updated
```

An atom is there because the current shape lifts this concern *above* user state, but they're the same thing at the level of the algebra.

### G3. Tracing is a side-effecting printf, not a writer log.

The algebra says: each handler-map slot returns `(s', drafts, events)`. Events accumulate purely; the interpreter publishes them at the edge.

The code says: every phase boundary calls `sp-pub` directly — there are ~15 `sp-pub` calls spread across `wrap-proc`, `process-outputs`, `emit-event`, and lifecycle functions. This means:
- The "pure" synthesis pass (`process-outputs`, line 506) actually has side effects (emits `:split`/`:merge` via `emit-event` at line 501).
- Tests that reason about event counts are reasoning about interleaved imperative state.
- You can't unit-test synthesis without mocking pubsub.

A writer-log encoding returns `(drafts, [events])` from synthesis and lets the interpreter publish at one known boundary.

### G4. Two stacked closures for what is one function.

`handler-factory` (543-569) lifts `(ctx, s, data) → (s', outputs)` into a core.async.flow 4-arity proc-fn. Then `wrap-proc` (755-810) wraps *that* proc-fn into a 4-arity proc-fn with envelope dispatch and tracing. The sandwich:

- `handler-factory` fn: `([] spec) ([_] {}) ([s _] s) ([s in-port m] ...)`
- `wrap-proc` fn: `([] (inner)) ([arg] (inner arg)) ([s arg] (inner s arg)) ([s in-id m] ...)` — three pure forwarding arities (763-765) because the outer wrapper duplicates the shape.

The forwarding arities are not doing any work. They exist because the code is expressed as "wrapper around wrapper." A single function that takes `(id, ports, handler-map, ctx)` and directly produces the 4-arity proc-fn erases them.

### G5. Two-pass synthesis where one fold suffices.

`process-outputs` (506) does this:
1. `coerce-plain-outputs` — normalize plain values to drafts.
2. `collect-dag` (447) — backward walk to produce `[topo nodes sources]`.
3. `synthesize-tokens` (471) — iterate by source, split K-ways, merge slices into drafts.
4. `materialize-msg` — strip refs, apply stamped metadata.
5. `emit-event` — side-effect per node.

The algebra is: one backward fold from drafts to sources, assigning tokens during the walk using out-degree at each branch. Steps 2 + 3 are two passes over the same tree returning intermediates that feed each other; they can be one pass. Step 5 is a writer log (G3). Step 1 is sugar that could move into draft constructors (plain values become `child` drafts at construction, not coercion time).

## The Feynman-sparse form

Here's the whole machine in ~60 lines of pseudo-Clojure, with every piece of gunk removed:

```clojure
;; -- Free algebra ---------------------------------------------------

;; Draft constructors build terms in a free algebra over input messages.
;; Each draft carries ::parents (a vec of either drafts or real msgs).

(defn child     [parent data]         {::parents [parent] :data data})
(defn children  [parent datas]        (mapv #(child parent %) datas))
(defn pass      [parent]              {::parents [parent] :data-id (:data-id parent) :data (:data parent)})
(defn signal-d  [parent]              {::parents [parent]})  ; no :data
(defn merge-d   [parents data]        {::parents parents :data data})

;; -- Synthesis: one fold --------------------------------------------

(defn synthesize
  "Returns (concrete-msgs-per-port, [events]).  Pure."
  [drafts-per-port]
  (let [drafts     (mapcat second drafts-per-port)
        sources-of (memoize (fn visit [d]
                              (if (draft? d)
                                (into #{} (mapcat visit) (::parents d))
                                #{d})))
        out-degree (frequencies (mapcat sources-of drafts))]
    (for [[port draft] drafts-per-port]
      (let [tokens (reduce xor-merge {}
                     (for [src (sources-of draft)]
                       (xor-split (:tokens src) (out-degree src))))]
        [port (materialize draft tokens) (split-event ...)]))))

;; -- Step handler-map: five slots, defaults for four -----------------

{:on-data   (fn [ctx s data] ...user...)
 :on-signal (fn [ctx s]      [s (for [p outs] [p (signal-d (:msg ctx))]) []])
 :on-done   (fn [ctx s port]
              (let [closed (conj (:closed s) port)]
                (if (= closed all-ins)
                  [(assoc s :closed closed) (for [p outs] [p (done)]) []]
                  [(assoc s :closed closed) [] []])))
 :ports     {:ins ... :outs ...}}

;; -- Interpreter: core.async.flow adapter ---------------------------

(defn run-step [hmap envelope s]
  (let [dispatch     (case (kind-of envelope)
                       :data   #(:on-data   hmap ctx s (:data envelope))
                       :signal #(:on-signal hmap ctx s)
                       :done   #(:on-done   hmap ctx s (:port envelope)))
        [s' drafts evs1]      (dispatch)
        [concrete evs2]       (synthesize drafts (:tokens envelope))]
    [s' concrete (concat evs1 evs2)]))

(defn proc-fn [hmap pubsub]
  (fn ([]      {:params {} :ins (-> hmap :ports :ins) :outs (-> hmap :ports :outs)})
      ([_]     {})
      ([s _]   s)
      ([s port m]
       (let [[s' outs events] (run-step hmap (envelope-of port m) s)]
         (doseq [e events] (sp-pub pubsub e))
         [s' outs]))))
```

What disappeared: `handler-factory`, `wrap-proc` cond, the `done-ins` atom, `split-across-children`, `emit-event` side effects, forwarding arities, two-pass DAG walk. What showed up: one `run-step` function and one `proc-fn` adapter. Everything else is pure.

## What this frees up for users

- **Reactive signals.** A user who wants to flush a buffer on a tick supplies `:on-signal`. Today that requires raw `proc` and reimplementing envelope dispatch.
- **Participating in done.** `:on-done` can drain pending state (e.g., emit a final partial batch) instead of the framework dropping it. This is what combinators.clj's `batch` wants and can't have.
- **Pure testing of handler-maps.** `(run-step hmap envelope state)` is a pure function. No flow, no pubsub, no core.async. You drive it with synthetic envelopes and check the returned tuple.
- **Pure testing of synthesis.** `(synthesize drafts input-tokens)` is a pure function over the free algebra. Property tests become trivial.
- **One place to look for the interpreter.** When something goes wrong at runtime, there is exactly one function that does dispatch + tracing + lifecycle. Not three.

## What stays irreducibly procedural

Not everything is algebra; some things are just plumbing inherited from the environment.

- **core.async.flow's 4-arity proc shape.** We must satisfy it. `proc-fn` above is the minimal adapter.
- **Subflow inlining with prefixed ids.** `inline-subflow` at 846 is doing real work — flattening a nested step map into a single graph with unique proc ids. Can shrink but not disappear.
- **Endpoint resolution through alias tables.** `resolve-endpoint` at 861 handles "I connected from outside to a subflow's logical `:in`; where is the physical port?" Necessary for compositional subflows.
- **Backpressure and channel-level done propagation.** core.async.flow's job. Not ours.
- **Quiescence detection.** The counter dance at 945-952 is a real distributed-systems problem (when is the graph done?). Can improve (S-candidate: watch close instead of polling counters) but can't remove.

## The rewrite path

Four files + one unchanged. Each has a clearly defined algebraic role; nothing crosses lines.

| File | Role | Rough LoC |
|------|------|-----------|
| `msg.clj` | Envelope sum type + draft constructors + `synthesize` (pure fold) | ~150 |
| `step.clj` | Step records with `{:on-data :on-signal :on-done :ports}`, composition (`serial`, `merge-steps`, `connect`, `input-at`, `output-at`), sensible defaults for signal/done | ~120 |
| `trace.clj` | Event constructors, scoped pubsub, counters. Pure constructors + one-place publishing | ~80 |
| `flow.clj` | Subflow inlining, endpoint resolution, `run-step`, `proc-fn`, lifecycle (`start!`, `inject!`, `stop!`, `run!`, `run-seq`). Only file with a `core.async.flow` require | ~200 |
| `token.clj` | Unchanged | 32 |

**Order of construction:**

1. `token.clj` → keep as-is.
2. `msg.clj` → envelope + free algebra + synthesis. Unit-test the three token conservation laws as property tests over randomized draft forests. No core.async anywhere.
3. `step.clj` → the handler-map record, composition, defaults. Drive it with synthetic envelopes and the pure `synthesize` to confirm behavior end-to-end without a flow.
4. `trace.clj` → drop-in for event constructors and pubsub scoping.
5. `flow.clj` → interpreter. Port `start!`/`inject!`/`stop!`/`await-quiescent!`/`run!`/`run-seq`. This is the only place effects live.
6. Combinators: `fan-out` and `fan-in` are primitives (they exercise the token algebra in user-visible ways); `router`, `retry`, `fcatch` move to a `recipes.md` because they're ≤6-line patterns, not framework features.

Expected total: ~580 lines vs. 1580 today. The savings trace back to the five pieces of gunk identified above — removing them is not micro-optimization, it's restoring the algebra.

---

## Verification for the rewrite

A rewrite is complete when:

- `datapotamus_test.clj` (or its rewritten equivalent) passes against the new implementation. Keep the test file intact during the rewrite — it's your north star for behavior.
- Token conservation property tests pass for randomized DAG shapes (write these if they don't exist; the 4 tests in `token_test.clj` cover the primitives but not end-to-end).
- The LLM-streaming canonical example (pull from `combinators_test.clj` or write: `mapcat`-split → per-chunk-transform → `fan-in` → sink) produces the same output events as the current implementation.
- `run-seq` over a 10-input collection attributes every output to the right input under both `fan-out`/`fan-in` and `router` patterns.
- Subflow scoping: nested `as-step` produces scope segments `[[:flow f1] [:step s1] [:flow f2] [:step s2]]` in events.

---

## Critical files to have open while rewriting

- `src/toolkit/datapotamus.clj:1-132` — the existing ns docstring, already a half-spec. Treat it as the invariant you're preserving.
- `src/toolkit/datapotamus.clj:41-108` — the token conservation laws + escape hatch sketch. This is the hardest-to-re-derive part.
- `src/toolkit/datapotamus.clj:447-531` — synthesis. Re-deriving this cleanly is the main intellectual exercise.
- `src/toolkit/datapotamus.clj:755-819` — `wrap-proc`, the three-branch dispatch.
- `src/toolkit/datapotamus/token.clj` — 32 lines, copy it.
- `src/toolkit/datapotamus/edge-cases.md` — read the "what the core could shed" section (~496-621) before cutting anything; it has the prior reasoning on removal candidates.
- `src/toolkit/datapotamus_test.clj` — your behavioral regression suite.
