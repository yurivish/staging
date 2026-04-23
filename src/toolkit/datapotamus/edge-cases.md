# Edge Cases and Validation

A running catalog of the system's known edge cases, their current
disposition, and a design sketch for the whole-program analysis pass that
would close the remaining structural concerns.

This document is a companion to `combinators.md` — that file tracks
combinator design questions; this file tracks system-level correctness
properties and what we do to enforce them.

---

## Currently enforced

Things the runtime rejects or reports, with the mechanism that does so.

### Handler output ports must match declared `:outs`

`wrap-proc` captures each step's declared `:outs` at factory time (the
c.a.flow 0-arity call is pure per contract). When a handler returns
`[s port-map]`, wrap-proc asserts every non-namespaced key in
`port-map` is a member of `declared-outs`. Typos like
`(dp/emit m :ot result)` or `{:out-port [...]}` produce an ex-info
with the step id, unknown port(s), and the declared port set — caught
by wrap-proc's existing try/catch and surfaced as a normal `:failure`
trace event. The failed message does not propagate downstream.

### `inject!` target step and port must be declared

`start!` builds a `::port-index` — `{sid {:ins #{port-kw} :outs #{port-kw}}}`
— by calling each instrumented proc's 0-arity. `inject!` validates
the resolved `flow-in` and `port` against this index before calling
`flow/inject`. Unknown step id → ex-info listing the bad id and all
known sids. Unknown port on a known step → ex-info listing the bad
port and that step's declared input ports. Validation happens before
the `:sent` counter bump and before the `:seed` pubsub publish, so
failed injects leave no phantom state.

### Token conservation across branching

`emit` (multi-child), `fan-out`, and `router` split the incoming
message's existing tokens across their N children via
`tok/split-tokens`, so any upstream zero-sum group's XOR invariant is
preserved through intermediate branching steps. This prevents
premature fan-in firings in nested topologies — see the
`nested-fan-out-fan-in-composes` regression test.

### Per-input-port `done` quorum

`wrap-proc` tracks which input ports of a multi-input step have
received `done`; the cascade fires only when all declared `:ins` have
closed. Partial closure of a single input port does not propagate
done downstream. See `done-multi-input-holds-until-all-inputs-closed`
in `datapotamus_test.clj`.

---

## Documented but not code-enforced

Cases where the runtime behaves consistently with the algebra but
doesn't catch user mistakes eagerly. The cost of catching them is
higher than the cost of documenting them.

### Group-id collisions within a flow

Two fan-out-like steps that share a group-id name in the same flow
cause confusing downstream behavior. Their `gid` values differ
(because each fan-out encodes the parent msg-id in the gid), but
`fan-in`'s prefix filter matches both. Each distinct gid becomes its
own group in fan-in's state map and fires independently — so the
fan-in emits multiple merges per distinct parent source, instead of
one coherent rejoin. The algebraic behavior is defensible; the user
expectation usually isn't.

Mitigation: pick distinct `group-id` values per fan-out within a
single flow. The docstrings of `fan-out` and `fan-in` both note this.

A structural check is sketched below in the analysis section.

### Interleaved vs. well-nested fan-out/fan-in pairs

Interleaved pairs (`FO1 → FO2 → FI1 → FI2`) produce algebraically
correct tokens — XOR is abelian so the groups compose regardless of
order — but the resulting merged data vectors nest in fire-order at
each fan-in rather than user structure. Well-nesting
(`FO1 → FO2 → FI2 → FI1`) produces the expected `[[a b] [c d]]`
shape.

Mitigation: prefer well-nesting. See `fan-out`'s docstring and the
"Nesting fan-out/fan-in — resolved" section in `combinators.md`.

### Zero-input steps (`:ins {}`)

A step with no input ports is inert — nothing can connect to it. The
failure mode is "my flow does nothing," which users notice quickly.
Construction-time validation is cheap to add but hasn't been, given
the low cost of diagnosis.

### `fan-in` with a group-id no upstream emits

The fan-in will accumulate indefinitely — no messages ever match its
prefix, so the group never fires. Today this is silent. The natural
place to surface it is when `fan-in` integrates `done`: receipt of
done with pending groups is exactly the moment to emit a warning or a
partial merge. See "Pending: fan-in behavior on done receipt" in
`combinators.md`.

---

## Future direction: whole-program analysis

The enforced checks above are local — wrap-proc validates handler
output per-step; inject! validates the target per-call. Several
potential bugs are **global structural properties** and can't be
caught locally.

### Motivation

Structural bugs surface as confusing runtime behavior:

- Premature or multi-fire merges (group-id collision).
- Stuck groups that never close (bad pairing, typo in `fan-in`'s
  group-id).
- Unreachable steps that look fine locally but receive no data.
- Interleaved fan-out/fan-in pairs producing unexpected nested data.

Catching these at `start!` — before any data flows — would turn a
whole class of mystifying bugs into immediate, named errors.

### Metadata convention

Tag steps that participate in token-coordinated groups with metadata
on their returned step map:

```clojure
(with-meta
  (dp/fan-out :split 3)
  {::dp/kind :fan-out :group-id :split})
```

Built-in `fan-out` / `fan-in` would emit this tag natively. User
custom fan-out-flavored helpers would opt in explicitly:

```clojure
(defn my-custom-fan-out [id gid n]
  (with-meta
    (dp/step id ...)
    {::dp/kind :fan-out :group-id gid}))
```

A thin helper could reduce boilerplate:

```clojure
(dp/tag-fan-out step :group-id)  ; returns step with ::dp/kind :fan-out meta
(dp/tag-fan-in  step :group-id)
```

### Analysis pass at `start!`

Gated behind `(start! flow {:validate? true})` initially — mandatory
once battle-tested. The pass walks the instrumented procs and their
`:conns`:

1. **Collect tagged steps** — `(for [[sid pfn] procs :let [m (meta (:factory pfn))] :when m] ...)`. 
   (Implementation note: meta has to survive the instrument-flow pipeline, or be
   re-attached from a side table — tbd.)
2. **Group-id uniqueness** — no two `::dp/kind :fan-out` steps share a
   `:group-id` in one flow. Report: `{:kind :fan-out-collision :group-id :g
   :steps [:fo1 :fo2]}`.
3. **Pairing** — every `:fan-out :g` has at least one matching
   `:fan-in :g` reachable via conns from it; and vice versa. Report:
   `{:kind :unpaired-fan-out :group-id :g :step :fo1}`.
4. **Well-nesting** — topo-order the fan-out/fan-in steps along the
   conn graph; walk in order maintaining a stack. On `fan-out :g`
   push `:g`; on `fan-in :g` require `:g` is stack-top and pop. If
   the pop is a different id (or the stack is empty), report
   interleaving: `{:kind :interleaved-fan :at [:fo-B :fi-A]}`.
5. **Conn sanity** — every `[sid port]` in `:conns` references a
   declared port on a declared step. Essentially the `inject!`
   validator applied to the flow's wiring at assembly time.
6. **Reachability** — from the flow's declared `:in`, every step
   should be reachable by following conns. Report unreachable steps
   as `{:kind :unreachable :step :orphan}`.

### Error API

`start!` could either:

- Throw an `ex-info` with a `:kind :validation-failed` and a `:report
  [...]` vector of individual findings. Easy for callers; aborts hard.
- Return a handle in a `:failed` state with the report accessible
  via a new `::validation-report` key. Enables tooling that wants to
  inspect multiple issues and present them together.

Leaning toward the former for simplicity. Revisit if tooling appears.

### Opt-in vs mandatory

Start with opt-in via `:validate? true` so user flows that
deliberately exploit unusual shapes (testing internals, exotic
graphs) aren't broken. Promote to default-on after a real-world
burn-in period. Users who want to skip the check even after that can
pass `:validate? false`.

---

## Non-goals (current)

- **Static type-checking of message payloads.** Malli-style schemas
  per port could catch more bugs but are a substantially larger
  commitment.
- **Cycle detection.** Loops are legitimate (see `loop-until` in
  `combinators.md`). An analyzer should tolerate cycles, not flag
  them.
- **Upstream backpressure validation.** c.a.flow's channel sizing
  handles this at runtime; no static analog planned.
- **Determining at construction time that a `fan-in` will receive the
  right tokens.** That's dataflow-dependent and generally undecidable;
  pairing + uniqueness above covers the structural piece.

---

## New frontiers: algebraic framing and composability

This section is a reflection on the shape of the primitives we've
built, what's clean about them, what remains rough, and which lessons
from the big stream processing systems are worth importing even at
our "single machine, single process" scope.

The intuition driving it: a single-JVM dataflow system is in many
ways a miniature distributed system. Messages cross thread boundaries
instead of network hops, queues are in-memory channels instead of
brokers, but the questions are the same — how does termination
propagate? how are failures routed? what does "exactly once" mean
when there's no exactly-once? The goal is to pick a minimal set of
primitives that *learns* the answers those systems converged on,
without taking on their operational complexity.

### Lineage

Most of what's in this package can be traced to a specific prior
system or paper. Attribution and what we inherited:

| System | What it gave us |
|---|---|
| **Apache Storm** (Nathan Marz, 2011) | The XOR ack tree. Our `tokens` are structurally Storm's per-tuple tracking, repurposed at the operator-semantic layer (fan-in) rather than the transport layer (at-least-once delivery). |
| **Reactive Streams** (Kriens/Lightbend, 2014) | The `onNext` / `onComplete` / `onError` signal triad. Our `data` / `done` / `failure`-event separation mirrors this; `signal` is our additional primitive for coordination-only. |
| **Kahn Process Networks** (Gilles Kahn, 1974) | Bounded FIFO channels as the inter-step medium, blocking reads. core.async.flow's channel model is a descendant. |
| **CSP / Go** (Hoare, 1978; Go, 2009) | First-class channel close. Our `done` is the "close" signal explicitly present in the message stream rather than a channel API. |
| **Apache Flink / Beam** (2014–) | Watermarks + windowing. We haven't adopted either, but they're the right answer for event-time reasoning if we ever grow that way. |
| **Jepsen / Kyle Kingsbury** | `fcatch` — lift exceptions into the data domain so they become first-class routable values. |
| **Akka Streams / Project Reactor** | The "blueprint vs. materialized graph" distinction. A dp step map is a blueprint; `start!` materializes. |
| **Differential / timely dataflow** (Frank McSherry, Naiad) | Timestamps + frontiers for incremental computation. Not adopted; worth knowing exists if incremental reasoning ever matters. |

### Where the algebra is currently clean

A handful of invariants compose well enough that they feel load-bearing
and unlikely to need revisiting:

1. **Three-case message sum** (`data` / `signal` / `done`) with
   key-absence as the discriminator. Structurally this is the stream
   coalgebra `Msg = Data | Signal | Done`, expressed as a disjoint
   union at the envelope level. The three cases are exactly the
   "payload carrier," "coordination-only carrier," and "end-marker"
   roles — and every stream-processing tradition has landed on some
   version of this triad.

2. **Tokens form an abelian group** (XOR on `uint64`, with `0` as
   identity). Associative, commutative, every element its own
   inverse. This is the strongest possible algebra for a "conservation
   of mass" invariant — strictly more expressive than what nesting
   requires — and it's the reason `tok/split-tokens` + `tok/merge-tokens`
   compose freely. Signal materializes the identity; fan-out is a
   tensor-decomposition morphism; fan-in is its dual.

3. **Token conservation is a free law**, not something users have to
   enforce. Whenever any primitive (emit, fan-out, router) produces
   N children from one input, it calls `tok/split-tokens` on the
   parent's tokens. The XOR across all children equals the parent's
   tokens *by construction*. Downstream fan-ins see complete token
   mass regardless of path.

4. **Per-input-port `done` quorum** makes the multi-input case
   uniform with the single-input case: a step's outputs close when
   *all* inputs have closed, and the wrap-proc layer manages this
   without user involvement. Reactive Streams' `onComplete`
   semantics, applied uniformly.

5. **Failures as first-class routable data** (`fcatch`). Exceptions
   that would otherwise escape become values downstream steps can
   match on. This is the ZIO/Cats-Effect "error channel" pattern,
   opt-in at the fn level rather than implicit at the step level.

### Where the algebra could be cleaner

Things that work but have a slight ad-hoc-ness to them:

1. **Message sum type is nominal, not structural.** `data?` / `signal?` /
   `done?` are predicates over key presence in a plain map. This is
   deliberately lightweight (no type system, no defrecord commitment),
   but it does mean the type split lives in convention rather than in
   the compiler. A defrecord migration — `DataMsg` / `SignalMsg` /
   `DoneMsg` as distinct records — would make the sum structural and
   enable `instance?`-based dispatch without giving up the key-absence
   predicates. A deferred choice; see the note at the top of
   `datapotamus.clj`'s envelope section.

2. **Composition is untyped.** The category of steps is formally a
   monoidal category, but its objects (port types / message shapes)
   are implicit. Conns connect any out-port to any in-port regardless
   of what flows through. Adding malli schemas per port would elevate
   the category to a typed one — caught at `start!` — but is a
   substantial design commitment.

3. **Cycles lack principled treatment.** Loops work (see `agent-loop`
   test) but only because core.async.flow's channels happen to allow
   them. The mathematical formalism is a **traced monoidal category**
   (Joyal-Street-Verity, 1996), with a trace operator `tr` that
   closes an output back to an input. If `loop-until` lands, adopting
   the trace laws explicitly — rather than relying on the back-edge
   happening to work — would give loops the same structural guarantees
   the acyclic primitives already have.

4. **Arrival order is scheduler-dependent.** This is documented but
   philosophically annoying: the same flow with the same input could
   produce different data vectors at a fan-in depending on scheduling.
   For replay / testing / determinism, encoding position into the data
   payload and sorting at fan-in is the user's responsibility. A
   principled fix would lift positional annotations into tokens
   themselves (see combinators.md Q2, decided against).

5. **The handler output-map is a mixed bag.** Port keys + namespaced
   control keys (`::merges`, `::derivations`) share a map. This works
   but means `wrap-proc` has to understand both layers. A cleaner
   shape would separate the data plane (`{:emissions {port [...]}`)
   from the trace annotations (`{:merges [...] :derivations [...]}`).
   Engineering churn; deferred.

### Edge cases remaining for full compositional correctness

What the system does not yet handle principally, roughly ordered by
how much it matters in practice at single-machine scale:

#### Cancellation propagation through in-flight work

Today `(:cancel ctx)` is a promise delivered on `stop!`. A step doing
blocking work (HTTP call, SQL query, long-running CPU loop) won't
notice unless it's written to check. There's no convention for
"cooperative cancellation" across spans, virtual threads, or nested
futures.

The pattern Reactive Streams / ZIO got to is: every operation
carries an implicit cancellation scope, and *all* built-in combinators
honor it. For us, the minimum discipline would be: spans automatically
observe `(:cancel ctx)` and short-circuit on delivery; `vt-map` (the
virtual-thread helper) interrupts worker threads when cancellation
fires. Not critical until someone has a long-running graph they want
to halt cleanly.

#### Side-effect idempotency under retry

`fcatch` lifts exceptions to values, but doesn't address the problem
that *if the step's side effect already happened* (DB write, external
API call), retrying duplicates. `retry` combinator has the same issue.

The stream-processing canon's answers:
- **Exactly-once via snapshots** (Flink) — needs distributed consensus
  or single-process with a transaction log. Out of scope.
- **Idempotency keys** (Stripe, payment systems) — every side effect
  tagged with a user-supplied key; the sink deduplicates. A convention
  we could adopt (msg-id or data-id as the natural key) without
  building anything.
- **Transactional outbox** — side effects gated by a local transaction
  that also records the ack. Requires DB coupling.

At our scope, documenting "side effects should be idempotent and use
msg-id as the dedup key" is probably sufficient. Formalize later if
stakes rise.

#### State persistence across crashes

Step state lives in Clojure atoms / local bindings. A JVM restart
loses it. For single-seed `run!` this is irrelevant; for
long-running `start!`/`inject!` graphs with accumulated fan-in state
or stateful aggregations, it's a real gap.

The arc here is: eventually a step could declare `:state-persistence
:lmdb` (we have `toolkit.lmdb` sitting right there) and `wrap-proc`
would serialize `s` to disk on transitions. Out of scope for the core;
useful to note as a natural integration point.

#### Timing: event time vs processing time

We have no notion of event time. `:at (now)` on events is wall-clock
at receipt, not when the data was generated. Windowing combinators
(tumbling / sliding / session) need event-time, watermarks, and
allowed-lateness. Beam's windowing API is the canonical treatment.

At single-machine scope for batch-adjacent use cases, this doesn't
matter. If streaming sources enter the picture (kafka, logs), this
is the gate for any time-based aggregation.

#### Backpressure as a first-class signal

c.a.flow's channels have bounded buffers, so backpressure happens
via blocking writes. We don't surface this as an observable at the
dp layer — you can't write a combinator that says "reduce upstream
fan-out when buffers fill." Reactive Streams makes demand an explicit
signal; Akka Streams' `OverflowStrategy` is a user choice per stage.

Probably not worth adopting at this scope unless specific flows start
exhibiting pathological buffering.

#### Memory admission control

A flow emitting many large messages could OOM the JVM. There's no
admission control — nothing rejects or sheds messages when memory
pressure hits. Streaming systems usually handle this with spill-to-disk
or explicit drop policies. Conscious non-goal for now; if it bites,
the answer is likely "use smaller payloads" rather than "build
admission control."

#### Duplicate detection / replay

Our msg-ids and data-ids are UUIDs but there's no framework-level
dedup. A flow re-injected with the same seed runs again; two seeds
with the same data produce two runs. For deterministic replay (useful
in tests, debugging, audit), we'd need:
- Deterministic UUID generation (seeded, or content-based hashing) so
  msg-ids are stable across runs.
- Event log that records input → output pairs at each step.

Neither feels load-bearing now but both are cheap add-ons if needed.

### What's portable if ambition grows

If the scope later expands — long-running pipelines, multi-process,
actual streaming workloads — the patterns most directly importable
from the lineage, in order of plausible adoption:

1. **Flink-style watermarks** for event-time reasoning. Easy to layer
   on: a new message kind (or a token key convention) carrying a
   monotone timestamp; windowing combinators key off it.
2. **Beam-style side outputs** as named-port emissions. We already
   have named ports; formalizing the convention (e.g. `:err`, `:dlq`,
   `:metrics` as reserved secondary outputs) is near-free.
3. **ZIO-style error channel** as a first-class reserved port. Every
   step has an implicit `:err` that wraps any thrown exception.
   Generalizes `fcatch` to the step level.
4. **Akka Streams' materializer pattern** — already half-present in
   the `step-map → start! → handle` flow. A full materializer would
   let different runtimes (sync for tests, c.a.flow for production,
   a distributed runtime eventually) share blueprints.
5. **Flink-style state backends** for checkpointed state, if long-
   running flows need crash recovery.

### Deliberate non-goals

Things we are not trying to solve, and why single-machine scope
makes that honest rather than lazy:

- **Exactly-once delivery semantics.** Requires distributed consensus
  or a transaction log. At-least-once is achievable but expensive.
  At-most-once (current default) is fine for most single-machine
  use where duplicates matter less than latency.
- **Horizontal scaling.** core.async.flow scales within a JVM.
  Scaling beyond means adopting Kafka or similar — a different
  system.
- **Distributed consensus.** No partitions, no leader election, no
  replication.
- **Durable queues.** Channels are in-memory. Process restart loses
  in-flight messages.
- **Event-time reasoning out of the box.** Can be added; not worth
  paying for until a use case demands it.

### The thesis

The reason to keep this small is that *composability requires the
primitives to be honest*. Each primitive should have an invariant
you can state in one sentence; each combinator should preserve those
invariants without special cases. When we found `emit` duplicating
tokens, it was because a primitive had been quietly violating its
stated invariant; the fix was to restore the invariant, not to paper
over the downstream consequences. Same for `fan-out`'s parent-token
duplication. The more primitives we add, the more invariants cross
products explode. Better to ship few primitives that compose freely
than many that work individually but fail in combination.

Most of what's sketched above in "what's portable" is additive —
adopting any one doesn't require reworking the core. That's the
payoff for keeping the current core small and algebraically honest:
extending it later is a linear cost, not a redesign.

---

## What the core could SHED

The other direction of the same question: what's in the core today
that could be removed — reimplemented at a higher level with no
meaningful performance loss — making the foundation smaller and more
honest about what's truly primitive?

### Framing: "primitive" has two meanings

- **Technically primitive**: accesses framework internals users can't
  reach (wrap-proc behavior, channel lifecycle, pubsub machinery,
  token envelope construction).
- **Conceptually primitive**: represents a fundamental operation in
  the algebra — removing it would force users to reinvent the same
  wheel in every flow.

Almost every combinator in this package is technically derivable —
they're just specific patterns over `handler-factory` + `emit` +
port-maps. The question is which are conceptually primitive enough
that baking them in earns their keep, and which are convenience that
could live as user-space recipes.

### Classification of the current surface

**Framework primitives** — can't be moved; depend on internals:

| Name | Why it's primitive |
|---|---|
| `wrap-proc` | Defines the message control-plane (data/signal/done dispatch, counters, events). |
| `start!` / `inject!` / `stop!` | Channel / pubsub lifecycle. |
| `instrument-flow` / subflow flattening | Rewrites step-maps before c.a.flow sees them. |
| `step` / `proc` | Construct the factory shape c.a.flow consumes. |
| `handler-factory` (private) | Bridges user handler shape to c.a.flow proc-fn. |
| Envelope constructors (`new-msg`, `new-done`, `child-*`) | Produce messages with correct UUID/lineage shape. |
| `new-signal`-via-inject shape | Only inject! can produce a seed; signals-as-root need API. |
| `with-span` | Observability primitive; extends ctx scope. |

**Algebraic primitives** — technically derivable, but the mental model
depends on them being first-class:

| Name | Why keep |
|---|---|
| `fan-out` | The "split N ways, zero-sum token group" operation. Users *could* write it with `tok/split-value` + `child-same-data` + manual port-map building, but it's the canonical group-introduction morphism. Removing it hides the structure. |
| `fan-in` | Dual of `fan-out`. XOR-based group closure. Technically could be a user step with the same accumulator pattern, but the pattern is the whole point. |
| `emit` | Handles token-splitting across siblings — a non-obvious correctness invariant. Users writing manual port-maps forget to split. |
| `serial` / `merge-steps` / `connect` / `input-at` / `output-at` | The composition calculus. Nothing special about any one of them, but together they're *the DSL*. |

**Derived conveniences** — technically and conceptually derivable,
present for ergonomics:

| Name | What it actually is | Removable? |
|---|---|---|
| `run!` | `(-> step start! (inject! opts) await-quiescent! stop!)` | Candidate. Pure convenience for one-shots. |
| `passthrough` | `(step id identity)` | Candidate. One-liner. |
| `sink` | `(step id {:outs {}} (fn [_ s _] [s {}]))` | Borderline. Frequently used; multi-line to write inline. |
| `as-step` | `{:procs {id inner} :conns [] :in id :out id}` | Candidate. Plain map literal. |
| `router` | `(step id {:outs ports} (fn [_ s m] [s (apply emit m (mapcat (juxt :port :data) (route-fn (:data m))))]))` | Strong candidate. Emit does the token-splitting; router is just a pattern. |
| `retry` | `(fn [_ s m] (let [v (try-with-retries ...)] [s (emit m :out v)]))` | Strong candidate. A loop in a handler; no framework cooperation needed. |
| `fcatch` | `(fn [& args] (try (apply f args) (catch Throwable t t)))` | Strong candidate. Four lines; user-code-ish. |

### What I'd actually remove

If minimalism is the goal, this is the trim I'd make:

1. **`router`** — move to a "recipes" section in the docstring of
   `emit`. Its real content is "apply emit with a runtime-built
   port/data sequence." Once users see that pattern, they don't need
   `router` named.
2. **`retry`** — remove. It's a specific loop over `fcatch` that
   could be a 6-line user utility. Framework-level retries would
   need to integrate with token/done semantics (what happens if a
   retry loop fails mid-fan-in-group?), which isn't how the current
   implementation works anyway.
3. **`fcatch`** — remove, or move to a separate
   `toolkit.datapotamus.util` namespace. It's a pure-fn utility
   that has no dependency on anything in the dp core.
4. **`as-step`** — remove. The replacement is `{:procs {id inner}
   :conns [] :in id :out id}` which is identical and self-documenting.

### What I'd keep despite being "derivable"

These are derivable but earn their keep:

1. **`run!`** — every `start!`/`inject!`/`stop!` sequence is subtly
   error-prone (forget `await-quiescent!`, forget `stop!` on
   failure). The wrapper is a correctness win, not just ergonomics.
2. **`sink`** — a literal `(dp/step :id {:outs {}} (fn [_ s _] [s {}]))`
   is verbose enough that users would either open-code it badly or
   skip it and wire things to `passthrough`. Sinks are too common to
   make awkward.
3. **`passthrough`** — on the bubble. Keep if we keep `id-step`
   intent; drop if we don't. Near-zero weight either way.
4. **`fan-out` / `fan-in`** — technically derivable but conceptually
   the algebraic core. Removing would shift the mental model from
   "this system has groups" to "this system has tokens; here's how
   you use them to fake groups." Keep.

### Performance cost of removal

Near zero. Clojure compiles top-level `defn` forms the same whether
they live in the library or user code. There's no protocol dispatch,
no reflection, no boxing introduced by factoring these up to user
level. The cost is entirely ergonomic, not runtime.

The one nuance: removing `emit` would be a correctness cost (users
would routinely forget token-splitting across siblings). That's
exactly why `emit` is a framework primitive — it enforces an
invariant rather than just formatting a map.

### Recommendation

Keep the algebraic primitives (`fan-out`, `fan-in`, `emit`, `signal?`,
`done?`, `data?`, envelope constructors) and the composition calculus
(`serial`, `merge-steps`, `connect`, `input-at`, `output-at`, `step`,
`proc`). Keep `run!` and `sink` as ergonomic wins. Consider dropping
`router`, `retry`, `fcatch`, `as-step`, `passthrough` — each is a
pattern-level convenience that could be a docstring example or a
separate `dp.util` namespace without loss of expressive power.

The trim would take the core from 17-ish public combinators to
roughly 12, each with a crisp invariant and no overlap with the
others. Worth doing if we want the surface to feel more like an
algebra textbook than a toolkit. Worth skipping if the current
ergonomics are already earning their keep — which, honestly, they
mostly are. A deliberate choice to revisit once the system has more
real-world use to show which conveniences justify their size.
