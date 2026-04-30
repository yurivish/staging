# Simplification ideas — datapotamus core (excluding combinators)

## Summary

Eighteen items, organised: seven headline findings (each frames a
question, gives a recommendation, sometimes a counter-argument),
seven architectural judgment calls where I lean keep-as-is and
explain why, four cosmetic notes, and a 7-item concrete shortlist
if you want to act on any of it.

The biggest themes:

- **Dead public surface** — `msg/data?`/`signal?`/`done?` have zero
  callers; the seven `trace/*-event` constructors are public but
  only `flow.clj` calls them.
- **Dual identity reps** — scope-segment ids are inconsistently
  keyword-or-string; downstream consumers branch on `keyword?`
  to normalise.
- **Counter API asymmetry** — `record-inject!` exists separately
  from `record-event!` purely because injects don't have an event;
  collapsing into one dispatch table is straightforward.
- **Idempotent-but-redundant lineage** — `inject-attribution`'s
  case list processes `:split`, `:merge`, AND `:send-out`. A msg
  goes through synthesis getting both a `:split`/`:merge` and a
  `:send-out` with identical parents; the fold double-handles it
  harmlessly. Probably one of the cases can be dropped.

**The piece that most invited simplification — the seven trace
event constructors — I argue *against* collapsing.** A generic
`event-of` helper would lose the schematic-by-construction
property: each constructor encodes which extra fields are valid
where (`:in-port` only on done recv, `:port` only on send-out,
`:error` only on failure). Verbose but auditable. This is exactly
the spot where the simplify instinct conflicts with
audit-by-construction, and where I'd resist the simplify instinct.

---

Findings from a simplify-pass over `msg.clj`, `step.clj`, `flow.clj`,
`trace.clj`, `counters.clj`, `token.clj`. Combinators excluded.
The user is more interested in the **questions** that recur than in
the specific decisions, so each finding is framed around the judgment
call, with a brief recommendation.

Files surveyed (1,215 lines total):

| File          | LOC | Role                                              |
|---------------|-----|---------------------------------------------------|
| token.clj     |  32 | XOR token algebra                                 |
| counters.clj  |  57 | LongAdder quiescence accounting                   |
| trace.clj     | 137 | Event constructors + scoped pubsub                |
| step.clj      | 219 | Pure handler-map / step composition               |
| msg.clj       | 233 | Envelopes + free algebra + synthesis              |
| flow.clj      | 537 | core.async.flow interpreter + lifecycle           |

The code is already terse and well-factored. Most opportunities are
small or surface judgment calls that could go either way; a few
genuinely simplify. The biggest theme is **dual surfaces** — places
where two APIs cover the same need.

## Headline findings

### 1. Several public functions are dead code

- `msg/data?`, `msg/signal?`, `msg/done?` — zero call sites.
- `msg/pending?` — used only inside `msg.clj`.
- `trace/status-event` — used only inside `trace.clj` (by `emit`).
- All five lifecycle event constructors in `trace.clj`
  (`recv-event`, `success-event`, `failure-event`, `send-out-event`,
  `flow-error-event`) are called only from `flow.clj`. They're
  "public" but in practice the framework's private vocabulary.

**Recommendation.** Privatize the unused-externally fns. `data?` /
`signal?` / `done?` can go entirely (there's only one dispatch
site, in `flow.clj:124`, which already uses `(case (msg/envelope-kind m) ...)`).
No semantic change; tightens the public surface.

### 2. `record-inject!` exists only because `inject!` doesn't have an event

`record-event!` already dispatches on `:kind` to update the right
counter; injects need their own path because there's no `:inject`
case in the dispatch.

**Question.** Is the inject path different *in kind* from the
event path, or just different *in plumbing*? It's just plumbing.

**Recommendation.** Add `:inject` to `record-event!`'s case and
delete `record-inject!`. `inject!` becomes
`(record-event! counters {:kind :inject})`. One counter API, one
dispatch table. Saves a function and a small asymmetry.

### 3. Trace event constructors are seven near-identical 1-3 line wrappers

Each one is `(assoc (msg-envelope m) :kind <kind> :step-id step-id …)`.
`flow-error-event` is structurally different (no `m`, scope/at
stamped inline because it's published outside `proc-fn`).

**Recommendation.** A single private `event-of [kind m step-id & extras]`
helper collapses six of them into one-liners. Keep
`flow-error-event` as the genuine outlier; its specialness is the
"publishes from outside the normal proc-fn pump" path, which
deserves the bespoke shape (or a sibling `pub-flow-error` that
hides the bypass).

**Counter-argument.** Each constructor encodes which extra fields
are valid (`:in-port` on done recv, `:port` on send-out, `:error`
on failure). A generic `event-of` would lose that
schematic-by-construction property unless each call site passes
the right keys. The 7-constructor zoo is verbose but auditable.
Reasonable people will disagree.

### 4. `from-opts` and the three `new-*` constructors are partly redundant

`from-opts` covers four quadrants of `(data?, tokens?)`. It calls
`new-msg`, `new-signal`, `new-done` internally — but quadrant
`[true true]` does `(assoc (new-msg data) :tokens tokens)`, which
is awkward (build-then-override). `new-msg` could take optional
tokens directly.

**Question.** Should `new-msg` accept tokens? Right now its
signature implies "root data msg with empty tokens"; a
`(new-msg data tokens)` arity makes the override go away but
weakens the "root = empty" identity.

**Recommendation.** Leave `new-msg` alone but inline `from-opts`
into `inject!` (its only caller). The four-case dispatch is
2 lines and lives at the one place that needs it.

### 5. `scope` segment ids are inconsistently keyword-or-string

- `start!` accepts `:flow-id` as either keyword or string,
  defaulting to `(str (random-uuid))`. So fid is sometimes kw,
  sometimes string.
- `inline-subflow` calls `(name outer-sid)` to convert subflow
  ids to strings before pushing them into scope.
- Downstream consumers handle both: `viz.clj` and `trace.clj`
  both have `(if (keyword? id) (name id) (str id))` to normalise.

This costs three branchy converters and an audit-time question
"is this id a kw or a string here?"

**Recommendation.** Stringify at the boundary: in `start!`, do
`(or (some-> (:flow-id opts) name) (str (random-uuid)))`. Then
every scope segment id is unambiguously a string, downstream
`keyword?` branches become `name`, and the dual-rep disappears.

### 6. The redundancy of `:send-out` in `inject-attribution` deserves verification

`inject-attribution` (`flow.clj:481-492`) processes `:inject`,
`:split`, `:merge`, AND `:send-out` events. A msg that goes
through synthesis emits both a `:split`/`:merge` event AND a
`:send-out` event with the same `parent-msg-ids` and `msg-id`.
The fold processes both — idempotent, so harmless but redundant.

The one case where `:send-out` is the *only* lineage edge is
the appended-done in `run-done`: `(msg/new-done)` is appended
to the port-map AFTER synthesis (`flow.clj:102`), so it gets
no `:split`. But `new-done` has empty `:parent-msg-ids`, so it
contributes nothing to attribution anyway.

**Recommendation.** Drop `:send-out` from the attribution case
list and add a test confirming attribution is unchanged. If
it's load-bearing in some edge case, the test will fail and
we learn something. Removes one moving part from the most
complex piece of `flow.clj`.

### 7. `materialize`'s asymmetric empty-checks

`msg.clj:172-181`:

```clojure
t1 (if added       (tok/merge-tokens tokens added) tokens)
t2 (if (seq dropped) (apply dissoc t1 dropped) t1)
```

`added` is checked truthy (any non-nil); `dropped` is checked
non-empty. An empty-map `added` would go through `merge-tokens`
needlessly; an empty-set `dropped` is short-circuited.

**Recommendation.** Use `(seq added)` consistently, or rewrite
as a `cond->` over `tokens`:

```clojure
(cond-> tokens
  (seq added)   (tok/merge-tokens added)
  (seq dropped) (#(apply dissoc % dropped)))
```

Same semantics, expresses the "decorate base tokens" shape
directly.

## Architectural observations (judgment calls, not changes)

### 8. The two-arity-with-implicit-ctx pattern in msg constructors

`child`, `children`, `pass`, `signal` all have a 2-arity that
reads `(:msg ctx)` as the parent and a 3-arity that takes an
explicit parent. `merge` only has explicit-parent form. Three
places do `(let [parent (:msg ctx)] …)`.

**Question.** Is the implicit-ctx form pulling its weight, or is
it a thin convenience that obscures the parent semantics? It
IS used heavily (90+ call sites in the codebase). The asymmetry
with `merge` is unavoidable (merges have multiple parents).

**Take.** Keep it. Could share a `(parent-of ctx)` helper to
dedupe the three small `let` forms — saves ~6 lines and gives
the implicit dependency a name.

### 9. `step?` and `handler-map?` predicates are structural

They check `(contains? v :on-data)` etc., which is duck-typing.
This is fine until someone passes a malformed map and gets a
confusing error mid-`instrument-flow`. The current behavior
throws "Unrecognized proc value" with the value, which is OK.

**Question.** Should the step shape be a malli schema with a
single source of truth? Pros: one definition, generated docs,
precondition checks. Cons: malli dependency, schema sprawl
if every tier-3 escape hatch needs its own schema.

**Take.** Don't add malli unless other parts of the codebase do
too. The current predicates are direct.

### 10. `inject!` re-arms the `done-p` promise

`flow.clj:427`: `(swap! done-p (fn [p] (if (realized? p) (promise) p)))`.
The semantics ("after the flow has reached quiescence and
delivered done-p, a new injection means we're no longer
quiescent and need a fresh promise to track the next quiescence")
is subtle and not commented.

**Take.** Add a one-line comment. Trivial.

### 11. `run-seq`'s collector-step is structurally a `step/sink` with side effects

`flow.clj:475-479` defines a private `collector-step` that
swaps the input msg-id+data into an atom and emits nothing.
`step.clj` already exposes `sink` for the "consume, emit
nothing" shape.

**Question.** Should there be a `recording-sink` in `step.clj`
that takes an atom? It would generalise this pattern (which
will recur for any "collect what reaches the boundary" tool).

**Take.** Promote `collector-step` to `step/recording-sink id atom`
— small, makes `run-seq` shorter and the pattern reusable.

### 12. `start!`'s 10-key handle map is a pile

Returns: `::step ::graph ::pubsub ::scope ::fid ::cancel
::counters ::done-p ::error ::err-done ::port-index`. Each is
used somewhere in `inject!` / `stop!` / `await-quiescent!` etc.
— none are dead — but read as ten loose lifecycle ends.

**Question.** Group as `::topology` (step/graph/scope/fid/port-index),
`::tracking` (counters/done-p/error), `::control` (cancel/err-done)?
Tighter destructuring at use sites, three new namespacing decisions.

**Take.** Don't. Each key is referenced independently; grouping
forces `get-in` instead of `::keys`. Flat is fine.

### 13. `instrument-flow` does both inlining and aliasing in one fold

The single reduce produces three things:
`{:procs … :inner-conns … :aliases …}`. Each input proc
contributes to one of the three based on its type. The
accumulator triple is fine but the responsibility is "do
everything to the procs map at once."

**Take.** Could split into two passes (first inline, then
resolve refs), but the current one-pass is O(n) and direct.
Keep.

### 14. Trace's `:scope` and `:scope-path` are dual

Every event has both: `:scope` is `[[:scope/:step kw id] …]`
(structured), `:scope-path` is `[id …]` (flat, for filtering).
The latter drops `:step` segments. They're computed once in
`push-scope` for performance.

**Question.** Worth keeping both, or compute one from the other
on demand?

**Take.** Keep. Filter performance matters and the precompute
cost is paid once per push-scope. The dual is documented.

## Minor / cosmetic

### 15. `fid` (binding) vs `:flow-id` (key)

Internal abbreviation inconsistency. Rename `fid` → `flow-id`
everywhere internal to match the public key. Trivial.

### 16. `token/rand-token` is a 1-line helper called once

Inline at the call site or keep — pure cosmetic.

### 17. `token/merge-tokens` is `(merge-with bit-xor a b)`

Named because the operation is conceptually XOR-merge — the name
earns its keep over inlining.

### 18. `counters/snapshot` could be `(into {} (map …))` instead of explicit map literal

Trivial.

## Concrete shortlist

If actually editing, in priority order — each is small and orthogonal:

1. Privatize unused-externally APIs: `msg/data?`, `msg/signal?`,
   `msg/done?`, `msg/envelope-kind`, `trace/*-event` constructors,
   `trace/status-event`. Delete the predicates outright (zero
   callers).
2. Stringify scope ids at `start!`. Drop `keyword?` checks in
   `viz.clj` and `trace.clj`.
3. Collapse `record-inject!` into `record-event!` via an `:inject`
   case.
4. Rewrite `materialize` decoration as `cond->`.
5. One-line comment on the `inject!` `done-p` re-arm.
6. Promote `collector-step` to `step/recording-sink`.
7. Investigate dropping `:send-out` from `inject-attribution`'s
   case list (test first).

These together remove ~40-60 lines and three distinct classes of
inconsistency (kw/string ids, dead public API, inject-vs-event
counter asymmetry). None change semantics.

## Things to leave alone

Synthesis (`msg/synthesize`) is dense but does exactly one thing
and is a single 30-line fold. The local helpers (`leaves-of`,
`coerce-data`, `materialize`, `flatten-outputs`) are each tiny
and named — over-decomposed for the size, but the names earn
their keep when reading the fold. Keep.

`run-step`'s three-way dispatch on envelope kind is the right
shape — done is genuinely different from data/signal, and
data/signal share a code path via `run-data-or-signal`. The
asymmetry is honest.

## Verification (if changes were made)

The test suite (`clojure -X:test`) covers all of these areas:
`close_done_test.clj` (close + done cascade), `datapotamus_test.clj`
(synthesize + flow lifecycle + tokens + status events), the podcast
multi-task tests (multi-task pubsub scoping). 533 tests / 919k
assertions today; no behaviour changes expected from any item on
the shortlist. Each change is self-checking: privatising adds
arity errors at call sites, scope-id stringification removes
branches that consumers can no longer exercise.
