# Pipeline shape decomposition + rendering

How `shape.clj` turns a flat topology into a typed tree, how `render.clj`
turns that tree into terminal output, and the notation that comes out
the other end. Reference for anyone debugging the renderer or extending
the shape vocabulary.

## The input: a flat clustered digraph

`step/topology` produces `{:nodes :edges}` with one entry per proc. The
hierarchy is implicit: every node carries a `:path` from root down, and
**every edge is between siblings** — `:from-path` and `:to-path` share a
parent. No edge crosses a combinator boundary.

That gives `shape/decompose` a one-line move to recover the hierarchy:

```clojure
nodes-by-parent (group-by #(vec (butlast (:path %))) nodes)
edges-by-parent (group-by #(vec (butlast (first %))) edges)
```

`(butlast path)` is "your parent's path." Group by that and each
combinator gets the exact subgraph of its direct children.

## Per-level decomposition

`classify-level` is called once per container with `(paths, edges)` over
its children. It produces a `:shape` map. Three algorithms in sequence:

### 1. Tarjan — isolate cycles

`tarjan` (single DFS, O(V+E), `shape.clj:38`) returns the non-trivial
SCCs. Singletons stay as ordinary nodes; multi-node SCCs (and self-loop
singletons) are condensed to one super-node each.

The condensation is provably a DAG. Cycles cannot span combinator
boundaries — the flat-edge property forbids it — so a per-level pass
finds every cycle in the whole pipeline.

Free bonus: SCCs come out in reverse topo order of the condensation, so
no second sort is needed before feeding the result downstream.

### 2. `classify-dag` — type the condensation

A 4-way classifier on the condensed DAG (`shape.clj:146`):

- `:empty` — no nodes.
- `:chain` — exactly one source, one sink, every middle node has in/out
  degree 1. `chain-order` walks source-to-sink and returns the path or
  nil.
- `:scatter-gather` — one source, one sink, edges are exactly
  `src→middle`, `middle→middle` *within one weak component*, and
  `middle→sink`. Each weak component on `middle` becomes one branch;
  branches are emitted in topo order via `chain-order` so they read
  source→sink (not arbitrary set order).
- `:prime` — anything else. Falls back to `kahn-order` for a
  deterministic linear sort, keeps every edge in `:internal-edges`.

### 3. Eades–Lin–Smyth — order cycle members

Once an SCC is chosen for `:cycle`, `eades-order` (`shape.clj:212`)
gives a member order that minimizes backward edges:

- Peel sources (append to head).
- Peel sinks (prepend to tail).
- Otherwise pick the node maximizing `out-deg − in-deg`.
- Ties broken by lex order on `node-key`.

Concatenate `head ++ tail`. Consecutive-pair edges in the result form a
near-maximum spanning chain through the SCC; the rest are the feedback
arc set, surfaced as off-spine annotations at render time.

This is a heuristic — minimum FAS is NP-hard — but SCCs in real
pipelines are tiny (single-digit member counts) and Eades is provably
within a constant factor of optimum. Good enough.

### Re-inflation

After classifying the DAG, SCC markers in the result are replaced with
`{:kind :cycle :order ... :internal-edges ...}` records inline.
Cycles-inside-chains and cycles-inside-scatter-gathers nest naturally;
the same vocabulary covers any directed graph at any level.

## The four shapes

| Kind | Has | What it represents |
|---|---|---|
| `:empty` | — | container with no internal edges |
| `:chain` | `:order` | linear sequence; off-spine = ∅ |
| `:scatter-gather` | `:source`, `:sink`, `:branches` | one→K disjoint chains→one; off-spine = ∅ |
| `:cycle` | `:order`, `:internal-edges` | non-trivial SCC; off-spine = back-edges |
| `:prime` | `:order`, `:internal-edges` | irreducible non-SP residue; off-spine = forward skips |

Within `:order` and `:branches`, an element is either a path (atomic
node) or a nested `{:kind :cycle ...}` record. `:internal-edges`
endpoints match `:order` elements one-to-one.

## Spine + off-spine: the unifying frame

The renderer doesn't case-analyze on `:kind`. It only needs `:order`
and (where present) `:internal-edges`. From those:

- **Spine** = consecutive pairs along `:order` that exist in
  `:internal-edges`. These render as fall-through edges (the `↓` rail).
- **Off-spine** = every other internal edge. These render as inline
  annotations (`⬏ <name>` for back, `⬎ <name>` for forward).

By construction:

- `:chain` — every edge is consecutive; off-spine = ∅.
- `:scatter-gather` — branches are independent; off-spine = ∅.
- `:cycle` — Eades minimizes off-spine but it's never empty (an SCC
  must have at least one cycle); off-spine arrows go *backward*.
- `:prime` — Kahn topo order; off-spine arrows skip *forward* over
  spine positions.

Same data, four interpretations.

## Block aggregation in cycles (render layer)

`shape.clj` stops at the four shapes. The renderer adds one more pass
over `:cycle`/`:prime` shapes: 1-WL color refinement
(`render.clj:wl-refine`) partitions members into equivalence classes —
two members are "the same" iff they're recursively indistinguishable
(intrinsic name + neighborhood multiset converge to the same color).

`round-robin-workers` produces one big class (16 identical workers);
`stealing-workers` produces three matched classes (`shim-K`, `worker-K`,
`exit-K`). A heterogeneous `cc/parallel` (named specialists with
different inner content) produces all-singleton classes — no
aggregation possible.

When inter-class edges are summarizable (`fan-in` / `fan-out` /
`bijection` / `complete`), each class collapses to one `K× <name> K`
row. When they aren't — the **faithfulness gate** bails — the cycle
falls back to per-member rendering with name-suffix pattern
compression (`find-pattern-at` looks for `[s_i, w_i, e_i]` triples
repeating in `:order`).

`:aggregate? false` skips the WL pass globally and forces per-member
rendering everywhere. Disaggregation is lossless.

## The notation

Every line is three slots:

```
[left] [indent + body] [right annotations]
```

**Left slot** (column 0, 2 chars):

- `↓ ` — falls through to next line at this indent level (a real spine
  edge).
- `  ` — no fall-through.

Trace a contiguous `↓` column to read the spine.

**Body**:

- `<name>` — leaf or container.
- `<name> (combinator)` — container built by a named combinator
  (`parallel`, `round-robin-workers`, `stealing-workers`). Lifted from
  `:combinator` metadata stamped by the combinator at construction time
  — saves the reader from inferring shape from inner procs.
- `⎡↓<name>` / `⎡ <name>` / `⎢↓<name>` / `⎢ <name>` / `⎣ <name>` —
  scatter-gather bracket rail. Each rail row is a 2-char prefix flush
  against the content. `⎡` = first row, `⎣` = last row, `⎢` = middle.
  The second char absorbs in-branch fall-through (`↓` = chain
  successor in this branch is the next line).
- `K× <name>` — K identical members collapsed by block aggregation.
  When the class size is > 1, digit-suffixed names display with `K` as
  a placeholder (`worker-3` → `worker K`). On non-aggregated lines,
  the underlying step id is the real digit.

**Right annotations** (after the name):

- `⬏ <name>` — back-edge to a line above (always named).
- `⬎ <name>` — forward off-spine to a non-adjacent line below.

Adjacent forward edges aren't annotated — that's the spine, encoded by
left `↓` only.

### Reading rules

- `↓` in the left column: "next line at this indent is a real edge."
- `↓` next to a bracket char: "next line in this branch is a real edge"
  (in-branch chain flow).
- A name at the bracket-root column: a new branch starts here.
- A name at deeper indent inside a bracket: continuation of the
  previous branch's chain.

### Sample (from `dev/data/pipelines.md`)

```
↓ fetch top ids
↓ split ids
↓ story fetchers (round-robin-workers)
    router
    16× worker K
      fetch story
    join
↓ split comments
  comment fetchers (round-robin-workers)
    router
    16× worker K
      fetch comment
    join
```

Cycle aggregation in action (16 identical worker triples):

```
↓ counters (stealing-workers)
    ext  ⬎ coord
↓   16× shim K
↓   16× worker K
      count cell
↓   16× exit K
↓   coord
    drop
```

The `↓` column reads the spine; the `⬎ coord` annotation marks the
feedback edge from `ext` going across-and-down to `coord` that closes
the SCC.

## Algorithm-boundary demos

The everyday pipelines (registry in `src/datapotamus_export.clj`) are
all series-parallel with symmetric cycles by construction, so several
algorithms in this pass exist for shapes that real pipelines never
trigger. Three handcrafted demos isolate each one. Generator at
`dev/render_algorithm_demos.clj`.

### `prime-bowtie` — Kahn topo sort + `:prime` fallback

Two independent input streams converge at a merge node, then fan out
to two independent sinks. Two sources (`stream-a`, `stream-b`) and two
sinks (`sink-x`, `sink-y`) at one container level fail
`classify-dag`'s single-source / single-sink check, so it falls
through to `:prime`. `kahn-order` produces `:order`; every edge ends
up in `:internal-edges`; off-spine forwards (`⬎`) annotate the
non-consecutive edges.

```
  stream a  ⬎ merge
↓ stream b
↓ merge  ⬎ sink y
  sink x
  sink y
```

No bracket rail (not scatter-gather). The two `⬎` annotations show
where edges skip spine positions: `stream-a → merge` skips `stream-b`,
`merge → sink-y` skips `sink-x`.

### `asymmetric-cycle` — Eades–Lin–Smyth FAS heuristic

A 5-node validation loop with asymmetric in/out degrees:

| node | out | in | score |
|---|---|---|---|
| `intake` | 2 | 1 | +1 |
| `format-check` | 1 | 1 | 0 |
| `content-check` | 1 | 1 | 0 |
| `decide` | 1 | 2 | −1 |
| `retry` | 1 | 1 | 0 |

Eades has no empty source / empty sink to peel, so it picks the
highest-score node — `intake`. After dropping intake's edges, both
checkers are empty sources (lex tiebreak picks `content-check` first),
then `format-check`, `decide`, `retry`. The single back-edge
(`retry → intake`) closes the loop and shows up as `⬏ intake` on the
last line — exactly the FAS we want.

```
↓ intake  ⬎ format check
  content check  ⬎ decide
↓ format check
↓ decide
  retry  ⬏ intake
```

Compare with `stealing-workers` / `round-robin-workers`: every member
there has the same degree by construction, so the score is a wash and
lex tiebreak does all the work. Here the score actually discriminates,
putting the source-leaning node at the start regardless of name.

### `partial-bipartite-cycle` — faithfulness-gate bail

Six-node SCC: three `shifter-*` and three `target-*`. Each shifter
sends to two consecutive targets in a cyclic shift; each target sends
back to one shifter. All three shifters have identical structural
neighborhoods (1 in, 2 outs to target-class members) so 1-WL keeps
them in one class; same for targets.

But the shifter→target edge multiset has 6 edges out of 9 possible:
not a bijection (`|ij| ≠ |Ci|`), not complete
(`|ij| ≠ |Ci|·|Cj|`). The pattern is `:partial`, so `aggregate-cycle`
returns `nil` and the renderer falls back to per-member output.

```
  shifter 0  ⬎ target 0, ⬎ target 1
↓ shifter 1  ⬎ target 2
↓ target 1
↓ shifter 2  ⬎ target 2
  target 0  ⬏ shifter 1
  target 2  ⬏ shifter 0
```

Without this demo the gate's `:partial` branch is untested by the
registry — every real cycle hits one of the four faithful patterns.

## Mathematical framing

Two literatures speak directly to this decomposition.

### Series-parallel digraphs

Valdes–Tarjan–Lawler 1982: a two-terminal SP-DAG is built recursively
by series composition (our `:chain`) or parallel composition (our
`:scatter-gather`); anything else is *prime*. Linear-time recognition,
unique tree up to associativity. Our `:order` and `:branches` *are* an
SP-tree.

Modular decomposition (Gallai 1967; Cunningham 1982 for digraphs)
generalizes SP to a unique tree with linear / complete / prime nodes.
SP-DAGs are essentially the prime-free MD trees.

Both theories assume DAGs. Our cycle handling — Tarjan-condense,
classify the DAG, re-inflate `:cycle` records — is the standard
*condensation → DAG analysis → re-inflate* pattern, adding `:cycle` as
a fourth atom alongside S, P, prime.

### Categorical lens

Pipelines are morphisms in a **traced symmetric monoidal category**
(Joyal–Street–Verity 1996):

- objects = port types,
- morphisms = boxes with `n` inputs and `m` outputs,
- composition `∘` = series,
- monoidal product `⊗` = parallel,
- trace = feedback (`:cycle`).

A leaf is a generating morphism; a container is a composite. The whole
stepmap is a morphism in **Spivak's operad of wiring diagrams** (2013).
The flat-edge property is automatic: composition can only wire ports of
*direct* sub-morphisms — reaching across a box's boundary would violate
categorical encapsulation. The structural constraint we enforce in the
data is the constraint a category gives for free.

### One phrase

Discrete-math framing: **a flat clustered digraph with per-level
series-parallel + Tarjan-cycle decomposition**.

Categorical framing: **a morphism in the free traced symmetric
monoidal category, presented as a tree of wiring diagrams**.

## Edges have no metadata

A natural question: should combinators that introduce structural
feedback (e.g. `stealing-workers`'s `:work` port) tag those edges with
a `:role :feedback` flag, so the renderer doesn't need to compute a
FAS?

We don't, and we don't plan to. SCCs in real pipelines are tiny —
Eades–Lin–Smyth at O(V+E) is way beyond good enough at that scale.
Adding metadata to edges would couple visualization concerns into
combinator authoring and into every downstream consumer of the
topology, scaffolding for a problem we don't have evidence for. If a
real readability case ever shows up (a combinator where two edges look
algorithmically equivalent but only one is "the feedback edge"), we
can add a tag then, with that motivating example in hand.

## Bonus: SP-algebra one-liners

Valdes–Tarjan–Lawler's SP-algebra notation gives a single-line form:
series as `;`, parallel as `|`. `hn.core` becomes roughly:

```
fetch-top-ids ; split-ids ; (16× fetch-story) ; split-comments ; (16× fetch-comment) ; (4× summarize)
```

The `:shape` tree maps onto this 1:1: `:chain` → `;`-list,
`:scatter-gather` → `|`-list, `:cycle` → `μ(...)`, `:prime` → opaque
box `⟨name⟩`. Useful if you ever want a single-line summary alongside
the indented view, or for log/CLI output where vertical space is tight.
Not implemented; here in case it becomes useful.
