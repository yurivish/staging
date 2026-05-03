# Pipeline diagrams and how to read them

The terminal renderer in `toolkit.datapotamus.render` produces an
indented nested-list view of a pipeline. This is the reader's guide:
the slot model, the glyphs, and the invariants that let you trace flow
just by scanning the page. Each example below pairs the tree
rendering with the node-link topology it represents, so you can see
what the line layout is encoding.

## The three-slot model

Every line has three slots:

```
[left ] [indent + body                ] [right annotations]
```

- **Left** (column 0–1): a 2-char fall-through marker — `↓ ` or `  `.
- **Body**: the indent (tree depth) + the line content (name,
  optionally with a rail char `⎢` and/or a `K× …` compression
  prefix).
- **Right**: zero or more inline annotations describing edges that
  don't lie along the visible spine — `⮥ <name>` (back) or
  `⮧ <name>` (forward off-spine).

## Glyph reference

| Glyph | Meaning |
|---|---|
| `↓` (left column) | This line's element flows to the line(s) directly below. Two distinct cases share this glyph: chain successor (next chain element below) and **branch exit** (last row of a parallel branch feeding the line below the rail). See the fall-through invariants. |
| `↓` (in the rail, `⎢↓name`) | In-branch chain flow within a parallel arm. **Mutually exclusive with left-column `↓`** on the same line — they describe different flows. |
| `⎢` | Parallel rail — the row is in pure parallel with the other `⎢` rows in the same block. See the rail invariant. |
| `K× <name>` | K identical members collapsed (round-robin / stealing-workers / parallel with identical branches). When the class size > 1, digit-suffixed step ids display as `name K` (placeholder). |
| `<name> (combinator)` on a container line | Marks containers built by named combinators (`parallel`, `round-robin-workers`, `stealing-workers`). Lifted from `:combinator` metadata. |
| `⮥ <name>` (right) | Back-edge to a line above (always named). The path is across-then-up. |
| `⮧ <name>` (right) | Forward off-spine edge to a non-adjacent line below. Across-then-down. |
| `(direct)` rail row | A parallel branch with no intermediate steps — a direct source→sink path through the bracket. Without this marker the path is invisible and the rail looks like it has fewer members than it does. The row carries a left-column branch-exit `↓` like any other branch. |
| `<name>  (cycle)` suffix | A non-trivial SCC nested inside a chain. The marker attaches to the first cycle member's line; remaining members follow at the same indent. Top-level cycles render without this suffix (the whole rendering IS the cycle). |
| `<name>  (prime)` suffix | A non-series-parallel cluster nested inside a chain (e.g., a wheatstone bridge between two chain neighbors). Marker attaches to the first interior member; off-spine edges between interior nodes render via `⮥`/`⮧` like top-level primes. Rare — only triggered by hand-built non-SP topologies, never by the registered combinators. |
| `⎢` `⎢` (two rails on one row) | Nested parallel — the outer rail row holds a sub-parallel. The inner `⎢` rail wraps sub-branches at one deeper indent (col 4). |

## The rail invariant

**`⎢` rows are siblings in pure parallel.** Every row marked `⎢` is
fed by the same source (the line directly above the rail with its `↓`
pointing down) and feeds the same sink (the line directly below the
rail). Rail rows do **not** flow to each other — the rail is the
notation for "these run side-by-side."

Corollary: a row that's downstream of a parallel pair (i.e., a join
node) is **not** in the rail. It appears below the rail at the
chain's level, not at the rail's indent.

This is enforced by the shape decomposition: when a topology has a
parallel structure followed by a join, `classify-dag`'s recursion
finds the join via series-cut and emits
`chain[..., source, parallel-sub-shape, join, ...]` — the parallel
sub-shape's rail contains only the parallel members, and the join
appears as its own chain element below.

## Branch boundaries within the rail

When multiple branches are rendered together in one rail (e.g., a
parallel of chains), the visual cues for distinguishing branches are
combinations of four glyphs:

- **`⎢↓` (in-rail `↓`)**: this row's element has a chain successor
  *within the same branch* (the next row continues the branch's
  chain).
- **`↓ ⎢` (left-column `↓` + rail)**: this row is the *exit* of its
  branch — last row of a multi-element branch, or the lone row of a
  single-leaf branch — and feeds the line below the rail (the SG
  sink or chain successor).
- **`⎢` (no in-rail `↓`, no left-column `↓`)**: a non-terminal row
  inside a multi-row sub-shape (e.g., the source line of a nested
  scatter-gather branch).
- **`⎢ ⎢` (nested rail)**: this row's element is itself a sub-parallel
  (a scatter-gather nested inside this branch's chain). The inner
  rail wraps sub-branches.

A branch ENDS at a row marked with `↓ ⎢ ` (left-column `↓`); the
next row at the rail-root column is the next branch's first element.

Example with mixed branch shapes:
```
↓   group fan out
  ⎢↓window         ← branch 1 first element, in-branch successor
↓ ⎢ explode        ← branch 1 exit (left-column ↓ → feeds fan in)
  ⎢↓rate limit     ← branch 2 first element
↓ ⎢ bucket         ← branch 2 exit
↓   group fan in
```

The two `↓` glyphs never co-occur on the same row: in-rail `↓`
means "in-branch chain flow," left-column `↓` means "branch exit
into the next stage," and a single line is at most one of these.

## The fall-through invariants

A `↓` in the left column means: **the line's element flows to one or
more of the lines immediately below**. Concretely:

1. **Chain successor.** If the next line at the same shape level is a
   real edge from this one, `↓`.
2. **Container entry.** A container's header gets `↓` if the container
   has a chain successor at the parent level. The next visible line is
   either the container's first inner element OR the next outer
   sibling depending on geometry — either way, flow continues there.
3. **Container exit.** The last visible line of a container's
   rendering inherits the container's outer fall-through. So if
   `worker K` (a class block) flows to `exit K`, the inner
   `count cell` line — which is `worker K`'s exit point — also gets
   `↓`. The reader sees an unbroken `↓` column traversing the
   container boundary.
4. **Scatter-gather source (fan-out).** Always gets `↓`. The source
   has a real edge to the first branch (and to every other branch);
   the next visible line is one of those branches.

   Reading rule: **a `↓` directly above a `⎢` rail means "fans out
   to every member of the rail below."**

5. **Scatter-gather sink (fan-in).** Gets `↓` if the parallel
   container has a chain successor at the outer level (via the same
   container-exit propagation as #3).
6. **Branch exit.** The last row of each parallel branch within a
   `⎢` rail gets a left-column `↓` showing the row feeds the line
   below the rail (the SG sink, or for inline scatter-gathers the
   chain successor). This signal is independent of in-rail `⎢↓`,
   which marks in-branch chain flow within a multi-element branch.

A line **without** `↓` either has no successor in the rendering (last
visible line, or a non-terminal row of a multi-row sub-shape inside a
branch) or its successor is annotated explicitly on the right
(`⮥` / `⮧`).

## Right-side annotations

When an edge isn't reflected by the spine — i.e., the source and
target aren't immediately adjacent — it shows up as a named annotation
on the source line:

- `⮥ <name>` — back-edge. The target is above the source; the arrow
  shape traces "right then up" to find it.
- `⮧ <name>` — forward off-spine. The target is below but not
  adjacent; the arrow traces "right then down."

Adjacent forward edges aren't annotated — that's the spine, encoded
by the left-column `↓` only.

## Examples

Each example pairs the tree rendering with a node-link topology. The
tree is what the renderer prints; the node-link is what's actually in
the graph.

### Single-leaf parallel branches

`cc/parallel :specialists` with three named leaf workers and a
downstream `sink`.

**Tree:**

```
↓ specialists (parallel)
↓   specialists fan out
↓ ⎢ skeptic
↓ ⎢ facts
↓ ⎢ solve
↓   specialists fan in
  sink
```

**Node-link:**

```
        fan out
       /   |   \
      v    v    v
   skeptic facts solve
      \    |    /
       v   v   v
         fan in
           |
           v
          sink
```

Each rail row is a complete branch (single leaf). The fan-out has
edges to all three; all three feed fan-in; fan-in feeds sink. The `↓`
on `fan out` directly above the rail is the rule "fans out to every
rail row." The left-column `↓` on each rail row is the branch-exit
marker — every branch is a single-leaf branch, so each row is
simultaneously the branch's first and last element, and feeds fan-in
below.

### Multi-element chain branches

Each parallel arm is a 2-step chain.

**Tree:**

```
↓   group fan out
  ⎢↓window
↓ ⎢ explode
  ⎢↓rate limit
↓ ⎢ bucket
↓   group fan in
```

**Node-link:**

```
        fan out
        /     \
       v       v
    window   rate limit
       |       |
       v       v
    explode  bucket
        \     /
         v   v
         fan in
```

Two branches: `window → explode` and `rate limit → bucket`. The
rendering interleaves them: rail row 1 is branch A's entry, row 2 is
branch A's continuation, row 3 is branch B's entry, row 4 is branch
B's continuation. The `⎢↓` rail-↓ marks the in-branch chain edge
(`window → explode`, `rate limit → bucket`). The left-column `↓` on
`explode` and `bucket` marks them as branch exits feeding fan-in
below the rail.

There's **no** edge `window → rate limit` despite their geographic
adjacency in the rendering; the rail just visually groups them. The
indentation distinguishes branch entries (rail-aligned column) from
within-branch continuations (one indent deeper).

### Collapsed K× (round-robin-workers)

When all parallel branches are structurally identical, they collapse
to a single `K× …` row.

**Tree:**

```
↓ pool (round-robin-workers)
↓   router
↓ ⎢ 4× worker K
↓ ⎢   work
↓   join
  sink
```

**Node-link** (one slice; the K=4 multiplicity sits inside the K×):

```
                 router
               /  |  |  \
              v   v   v   v
         worker  worker  worker  worker
           |      |      |      |       (each contains: work)
           v      v      v      v
                 join
                  |
                  v
                 sink
```

The K× block stands in for K identical worker containers, each with
inner `work`. The `⎢` rail covers the K× header *and* its inner
content (`work`) since both are part of the collapsed parallel
section. After the rail, `join` is back at the chain level (no rail),
and `sink` follows.

The `↓` propagation traces the visual flow: router → K× → work
(propagated from the K×) → join → sink would be the unbroken column,
but only if all those edges are real. Here they are.

### Cycle K× with propagated fall-through (count cell)

A stealing-workers cycle with a `step` inner. Each worker class
member is a container wrapping a `count cell` step.

**Tree:**

```
↓ counters (stealing-workers)
      ext  ⮧ coord
↓     16× shim K
↓     16× worker K
↓       count cell
↓     16× exit K
↓     coord
      drop
```

**Node-link** (one slice; multiplied ×16 across the worker classes):

```
              ext ────────────┐  (off-spine forward to coord)
               │              │
               v              │
             shim             │
               │              │
               v              │
            worker            │
            ┌───┴──┐          │
            │count │          │
            │ cell │          │
            └──────┘          │
               │              │
               v              v
             exit ─────→  coord
                            │
                            ├───→ shim  (cycle closes, back-edge)
                            │
                            v
                          drop
```

`count cell` lives inside each worker — it's not a peer of `worker K`
in the cycle's class spine. Its `↓` is *propagated* from the parent
worker class block: worker K's emission is count cell's emission, so
both get the same fall-through. The `⮧ coord` annotation on `ext`
shows the off-spine forward edge that the Eades-ordered class spine
doesn't visit consecutively.

## K× compression details

When K branches (or K cycle members) share the same recursive
structure, they collapse to a single `K× <name>` row. The block keeps
the `⎢` rail when it has inner content (so the parallel signal stays
visible across the multi-row block). When the K× block is a single
row (K identical leaves with no inner content), it renders without
the rail — the `K×` prefix carries the parallel signal alone.

## Edges and metadata

Internal-edge metadata (the actual graph edges between siblings at
each combinator level) is what the renderer consumes; see
`kb/pipeline-shapes.md` for the shape decomposition that produces it.
Every `↓`, `⎢`, `K×`, and `⮥/⮧` annotation traces back to a real edge
or a real shape.

## Generative verification

The decomposition algorithm is checked against random SP-DAGs by a
property-based test suite (`shape_test.clj`, using
`clojure.test.check`). Four properties hold for any SP composition:

- **`sp-dag-decomposes-without-prime`** — random SP-trees are
  decomposed without producing a `:prime` node anywhere.
- **`sp-dag-leaves-preserved`** — every node in the topology appears
  exactly once in the decomposition tree.
- **`sp-dag-decomposition-is-idempotent`** — repeated decomposition
  gives the same result.
- **`render-is-total-on-sp-dags`** — every node's name appears in the
  rendered output.

Plus two for non-SP topologies:

- **`wheatstone-insertion-yields-prime`** — when a wheatstone bridge
  sub-graph is inserted into an SP-DAG, the decomposition correctly
  identifies a `:prime` cluster somewhere in the tree.
- **`render-is-total-on-perturbed-dags`** — perturbed (non-SP) DAGs
  still render without crashing.

50 random trials per property, holding across many seeds. Strong
empirical evidence the algorithm handles arbitrary SP-DAGs and
gracefully reports non-SP fragments as `:prime` clusters.

## Inline non-SP clusters

When a hand-built topology includes a non-SP region (e.g., a
wheatstone bridge) embedded in an otherwise SP frame, the
decomposition isolates the non-SP region as a `:prime` sub-shape
nested in the parent chain's `:order`. The renderer suffixes
`(prime)` onto the first interior member's line; remaining members
follow at the same indent, with off-spine edges between them
visualized as `⮥`/`⮧`:

```
↓ src
↓ a
↓ p
↓     q  (prime)  ⮧ s, ⮧ u
↓     r
↓     s  ⮧ u
↓ u
↓ d
  sink
```

The `(prime)` suffix signals the cluster boundary; `:source` and
`:sink` are the chain neighbors above and below (`p` and `u` here),
and they remain in the chain spine. Interior off-spine edges (e.g.,
`q → s`, `q → u`) and boundary-crossing edges into the chain sink
(e.g., `s → u`) annotate the relevant interior rows.

None of the 27 registered pipelines contain non-SP regions; this
rendering only appears for hand-built `step/connect` topologies.

## Resolved limitations

These were previously documented as deferred; they were surfaced and
fixed while building `kb/pipeline-menagerie.md`. Behavior described
above is current.

- **Branch-exit `↓` marker.** Branch-exit rows now carry a left-column
  `↓` distinct from in-rail `⎢↓`. Implemented in `apply-bracket-rail`
  via a new `:branch-exit?` flag (separate from `:fall-through?` so
  nested rails don't confuse the two signals).
- **Inline `(prime)` interior edges.** `render-inline-prime` runs
  `classified-annotations` on the prime's `:internal-edges`, so
  interior and boundary-crossing off-spine edges render as `⮥`/`⮧`
  on interior member lines, just like top-level primes.
- **Standalone marker rows.** `(cycle)` and `(prime)` no longer float
  on their own rows; they're suffixed onto their first interior
  member's line, eliminating the visual ambiguity about cluster
  boundaries.

## Open limitations (cosmetic)

- **Inner-rail branch exits in nested parallels.** When a parallel
  branch is itself a sub-SG, the inner-rail branch exits don't get
  an inner-rail `↓` marker — there's no spare slot between the outer
  and inner rail glyphs. The inner-sink line at outer indent
  visually resolves the flow, so reader inference still works, but
  it's less explicit than the outer-rail case.
- **Boundary edges from S into a `(prime)` cluster.** Edges from the
  upstream chain neighbor into interior nodes (e.g., `p → q`,
  `p → r`) aren't annotated on the `p` line because chain elements
  outside the cluster don't carry cluster-specific annotations. The
  reader infers entry edges from the `(prime)` marker on the first
  interior member.
