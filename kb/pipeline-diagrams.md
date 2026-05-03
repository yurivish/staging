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
| `↓` (left column) | This line's element flows to the line(s) directly below — see the fall-through invariants. |
| `↓` (in the rail, `⎢↓name`) | In-branch chain flow within a parallel arm. |
| `⎢` | Parallel rail — the row is in pure parallel with the other `⎢` rows in the same block. See the rail invariant. |
| `K× <name>` | K identical members collapsed (round-robin / stealing-workers / parallel with identical branches). When the class size > 1, digit-suffixed step ids display as `name K` (placeholder). |
| `<name> (combinator)` on a container line | Marks containers built by named combinators (`parallel`, `round-robin-workers`, `stealing-workers`). Lifted from `:combinator` metadata. |
| `⮥ <name>` (right) | Back-edge to a line above (always named). The path is across-then-up. |
| `⮧ <name>` (right) | Forward off-spine edge to a non-adjacent line below. Across-then-down. |

## The rail invariant

**`⎢` rows are siblings in pure parallel.** Every row marked `⎢` is
fed by the same source (the line directly above the rail with its `↓`
pointing down) and feeds the same sink (the line directly below the
rail). Rail rows do **not** flow to each other — the rail is the
notation for "these run side-by-side."

Corollary: a row that's downstream of a parallel pair (i.e., a join
node) is **not** in the rail. It appears below the rail at the
chain's level, not at the rail's indent.

See "Known limitations" below for an edge case where the renderer
violates this invariant on hand-built Y-shaped topologies.

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

A line **without** `↓` either has no successor in the rendering (last
visible line, or a parallel sibling whose neighbors are not real
successors) or its successor is annotated explicitly on the right
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
  ⎢ skeptic
  ⎢ facts
  ⎢ solve
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
rail row." The plain rail rows (no `↓`) are not chain successors of
each other.

### Multi-element chain branches

Each parallel arm is a 2-step chain.

**Tree:**

```
↓   group fan out
  ⎢↓window
  ⎢ explode
  ⎢↓rate limit
  ⎢ bucket
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
(`window → explode`, `rate limit → bucket`). The plain rail (`⎢ `)
on `explode` and `bucket` indicates "no in-branch successor" —
combined with the rail invariant, that means these are branch exits
feeding fan-in.

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

## Known limitations

### Non-chain branches (Y-shape) — rail can mislead

Hand-built topologies with a "join inside a branch" can confuse
`classify-dag`'s branch detection. Example: `src` fans out to A and
B, both feeding C, then C feeds `sink`.

**Topology (node-link):**

```
        src
       /   \
      v     v
      A     B
       \   /
        v v
         C
         |
         v
        sink
```

**What the renderer currently produces:**

```
↓ src
  ⎢ A
  ⎢   B
  ⎢     C
↓ sink
```

This is **wrong** by the rail invariant: C is downstream of A and B,
not parallel with them. The rail should only contain `A` and `B`; C
should be below the rail at the chain level.

**What it should produce:**

```
↓ src
  ⎢ A
  ⎢ B
↓ C
↓ sink
```

Why it doesn't: `classify-dag` treats `{A, B, C}` as one
weakly-connected component (linked via A→C and B→C edges) and emits
one branch with `chain-order` falling back to lex-sort. To get the
right rendering, `classify-level` would need to recursively recognize
the series-parallel structure inside the branch — `parallel(A, B) ; C`
— and treat the level's overall shape as `chain(src, parallel(A,B), C, sink)`.

In practice: every parallel structure in our 27 pipelines comes from
`cc/parallel` / `cw/round-robin-workers` / `cw/stealing-workers`,
which produce explicit `fan-out`/`fan-in` nodes. None produces a
Y-branch. The bug is only triggered by manual `step/connect` topology
that omits an explicit join node.

### Parallel branches feeding the outer stage — no explicit marker

In a multi-element chain branch, each branch's exit row has no
explicit `↓` indicating "I feed the line below the rail." The rail
itself implies fan-in, but the reader has to infer it. For example:

```
  ⎢↓window
  ⎢ explode    ← feeds fan in below, but no marker says so
  ⎢↓rate limit
  ⎢ bucket     ← same
↓   group fan in
```

A possible fix: `↓` in the **left column** on branch-exit rows (rows
with no in-rail `↓` and no deeper rail row inside the same branch).
That'd give: `↓ ⎢ explode`, `↓ ⎢ bucket`. The reading rule would be:
"`↓` left of `⎢` means this branch's exit feeds the line below the
rail." Not yet shipped — flagged for design when we revisit
parallel-feeding-outer-stage cases.
