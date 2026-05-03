# Pipeline diagrams and how to read them

The terminal renderer in `toolkit.datapotamus.render` produces an
indented nested-list view of a pipeline. This is the reader's guide:
the slot model, the glyphs, and the invariants that let you trace flow
just by scanning the page.

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
| `⎢` | Parallel rail — the row is one branch of a scatter-gather. No corners; the indent + parent line above bound the bracket. |
| `K× <name>` | K identical members collapsed (round-robin / stealing-workers / parallel with identical branches). When the class size > 1, digit-suffixed step ids display as `name K` (placeholder). |
| `<name> (combinator)` on a container line | Marks containers built by named combinators (`parallel`, `round-robin-workers`, `stealing-workers`). Lifted from `:combinator` metadata. |
| `⮥ <name>` (right) | Back-edge to a line above (always named). The path is across-then-up. |
| `⮧ <name>` (right) | Forward off-spine edge to a non-adjacent line below. Across-then-down. |

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

## Walked example

Below is `hn-buzzword-obituaries.core` (from `dev/data/pipelines.md`):

```
  hn buzzword obituaries
↓   emit cells
↓   counters (stealing-workers)
      ext  ⮧ coord
↓     16× shim K
↓     16× worker K
↓       count cell
↓     16× exit K
↓     coord
      drop
    aggregate
```

Reading line by line:

- `hn buzzword obituaries` — the root container. No `↓` because it has
  no outer chain successor.
- `↓ emit cells` — flows to the next sibling in `hn-buzz`'s chain.
- `↓ counters (stealing-workers)` — `(combinator)` annotation tells
  you this is `c/stealing-workers`. The `↓` says it flows to
  `aggregate` (its outer chain successor) — even though several lines
  intervene, those are *inside* `counters`.
- `ext  ⮧ coord` — `ext` is the entry-point of the cycle; its
  feedback edge to `coord` skips spine positions, so it's annotated.
  No `↓` because `ext` doesn't directly feed the next visible line in
  the spine order.
- `↓ 16× shim K` — class-level chain spine: shim flows to worker.
- `↓ 16× worker K` — worker flows to exit.
- `↓ count cell` — *propagated*: `count cell` is the exit point of
  each `worker K` member. Worker K → exit K is a real flow; count
  cell's emission is the worker's emission, so it gets the same `↓`.
- `↓ 16× exit K` — exit flows to coord.
- `↓ coord` — coord flows to drop. (Note: per the cycle's Eades order,
  `drop` happens to be the next class.)
- `drop` — last in the cycle's class spine.
- `aggregate` — last in `hn-buzz`'s outer chain.

The continuous `↓` column from `emit cells` down through `count cell`
and back out to `aggregate` is the visual story of where messages go.

## Parallel example

```
↓ specialists (parallel)
↓   specialists fan out
  ⎢ skeptic
  ⎢ facts
  ⎢ solve
↓   specialists fan in
  sink
```

- `↓ specialists (parallel)` — flows to `sink`.
- `↓ specialists fan out` — fans out to every rail member below
  (skeptic, facts, solve). The `↓` directly above a `⎢` block reads
  as "to all of them."
- `⎢ skeptic`, `⎢ facts`, `⎢ solve` — parallel branches. None gets
  `↓`: branches are not chain successors of each other, and the rail
  itself is the parallel notation.
- `↓ specialists fan in` — fan-in's emission flows to `sink`. (The
  edge from each branch into fan-in is implicit in the rail; we don't
  individually annotate it.)
- `sink` — terminal.

## K× compression

When K branches (or K cycle members) share the same recursive
structure, they collapse to a single `K× <name>` row. The block keeps
the `⎢` rail when it has inner content:

```
↓ pool (round-robin-workers)
↓   router
↓ ⎢ 4× worker K
↓ ⎢   work
↓   join
  sink
```

- `↓ ⎢ 4× worker K` — the K× header. `⎢` says "this is a parallel
  bracket"; `4× worker K` says "4 identical members." `↓` flows to
  `join`.
- `↓ ⎢   work` — inner content of each worker. `↓` is propagated:
  every worker's `work` step's emission flows out to `join`.
- The rail `⎢` continues for as many rows as the K× block has visible
  content (header + inner). After the block, the next line (`join`)
  has no rail.

When the K× block is a single row (K identical leaves with no inner
content), it renders without the rail — the `K×` prefix carries the
parallel signal alone.

## Edges and metadata

Internal-edge metadata (the actual graph edges between siblings at
each combinator level) is what the renderer consumes; see
`kb/pipeline-shapes.md` for the shape decomposition that produces it.
Every `↓`, `⎢`, `K×`, and `⮥/⮧` annotation traces back to a real edge
or a real shape.

## Open question (deferred)

The renderer doesn't yet show edges from individual rail branches into
the next outer stage. In:

```
  ⎢ skeptic
  ⎢ facts
  ⎢ solve
↓   specialists fan in
```

`solve → fan in` is a real edge but `solve` has no `↓` — the rail
itself implies "all branches eventually merge," but a reader who wants
explicit confirmation has to infer it. Same shape comes up with
multi-element branches that feed into a downstream stage. Tracked as
"parallel steps that feed into other steps"; the right glyph and
position are still TBD.
