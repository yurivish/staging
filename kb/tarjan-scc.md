# Tarjan's SCC

## What it operates on

A directed graph. For our pipelines: stages as nodes, data-flow as directed edges.

## What it finds

_Strongly connected components_ — the maximal node-sets in which every node can reach every other. In a DAG every SCC is a singleton; multi-node SCCs are exactly the cycles.

## Why it's the right primitive for "higher-level structure"

Quotient the graph by its SCCs and the result — the _condensation_ — is provably a DAG. So Tarjan cleanly partitions a pipeline into "feedback atoms" (cycles, collapsed to a single super-node) and a feedforward skeleton you can then decompose further (series, parallel, …). Cycles are the _only_ obstruction to DAG-shaped reasoning, and this names each one as an atom.

## How it works — one DFS, two numbers per node

- `index(v)` = preorder discovery number.
- `lowlink(v)` = the smallest `index` reachable from `v`'s subtree via tree edges plus _at most one_ back/cross edge to a node **still on the visit stack**.

Push every newly visited node onto a stack. When recursion on `v` finishes:

- if `lowlink(v) == index(v)`, pop the stack down through `v` — that pop is exactly one SCC;
- otherwise propagate upward: `lowlink(parent) = min(lowlink(parent), lowlink(v))`.

The "still on stack" condition is the whole trick: a node off the stack already belongs to an emitted SCC, so ignoring its index keeps the boundary clean and prevents accidental merging across SCCs.

## Cost

O(V + E). Single pass.

## Free bonus

SCCs come out in **reverse topological order** of the condensation. Reverse the emission and you have a topo-sort of the high-level DAG with zero extra work — exactly the order you want to feed into the next layer of decomposition.

## In our pipeline pass

1. Run Tarjan → list of SCCs. Singletons = ordinary stages; larger ones = irreducible feedback loops.
2. Build the condensation by quotienting on those SCCs. DAG, topo-sorted, for free.
3. Recurse on that DAG (series chains, parallel forks, …) to produce the hierarchical shape.

Linear time, isolates cycles into named atoms, hands you a topo-sorted DAG to decompose further.

# Beyond the algorithm: where it sits in our pipeline model

## How the combinator hierarchy is encoded

The hierarchy lives in the input stepmap itself. A `step` has `:procs` (sid → proc) and `:conns`; a proc is either another step (a sub-combinator) or a `handler-map` (a leaf). That nesting _is_ the combinator tree.

`step/topology` preserves the hierarchy by encoding each node's location as a `:path` — a vector of sids from root to that proc. `conn->edge` builds every edge with `:from-path`/`:to-path` sharing the same `parent-path`, so **every edge is between siblings** by construction. No edge crosses a combinator boundary.

`shape/decompose` recovers the hierarchy with one `group-by`:

```clojure
nodes-by-parent (group-by #(vec (butlast (:path %))) nodes)
edges-by-parent (group-by #(vec (butlast (first %))) edges)
```

`(butlast path)` = "your parent's path." Group by that and you have, for each combinator, the exact subgraph of its direct children. Run Tarjan on each subgraph independently.

## What ends up in `pipeline.json`

- `:topology` — flat `{:nodes :edges}` over all paths at all depths. Lossless. Good for arbitrary graph algorithms downstream.
- `:shape` — the same edges reorganized by the combinator hierarchy with per-level Tarjan applied. Higher-level interpretive view: `:chain` / `:scatter-gather` / `:cycle` / `:prime` at each container.

Both kinds of cycles are encoded uniformly: cycles among leaves of one combinator, and cycles among sibling combinators, both surface at the level where they live as `{:kind :cycle :members [...] :back-edges [...]}`. Cycles cannot span combinator boundaries — the flat-edge property forbids it.

## The `:back-edges` naming wart

A _back edge_ (DFS classification) is the edge that closes a loop — the one pointing to a node still on the recursion stack. Removing the back edges of an SCC breaks all of its cycles.

Our `:cycle` records do **not** store back edges. They store every edge whose both endpoints are inside the SCC (shape.clj:186–190). For `A ⇄ B` that's one extra edge; for larger SCCs it can be many. Fine for "show me everything in this loop"; misleading if a consumer wants the minimum feedback edge set. Renaming to `:internal-edges` would remove the wart with no behavior change.

## The mathematical object

A **clustered (or compound) digraph with the flat-edge constraint**. ("Block" already means 2-edge-connected component in graph theory; the right term for nested clusters is _cluster_ / _compound_.)

A _clustered graph_ (Feng–Cohen–Eades 1995; Sugiyama–Misue 1991 for compound graphs) is a pair $(G, T)$ with $G = (V, E)$ a digraph and $T$ a rooted tree whose leaves are $V$. Our specialization adds:

> For every edge $(u, v) \in E$: $\mathrm{parent}_T(u) = \mathrm{parent}_T(v)$.

Equivalently: $T$ induces a recursive partition of $V$, and $E = \bigsqcup_c E_c$, one digraph $E_c$ per internal node, on its children (each child treated as a single point — possibly atomic, possibly a subtree). The hierarchy _is_ the typology; nothing is inferred from $G$.

### Per-level taxonomy

Two well-developed theories speak directly to our `:chain` / `:scatter-gather` / `:prime` trichotomy:

- **Series-parallel digraphs** (Valdes–Tarjan–Lawler 1982): a two-terminal SP-DAG is built recursively by series composition (`:chain`) or parallel composition (`:scatter-gather`); anything else is _prime_. Linear-time recognition; the SP-tree is unique up to associativity/commutativity. Our `:order` and `:branches` _are_ an SP-tree.
- **Modular decomposition** (Gallai 1967; Cunningham 1982 for digraphs): generalizes SP to a unique tree with linear / complete / prime nodes. SP-DAGs are essentially the prime-free MD trees of digraphs.

Both theories assume DAGs / undirected graphs. Our cycle handling — Tarjan-condense, classify the resulting DAG, re-inflate as `:cycle` records — is the standard _condensation → DAG analysis → re-inflate_ pattern, adding a fourth atom (`:cycle`) alongside S, P, prime.

### Categorical lens

Pipelines are morphisms in a **traced symmetric monoidal category** (Joyal–Street–Verity 1996):

- objects = port types,
- morphisms = boxes with $n$ inputs and $m$ outputs,
- composition $\circ$ = series,
- monoidal product $\otimes$ = parallel,
- trace = feedback (`:cycle`).

A leaf is a generating morphism; a sub-step is a composite. The whole stepmap is a morphism in **Spivak's operad of wiring diagrams** (2013). The flat-edge property is automatic in this view: composition can only wire ports of _direct_ sub-morphisms — reaching across a box's boundary would violate categorical encapsulation. The structural constraint we enforce in the data is the constraint a category gives for free.

### One phrase

Discrete-math framing: **a flat clustered digraph with per-level series-parallel + Tarjan-cycle decomposition**.

Categorical framing: **a morphism in the free traced symmetric monoidal category, presented as a tree of wiring diagrams**.

The two framings are the same object viewed from different sides — the first emphasizes the combinatorial structure available for analysis, the second emphasizes the compositional semantics that produced it.

### Notes on Notation

The one literature pointer that's actually useful: SP-algebra notation from Valdes–Tarjan–Lawler 1982 — the same paper your notes already cite. it gives you a one-line compact form: series as ;, parallel as |. hn.core becomes roughly:
fetch-top-ids ; split-ids ; (16× fetch-story) ; split-comments ; (16× fetch-comment) ; (4× summarize)
your :shape tree maps onto this notation 1:1 with no extra work — :chain → ;-list, :scatter-gather → |-list, :cycle → some loop bracket like μ(...), :prime → opaque box ⟨name⟩. useful if you ever want a single-line summary alongside the indented view, or for log/CLI output where vertical space is tight.
