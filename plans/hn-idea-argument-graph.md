# hn-idea-argument-graph: Argument graph extraction

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-thread" → Argument graph
> extraction.

## Context

For one deep HN thread, extract the implicit argument structure —
claims, supports, counters, the central contested question, and the
unstated assumption that both sides share — as a structured graph.
Output is a Toulmin-flavored graph per thread, suitable for rendering
or as input to other analyses.

This is the first pipeline that **reasons over a whole subtree as one
input.** All existing pipelines either work per-comment, per-edge, or
across whole stories statistically. Linearizing a subtree and handing
it to one Sonnet call is a different shape worth demoing.

## Inputs

- `:n-stories` (default 30) or `:story-id` (single-story mode).
- `:min-depth`, `:min-comments` — subtree filters (defaults: depth
  ≥ 4, comments ≥ 8). Anything smaller doesn't have argument
  structure worth extracting.
- `:max-threads-per-story` (default 3) — pick the deepest subtrees.

## Pipeline shape

```
   top-stories ─→ fetch-tree (from hn_density)
                       ▼
            select-subtrees (pure: depth + size filter, top-K per story)
                       ▼
                linearize-thread (pure)
                       ▼
   c/stealing-workers :judge → extract-argument-graph (Sonnet)
                       ▼
                    collector
```

## Stages in detail

### Select subtrees

A subtree is rooted at any non-root comment in a story. Pure
function over the fetched story tree:

1. Walk the tree producing every subtree.
2. Filter: depth ≥ `:min-depth`, total comments ≥
   `:min-comments`.
3. Score each surviving subtree by `(depth × comments)` and keep
   top `:max-threads-per-story` per story.

### Linearize thread (pure)

Output a compact text representation that the LLM can read in
context:

```
[1] <author1>: <comment text, clipped 400 chars>
  [2] <author2> → [1]: <text>
    [3] <author3> → [2]: <text>
    [4] <author1> → [2]: <text>
```

Numbered, indented by depth, each line carries the responder's id
and which line they're replying to. This is friendlier than raw
JSON and more compact than a full nested representation. Numbers
are local 1..N within the linearized thread (we keep a mapping back
to HN comment ids).

### Extract argument graph (Sonnet)

One ChatRequest per thread. Tool with a nested schema:

```
{
  "central_question":   string ≤ 25 words,
  "claims":             array of { "id": string,
                                   "by_line": int,         # 1-indexed line in linearization
                                   "text": string ≤ 30 words,
                                   "stance": enum: {supports, opposes, neutral} },
  "supports":           array of { "from_claim": string,
                                   "to_claim":   string,
                                   "kind": enum: {data, citation, lived-experience,
                                                   analogy, authority, edge-case},
                                   "summary": string ≤ 20 words },
  "counters":           array of { "from_claim": string,
                                   "to_claim":   string,
                                   "summary":    string ≤ 20 words },
  "shared_assumption":  string ≤ 25 words,
  "best_question_unasked": string ≤ 20 words
}
```

System prompt: present the linearized thread, instruct the model to
identify the contested central question, the claim graph (each
claim attributed to a line number), the support and counter
relationships, the assumption every participant takes for granted,
and the most important question nobody actually asked.

### Collector

Emit one row per thread. Map the LLM's local line numbers back to
HN comment ids before output so downstream consumers can deep-link.

## Output

```clojure
[{:story-id ... :story-title ...
  :thread-root-id ...
  :n-comments 23
  :depth 7
  :graph {:central-question "..."
          :claims [{:id "C1" :comment-id ... :by "..." :text "..." :stance "supports"} ...]
          :supports [{:from "C1" :to "C2" :kind "data" :summary "..."} ...]
          :counters [{:from "C1" :to "C3" :summary "..."} ...]
          :shared-assumption "..."
          :best-question-unasked "..."}}
 ...]
```

## Datapotamus shape demoed

- **Thread-as-unit input** — the LLM call's input is a whole
  linearized subtree, not a single comment or edge. Different
  combinator usage from the existing per-item pipelines.
- The subtree-selection + linearization steps are pure and
  composable; the LLM step rides on `c/stealing-workers` because
  per-thread cost varies (token count varies wildly).

## Caching

- `[:hn :item id]` — shared.
- `[:argument-graph thread-root-id model schema]` — keyed by the
  thread root + the time of fetch (a still-active thread may have
  added comments since the cached graph was extracted; embed the
  comment-count in the key, or a content hash, to avoid stale
  hits).

## Compute

Sonnet input ≈ 2–8K tokens per thread, output ≈ 500 tokens. ≈
$0.05–0.20 per thread. 30 stories × ~2 threads each = ~60 calls
≈ $3–12 per run.

## Verification

```
clojure -M -e "(require 'hn-argument-graph.core) \
              (hn-argument-graph.core/run-once! \
                \"argument-graph.json\" {:trace? true :n-stories 10})"
```

Spot-checks:

1. Each emitted graph's `:claims[].comment-id` resolves to a real
   HN comment in the source thread.
2. `:supports` and `:counters` reference claim ids that exist.
3. Read 3 outputs end-to-end: the `:central-question` is plausible
   and the `:shared-assumption` reads as a real unstated premise.

Property test: `select-subtrees` must produce subtrees that all
satisfy `:min-depth` and `:min-comments`.

## Open questions

- **Linearization vs. tree input** — indented text is the bet here.
  An alternative is JSON tree input; less ergonomic for the LLM,
  more parseable. Try indented first, A/B if extraction quality
  is poor.
- **Single-graph-per-story vs. per-subtree** — multiple subtrees
  per story is the v1 default; some stories' main argument lives
  across multiple sibling threads. Stitching them is a future
  extension.
- **Confidence calibration** — the schema doesn't ask for a
  per-claim confidence. Add if downstream consumers need it.
