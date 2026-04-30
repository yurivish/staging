# hn-idea-shared-premise: Hidden-shared-premise extractor

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-thread" → Hidden
> shared-premise extractor.

## Context

For one contested HN thread, surface the unstated assumption that
*everyone* — including the participants who disagree with each other
— is taking for granted. The result is often the most interesting
thing about a discussion (and frequently funny). Output is per-thread:
the assumption plus a one-line excerpt from each "side" showing how
the assumption surfaces in their language.

A tighter sibling of [`hn-idea-argument-graph`](hn-idea-argument-graph.md):
same source and linearization, much smaller LLM stage. Standalone
because the schema is small enough that running the heavier
extraction is overkill if all you want is the punchline.

## Inputs

Same as `hn-idea-argument-graph`:

- `:n-stories` (default 30) or `:story-id`.
- `:min-depth`, `:min-comments`, `:max-threads-per-story`.

## Pipeline shape

```
   top-stories ─→ fetch-tree (hn_density helper)
                       ▼
            select-subtrees (shared with argument-graph)
                       ▼
              linearize-thread  (shared)
                       ▼
   c/stealing-workers :extract → premise-extract (Sonnet)
                       ▼
                    collector
```

Stages 1–3 are copy-paste with `hn-idea-argument-graph`. Once that
pipeline ships, extract `select-subtrees` and `linearize-thread` to
a small shared helper namespace (`hn_thread.core`) and have both
require it.

## Premise extract (Sonnet)

Tool schema:

| field | type | notes |
| --- | --- | --- |
| `shared_premise` | string ≤ 25 words | the unstated assumption |
| `confidence` | number `[0,1]` | |
| `evidence_a` | string ≤ 25 words | quoted or paraphrased line where the premise surfaces on one side |
| `evidence_b` | string ≤ 25 words | likewise from the other side |
| `is_substantive` | boolean | false if the thread is too small or too pleasant to have a hidden premise |

System prompt sketch:

> Read this Hacker News thread. The participants disagree with
> each other, but they share an unstated assumption — something
> all of them treat as obvious that an outsider might question.
> State the shared premise, then give two short excerpts (one
> from each side) that show the premise at work. If the thread
> doesn't have one (it's too small, or all participants agree),
> set `is_substantive` to false.

## Output

```clojure
[{:story-id ... :story-title ...
  :thread-root-id ...
  :n-comments 23
  :shared-premise "..."
  :confidence 0.78
  :evidence-a "..."
  :evidence-b "..."
  :is-substantive true}
 ...]
```

Drop `:is-substantive false` rows from the final output.

## Datapotamus shape

Same thread-as-unit input as `hn-idea-argument-graph`. Worth its
own pipeline (a) because the smaller LLM call is much cheaper, and
(b) because the output reads as standalone editorial content
without the graph machinery around it.

## Caching

- `[:hn :item id]` shared.
- `[:premise-extract thread-root-id model schema thread-content-hash]`
  — content-hash because comments can be added to a still-active
  thread.

## Compute

Sonnet at ≈ $0.02–0.06 per thread. 30 stories × ~2 threads each ≈
$1–4 per run.

## Verification

```
clojure -M -e "(require 'hn-shared-premise.core) \
              (hn-shared-premise.core/run-once! \
                \"premise.json\" {:trace? true :n-stories 10})"
```

Read 5 random outputs. The premise should be something that, once
stated, makes you go "huh, yeah, no one is questioning that." If
premises are vague or trivially true ("computers are real"), the
prompt needs more bite.

## Open questions

- **Two-sides assumption** — some threads are 3+ camps. The schema
  asks for two evidence excerpts; if the dataset has many 3-way
  threads, generalize to an array of evidence quotes. Defer.
- **Fold into `hn-idea-argument-graph`?** The `argument-graph`
  schema already has `shared_assumption` as a field. They could be
  one pipeline run with `:mode :graph` vs `:mode :premise-only`.
  Keep separate for v1; merge if the duplication grows.
