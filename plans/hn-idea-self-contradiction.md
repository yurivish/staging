# hn-idea-self-contradiction: Self-contradiction finder

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-user longitudinal" →
> Self-contradiction finder.

## Context

For one user, find pairs of comments from different time periods that
take opposing positions on the same substantive topic. Useful both
honestly (people legitimately update their views; "did the world
change or did I?") and as a check on our own consistency. The output
is a small annotated list per user, with each pair's verdict —
*real-contradiction*, *scope-shift*, *world-changed*, or
*genuine-update* — and a short explanation.

Two patterns combine here, both new for this codebase:

- **Pairwise-within-group fan-out** — after grouping comments by
  topic, generate C(n,2) pairs per group and fan them downstream.
- **Filter → expensive two-stage** (same pattern as
  [`hn-idea-mind-changed`](hn-idea-mind-changed.md)) — a cheap
  Haiku scorer prunes; Sonnet judges only the survivors.

## Inputs

- `:user-id`.
- `:since`, `:until`, `:max-comments`.
- `:min-time-gap-days` — minimum gap between paired comments
  (default 90). Pairs from the same week are uninteresting and
  noisy.
- `:filter-threshold` — cutoff for the cheap scorer (0–10, default
  6).
- `:llm-tag-workers`, `:llm-filter-workers`, `:llm-judge-workers`.

## Pipeline shape

```
   user-id ─→ fetch-history (paginated Algolia, shared helper)
                          ▼
                   split-comments
                          ▼
   c/stealing-workers :tag  →  topic-tag + stance-summary (Haiku)
                          ▼
            group-by-topic aggregator (handler-map :on-all-closed)
                          ▼
              per-group: emit C(n, 2) pair msgs
                          ▼
   c/stealing-workers :filter  →  cheap-pair-score (Haiku)
                          ▼
              filter on opposed-score > τ  +  time-gap > min
                          ▼
   c/stealing-workers :judge  →  contradiction-judge (Sonnet)
                          ▼
                       collector
```

## Stages in detail

### 1. Source + history fetch

Reuse the paginated Algolia helper from `hn-idea-emotion`. Single
user only — batch mode is not interesting here.

### 2. Topic + stance tagger (Haiku)

Per comment, structured output:

| field | type | notes |
| --- | --- | --- |
| `topic` | string, 2–5 words, lowercase | normalized topic phrase |
| `stance_summary` | string, ≤ 20 words | the user's claim or attitude on that topic |
| `is_substantive` | boolean | false for jokes, single-link comments, etc. |

Filter out `:is-substantive false` rows.

Topic normalization for v1: rely on Haiku producing the same phrase
for same topic ("rust async", not "Rust's async story"). System
prompt explicitly says: "Use a short, lowercase, canonical topic
phrase. Prefer common terminology." Drop into a normalize step that
lowercases and trims punctuation. Good enough for ~5K-comment
users; revisit if top-K topics fragments badly.

### 3. Group-by-topic aggregator

`step/handler-map`:
- `:on-data` collects every tagged row.
- `:on-all-closed` groups rows by `:topic`, drops singletons,
  emits one msg per non-singleton topic carrying
  `{:topic ... :rows [...]}`.

### 4. Pair fan-out

A pure step that takes a topic group and emits C(n,2) pair msgs:

```clojure
{:topic ...
 :a {:comment-id ... :time ... :stance-summary ... :text ...}
 :b {:comment-id ... :time ... :stance-summary ... :text ...}}
```

Pre-filter pairs with `(abs (- (:time a) (:time b))) <
min-time-gap-days` here, before they reach any LLM stage. Saves
work cleanly.

### 5. Cheap pair scorer (Haiku)

Input: just the two stance summaries plus the topic. Cheap and
short.

| field | type |
| --- | --- |
| `opposed_score` | integer 0–10 |
| `note` | string ≤ 12 words, why |

Survivors: `:opposed-score > τ` (default 6).

### 6. Contradiction judge (Sonnet)

Input: full comment text for both A and B (clipped to ~600 chars
each), plus the story title and parent excerpt for each as context
(Algolia gives `story_title`; parent comment fetched lazily via
Firebase, cached).

Tool schema:

| field | type | notes |
| --- | --- | --- |
| `verdict` | enum: `real-contradiction`, `scope-shift`, `world-changed`, `genuine-update`, `not-actually-opposed` | the call |
| `confidence` | number `[0,1]` | |
| `summary_a` | string ≤ 25 words | what A claims |
| `summary_b` | string ≤ 25 words | what B claims |
| `reconciliation` | string ≤ 50 words | "what would explain both" |

System prompt sketch:

> Two comments by the same author at different times on the same
> topic. Decide whether they actually contradict. Be specific:
> the same author can change their mind (`genuine-update`), the
> world can change (`world-changed`, e.g. policy reversed,
> technology improved), the apparent contradiction can be a
> scope/sub-case difference (`scope-shift`), or the pair was a
> false positive (`not-actually-opposed`).

### 7. Collector

Sort by verdict (most interesting first: `real-contradiction`,
`genuine-update`, `world-changed`, `scope-shift`,
`not-actually-opposed`), then by confidence desc.

## Output

```clojure
{:user-id "..."
 :n-comments 5234
 :n-topics 412
 :n-pairs 18437
 :n-after-filter 1290
 :pairs
 [{:topic "rust async"
   :verdict "genuine-update"
   :confidence 0.86
   :a {:comment-id ... :time "2021-...":summary "..." :preview "..."}
   :b {:comment-id ... :time "2024-...":summary "..." :preview "..."}
   :reconciliation "..."}
  ...]}
```

## Datapotamus shapes demoed

- **Pairwise-within-group fan-out** — group-by aggregator emits
  topic groups, downstream step fans out C(n,2) pairs per group.
  Useful template for any "compare items within a cohort"
  pipeline.
- **Filter → expensive two-stage** (shared with `mind-changed`).

## Caching

- Shared HN/Algolia keys.
- `[:topic-tag comment-id model schema]`,
  `[:contradiction-filter pair-key model]`,
  `[:contradiction-judge pair-key model]` where `pair-key` is a
  sorted tuple of the two comment ids.

## Compute

A 5K-comment user → ~500 topics → typical groupings give ~10K
within-group pairs, ~5K after the time-gap filter, ~10–20% pass
the cheap scorer, so Sonnet runs on a few hundred to ~1K pairs.
Total ≈ $5–15. Tag stage ≈ $1.

## Verification

```
clojure -M -e "(require 'hn-self-contradiction.core) \
              (hn-self-contradiction.core/run-once! \
                \"tptacek\" \"contradiction.json\" {:trace? true})"
```

Spot-checks:

1. Trace `:event :grouped` shows reasonable topic count and
   distribution (no single topic with >50% of comments).
2. `:event :filter-done` survival rate 5–25%.
3. Output: top 5 `:real-contradiction` rows are convincing on
   reading.

## Open questions

- **Topic normalization** — exact-string match on Haiku's output is
  v1. If fragmentation is bad, post-cluster topics with embeddings.
- **Pair compression** — if a topic group has 200 comments, C(200,2)
  = ~20K pairs is heavy on the cheap scorer alone. Cap per topic at
  e.g. 50 most-recent + uniform-sample of older to stay tractable.
- **Cross-user mode** — interesting future extension (do users with
  similar voice-fingerprints contradict on the same topics?). Out
  of scope.
