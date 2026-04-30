# hn-idea-emotion: Emotional landscape over time

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-user longitudinal" →
> Emotional landscape over time.

## Context

For a given HN user, characterize what they feel, about what, with what
valence, and how grounded vs. opinionated their commentary is — across
time. The output is one row per (user, month), with per-emotion counts,
mean valence, mean grounding, and top topics for that month, plus a few
representative comments.

This is the first pipeline that needs **paginated history** (Algolia,
not Firebase) and a **windowed reduce** on the time axis. Both are
shapes the existing pipelines don't exercise. It also slots cleanly
beside `hn_density` (intensity per recent slice) by adding the
longitudinal dimension.

Two entry shapes share the same downstream flow:

- `(run-once-for-user! "tptacek" "out.json" {})` — single-user deep
  dive; source is one literal user-id.
- `(run-once-top-n! "out.json" {:n-stories 30 :m-commenters 50})` —
  reuse `hn_density`'s discovery pattern as the source, then run the
  same per-user analyzer on each.

## Inputs

- `:user-id` (single mode) or `{:n-stories N :m-commenters M}`
  (batch mode).
- `:since`, `:until` — optional `Instant` bounds (default: all-time).
- `:max-comments` — per-user cap, default 10_000.
- `:llm-workers`, `:fetch-workers` — concurrency knobs.

## Pipeline shape

```
                          single mode       batch mode
                              │                 │
                              ▼                 ▼
                         user-id source    front-page → rank-step (from hn_density)
                              │                 │
                              └────────┬────────┘
                                       ▼
                       fetch-history (paginated Algolia)
                                       ▼
                          split-comments  (msg/children)
                                       ▼
              c/stealing-workers :classifiers  →  classify-comment (Haiku)
                                       ▼
                       bucket-by-month  (pure function)
                                       ▼
                 aggregator handler-map  → on-all-closed →
                   group by user-id → group by month →
                     emit one record per (user, month)
```

Reused combinators: `c/stealing-workers`, `step/handler-map` aggregator
with `:on-all-closed` + `msg/merge` (same shape as
`hn_density/aggregate-by-user`).

New shape: the paginated source step and the per-user windowed
reduce. The windowed reduce is the second-stage aggregator: it does
`group-by user-id` like `hn_density`, then within each group does a
secondary `group-by month` and emits one row per (user, month) instead
of one summary per user.

## Stages in detail

### 1. Source

- Single-user: a tiny step that emits `{:user-id "..."}` once.
- Batch: copy `hn_density/mk-fetch-top-ids`, `split-ids`,
  `fetch-tree-step`, `rank-step` verbatim. Their output shape
  (`{:user-id "..." :n-in-top-stories N}`) feeds directly into stage 2.

### 2. `fetch-history` (paginated Algolia)

`https://hn.algolia.com/api/v1/search_by_date?tags=comment,author_<u>&hitsPerPage=1000&page=<n>`
returns `{nbPages, hits[]}`. Each hit gives `objectID`,
`created_at_i`, `comment_text`, `story_id`, `parent_id`,
`story_title`, `author`. We get the story title for free, which is
useful for topic context.

Steps:

1. Issue page 0 to learn `nbPages`.
2. Fan out remaining pages onto the virtual-thread executor (same
   pattern as `hn_density/fetch-tree`'s parallel kids).
3. Concatenate, sort by `created_at_i`, truncate to
   `:max-comments`, filter by `:since` / `:until`.
4. Emit one msg per user: `{:user-id ... :comments [...]}`.

Each comment row keeps `:id :time :text :story-id :story-title`.

LMDB cache key: `[:algolia :by-author author page]` — Algolia is
append-only by date so cached pages never go stale.

### 3. `classify-comment` (Haiku, structured output)

Same langchain4j pattern as `hn_density/score-comment!`:
`AnthropicChatModel` + `ChatRequest` + `ToolSpecification` carrying a
`JsonObjectSchema`.

Schema:

| field | type | notes |
| --- | --- | --- |
| `emotions` | array of strings, max 3 | enum from a fixed vocabulary (see below) |
| `valence` | number in `[-1, 1]` | overall emotional polarity |
| `grounding` | number in `[0, 1]` | share of claim-support (links, numbers, named entities, citations) vs. raw opinion |
| `topic` | string, 2–5 words | freeform short phrase about subject matter |

Fixed emotion vocabulary (Plutchik-flavored, kept small for clean
aggregation): `joy, anger, fear, sadness, disgust, surprise,
anticipation, trust, admiration, contempt, hope, frustration, pride,
amusement, curiosity, neutral`. The model picks ≤3.

System prompt sketch:

> You classify a single Hacker News comment along four axes —
> EMOTIONS (multi-label from the fixed vocabulary), VALENCE (-1
> negative, +1 positive), GROUNDING (0 = pure opinion, 1 = nearly
> every claim has support), and TOPIC (a short phrase capturing
> what the comment is about). The story title is provided as
> context only — classify the comment, not the story. You MUST
> respond by calling the `submit_classification` tool.

User message: comment text plus `Story title: <title>` as a
one-line context line.

LMDB cache key: `[:emotion-classify comment-id model-version
schema-version]`.

### 4. `bucket-by-month`

Pure step. `created_at_i` → `(year, month)` via `java.time`. Stamp
each row with `:year-month "2024-07"`.

### 5. Aggregator (on-all-closed, two-level group)

Same skeleton as `hn_density/aggregate-by-user`:

- `:on-data` stashes every classified row, returns `msg/drain`.
- `:on-all-closed` does `group-by :user-id` then within each user
  `group-by :year-month`, computes per-bucket stats, emits one
  msg per (user, month) via `msg/merge` over that user's parent
  envelopes.

Per-bucket record:

```clojure
{:user-id "tptacek"
 :year-month "2024-07"
 :n-comments 42
 :emotions {:anger 11 :curiosity 8 :amusement 5 ...}
 :valence-mean -0.18
 :valence-std   0.42
 :grounding-mean 0.71
 :top-topics [["go modules" 6] ["rust async" 4] ["TLS pitfalls" 3]]
 :sample-comments [{:comment-id ... :preview "..."} ...]   ;; 3 high/low/mid valence
 :representative-by-emotion {:anger {:comment-id ... :preview "..."}
                              :curiosity {...}}}
```

`top-topics` uses the freeform `:topic` strings, lowercased and
counted; for v1, no clustering — repeated identical phrases
self-aggregate. If sparseness becomes a problem, add a follow-up step
that embeds topics with a local model and clusters them post-hoc;
defer until needed (per "Prefer minimal infrastructure").

## Output

Top-level JSON: `[{:user-id ... :months [<row>, <row>, ...]}, ...]`,
sorted by user-id then month ascending. Single-user mode emits a
1-element array (consistent shape across modes simplifies downstream
viz).

## Datapotamus shapes demoed

- **Paginated source** — first pipeline that pages an external API.
  The pattern is "issue page 0, learn count, fan out the rest on the
  vt-exec." Worth promoting to a helper if a second pipeline reuses
  it.
- **Windowed reduce inside an aggregator** — secondary `group-by` on
  the time axis after the per-user grouping. Cleanly extends the
  `hn_density` aggregator pattern.

Re-uses: `c/stealing-workers` for variable-cost LLM calls,
`step/handler-map` aggregator, `msg/children` fan-out.

## Caching

- `[:algolia :by-author <author> <page>]` — append-only.
- `[:emotion-classify <comment-id> <model> <schema-version>]` —
  invalidate when the schema or model changes.
- Existing `toolkit.llm.cache` already keys requests → responses, so
  if calls go through it the LLM-side cache is automatic; but a
  separate per-comment key is cleaner for ad-hoc replays.

## Compute

Per-call: ~300–800 input tokens, ~100 output tokens. Haiku at current
prices: ~$0.0001 per comment. A 5K-comment single-user run is under a
dollar; a 50-user × 1K-comment batch is ~$5. Cache makes re-runs
free.

Mac-mini constraints: I/O-bound. Algolia rate limits to ~10k req/h
unauthed; pagination is fine. No local-model concerns.

## Verification

Single-user one-shot, watching the trace event stream:

```
clojure -M -e "(require 'hn-emotion.core) \
              (hn-emotion.core/run-once-for-user! \
                \"tptacek\" \"emotion.json\" {:trace? true \
                                              :max-comments 500})"
```

Spot-check expectations:

1. Trace shows `:event :history-fetched` with `:n-comments` matching
   the user's recent activity.
2. `:event :classified` events tag emotions / valence / grounding;
   per-comment latency under 2s (Haiku).
3. Final JSON has `:months` covering a contiguous range; no months
   with `:n-comments` 0 (we omit empty months).
4. Re-run the same command: cache hit rate ~100%; total wall time
   drops to network-only.

Property tests (mirror existing `*_property_test.clj` patterns): the
windowed reduce should be associative on contiguous time ranges —
splitting a user's comments into two halves and aggregating each,
then merging, should yield the same per-month rows as aggregating the
full set.

## Open questions

- **Single-user vs. batch entrypoints — one ns or two?** Both modes
  share 90% of the flow; one ns with two `run-once-*` exports,
  branching on the source step, matches the rest of the project.
- **Topic clustering: defer or include in v1?** Defer. Freeform
  short phrases self-aggregate well enough on a single user; revisit
  only if the top-K is unreadable.
- **Submissions vs. comments only?** Comments-only for v1. Stories
  are a different unit (no parent context, different LLM prompt).
- **Open-ended emotion vocab?** Fixed for v1 (clean aggregation,
  reproducible). The schema can grow if the fixed list misses
  something common.
