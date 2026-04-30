# hn-idea-reaction-half-life: Reaction half-life

> Seed: [hn-ideas.md](hn-ideas.md) → "Cross-temporal" → Reaction
> half-life.

## Context

When a story breaks on HN — a layoff, a product launch, an outage
postmortem — the first 100 comments are usually a different
discussion than the comments 24 hours later. Initial reactions are
emotional and pattern-matching; later comments are post-hoc analysis.
This pipeline compares early-window vs. late-window framing for the
same story, both quantitatively (intensity, edge-class mix, topic
distribution) and qualitatively (a Sonnet "what changed" summary).

The new Datapotamus pattern is **temporal join**: the same story
appears as multiple time-bounded slices, each processed independently
and then joined back for comparison.

## Inputs

- `:source` — `:retrospective` (Algolia date-range search for
  high-velocity stories) or `:story-ids [...]` (explicit list).
- `:since-days` (default 30) for retrospective mode.
- `:min-comments` (default 50) — minimum comment count to qualify.
- `:slices` — slice definitions (default below).
- `:llm-workers`.

## Slice definitions

Time origin t=0 is the story's `created_at`.

| slice | bounds |
| --- | --- |
| `early` | first 100 comments by `created_at`, OR all comments in `[t0, t0 + 1h]`, whichever is fewer |
| `mid` | comments in `[t0 + 1h, t0 + 24h]` |
| `late` | comments in `[t0 + 24h, t0 + 7d]` |

Configurable per run.

## Pipeline shape

```
   story source ─→ fetch-tree (full, with timestamps)
                          ▼
              slice-by-time → emit one msg per (story, slice)
                          ▼
   c/parallel scatter-gather:
       :intensity → reuse hn_density scorer over slice
       :typing    → reuse hn_typing edge classifier over slice's edges
       :topics    → topic-tag (Haiku) per comment, top-K
                          ▼
              gather → per-(story, slice) summary stats
                          ▼
              aggregate-by-story (handler-map :on-all-closed)
                  → temporal-join: collect all slices for each story
                          ▼
   c/stealing-workers :delta → framing-delta-summary (Sonnet)
                          ▼
                       collector
```

## Stages in detail

### slice-by-time

Pure step. Input: a fetched story tree. Output: one msg per slice
that has ≥ `:min-slice-comments` (e.g. 5) comments. Each msg
carries `{:story-id, :slice-name, :comments [...]}`.

### Per-slice metrics (`c/parallel` scatter-gather)

Same combinator pattern as `hn_tempo`. Three named ports:

- `:intensity` — reuse `hn_density/score-comment!` per comment in
  the slice; emit mean + stdev.
- `:typing` — reuse `hn_typing` edge classifier on the slice's
  reply edges; emit class proportions.
- `:topics` — Haiku topic-tag per comment; emit top-K topics by
  count.

Gathered into one map per (story, slice).

### Temporal join (aggregator)

`step/handler-map`:

- `:on-data` stashes each (story, slice) summary, returns
  `msg/drain`.
- `:on-all-closed` groups by `:story-id`, emits one msg per story
  carrying `{:story-id, :slices [<early> <mid> <late>]}` —
  ordered by slice name.

### Framing-delta summary (Sonnet)

One call per story. Input: the per-slice stats plus 5 sample
comments per slice. Schema:

| field | type | notes |
| --- | --- | --- |
| `framing_shift_summary` | string ≤ 50 words | what changed early → late |
| `tone_shift` | string ≤ 25 words | emotional/intensity arc |
| `dominant_topic_early` | string ≤ 8 words | |
| `dominant_topic_late` | string ≤ 8 words | |
| `early_to_late_label` | enum: `cooling`, `escalating`, `widening-scope`, `narrowing-scope`, `pivoting`, `stable` | |

System prompt sketch:

> Two snapshots of the same Hacker News discussion: early reactions
> (first hour) and late reactions (after 24 hours). Quantitative
> stats and 5 sample comments are provided for each. Describe the
> shift between the two — what topic moved, what emotion changed,
> what kind of arc the discussion followed.

## Output

```clojure
[{:story-id ... :title ... :url ...
  :slices
  [{:slice "early" :n 73 :intensity {...} :typing {...} :top-topics [...] :samples [...]}
   {:slice "mid"   :n 184 ...}
   {:slice "late"  :n 92 ...}]
  :delta {:framing-shift-summary "..."
          :tone-shift "..."
          :dominant-topic-early "..."
          :dominant-topic-late "..."
          :early-to-late-label "cooling"}}
 ...]
```

## Datapotamus shapes demoed

- **Temporal join** — same source unit (story) materialized as
  multiple time-bounded slices, each independently scored, then
  joined for cross-slice comparison.
- **Reuse of three pipelines via `c/parallel`** — `hn_density`,
  `hn_typing`, plus a topic tagger. First pipeline that
  scatter-gathers across multiple existing scorer kernels.

## Caching

- All shared HN/Algolia keys.
- Per-comment scoring keys reused from `hn_density` and `hn_typing`
  pipelines.
- `[:framing-delta story-id model schema slice-keys]` keyed by
  the slice definitions so changing the slice bounds invalidates.

## Compute

Per story: scoring on ~300 comments × 2 LLM calls (`density` +
`typing`) ≈ $0.10; topic tag ≈ $0.03; delta summary Sonnet ≈ $0.05.
Total ≈ $0.20/story. 30 stories ≈ $6/run.

## Verification

```
clojure -M -e "(require 'hn-reaction-half-life.core) \
              (hn-reaction-half-life.core/run-once! \
                \"half-life.json\" {:trace? true :since-days 30})"
```

Spot-checks:

1. Slice counts roughly follow the expected timing distribution
   (most stories' first hour is dense; late slice trails off).
2. Delta summary on a known breaking story (e.g. an outage post)
   shows visible shift from "alarm + speculation" → "post-mortem
   analysis."
3. Re-run; per-comment caches at ≈100% hit rate.

## Open questions

- **Slice definitions** — fixed-time vs. fixed-count vs. relative
  ("first 25% of comments"). Default: fixed-time, but expose a
  knob.
- **Source selection** — retrospective Algolia search needs a
  velocity heuristic to find breaking stories. v1: filter to
  stories whose `points` exceeds some threshold *and* whose
  comment count crossed 50 within the first hour (computable from
  Algolia comment timestamps).
- **Delta summary cost** — Sonnet is overkill for some shifts.
  After a calibration run, consider Haiku for the delta and
  reserve Sonnet for high-volatility cases.
