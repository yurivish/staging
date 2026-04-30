# hn-idea-topic-graveyard: Topic graveyard

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-user longitudinal" →
> Topic graveyard.

## Context

For one user, identify topics they used to comment about and have
stopped. Output: a list of "graveyard" topics with their peak quarter,
peak count, and last-seen quarter, plus a few representative comments
from the peak. Adjacent to [`hn-idea-emotion`](hn-idea-emotion.md) but
trimmer — only `:topic` and a substantiveness gate. The interesting
part is the abandonment classifier, not the LLM stage.

## Inputs

- `:user-id`.
- `:since`, `:until`, `:max-comments`.
- `:peak-min` — peak count to consider a topic ever-active
  (default 5).
- `:silent-quarters` — consecutive zero-count quarters to call a
  topic "buried" (default 4).

## Pipeline shape

```
   user-id ─→ fetch-history (shared paginated Algolia)
                   ▼
            split-comments
                   ▼
       c/stealing-workers :tag → topic-tag (Haiku, minimal schema)
                   ▼
            bucket-by-quarter
                   ▼
   aggregator → group user × topic × quarter → counts → emit
                   ▼
        classify-trajectory (pure post-step)
```

## Stages in detail

### Topic tag (Haiku)

Same schema as `hn-idea-emotion`'s tag plus an `is_substantive`
boolean — same as in
[`hn-idea-self-contradiction`](hn-idea-self-contradiction.md). Reuse
the same per-comment LLM cache key so results are shared across
pipelines.

### Aggregator: three-axis grouping

`step/handler-map` `:on-all-closed` does
`group-by [:user-id :topic :year-quarter]` → counts. The windowed
reduce extends to a third axis without changing the combinator.

Emits per `(user, topic)` a sparse time series:

```clojure
{:user-id "..."
 :topic "rust async"
 :quarters {"2018Q3" 4, "2019Q1" 12, "2019Q2" 9, "2020Q4" 1}
 :samples [{:quarter "2019Q1" :preview "..."} ...]}
```

### Trajectory classifier (pure post-step)

For each (user, topic) series, classify:

- `active` — non-zero count in the latest quarter.
- `graveyard` — `:peak-min` ever exceeded, then
  `:silent-quarters` consecutive zeros up to and including the
  latest quarter.
- `fading` — peak ever exceeded, latest quarter < 25% of peak,
  but not yet `:silent-quarters` zeros.
- `ephemeral` — never reached `:peak-min`.

Output keeps only `graveyard` and `fading` rows by default; `:keep
:all` for the full picture.

## Output

```clojure
{:user-id "..."
 :n-comments-classified 5234
 :n-topics 412
 :graveyard
 [{:topic "perl"
   :peak-quarter "2014Q3"
   :peak-count 22
   :last-seen "2017Q1"
   :total-comments 84
   :samples [...]
   :trajectory {"2014Q1" 6 "2014Q2" 11 "2014Q3" 22 ... "2017Q1" 1}}
  ...]
 :fading [...]}
```

## Datapotamus shapes demoed

- **Three-axis grouping inside the aggregator** (user × topic ×
  period). Trivial extension of the two-axis pattern from
  `hn-idea-emotion` but worth showing the same combinator scales
  cleanly.

## Caching

- Shared Algolia + topic-tag cache keys.
- No per-(user, topic, quarter) cache needed — counts come from the
  in-memory aggregator.

## Compute

Same envelope as the topic-tag subset of `hn-idea-emotion` —
Haiku-only, well under $1 per user. No Sonnet.

## Verification

```
clojure -M -e "(require 'hn-topic-graveyard.core) \
              (hn-topic-graveyard.core/run-once! \
                \"pg\" \"graveyard.json\" {:trace? true})"
```

For a long-tenured user (`pg`, `tptacek`, `rayiner`) expect a
recognizable graveyard list — old languages, defunct startups,
former interests. Spot-check one: does the trajectory match
intuition?

## Open questions

- **Peak threshold tuning** — `:peak-min 5` is a first guess.
  Calibrate by hand on 2–3 known long-tenured users.
- **Topic-aliasing** — same as `hn-idea-self-contradiction`: rely
  on the model producing canonical phrases for v1. Add embedding
  clustering only if fragmentation buries real graveyards.
- **Story-context vs comment-context for the topic** — should the
  topic come from the *story title* or the *comment*? Comment is
  closer to "what the user is talking about" and matches
  `hn-idea-emotion`. Stick with comment.
