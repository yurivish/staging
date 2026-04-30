# hn-idea-topic-seasonality: Topic seasonality

> Seed: [hn-ideas.md](hn-ideas.md) → "Cross-temporal" → Topic
> seasonality.

## Context

What does HN talk about in January vs. June vs. November? Across many
years, are there topics that show consistent month-of-year peaks —
"new year's resolution" software, summer-launch announcements,
end-of-year layoff postmortems, advent-of-code, hackathon season?

The novel time-axis trick is **modular bucketing**: instead of
windowing by absolute (year, month), we bucket by month-of-year and
aggregate across many years. Topics with high cross-year consistency
*and* high month-of-year variance are seasonal.

## Inputs

- `:since`, `:until` — window spanning multiple years (default
  last 5–10 years).
- `:n-stories-per-day` (default 10) — sampling rate to keep cost
  bounded.
- `:min-points` (default 50).

## Pipeline shape

```
   Algolia search_by_date stories per (year, month) bucket
                       ▼
                sample top-N per day
                       ▼
   c/stealing-workers :tag → topic-tag (Haiku)
                       ▼
            bucket-by-month-of-year (pure)
                       ▼
        aggregator → group by (topic, month-of-year), count
                       ▼
            seasonality-classify (pure post-step)
```

## Stages in detail

### Sampling

Algolia by-date with `points >= :min-points` and pagination,
day-by-day or month-by-month. Sample at most
`:n-stories-per-day` per day to bound the run.

### Topic tag (Haiku)

Same schema as other pipelines: `topic` short phrase plus
`is-substantive`. Cache shared.

### Bucket by month-of-year

Pure step. Compute month-of-year `1..12` from `created_at_i`. Stamp
each row with `:moy 1` etc. Drop the year — we want
cross-year aggregation here.

### Aggregator

`:on-all-closed`: `group-by [:topic :moy]` → counts. Within each
topic, also compute `total-n` (sum across moy) and per-moy
fraction.

### Seasonality classify (pure post-step)

For each topic:

- `:base-rate` = `total-n / 12` (uniform expectation).
- `:peak-moy` = month with max count.
- `:peak-z` = `(peak-count − base-rate) / sqrt(base-rate)` —
  Poisson z-score.
- `:seasonality` enum:
  - `seasonal` — `peak-z > 3` AND `peak-count > 2 × base-rate`.
  - `weakly-seasonal` — `peak-z > 1.5`.
  - `aseasonal` — otherwise.

Drop topics with `:total-n < 20` (too sparse to classify).

## Output

```clojure
[{:topic "advent of code"
  :total-n 412
  :peak-moy 12
  :peak-count 386
  :base-rate 34.3
  :peak-z 60.1
  :seasonality :seasonal
  :by-moy {1 0, 2 0, ..., 11 12, 12 386}}
 {:topic "new year's resolutions"
  :seasonality :seasonal
  :peak-moy 1 ...}
 ...]
```

## Datapotamus shape

- **Modular time bucketing** — month-of-year, dropping the year.
  Variant of the windowed-reduce pattern; same combinator, new
  bucketing function.

## Caching

- Shared HN/Algolia and topic-tag keys with other pipelines. Big
  win when re-running across overlapping time windows.

## Compute

Sample 10 stories/day × 365 × 5 years = ~18K stories. Haiku tag
≈ $2–5 first run; near-zero subsequent.

## Verification

```
clojure -M -e "(require 'hn-topic-seasonality.core) \
              (hn-topic-seasonality.core/run-once! \
                \"seasonality.json\" {:trace? true \
                                      :since \"2019-01-01\" \
                                      :until \"2024-12-31\"})"
```

Spot-checks:

1. `:advent of code` peaks December.
2. Hackathon-related topics peak around YC batch deadlines.
3. New-year resolution / productivity topics peak January.
4. Most topics classify `:aseasonal` — sanity check that the
   threshold isn't too generous.

## Open questions

- **Topic granularity** — same as other tag-using pipelines.
  Consider clustering before seasonality classification if
  fragmentation hides real seasonal signals. Defer.
- **Zero-inflation handling** — many topics have months with
  zero hits. The Poisson z-score handles this naively; for
  rigor, use a mixture model. Defer.
- **Sampling bias** — `top-N per day` over-represents high-
  attention stories. For neutral seasonality of *all* HN
  discussion, drop the `:min-points` floor and sample uniformly.
  Trade-off: cost.
