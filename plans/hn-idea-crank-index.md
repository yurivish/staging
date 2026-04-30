# hn-idea-crank-index: Crank index

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-user longitudinal" → Crank
> index.

## Context

For one user, identify topics that draw outsized emotional intensity
relative to that user's own baseline. The interesting signal is
*relative*: a generally-calm commenter who lights up only on one
topic is a more interesting find than someone who's calm on
everything. Output is a per-user ranked list of (topic, mean
intensity, z-score, count) — the user's "cranks."

This is a derivative of [`hn-idea-emotion`](hn-idea-emotion.md):
same classifier, different aggregation.

## Inputs

- `:user-id` (or top-N batch source).
- `:since`, `:until`, `:max-comments`.
- `:min-count` — topic must have at least this many comments to
  qualify (default 5).

## Pipeline shape

```
   user-id ─→ fetch-history (shared)
                   ▼
            split-comments
                   ▼
   c/stealing-workers :classify → topic + intensity (Haiku)
                   ▼
   aggregator → per-(user, topic) sums and counts
                + per-user grand totals
                → emit per-user ranked topic list
```

## Per-comment classifier (Haiku)

Two fields suffice — the rest of the `hn-idea-emotion` schema is
unnecessary cost here:

| field | type |
| --- | --- |
| `topic` | string, 2–5 words, lowercase |
| `intensity` | integer 0–10 |

Cache shares the `[:emotion-classify ...]` key from `hn-idea-emotion`
when the schema is a strict subset; otherwise use a separate
`[:crank-classify comment-id ...]` key.

## Aggregator: two-pass within `:on-all-closed`

In `:on-data` stash rows; in `:on-all-closed`:

1. Group rows by user.
2. For each user, compute baseline intensity:
   `μ_user = mean(intensity over all comments)` and
   `σ_user = stdev(intensity over all comments)`.
3. Group rows within user by topic. For each `(topic, ≥ min-count)`,
   compute `μ_topic = mean(intensity)` and the z-score
   `(μ_topic − μ_user) / σ_user`.
4. Emit per user: full ranked topic list (descending z-score).

Single-pass over data per user, with two reductions. No new
combinator.

## Output

```clojure
{:user-id "..."
 :n-comments 5234
 :baseline {:mean 4.6 :stdev 1.9}
 :topics
 [{:topic "javascript ecosystem" :n 142 :mean-intensity 7.8 :z 1.68 :samples [...]}
  {:topic "k8s complexity"        :n 41  :mean-intensity 7.5 :z 1.53 :samples [...]}
  ...]}
```

## Datapotamus shape

No new combinator — composition of the existing aggregator pattern
with a two-pass reduction inside `:on-all-closed`. Worth including
in the catalog because it shows the aggregator's flexibility:
`:on-all-closed` is a full data closure, free to do whatever it
wants with the stashed rows.

## Caching

Shared with `hn-idea-emotion` if the schema is a strict subset.
Otherwise its own classify cache.

## Compute

Same as the topic-tag subset of `hn-idea-emotion`. Haiku-only,
well under $1 per user.

## Verification

```
clojure -M -e "(require 'hn-crank-index.core) \
              (hn-crank-index.core/run-once! \
                \"<user>\" \"crank.json\" {:trace? true})"
```

Spot-checks:

1. Trace `:event :baseline` shows reasonable per-user mean (HN
   averages ≈ 5 on a 0–10 scale across most users).
2. Top-z topics on a known opinionated commenter match intuition.
3. `:samples` for a top-z topic are visibly more charged than the
   user's average comment.

Property: shuffling input order should not change z-scores (within
float epsilon).

## Open questions

- **Topic-count threshold** — start at 5; revisit if top-z is
  dominated by tiny topics with one very-charged comment.
- **Robust statistics** — z-score is sensitive to outliers. If a
  user has a few extreme comments, consider median + MAD instead
  of mean + stdev. Defer.
- **Fold into `hn-idea-emotion`?** Tempting (same classifier), but
  the aggregation is different enough that splitting is cleaner.
  Keep separate.
