# hn-idea-domain-personality: Domain personality

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-story discourse" → Domain
> personality.

## Context

What does an HN discussion look like, on average, when the linked
article comes from arxiv.org vs. nytimes.com vs. github.com vs.
substack.com? Aggregate per-story metrics across many submissions
from a given domain to characterize the *shape* of the discussion
each domain attracts: how many comments, depth, time-to-first-reply,
disagree-rate, info-density-mean, emotional-intensity-mean,
top-edge-classes.

Two-level fan-out: per (domain, story) → per-comment, two-level
group-by in the aggregator (domain → story → metrics → roll-up).

## Inputs

- `:domains` — list of hostnames (default
  `["arxiv.org" "github.com" "nytimes.com" "youtube.com"
    "substack.com" "medium.com" "wikipedia.org"]`).
- `:n-stories-per-domain` (default 50).
- `:since-days` (default 365) — sample window.
- `:k-comments-per-story` (default 30).

## Pipeline shape

```
   for each domain:
        Algolia search_by_date stories with points>τ in window
                       ▼
            filter client-side by hostname
                       ▼
            top-N by points
                       │
   merged stream:
        (domain, story) ─→ fetch-tree (hn_density helper)
                       ▼
               compute per-story metrics
                  (shape, density, typing — reuse existing)
                       ▼
            aggregate-by-domain (handler-map :on-all-closed,
                  group by domain → per-domain stats)
                       ▼
                    collector
```

## Per-story metrics

For each fetched story tree, compute:

- **From `hn_shape`**: depth, n-comments, n-direct-replies, fanout
  fingerprint, time-to-first-reply, lifetime, peak-rate.
- **From `hn_density`** (top-K comments only): per-comment
  `:density` and `:emotion`; aggregate to mean + stdev.
- **From `hn_typing`** (top edges only): edge-class histogram.

Each existing pipeline already computes these. The cleanest
factoring is to extract their per-story compute into reusable
functions in their own namespaces, and have this pipeline call
them. If that refactor is too invasive, copy the compute steps
verbatim — they're under 50 lines each.

## Per-domain aggregation

`:on-all-closed`: `group-by :domain`, then per-domain compute:

- `n-stories`
- For each per-story metric: pooled mean + stdev across stories
- Edge-class proportions: sum across stories, normalize
- Top-K representative stories (highest n-comments)

## Output

```clojure
[{:domain "arxiv.org"
  :n-stories 50
  :n-total-comments 12873
  :shape {:mean-depth 5.2 :mean-n-comments 257 :mean-time-to-first-reply-s 412 ...}
  :density {:mean 6.4 :stdev 1.1}
  :emotion {:mean 3.8 :stdev 1.4}
  :typing-proportions {:agree 0.18 :disagree 0.21 :correct 0.14 :extend 0.22
                       :tangent 0.07 :attack 0.04 :clarify 0.14}
  :representative-stories [{:story-id ... :title ... :n-comments ...} ...]}
 ...]
```

## Datapotamus shapes demoed

- **Two-level fan-out** with two-level aggregation: domain →
  story → comment, then comment → story → domain coming back.
- **Reuse of three existing pipelines' compute** as library
  functions, validating that the per-pipeline compute kernels are
  factorable. This is the first pipeline to demand that
  refactor; if the caller-vs-copy decision is unclear, copy for
  v1 and revisit.

## Caching

- `[:hn :item id]`, `[:algolia :search query date-range page]`
  shared.
- All per-comment scoring keys shared with `hn_density`,
  `hn_typing`. Big cache win if those pipelines have run.

## Compute

7 domains × 50 stories = 350 stories × ~30 top comments each ×
two LLM calls (`density` + `typing`) ≈ 21K Haiku calls ≈ $5–8.
Mac-mini-fine; runs in 30–60 minutes wall-clock.

## Verification

```
clojure -M -e "(require 'hn-domain-personality.core) \
              (hn-domain-personality.core/run-once! \
                \"domain-personality.json\" {:trace? true})"
```

Spot-checks:

1. arxiv.org: expect high `:density.mean`, low `:emotion.mean`,
   high `:correct` and `:extend` rates.
2. nytimes.com: expect lower `:density.mean`, higher `:emotion.mean`,
   higher `:disagree` and `:attack` rates.
3. github.com: expect short `:mean-depth`, lots of `:clarify`
   edges (questions about the repo).
4. Trace `:event :ranked` per domain shows comparable story counts
   (no domain massively under-represented).

## Open questions

- **Reuse vs. copy** — first pipeline that exposes the awkwardness
  of copy-paste compute kernels across `hn_shape`, `hn_density`,
  `hn_typing`. Worth biting off the refactor here, or copying
  verbatim and waiting for the second consumer? Recommend copy
  for v1, refactor when the second cross-domain pipeline lands.
- **URL canonicalization** — `youtube.com` vs `www.youtube.com` vs
  `m.youtube.com` are the same domain. Use `psl`-style public
  suffix list canonicalization.
- **Story selection bias** — top-N by points biases toward
  long-form/popular stories. Alternative: random sample from the
  window. Default top-by-points; add a flag.
