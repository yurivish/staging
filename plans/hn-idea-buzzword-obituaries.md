# hn-idea-buzzword-obituaries: Buzzword obituaries

> Seed: [hn-ideas.md](hn-ideas.md) → "Cross-temporal" → Buzzword
> obituaries.

## Context

Track term frequency in HN titles and comments by month across years.
For each term: produce a time series of monthly counts, identify the
peak month, and classify whether it's "buried" (current usage well
below peak), "fading," "sustained," or "rising." No LLM. Useful for
visualization (the eulogy chart), and for grounding qualitative
claims about technology cycles.

The cleverness here is that Algolia returns `nbHits` for any search,
so each (term, month) bucket is **one HTTP request that doesn't need
to fetch any results** — just the count.

## Inputs

- `:terms` — list of strings (default: a curated list — see below).
- `:since` (default `"2010-01-01"`).
- `:until` (default today).
- `:bucket` — `:month` (default) or `:quarter`.
- `:scope` — `:comments`, `:stories`, or `:both` (default).

Default term list (extend freely):

```
"web3", "blockchain", "metaverse", "GPT-3", "GPT-4", "LLM",
"agentic", "AGI", "RAG", "kubernetes", "k8s", "docker",
"serverless", "microservices", "monorepo", "rust", "go", "elm",
"clojure", "wasm", "WebAssembly", "edge computing", "5G",
"AR/VR", "deep learning", "machine learning", "big data",
"NoSQL", "GraphQL", "TypeScript", "Svelte", "React", "Vue"
```

## Pipeline shape

```
   (term, bucket) cross-product
                       ▼
     c/workers :counter → algolia-count (nbHits via hitsPerPage=0)
                       ▼
   aggregate-by-term (collect time series)
                       ▼
       trajectory-classify (pure post-step)
                       ▼
                    collector
```

## Algolia count step

```
GET https://hn.algolia.com/api/v1/search
  ?query=<URL-encoded term>
  &tags=<comment|story>          ;; or no tag for both
  &numericFilters=created_at_i>=<lo>,created_at_i<=<hi>
  &hitsPerPage=0
```

Response gives `nbHits`. Cache key
`[:algolia :nbhits term scope lo hi]` — append-only since the time
range is fully bounded.

## Per-term aggregation + classification

After all (term, bucket) counts are in:

- Time series: `[{:bucket "2017-04" :n 312} ...]`.
- `:peak` — bucket with max count.
- `:current` — most-recent bucket count.
- `:trajectory` — enum:
  - `buried` — `current ≤ 0.1 × peak` and time-since-peak ≥ 12
    months.
  - `fading` — `0.1 × peak < current ≤ 0.5 × peak` and
    time-since-peak ≥ 6 months.
  - `sustained` — `0.5 × peak < current ≤ 1.5 × peak` for ≥ 12
    months.
  - `rising` — `current > 1.5 × peak` over a recent 6-month
    window.

## Output

```clojure
[{:term "web3"
  :scope :both
  :n-total 12873
  :peak {:bucket "2022-01" :n 942}
  :current {:bucket "2026-04" :n 51}
  :trajectory :buried
  :series [{:bucket "2010-01" :n 0} ... {:bucket "2026-04" :n 51}]}
 ...]
```

## Datapotamus shapes demoed

- **Multi-query source over a cross-product** — terms × buckets is
  a clean cross-product fan-out, much smaller and cheaper than the
  comment-fan-out pipelines because each work unit is just a
  count.
- **No LLM at all** — pure I/O + arithmetic. Same family as
  `hn_shape` but with a different data source.

## Caching

- `[:algolia :nbhits term scope lo hi]` — every cell of the
  cross-product cached. Re-runs are free except for buckets that
  *include today* (those are still mutable; either skip caching
  the current bucket or re-fetch it on every run).

## Compute

35 terms × 16 years × 12 months = ~6700 cells. At 16-way
parallelism and 100ms per Algolia call, ~45s wall-clock first run;
near-zero on subsequent runs.

No LLM cost.

## Verification

```
clojure -M -e "(require 'hn-buzzword-obituaries.core) \
              (hn-buzzword-obituaries.core/run-once! \
                \"buzzwords.json\" {:trace? true})"
```

Spot-checks:

1. `web3` peaks in 2021–2022, classified `:buried`.
2. `blockchain` peaks in 2017–2018, classified `:buried` or
   `:fading`.
3. `LLM` rises sharply post-2022, classified `:rising` or
   `:sustained`.
4. Re-run; cache hit rate ~100% except for the most-recent bucket.

## Open questions

- **Phrase-matching gotchas** — Algolia tokenizes; multi-word
  terms like "edge computing" or "AI alignment" benefit from
  quoted searches (`"edge computing"`). Test default behavior.
- **Disambiguation** — "rust" the language vs. "rust" the
  oxidation. v1: rely on HN's domain bias and accept noise. For
  ambiguous terms, add a `:tags` filter (e.g. `comment`) and a
  required co-occurrence term (`tags=comment AND query=rust
  language`).
- **Auto-discovery of rising terms** — instead of a fixed list,
  diff month-over-month n-gram frequencies in titles/comments to
  surface emerging terms. Out of scope for v1; revisit once the
  curated-term version is shipped.
