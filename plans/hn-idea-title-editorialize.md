# hn-idea-title-editorialize: Title-vs-source editorialization

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-story discourse" → Title
> editorialization.

## Context

Submitted HN titles often differ from the linked page's actual
`<title>`. Sometimes that's a strict improvement (the source title is
clickbait, the submitter trims it); sometimes it adds spin, removes
context, or just makes a mistake. This pipeline measures the gap
across many submissions: per (HN-title, source-title) pair, compute
similarity metrics and divergence flags. No LLM by default.

A small companion pipeline. The interesting output is the *rate*
across the front page (or a date-range archive): what fraction of
submissions are essentially the source title, and what fraction
diverge meaningfully?

## Inputs

- `:source` — `:topstories` (default), `:beststories`, or
  `{:since "..." :until "..."}` for an Algolia archive sweep.
- `:n-stories` (default 100) for the topstories source.
- `:fetch-workers`.
- `:judge-divergent?` — boolean. If true, run an optional LLM
  stage on highly-divergent pairs to classify the kind of
  editorialization. Default false.

## Pipeline shape

```
   stories source ─→ filter out URLless (Show HN / Ask HN)
                       ▼
              c/workers :fetch-page → fetch-source-title
                       ▼
                 compute-metrics (pure)
                       ▼
   (optional, gated by :judge-divergent?)
              c/stealing-workers :judge → editorialize-classify (Haiku)
                       ▼
                    collector
```

## Stages in detail

### Source stories

- `:topstories` — Firebase top-stories ids → fetch each item.
- `:beststories` — same shape with `/v0/beststories.json`.
- `:since/:until` — Algolia search_by_date with `tags=story` and
  `points>20` over the window.

Drop items where `:url` is missing.

### Fetch source title

HTTP GET with a 10s timeout. Extract:
- `<title>` text (first match).
- `<meta property="og:title" content="...">`.
- `<meta name="twitter:title" content="...">`.

Pick the first non-empty one in that order. Cache `[:page-title
:url <url>]`.

Skip non-HTML responses; mark them as `:source-title-missing`.

### Compute metrics (pure)

For each pair `(hn-title, source-title)`:

| metric | type | notes |
| --- | --- | --- |
| `exact-match` | bool | string equality |
| `ci-match` | bool | case-insensitive equality |
| `token-jaccard` | float | over lowercased word sets minus stop-words |
| `normalized-edit-distance` | float | Levenshtein / max-len |
| `lcs-token-fraction` | float | longest common token subsequence length / longer-title length |
| `length-ratio` | float | hn-tokens / source-tokens |
| `bracket-tags` | array of strings | `[Show HN]`, `[Ask HN]`, `[paywall]`, `[YYYY]` etc. detected in HN title |
| `paren-asides` | array of strings | parenthesized additions in HN title not in source |
| `q-mark-delta` | int | hn `?`-count minus source |
| `excl-mark-delta` | int | likewise |
| `divergence-flag` | enum: `identical`, `near-identical`, `clipped`, `expanded`, `rephrased`, `divergent` | rule-based summary |

`divergence-flag` rules (rough):
- `identical` if `exact-match`.
- `near-identical` if `ci-match` or `normalized-edit-distance < 0.05`.
- `clipped` if `length-ratio < 0.7` and `lcs-token-fraction > 0.6`.
- `expanded` if `length-ratio > 1.3` and `lcs-token-fraction > 0.6`.
- `rephrased` if `lcs-token-fraction > 0.4` and `token-jaccard > 0.5`
  but neither clipped nor expanded.
- `divergent` otherwise.

### Optional LLM judge (Haiku)

Run only when `:judge-divergent? true` and `divergence-flag` ∈
`#{:rephrased :divergent}`. Schema:

| field | type | notes |
| --- | --- | --- |
| `kind` | enum: `summarize-shorter`, `add-context`, `add-spin`, `clickbait-fix`, `add-source-attribution`, `mistake`, `other` | what kind of editorialization |
| `summary` | string ≤ 25 words | one-sentence rationale |

### Collector

Sort by divergence (most divergent first). Compute aggregate:

- per-flag counts and rates
- top-10 most-divergent rows

## Output

```clojure
{:n-stories 100
 :n-with-source-title 87
 :flag-rates {:identical 0.31 :near-identical 0.21 :clipped 0.18
              :expanded 0.07 :rephrased 0.16 :divergent 0.07}
 :rows
 [{:story-id ... :hn-title "..." :source-title "..." :url "..."
   :metrics {...}
   :flag :divergent
   :judged {...}}      ;; only when :judge-divergent? was on
  ...]}
```

## Datapotamus shape

A no-LLM-by-default pipeline at the per-story unit, with a gated
optional LLM tail. Demonstrates per-story fan-out for a non-tree
purpose (no comment-tree work) and a "judge only the interesting
subset" pattern that doesn't need an aggregator-driven filter.

## Caching

- `[:hn :item id]` shared.
- `[:page-title :url url]`.
- `[:title-edit-judge story-id model schema]`.

## Compute

Network-bound on `fetch-source-title`. 100 stories × ~1s per fetch
at 16-way parallelism ≈ 10s. LLM judge adds ~$0.05 if enabled.

## Verification

```
clojure -M -e "(require 'hn-title-editorialize.core) \
              (hn-title-editorialize.core/run-once! \
                \"title.json\" {:trace? true :n-stories 50})"
```

Spot-checks:

1. The flag-rates sum to 1.
2. `:identical` rate on /topstories is plausibly 25–40%.
3. Manual inspection of 5 `:divergent` rows: each pair really is
   semantically different.

Property tests:

- Metrics are symmetric where they should be (`exact-match`,
  `ci-match`, `token-jaccard`).
- `length-ratio = 1` ⇒ both titles tokenize to same length.

## Open questions

- **Bracket-tag list** — `[paywall]` and date tags `[2019]` are
  the conventional HN editor adds. Survey existing editorial
  conventions to populate the bracket-tag detector.
- **Ignore-list of source quirks** — sites that append " | The
  Site Name" to every `<title>`. Strip a small set of common
  suffixes ("| NYT", "- Wikipedia", etc.) before comparison.
- **og:title vs html `<title>`** — sometimes they disagree. v1
  prefers og:title; expose a flag if needed.
