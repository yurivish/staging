# hn-idea-voice-fingerprint: Voice-fingerprint drift

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-user longitudinal" →
> Voice-fingerprint drift.

## Context

A user's writing style — verbosity, hedging, sentence shape, technical
density — tells a story independent of content. Drifting style reveals
mood shifts, professional development, life-stage changes, or
account-handoff. This is the LLM-free counterpart to
[`hn-idea-emotion`](hn-idea-emotion.md): same source, same flow shape,
but the per-comment classifier is replaced by a pure feature
computation. Triangulating the two gives a richer picture than either
alone.

The pipeline doubles as a demonstration that the paginated-source +
windowed-reduce pattern from `hn-idea-emotion` works without any LLM
stage.

## Inputs

- `:user-id` (single mode) or top-N batch source (shares the
  discovery preamble with `hn-idea-emotion`).
- `:since`, `:until`, `:max-comments`.
- `:bucket` — `:month` or `:quarter`. Default `:quarter` (more
  signal per bucket; style is slow-moving).

## Pipeline shape

```
   user-id source ─→ fetch-history (paginated Algolia)
                          ▼
                   split-comments
                          ▼
       c/workers :feature-step  →  compute-features (pure)
                          ▼
                  bucket-by-period
                          ▼
            aggregator → group user × period →
              per-feature mean+std → emit per-(user, period) row
                          ▼
            drift-step → consecutive-period distance series
```

Reused: paginated Algolia source helper from `hn-idea-emotion`
(extract once a second pipeline lands). Aggregator skeleton same as
`hn_density/aggregate-by-user`, with the secondary group-by-period
inside.

## Per-comment features

All pure functions of comment text. Group A is lexical, group B
stylistic, group C density/grounding, group D structural.

**A. Lexical**
- `length-tokens`, `length-chars`
- `mean-sentence-length` (split on `[.!?]`)
- `sentence-length-stdev`
- `mean-word-length`
- `vocab-richness` — MATTR (moving-average type-token ratio,
  window=50). Length-corrected; defaults to TTR for short comments.

**B. Stylistic markers** (rates per 100 tokens)
- `hedge-rate` — `{maybe perhaps might seems I think arguably
  possibly probably I guess sort of}`
- `assertion-rate` — `{obviously clearly of course definitely
  always never undoubtedly certainly without question}`
- `first-person-rate` — `{I me my mine}`
- `second-person-rate` — `{you your yours}`
- `negation-rate` — `{not no never n't}`

**C. Density / grounding proxies**
- `link-rate` — URLs detected by simple regex
- `code-fraction` — chars inside backticks / total chars
- `numeric-rate` — `\b\d[\d.,]*\b` matches per 100 tokens
- `capitalized-bigram-rate` — adjacent-token capitalized
  bigrams (named-entity proxy) per 100 tokens
- `quote-rate` — fraction of lines starting with `>`

**D. Structural**
- `n-paragraphs`, `mean-paragraph-length`
- `question-mark-rate`, `exclamation-rate`

About 20 features. Stored as a flat map per comment.

## Aggregator

Same skeleton as `hn_density/aggregate-by-user`:

- `:on-data` stashes each feature row, returns `msg/drain`.
- `:on-all-closed` does `group-by :user-id`, then within each user
  `group-by :year-period`. For each (user, period) bucket emit
  `{:user-id ... :year-period ... :n-comments N :features
  {<feat> {:mean ... :stdev ...}, ...}}`.

## Drift series

After the per-bucket aggregator, a small post-step. For each user:

- Sort buckets by period.
- For each consecutive pair, compute cosine distance between mean
  vectors. (Z-score features first using a per-user pooled stdev so
  the distance is unitless across heterogeneous features.)
- Emit `:drift [{:from "2023Q1" :to "2023Q2" :distance 0.18} ...]`.

This step lives in the same ns; it's pure and small. No need for
its own combinator.

## Output

```clojure
[{:user-id "tptacek"
  :periods [{:year-period "2023Q1"
             :n-comments 142
             :features {:length-tokens {:mean 73.4 :stdev 51.2}
                        :hedge-rate    {:mean 0.012 :stdev 0.018}
                        ...}}
            ...]
  :drift   [{:from "2022Q4" :to "2023Q1" :distance 0.11}
            ...]}
 ...]
```

## Datapotamus shapes demoed

- **No-LLM pipeline reusing the LLM-pipeline source/aggregator
  skeleton.** Validates the paginated-source + windowed-reduce
  pattern as compositionally clean — only the worker step swaps
  between LLM and pure compute.

## Caching

- `[:algolia :by-author author page]` — shared with `hn-idea-emotion`.
- No per-comment cache needed; feature compute is microseconds.

## Compute

Mac-mini-trivial. The whole thing for one 5K-comment user is bounded
by Algolia network time (~1 minute). 50-user batch is also fine, an
hour at worst.

## Verification

```
clojure -M -e "(require 'hn-voice-fingerprint.core) \
              (hn-voice-fingerprint.core/run-once-for-user! \
                \"tptacek\" \"voice.json\" {:trace? true})"
```

Spot-checks:

1. Trace `:event :history-fetched` matches Algolia hit count.
2. Output has all features present in every period (no nils).
3. Drift series length = number of periods − 1.

Property tests (`hn_voice_fingerprint/core_property_test.clj`,
mirroring existing `*_property_test.clj`):

- **Order invariance**: shuffling a bucket's comments does not
  change its `:features` means/stdevs.
- **Linearity**: splitting a bucket in half and combining
  per-feature means with the right weights matches computing on
  the full bucket (within float epsilon).
- **MATTR sanity**: a comment less than the MATTR window falls
  back to TTR; explicit equality test on a short fixture.

## Open questions

- **Bucket size** — quarter vs. month. Quarter gives more signal
  per bucket; month gives more time resolution. Default to quarter
  with a CLI knob.
- **Feature normalization for drift** — pooled stdev across buckets
  for the same user vs. cross-user pooled stdev. Per-user is more
  faithful to "this user's style is changing"; defer cross-user
  normalization until needed.
- **Stop-list and tokenizer** — keep the tokenizer dumb (regex
  word-split, lowercase) for v1. Anything fancier belongs in
  `toolkit.nlp`.
