# hn-idea-headline-stops-here: Headline-stops-here index

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-story discourse" →
> Headline-stops-here index.

## Context

What fraction of comments on a given HN story engage only with the
headline/lede/abstract, vs. the article body? A read-rate proxy. High
"headline-stops-here" rates correlate with stories where commenters
respond to a perceived premise rather than the actual content.

This pipeline is small: it can run **standalone** with its own LLM
classifier, or as a **post-step** over
[`hn-idea-section-attribution`](hn-idea-section-attribution.md)
output, since the section-attribution schema already records which
section each comment engages with. The post-step mode is the
preferred shape — same data, free.

## Inputs

Standalone mode:
- `:n-stories` (default 30).
- `:k-comments` (default 30).
- `:max-article-tokens` (default 5000).

Post-step mode:
- `:section-attribution-input` — path to a section-attribution
  JSON output. The pipeline becomes a pure reduction.

## Pipeline shape

### Post-step mode

```
   load section-attribution.json
              ▼
       per-row classify:
         section_idx == 0 OR engagement == "ignores-article"
              → "headline-stops-here"
         section_idx >= 1 AND engagement != "ignores-article"
              → "body-engaged"
         else → "unclear"
              ▼
       aggregate-by-story
              ▼
           collector
```

### Standalone mode

Same as `hn-idea-section-attribution` upstream stages, but with a
narrower LLM stage:

```
   top-stories ─→ fetch-tree → fetch-article → split-sections (intro vs body)
                       ▼
       split-comments-with-context (intro-text + body-text)
                       ▼
   c/stealing-workers :classify → headline-only? (Haiku)
                       ▼
                aggregate-by-story
```

In standalone mode, `split-sections` collapses to a binary
intro/body split: the intro is the article up to the first `<h2>`
or, headerless, the first 2 paragraphs.

## Standalone Haiku classifier

Schema:

| field | type | notes |
| --- | --- | --- |
| `engages_with` | enum: `intro-only`, `body`, `both`, `neither` | which part of the article the comment engages with |
| `confidence` | number `[0,1]` | |

System prompt sketch:

> A Hacker News story has an introduction (the lede) and a body.
> The body is what readers see only if they actually open and
> read the article. Decide which part this comment engages
> with. If the comment makes claims that are addressed *in* the
> body but not in the intro, that's `body`. If the comment only
> reacts to the headline/intro, that's `intro-only`. `neither`
> if the comment doesn't engage with article content at all.

`headline-stops-here` ≜ `engages_with == "intro-only"`.

## Per-story aggregator

```clojure
{:story-id ... :title ... :url ...
 :n-comments 28
 :n-headline-stops-here 17
 :n-body-engaged 8
 :n-other 3
 :headline-stops-here-rate 0.61
 :samples [{:comment-id ... :preview "..." :class "intro-only"} ...]}
```

Cross-aggregator over all stories: distribution of rates, top-10
stories by rate, correlation between rate and story points
(stories with high points and high rate are the canonical
"didn't-read-it" stories).

## Datapotamus shape

- **Pipeline composition / reduction over another pipeline's
  output** — first time we have a pipeline whose default mode is
  to consume another pipeline's JSON. Worth flagging as a pattern.
- Standalone mode reuses the dependent-fan-out shape from
  `hn-idea-section-attribution`.

## Caching

- Shared with `hn-idea-section-attribution` in standalone mode
  (article fetches, item fetches).
- `[:headline-stops-here comment-id model schema content-hash]`
  for the standalone classifier.

## Compute

Post-step mode: free.

Standalone mode: ~30 stories × 30 comments × Haiku ≈ $0.30.

## Verification

```
clojure -M -e "(require 'hn-headline-stops-here.core) \
              (hn-headline-stops-here.core/run-once! \
                \"hsh.json\" {:trace? true :n-stories 10})"
```

Spot-checks:

1. Per-story rate distribution: median plausibly 30–50%, with a
   long tail of high-rate "people didn't read it" stories.
2. Manual inspection of one high-rate story: top intro-only
   comments should obviously address only the framing.

## Open questions

- **Definition of "intro"** — first `<h2>` is the natural cut for
  HTML articles; for arxiv abstracts, the abstract itself is the
  intro. Add per-domain rules if needed.
- **Discount for short articles** — a 200-word article doesn't
  have a meaningful intro/body split; classify all comments as
  whole-article. Detect length and short-circuit.
- **Combine into `hn-idea-section-attribution`** — strongly
  considered. The post-step mode is essentially a reformulation
  of that output. Keep separate because the standalone variant
  is genuinely simpler (binary classifier vs section index) and
  some users will want only the headline-rate without paying for
  the full attribution.
