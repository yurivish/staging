# hn-idea-section-attribution: Section attribution

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-story discourse" → Section
> attribution.

## Context

For each top comment on a story, decide which paragraph or section of
the linked article it's actually responding to — and how (builds on,
disputes, tangent, references-without-engaging, ignores-article).
Output is per-(story, comment) with a section index, label, and
engagement type. Useful both descriptively (which sections of an
article actually drive discussion?) and analytically (which sections
attract dispute vs. tangent?).

This is the first pipeline that demonstrates **dependent fan-out**:
each per-comment LLM call needs story-level context (the fetched
article and its section split), so fan-out happens *after* a per-story
enrichment step rather than directly off the source.

## Inputs

- `:n-stories` (default 30).
- `:k-comments` (default 30) — top-K comments per story.
- `:max-article-tokens` (default 5000) — cap article text per call.
- `:llm-workers`.

## Pipeline shape

```
   top-stories ─→ fetch-tree (hn_density helper, just for top-K
                              by descendants)
                       ▼
              fetch-article (HTTP + Readability-like extract)
                       ▼
           split-sections (pure, paragraph- or header-based)
                       ▼
        emit one msg per story carrying (article-sections, top-comments)
                       ▼
          split-comments-with-context (msg/children with article context attached)
                       ▼
   c/stealing-workers :attribute → attribute-comment (Haiku)
                       ▼
                aggregate-by-story
                       ▼
                    collector
```

The "dependent fan-out" is at the `split-comments-with-context`
step: each child msg carries both the per-comment payload and the
per-story article sections, so the downstream classifier has the
context it needs to do its job.

## Stages in detail

### Fetch article + extract main text

HTTP GET with a 10s timeout and 2 retries. For HTML, use a
Readability-style extractor. For non-HTML (PDFs, images, video,
GitHub repos), skip the story — emit a sentinel and the per-comment
classifier shortcuts to `engagement: ignores-article`. Same for
"Show HN" / "Ask HN" stories without an external URL.

Cache `[:article :url <url>]` keyed by URL (immutable enough; if
articles change frequently, add a content hash).

### Split sections

If the article has `<h1>`/`<h2>`/`<h3>` headers, split on those —
section 0 is the intro before the first header; sections 1..N are
each header + following content.

If headerless, split into paragraph clusters of ~300 tokens each
(keep paragraphs whole; pack to the cap).

Output: `[{:idx 0 :label "Intro" :text "..."} ...]`. Cap total
tokens at `:max-article-tokens` by truncating per-section text
proportionally.

### Attribute comment (Haiku)

Input: section list (compact: `[{:idx 0 :label "..." :text "..."}
...]`) + the single comment text.

Tool schema:

| field | type | notes |
| --- | --- | --- |
| `section_idx` | integer or `null` | which section the comment most directly addresses |
| `confidence` | number `[0,1]` | |
| `engagement` | enum: `builds-on`, `disputes`, `tangent`, `references-without-engaging`, `ignores-article` | |
| `quoted_phrase` | string ≤ 25 words | what specifically the comment seems to engage with |

System prompt sketch:

> Given the sections of an article and a single comment from its
> Hacker News discussion, decide which section the comment
> primarily responds to and how it engages. If the comment is
> off-topic or addresses the article as a whole, set
> `section_idx` to null and pick the appropriate engagement type.

### Aggregate by story

Per story, summarize:

```clojure
{:story-id ... :story-title ...
 :url ...
 :sections [{:idx ... :label "..."} ...]   ;; without text, for compactness
 :n-comments 28
 :section-stats
 [{:idx 0 :n-comments 9 :engagement {:builds-on 2 :disputes 4 :tangent 2 :references-without-engaging 1}}
  ...]
 :tangent-rate 0.18
 :ignores-article-rate 0.07
 :comments [{:comment-id ... :section-idx 2 :engagement "disputes" :quoted-phrase "..." :preview "..."}
            ...]}
```

## Datapotamus shape demoed

- **Dependent fan-out** — comments fan out *with* per-story
  article context attached. The msg carries both, so each
  downstream worker has what it needs without going back to a
  shared store. Different from `hn_typing` where each edge fan-out
  is self-contained.

## Caching

- `[:article :url url]` — article fetches.
- `[:section-split url section-version]` — pure post-step but
  worth caching to keep the section ordering stable across runs.
- `[:section-attribute url comment-id model schema]` — keyed
  by article URL so a re-fetched article forces re-attribution.

## Compute

Per call: ~5K input tokens (article) + ~200 (comment) + ~50
output. Haiku ≈ $0.0003/call. 30 stories × 30 comments ≈ 900
calls ≈ $0.30. Cheap.

Network: one HTTP fetch per story for the article + the usual
HN tree fetch. The article fetch is the latency dominator.

## Verification

```
clojure -M -e "(require 'hn-section-attribution.core) \
              (hn-section-attribution.core/run-once! \
                \"section-attribution.json\" {:trace? true :n-stories 5})"
```

Spot-checks:

1. Open a story output, read the article, check 3 random comments'
   `:section-idx` and `:quoted-phrase` against intuition.
2. `:tangent-rate` and `:ignores-article-rate` are in plausible
   bands (5–30% combined; HN comments are often tangent-heavy).
3. Re-run; cache hits ~100% on `:hn :item`, `:article :url`,
   `:section-attribute` keys.

## Open questions

- **Article extraction quality** — Readability ports vary;
  evaluate on a sample of stories with messy HTML
  (Substack, paywalled, JS-rendered). Fall back to first-N
  paragraphs if extraction fails.
- **Per-(section, comment) matrix vs. single-section** —
  v1 picks one section per comment. A matrix mode (relate-strength
  per section) gives richer data but doubles cost. Defer.
- **Header sniffing** — some articles have nominal headers but
  meaningless section content. Consider a quick LLM-judged
  "are these sections coherent?" check; defer until needed.
