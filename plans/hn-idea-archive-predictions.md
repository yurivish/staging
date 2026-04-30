# hn-idea-archive-predictions: "We said this would happen"

> Seed: [hn-ideas.md](hn-ideas.md) → "Cross-temporal" → Archive
> predictions.

## Context

HN comments are full of predictions and skeptical assertions —
"this won't scale," "they'll be acquired in 18 months," "remote work
won't stick after the pandemic." Most are forgotten. This pipeline
mines old stories for predictive comments and retrospectively grades
each against what actually happened, using Anthropic's web search
tool to look up current state.

Output is a small, curated dataset of "predictions HN made N years
ago, graded today" — useful both as reading and as a reflection on
what kinds of predictions tend to be right.

Two patterns combine, both new in this codebase:

- **Filter → expensive** (shared with
  [`hn-idea-mind-changed`](hn-idea-mind-changed.md)).
- **First pipeline that needs LLM web search** — likely via
  `toolkit.llm` rather than langchain4j, depending on adapter
  support at the time of build.

## Inputs

- `:since`, `:until` — story date range. Default: 3–7 years ago.
- `:min-points` (default 100) — only stories with significant
  attention.
- `:n-stories` (default 50).
- `:k-comments` (default 30) — top per story by descendants.
- `:llm-filter-workers`, `:llm-judge-workers`.

## Pipeline shape

```
   Algolia search_by_date stories in [since, until], points >= min
                       ▼
                fetch-tree (top-K by descendants)
                       ▼
            split-comments
                       ▼
   c/stealing-workers :filter → is-prediction? (Haiku)
                       ▼
              filter on :is-prediction confidence > τ
                       ▼
   c/stealing-workers :judge → grade-prediction (Sonnet + web search)
                       ▼
                       collector
```

## Stages in detail

### Cheap prediction filter (Haiku)

Schema:

| field | type | notes |
| --- | --- | --- |
| `is_prediction` | boolean | strong-claim about future state |
| `confidence` | number `[0,1]` | |
| `horizon` | enum: `short` (≤ 1y), `medium` (1–3y), `long` (≥ 3y), `unspecified` | |
| `claim` | string ≤ 25 words | the predicted state in present-tense |

System prompt sketch:

> Decide whether this Hacker News comment makes a verifiable
> prediction or skeptical assertion about future state — the kind
> of thing that could later be checked. Hedged opinions and value
> judgments are not predictions. Restate the prediction as a
> short present-tense claim that could be verified today.

Filter: `:confidence > 0.7` AND `:horizon ∈ #{:short :medium}`
when the story is more than `:horizon` old (so the prediction is
already due).

### Prediction grader (Sonnet + web search)

This is the only stage that requires web search. langchain4j as of
the AGENTS.md notes does not expose Anthropic's documents/citations
API; whether web search is exposed needs to be verified. Likely
path: extend `toolkit.llm` (the unified-request HTTP driver) with a
web-search tool block, following the AGENTS.md guidance.

Tool schema (the model's structured output, separate from the
web-search tool it uses internally):

| field | type | notes |
| --- | --- | --- |
| `verdict` | enum: `correct`, `partly-correct`, `wrong`, `too-early`, `unknowable` | grade |
| `confidence` | number `[0,1]` | |
| `current_state` | string ≤ 50 words | what the world looks like now relative to the claim |
| `evidence_url` | string | a single URL the model cites as evidence |
| `notes` | string ≤ 50 words | caveats, partial-credit reasoning |

System prompt sketch:

> A Hacker News comment from <date> made the following
> prediction: <claim>. Use web search to determine the current
> state of the world for this claim. Return a structured grade
> with one supporting URL. If the prediction is too vague to
> verify or the world hasn't moved enough, mark as `unknowable`
> or `too-early`.

### Collector

Sort by verdict order (`correct`, `partly-correct`, `wrong`,
`too-early`, `unknowable`) then by Sonnet confidence descending.

## Output

```clojure
[{:story-id ... :story-title ... :story-url ... :story-date ...
  :comment-id ... :comment-author ... :comment-date ...
  :claim "..."
  :horizon "medium"
  :verdict "correct"
  :confidence 0.81
  :current-state "..."
  :evidence-url "https://..."
  :notes "..."
  :comment-preview "..."}
 ...]
```

## Datapotamus shapes demoed

- **Filter → expensive** (shared with `hn-idea-mind-changed`).
- **LLM with web-search tool** — the first pipeline that needs
  this. Forces extending `toolkit.llm` with a web-search tool
  block. The increment lives in the toolkit, not in this
  pipeline; the pipeline is the first consumer.

## Caching

- Shared HN/Algolia keys.
- `[:prediction-filter comment-id model schema]`.
- `[:prediction-judge claim-hash model schema run-date]` —
  the grade depends on *when* the judge ran (today's web is
  different from a year from now's), so embed the run-date into
  the cache key. Otherwise re-running next year would return a
  stale grade.

## Compute

Cheap filter on ~50 stories × ~30 comments = 1500 candidates ≈
$0.50 Haiku.

Survivors: maybe 10–20% (predictions are rare). Sonnet + web
search on ~200–300 calls ≈ $5–15 (web search adds latency and
cost).

## Verification

```
clojure -M -e "(require 'hn-archive-predictions.core) \
              (hn-archive-predictions.core/run-once! \
                \"predictions.json\" {:trace? true \
                                      :since \"2018-01-01\" \
                                      :until \"2020-01-01\"})"
```

Spot-checks:

1. Filter survival rate plausible (5–20%).
2. Manual review of 10 graded predictions: verdicts and
   `current-state` summaries are defensible; `evidence-url`
   resolves to a real source.
3. Distribution of verdicts: not all `correct` (sanity check on
   model bias toward agreeing).

## Open questions

- **Web-search availability** — needs the `toolkit.llm` Anthropic
  adapter to expose web search. If unavailable when this ships,
  fall back to a `current_state_self_reported` field where the
  model articulates what it remembers from training and flags
  uncertainty; mark such grades `:unknowable` if the model is not
  confident. Web search adds rigor; without it the output is
  weaker but still interesting.
- **Hindsight bias** — the judge sees the prediction *and* the
  current state simultaneously, which makes it easy to retrofit
  reasoning. Consider a two-stage prompt: first restate the
  prediction's resolution criteria *without* current state, then
  introduce the current state and grade.
- **Time-locked re-runs** — the cache key embeds the run-date,
  but re-running the same study a year later is the natural
  follow-up ("did the grades hold?"). Store the run-date
  prominently in the output JSON.
