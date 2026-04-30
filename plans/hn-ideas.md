# HN pipeline ideas

Catalog of follow-on Hacker News data pipelines, sized to the existing
`hn` / `hn_density` / `hn_drift` / `hn_shape` / `hn_tempo` / `hn_typing`
pattern: each is a small Datapotamus flow whose docstring states the
question, the data path, and the combinator pattern being demoed.

The catalog is grouped by *unit of analysis* — the dimension along which
each flow fans out. The two big gaps in the existing set are
**per-user longitudinal** and **per-thread as a whole** (vs.
edge-by-edge). Where a pipeline would exercise a Datapotamus shape that
hasn't been demoed yet, that's flagged inline.

Each row links to a per-idea plan with the actual flow shape, LLM stage
schemas, and verification recipe.

## Per-user longitudinal

Fan-out unit is one user; the inner shape is "all their items, over
time." First family to need an Algolia paginated source and a windowed
reduce on the time axis.

- [hn-idea-emotion](hn-idea-emotion.md) — Emotional landscape over
  time. Per-comment emotions × topics × valence × grounding-vs-opinion,
  bucketed monthly. Demos paginated source + windowed reduce.
- [hn-idea-self-contradiction](hn-idea-self-contradiction.md) — Pair
  same-author comments on the same topic across years; LLM judges real
  contradiction vs. context shift. Demos pairwise-within-group +
  filter→expensive two-stage.
- [hn-idea-topic-graveyard](hn-idea-topic-graveyard.md) — Topics this
  user used to comment about and no longer does. Topic × quarter
  heatmap.
- [hn-idea-voice-fingerprint](hn-idea-voice-fingerprint.md) —
  Per-quarter style vector (verbosity, hedging, sentence shape,
  technical density). No LLM, pure linguistic features.
- [hn-idea-crank-index](hn-idea-crank-index.md) — Topics that
  reliably draw outsized emotional intensity from this user vs. their
  own baseline.
- [hn-idea-reply-posture](hn-idea-reply-posture.md) — `hn_typing`
  rolled up per author. Histogram of edge classes (agree, correct,
  tangent, attack…) per user.

## Per-thread (whole subtree as one unit)

Unit is a full conversation, linearized for one LLM call. None of the
existing pipelines reason over a whole thread.

- [hn-idea-argument-graph](hn-idea-argument-graph.md) — Extract
  claim/support/counter as a Toulmin or IBIS graph for one deep thread.
- [hn-idea-mind-changed](hn-idea-mind-changed.md) — Find threads with
  explicit position-change language; classify what kind of evidence
  triggered the shift. Tiny dataset, high-signal output.
- [hn-idea-steelman](hn-idea-steelman.md) — Per reply edge, is the
  responder engaging with the strongest or weakest reading of the
  parent? Cheap-classifier-first → expensive-judge-second.
- [hn-idea-shared-premise](hn-idea-shared-premise.md) — What unstated
  assumption is everyone in this thread agreeing on?

## Per-story discourse

Adjacent to `hn_drift` but with sharper questions.

- [hn-idea-section-attribution](hn-idea-section-attribution.md) — Which
  paragraph of the article is each top comment really responding to?
  Demos dependent fan-out (story → article-sections × top-comments).
- [hn-idea-lurkers-question](hn-idea-lurkers-question.md) — The single
  most informative question NOT yet answered by the discussion.
- [hn-idea-domain-personality](hn-idea-domain-personality.md) —
  Aggregate discussion shape by submitter domain (arxiv, nytimes,
  github, …) over many submissions.
- [hn-idea-title-editorialize](hn-idea-title-editorialize.md) — Compare
  submitted HN title to the actual `<title>` of the linked page. No
  LLM, cheap, large-scale.
- [hn-idea-headline-stops-here](hn-idea-headline-stops-here.md) — % of
  comments that cite only the headline/abstract vs. the article body.
  Read-rate proxy.

## Cross-temporal / archive

Needs Algolia date-range search rather than the realtime API.

- [hn-idea-reaction-half-life](hn-idea-reaction-half-life.md) — First
  100 comments on a breaking story vs. the comments 24h later. Demos
  temporal join.
- [hn-idea-archive-predictions](hn-idea-archive-predictions.md) —
  Predictive/skeptical comments on old stories, retrospectively graded
  against what actually happened.
- [hn-idea-buzzword-obituaries](hn-idea-buzzword-obituaries.md) — Term
  frequency in titles + comments by month across years. No LLM.
- [hn-idea-topic-seasonality](hn-idea-topic-seasonality.md) —
  Front-page topics by month-of-year across multiple years.

## Compute angle

- **Mac-mini-trivial / no LLM:** `voice-fingerprint`,
  `title-editorialize`, `headline-stops-here`, `buzzword-obituaries`,
  `topic-graveyard`.
- **Tiny dataset / Sonnet-friendly to prototype:** `mind-changed`,
  `archive-predictions`, `argument-graph`, `shared-premise`.
- **High-fanout / stretches parallelism:** `emotion`,
  `section-attribution`, `crank-index`, `domain-personality`.

## Top picks if forced to choose three to build next

1. **`emotion`** — biggest gap (per-user longitudinal), forces a
   paginated-source pattern, naturally has a visual payoff.
2. **`mind-changed`** — small data, weirdly underexplored, output is
   genuinely interesting reading.
3. **`voice-fingerprint`** — pure-math counterpart to `emotion`, no
   LLM, fast to ship; complements the LLM picture with a
   non-LLM-derivable signal.

## Shared infrastructure that several plans assume

- **Algolia paginated user-history fetcher** — needed by `emotion`,
  `self-contradiction`, `topic-graveyard`, `voice-fingerprint`,
  `crank-index`, `reply-posture`. Build inline in the first one that
  ships (`emotion` is most likely); extract to a shared helper
  namespace once a second pipeline lands and the shape is stable.
- **Per-comment topic tagger** — Haiku, structured output. Reused by
  `emotion`, `topic-graveyard`, `crank-index`, `buzzword-obituaries`
  (sometimes), `topic-seasonality`. Same caveat: inline first time,
  extract on the second.
- **LMDB cache key conventions** for HN items and Algolia pages —
  several pipelines re-fetch the same comment ids; settle on
  `[:hn :item id]` and `[:hn :algolia-by-author author cursor]`
  early.
