# hn-idea-lurkers-question: The lurker's question

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-story discourse" → The
> lurker's question.

## Context

For each top story, generate the single most informative question
that an attentive reader would still want answered after reading both
the article and the discussion. Output is per-story: the question, a
short justification, and pointers to which existing comments come
closest to addressing it. Useful as editorial output and as a
diagnostic ("HN talked past the most important thing again").

Same flow shape as `hn_drift` — 1:1 per-story Sonnet call — applied to
a different question.

## Inputs

- `:n-stories` (default 30).
- `:k-comments` (default 50) — top comments by descendant count.
- `:max-article-tokens` (default 5000).
- `:llm-workers`.

## Pipeline shape

```
   top-stories ─→ fetch-tree (hn_density helper)
                       ▼
              fetch-article (shared with hn-idea-section-attribution)
                       ▼
            top-k-comments  (sort by descendants)
                       ▼
   c/stealing-workers :ask → generate-lurkers-question (Sonnet)
                       ▼
                    collector
```

## Generate question (Sonnet)

Input: article (clipped), top-K comments as a flat list with author
ids.

Tool schema:

| field | type | notes |
| --- | --- | --- |
| `question` | string ≤ 25 words | the single most informative unanswered question |
| `why_it_matters` | string ≤ 30 words | what the answer would unlock |
| `closest_comments` | array of comment ids | up to 3 comments that came nearest to addressing it |
| `is_actually_unanswered` | boolean | false if the discussion already answers it |

System prompt sketch:

> You are an attentive lurker. Read the article and the
> discussion. Identify the single most useful question a smart
> reader would still want answered. Return one. Be specific —
> name the variable, the scope, the stake. Avoid meta-questions
> like "what about X in general." Cite up to three comments that
> came closest to addressing it.

Drop rows where `is_actually_unanswered` is false.

## Output

```clojure
[{:story-id ... :story-title ... :url ...
  :question "..."
  :why-it-matters "..."
  :closest-comments [<id> <id> <id>]
  :closest-comment-previews ["..." "..." "..."]}
 ...]
```

## Datapotamus shape

1:1 per-story Sonnet call, same as `hn_drift`. The novelty is in
the question being asked of the model, not in the flow shape.

## Caching

- `[:article :url url]` — shared with `hn-idea-section-attribution`.
- `[:lurkers-question story-id model schema content-hash]` — embed
  a hash of the linearized discussion + article so re-runs against
  a still-active story regenerate when comments are added.

## Compute

Sonnet input ≈ 5K (article) + 4K (comments) + 200 output ≈ $0.05.
30 stories × $0.05 ≈ $1.50/run. Cheap.

## Verification

```
clojure -M -e "(require 'hn-lurkers-question.core) \
              (hn-lurkers-question.core/run-once! \
                \"lurkers.json\" {:trace? true :n-stories 10})"
```

Spot-check: read 5 outputs against the actual discussions. The
question should be specific and the closest-comment pointers
should genuinely come close. Vague meta-questions are a prompt
problem.

## Open questions

- **Multiple questions vs. one** — the constraint of one is
  deliberate; reading 5 questions per story is fatigue, reading one
  is editorial. If you want a top-3, change the schema to
  `questions: array(min=1, max=3)`.
- **Web context** — the model could hit web search to validate
  whether its proposed question is *actually* unanswered. Out of
  scope for v1; consider once the API supports the right tools.
