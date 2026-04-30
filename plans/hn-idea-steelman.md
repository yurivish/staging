# hn-idea-steelman: Steelman vs strawman

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-thread" → Steelman vs
> strawman.

## Context

When a reply disagrees with its parent, is it engaging with the
strongest reasonable reading of the parent's argument, or a weaker
version? Per reply edge, classify as `steelman`, `charitable`,
`neutral`, `strawman`, or `not-applicable`, with a short note quoting
what was actually engaged with.

Useful as a per-thread quality metric and as input to user-level
roll-ups (does this user habitually steelman or strawman?). Same
cost-shape as [`hn-idea-mind-changed`](hn-idea-mind-changed.md):
cheap pre-filter, expensive judge.

## Inputs

- `:n-stories` (default 30) or `:edges-source` — accept a JSON
  produced by `hn_typing` to skip re-classification.
- `:include-classes` — which `hn_typing` edge classes to evaluate.
  Default `#{:disagree :correct :attack}` (steelmanning is only
  meaningful when there's actual pushback).
- `:llm-judge-workers`.

## Pipeline shape

```
   top-stories ─→ fetch-tree (hn_density helper)
                        ▼
                select reply edges
                        ▼
        c/stealing-workers :classify → edge-classify (Haiku)
                        ▼
   filter: edge-class ∈ :include-classes  (otherwise straight-through)
                        ▼
        c/stealing-workers :judge → steelman-judge (Sonnet)
                        ▼
                     collector
```

If `:edges-source` is supplied, the `edge-classify` stage is
skipped and the edges are read from the input file directly. Cache
shares with `hn_typing` so re-running is cheap regardless.

## Stages in detail

### Edge-classify (Haiku)

Reuse `hn_typing`'s tool spec verbatim. Cache key
`[:edge-classify parent-id reply-id model schema]` is shared.

### Filter

`(get edge :class) ∈ :include-classes`.

### Steelman judge (Sonnet)

Input: full parent text and full reply text, clipped at ~600 chars
each, plus the story title.

Tool schema:

| field | type | notes |
| --- | --- | --- |
| `engagement` | enum: `steelman`, `charitable`, `neutral`, `strawman`, `not-applicable` | the call |
| `confidence` | number `[0,1]` | |
| `parent_strongest_reading` | string ≤ 25 words | what the strongest version of parent claims |
| `what_reply_addressed` | string ≤ 25 words | what the reply actually went after |
| `gap_summary` | string ≤ 30 words | what was missed if anything |

System prompt sketch:

> Two HN comments: a parent and a reply that pushes back. Decide
> whether the reply engages with the strongest reasonable reading
> of the parent (steelman / charitable), engages neutrally, or
> attacks a weaker reading (strawman). State explicitly the
> strongest reading of the parent and what specifically the reply
> addressed; the gap between those is the answer.

### Collector

Sort by `:engagement` then `:confidence` descending. Keep all rows
(per-edge output is the point).

## Output

```clojure
[{:story-id ... :story-title ...
  :parent-id ... :reply-id ...
  :parent-author ... :reply-author ...
  :class "disagree"           ;; from edge-classify
  :engagement "strawman"
  :confidence 0.82
  :parent-strongest-reading "..."
  :what-reply-addressed "..."
  :gap-summary "..."
  :parent-preview "..."
  :reply-preview "..."}
 ...]
```

A simple post-step over the JSON gives per-author rates of
`steelman` vs `strawman`, which is the natural follow-up.

## Datapotamus shape

Filter → expensive two-stage, shared with `hn-idea-mind-changed`.
The `:edges-source` knob makes the pipeline composable on top of
`hn_typing`'s output, which is the right ergonomics for chained
analyses — first time we have a pipeline that explicitly accepts
another pipeline's JSON as input.

## Caching

- `[:edge-classify parent-id reply-id model schema]` — shared with
  `hn_typing` and `hn-idea-reply-posture`.
- `[:steelman-judge parent-id reply-id model schema]`.

## Compute

30 stories × ~50 reply-edges/story = ~1500 edges. 30–40% are
disagreement-flavored. Sonnet on ~500 edges ≈ $5–10 per run.

## Verification

```
clojure -M -e "(require 'hn-steelman.core) \
              (hn-steelman.core/run-once! \
                \"steelman.json\" {:trace? true :n-stories 10})"
```

Spot-checks:

1. Trace shows the filter-survival rate (~30%).
2. Open 5 random `:strawman` rows — does the gap-summary read as
   real?
3. Open 5 random `:steelman` rows — does the reply genuinely
   engage with `:parent-strongest-reading`?
4. Re-run with `:edges-source` pointing at a prior `hn_typing`
   output — first stage skipped, cache hit on prior judge calls.

## Open questions

- **Per-edge vs. per-author roll-up** — emit raw edges in v1; let a
  downstream script roll up by author. Keeps this pipeline focused.
- **Calibration** — the line between `charitable` and `steelman` is
  fuzzy. Tighten the prompt with examples after first run.
