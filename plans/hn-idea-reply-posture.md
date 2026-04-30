# hn-idea-reply-posture: Reply posture per author

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-user longitudinal" →
> Reply posture per author.

## Context

`hn_typing` already classifies parent→reply edges from front-page
threads into `{agree, disagree, correct, extend, tangent, attack,
clarify}`. This pipeline rolls that up *per author*: take a user's
comment history, classify each reply edge (comment → its parent
comment), and report the user's posture distribution. Are they an
agreer? a corrector? a tangenter? an attacker? Useful as a personality
metric and as input to `hn-idea-emotion` interpretation.

## Inputs

- `:user-id` (single mode) or top-N batch source.
- `:since`, `:until`, `:max-comments`.
- `:include-top-level` — boolean. Default false: top-level comments
  are replies *to a story*, which has a different posture vocabulary
  (mostly "react" or "open question"). Excluding them keeps the
  classifier honest.

## Pipeline shape

```
   user-id ─→ fetch-history (shared paginated Algolia)
                   ▼
            filter to reply edges (parent_id is a comment)
                   ▼
            fetch parent comment (Firebase, cached)
                   ▼
   c/stealing-workers :classify → edge-classify (Haiku)
                   ▼
       aggregator → per-user histogram
```

The classify step **reuses the prompt and tool spec from
`hn_typing` verbatim** — same `edge-types`, same system prompt, same
JsonObjectSchema. Cache key: `[:edge-classify parent-id reply-id
model schema]` — so that hits from `hn_typing` runs are reused here
and vice versa.

## Stages in detail

### Filter to reply edges

Algolia hits give `parent_id` and `story_id`. Drop rows where
`parent_id == story_id` (top-level) unless `:include-top-level true`.

### Fetch parent

Firebase `/v0/item/<parent_id>.json`. Cached at `[:hn :item id]`.
Parallel via vt-exec.

### Aggregator

`group-by :user-id`, count edge types, compute proportions.

```clojure
{:user-id "..."
 :n-edges 1842
 :counts {:agree 312 :disagree 421 :correct 198 :extend 144
          :tangent 67  :attack 102 :clarify 598}
 :proportions {...}
 :dominant-class :clarify
 :samples-per-class {:correct [{:edge-id ... :preview "..."} ...]
                     ...}}
```

## Datapotamus shape

Reuses `hn_typing`'s edge-classifier exactly; the new bit is the
per-user paginated source feeding it. Same shape as
`hn-idea-emotion`'s source, different downstream worker.

## Caching

- Shared `[:edge-classify ...]` cache with `hn_typing`. Big win if
  both pipelines have run.
- Shared `[:hn :item id]`.

## Compute

5K-comment user → ~3K reply edges → Haiku at $0.0001/edge → ~$0.30.

## Verification

```
clojure -M -e "(require 'hn-reply-posture.core) \
              (hn-reply-posture.core/run-once! \
                \"<user>\" \"posture.json\" {:trace? true})"
```

Spot-checks:

1. Histogram sums to `:n-edges`.
2. `:samples-per-class` for `:correct` and `:attack` look right on
   a known commenter.
3. Re-running shares cache with prior `hn_typing` runs (zero LLM
   calls if every parent→reply edge has been seen).

## Open questions

- **Multi-class vs. single-class** — `hn_typing` picks one class.
  Some replies legitimately do two things ("clarify + extend"). Keep
  single-class for v1 to share the cache; revisit if needed.
- **Top-level inclusion** — keep default off; flip the flag to study
  top-level personality separately.
