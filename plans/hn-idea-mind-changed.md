# hn-idea-mind-changed: Mind-changed traces

> Seed: [hn-ideas.md](hn-ideas.md) → "Per-thread" → Mind-changed
> traces.

## Context

Public mind-changing is rare on the internet, and HN is one of the few
places it visibly happens. The output of this pipeline is a small,
curated dataset of "things that changed someone's mind on HN," each
annotated with what kind of evidence or move actually triggered the
shift. Useful as reading and as a question generator.

The headline shape is **two-stage filter → expensive judge**, which
none of the existing pipelines demo: a cheap upstream classifier
prunes the candidate set so the smarter, more expensive model only
runs on plausible cases.

## Inputs

- `:since`, `:until` — date range. Default: last 30 days.
- `:phrases` — seed phrase list (default below).
- `:llm-filter-workers`, `:llm-judge-workers`.

## Pipeline shape

```
   seed phrases ────→ Algolia search by date  (fan out: phrases × pages)
                              │
                              ▼
                       dedup by objectID
                              ▼
                  hydrate context (parent + grandparent)
                              ▼
            c/workers :filter  →  cheap-classify (Haiku)
                              ▼
              filter on :is-mind-change? confidence > τ
                              ▼
        c/stealing-workers :judge  →  trigger-judge (Sonnet)
                              ▼
                          collector
```

## Stages in detail

### 1. Multi-query source

Default seed phrases (tweak freely; phrasing is heuristic, the cheap
classifier sweeps up the false positives):

```
"you're right", "you are right", "fair point", "good point",
"i stand corrected", "i was wrong", "i take that back",
"you've convinced me", "you have convinced me",
"changed my mind", "changes my mind", "i hadn't considered",
"i had not considered", "fair enough", "i concede"
```

For each phrase, hit Algolia
`https://hn.algolia.com/api/v1/search_by_date?tags=comment&query=<phrase>&numericFilters=created_at_i>=<lo>,created_at_i<=<hi>&hitsPerPage=1000&page=<n>`
and paginate until exhausted. Phrases × pages fan out on the
virtual-thread executor. Concatenate; dedup by `objectID`.

### 2. Hydrate context

For each candidate comment id, fetch via Firebase
`/v0/item/<id>.json`, then walk up the `:parent` chain to retrieve
the parent and grandparent. Cap walk at depth 4 (root tends to be a
story not a comment).

Emit `{:candidate-id ... :candidate-text ... :parent-text ...
:grandparent-text ... :story-id ... :story-title ...}`.

LMDB cache keys: `[:hn :item id]` for every fetched item — shared
across pipelines.

### 3. Cheap filter (Haiku)

`is_real_mind_change?` Tool with schema:

| field | type | notes |
| --- | --- | --- |
| `is_mind_change` | boolean | conservative — only true if the candidate clearly concedes a substantive point |
| `confidence` | number `[0,1]` | model-self-rated confidence |
| `is_sarcasm` | boolean | flag rhetorical/sarcastic uses |

System prompt sketch:

> Decide whether the candidate comment is a real intellectual
> concession in response to its parent — i.e. the author publicly
> updates a substantive position. Mark sarcasm and politeness
> filler as not-a-mind-change. Use the parent text for context.

Filter step then drops rows below `:confidence` threshold (default
0.7) or where `:is-sarcasm` is true.

### 4. Trigger judge (Sonnet)

Run only on filtered survivors. Sonnet is appropriate here — the
reasoning is "given the original claim, the counterargument, and the
concession, what kind of move actually changed the mind." That's
where the smaller model gets confused.

Tool schema:

| field | type | notes |
| --- | --- | --- |
| `direction` | enum: `full-reversal`, `partial-concession`, `scope-narrowing`, `unclear` | how big a shift |
| `what_was_conceded` | string, ≤ 25 words | one-sentence summary |
| `trigger_type` | enum: `data-or-citation`, `lived-experience`, `reframing`, `edge-case`, `authority`, `clarifying-question`, `emotional-appeal`, `scope-narrowing`, `unclear` | what kind of move did it |
| `trigger_excerpt` | string, ≤ 40 words | quoted/paraphrased line from the parent that did the work |
| `original_position` | string, ≤ 25 words | what the candidate originally argued |

System prompt: present the grandparent → parent → candidate as a
labeled three-message exchange; instruct the model to identify the
specific argumentative move in the parent that triggered the
concession.

### 5. Collector

No grouping. Emit one validated row per surviving candidate, sorted
by `:created-at` descending. Include all source text fields so the
output JSON is usable as standalone reading.

## Output

JSON array. One element per mind-change:

```clojure
{:candidate-id "..."
 :story-id "..."
 :story-title "..."
 :created-at "2025-..."
 :original-position "..."
 :what-was-conceded "..."
 :trigger-type "data-or-citation"
 :trigger-excerpt "..."
 :direction "partial-concession"
 :grandparent-text "..."
 :parent-text "..."
 :candidate-text "..."}
```

A markdown digest of "top mind-changes this month, grouped by
trigger-type" is then a trivial downstream script.

## Datapotamus shapes demoed

- **Filter → expensive two-stage** — cheap Haiku classifier prunes;
  Sonnet judges only the survivors. New pattern in this codebase.
- **Multi-query source** — multiple seed phrases each generate a
  paginated stream; merged and deduped. Cleaner version of the
  paginated-source idea from `hn-idea-emotion`.

## Caching

- `[:algolia :search query date-range page]` — append-only by
  date.
- `[:hn :item id]` — Firebase items, shared with other pipelines.
- `[:mind-change-filter id model schema-version]`,
  `[:mind-change-judge id model schema-version]` — separate keys
  so re-running the judge with a smarter model doesn't re-run the
  filter.

## Compute

A 30-day window typically yields ~3–10K phrase-match candidates.
Roughly 10–30% survive the cheap filter. Haiku filter is well
under $1 for the window; Sonnet judge on ~1–3K survivors is a few
dollars. Genuinely small.

## Verification

```
clojure -M -e "(require 'hn-mind-changed.core) \
              (hn-mind-changed.core/run-once! \
                \"mind-changed.json\" \
                {:trace? true :since-days 30})"
```

Spot-checks:

1. Trace `:event :search-done` shows non-zero hits per phrase; total
   raw candidates roughly proportional to `since-days`.
2. `:event :filter-done` shows the survival ratio. Sanity range:
   10%–40%. Outside that range, the threshold is mistuned.
3. Output JSON: open 5 random rows. Each should have a parent that
   plausibly did persuade the candidate; obvious sarcasm should be
   absent.
4. Re-run with a tighter `:since-days`; cache hits show every
   candidate from before the new window comes from cache only for
   `:hn :item` keys (search keys are query+range so won't reuse).

## Open questions

- **Implicit mind-changes** (no concession phrase, but a substantive
  stance-flip across a thread). Higher recall, much more expensive,
  more false positives. Defer; v1 sticks with the explicit-phrase
  recall.
- **Cross-thread sourcing** — does the same author concede on the
  same topic across threads? Out of scope here; closer to
  `hn-idea-self-contradiction`.
- **Threshold τ on the filter.** Start at 0.7, calibrate by hand on
  a few hundred examples.
