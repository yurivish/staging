# podcast — structured extraction from long-form transcripts

Two LLM-driven extractors over Joe-Rogan-style podcast transcripts:
sentiment toward people/places/things, and conspiracy theory discussion.
Provider-neutral (Anthropic, Gemini, OpenAI-compatible incl. Ollama and
llama.cpp), provenance-grounded (every record carries a paragraph_id +
verbatim quote, validated against source), with a built-in quality
auditor.

## Pipeline shape

```
paragraphs ─► chunk ─► A: mentions  (per chunk, parallel)
                       │
                       ▼
                       B: resolve   (one call OR pure-code grouping)
                       │            (controlled by :resolve-strategy)
                       ▼
       chunks ─────►   C: records   (per chunk, parallel,
                                     registry as side input)
                       │
                       ▼
                       D: aggregate (pure group-by entity)
```

- **Chunking**: non-overlapping focus of N paragraphs with K backward
  paragraphs of context (default 8 + 5). Backward-only is the
  correctness lever — every paragraph is the focus of exactly one
  chunk, so no cross-chunk dedup is needed.
- **Stage A (mentions)**: per chunk, list every entity referenced in
  the focus paragraphs. Parallelised across `(detect-slots)` workers.
- **Stage B (resolve)**: cluster Stage A's mentions into a canonical
  registry. Two strategies:
  - `:llm` — single LLM call with the full transcript as a side
    document. Best quality (catches "Trump" / "the president" /
    "him" as one entity), but requires output-budget headroom that
    reasoning models often blow through.
  - `:group` — deterministic exact-match grouping by normalised
    `mention_text`. Pure Clojure, milliseconds, never truncates.
    Default for local reasoning models (gemma4, qwen3) where the
    LLM strategy doesn't fit in the 32k context slot.
- **Stage C (records)**: per chunk, extract task-specific records
  (sentiment polarity / conspiracy stance). Parallelised. Each record
  carries `paragraph_id` + `quote` for grounding; both validated
  against the source via `toolkit.approxmatch` (Sellers' algorithm).
- **Stage D (aggregate)**: pure group-by, sorted by paragraph
  timestamp.

## Provider-neutral structured output

All stages use `toolkit.llm/query` with `:response-schema` (a JSON-Schema
map). Each provider adapter translates this to its native shape:

| Provider | Field |
|---|---|
| Anthropic | `output_config: {format: {type: "json_schema", schema: ...}}` |
| Gemini    | `responseMimeType: "application/json"` + `responseSchema` |
| OpenAI / Groq / Ollama / llama.cpp | `response_format: {type: "json_schema", json_schema: {name, schema, strict: true}}` |

No forced tool-calls. The model emits JSON in `text`; we parse via
`maybe-parse-json` into `:structured`.

## Switching providers

In `src/podcast/llm.clj`:

```clojure
(def ^:private llm-client local-client)   ; or anthropic-client
```

Each model config carries `{:model "..." :max-tokens N}`; the unified
adapter handles the wire format. Configs in `src/podcast/core.clj`
(`sentiment-config`, `conspiracy-config`) point at:

- **Anthropic Haiku** (`claude-haiku-4-5`) — default in earlier runs.
- **Local gemma4-26B via llama.cpp** — current default. `:resolve-strategy
  :group` because the reasoning model can't fit a full LLM resolve in
  the 32k slot.

## Caching

`toolkit.llm.cache` (LMDB, `cache/podcast/lmdb/`) keys every call on
`(stage, model, system, content-parts, schema)`. Editing one prompt
invalidates that one stage's cache; everything else hits. Cache is
shared across providers but keys differ naturally (different model
name → different key).

## Parallelism

`(extract! ... :workers N)` controls per-stage concurrency for Stages A
and C. Default: auto-detected from llama.cpp's `/props.total_slots`
endpoint via `podcast.llm/detect-slots`. Falls back to 1 (sequential)
for providers without a slot model — Anthropic's rate limits make
heavy concurrency a foot-gun there.

Bounded via a `Semaphore` + `future`s; order-preserving.

## Versioned outputs

`extract!` writes into a freshly-allocated `out/N/` directory (next
available N). Sentiment + conspiracy from one bundle land in the same
N. A `run.json` companion records:

- task, slice, chunking
- timestamp, total tokens, per-stage tokens
- per-stage wall-clock, worker count
- the model config used

So multiple comparison runs accumulate side-by-side without
overwriting each other; `dev/podcast_compare.py` diffs any pair.

## Quality assessment

`(podcast.judge/judge-output! config "out/N/sentiment-slice.json"
 "stumpf.json" :sample 20)` re-checks every record (or a sample) by
sending the source paragraph + N-paragraph backward window + the
extracted claim to the model and asking "does the source support
this?". Verdicts:

- `supported`     — claim is justified by the source.
- `partial`       — entity/quote right but polarity/stance off, or
                    rationale stretches the source.
- `unsupported`   — source doesn't justify the claim.
- `contradicted`  — source says the opposite.

Output written to `judgments.json` next to the source. Cached like
extraction calls — re-running judgment after fixing a record only
re-fires that one.

`dev/podcast_compare.py` surfaces verdict counts when a
`judgments.json` sibling exists.

## Known quality issues / limitations

- **Stage A on reasoning models over-extracts.** gemma4 emits ~50
  mentions per 8-paragraph chunk vs Haiku's ~10. Most are valid but
  redundant; the registry inflates accordingly.
- **`:group` strategy doesn't merge cross-form references.** "Trump"
  and "the president" stay separate entities. Quality cost is real
  but bounded — multi-occurrence entries still capture most patterns.
- **Sarcasm.** Both Haiku and gemma4 missed "Well, they're doing
  good" (sarcastic about California tax policy). The judge catches
  these consistently.

## Files

- `src/podcast/core.clj` — pipeline assembly, configs, `extract!`
- `src/podcast/llm.clj`  — LLM-side glue: client selection, schemas,
                           prompts, validation, pre-clustering
- `src/podcast/judge.clj` — quality-assessment pass
- `dev/podcast_compare.py` — side-by-side run comparison
- `dev/podcast_replay.clj` — re-validate cached records under a new
                              validator (no API spend)
- `dev/podcast_quality.py` — single-run quality summary
- `dev/mac_mini.clj` — local-server probes (hello, structured,
                       random-int trial)
