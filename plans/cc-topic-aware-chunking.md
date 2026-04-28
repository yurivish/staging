# Topic-aware chunker for `podcast.cc` extractor (Phase 3c)

## Context

`podcast.cc/extract!` (Architecture v2) splits the transcript into ~100-paragraph chunks via `paragraph-chunks-for-extractor`, runs one extractor call per chunk, and aggregates records. The chunker is purely positional — it cuts every N paragraphs regardless of conversational structure.

After the first full-transcript Sonnet run (`out-cc/9` on `stumpf.json`, 7 chunks), we manually inspected the paragraphs around all 6 chunk boundaries to judge whether they land at natural breaks.

### Findings: 5 of 6 boundaries cut mid-topic

| boundary | what's on either side | verdict |
|---|---|---|
| 99 / 100 | Mid-conversation about cold plunges. p99: *"I almost don't do it every day."* p100: *"Yeah, it's harder emotionally than it is physically."* | **bad** — splits a back-and-forth |
| 199 / 200 | p199 ASKS about military shoes, p200 ANSWERS. | **bad** — splits Q&A |
| 299 / 300 | p299 ends a riff on COVID comorbidities; p300 asks *"Do you think we learned anything during that time period?"* | **decent** — clear topical pivot |
| 399 / 400 | Mid-Starlink discussion. p399 mentions Starlink, p400: *"Yeah, that's the shit."* | **bad** — splits a topic |
| 499 / 500 | Andy mid-monologue about wingsuit risk; chunk 4 ends mid-thought, chunk 5 begins with the continuation. | **bad** — splits a single thought |
| 599 / 600 | Continuous UFO / alien disclosure topic. | **bad** — same conversation |

Naive 100-paragraph chunking is roughly **17% of "ideal"** (where ideal = topical / structural chunker on this transcript).

### Practical impact

Less than the boundary stats suggest, because the extractor system prompt explicitly grants Read/Grep access to the full transcript for anaphora resolution:

> "Use Read/Grep on neighboring paragraphs in paragraphs.txt to resolve anaphora when needed, but emit records ONLY for the focus range."

Sonnet exercises this — in `out-cc/9` chunk x-5 used 20 turns vs other chunks' 4-10, mostly extra Greps for cross-chunk anaphora. And the validator passed 162/162 records.

But cross-chunk Read doesn't fully compensate. Specific failure modes silently introduced by bad cuts:
- A multi-paragraph emotional arc spanning a boundary is split — chunk N may emit a sentiment record from the first half but miss the rationale that lives in chunk N+1's text.
- Sentiment expressed in chunk N+1 *about* an entity that chunk N's first half introduced — chunk N+1's agent has Read access but might not look back.
- Q&A pairs split across chunks: question in chunk N, answer in chunk N+1. The sentiment is in the answer; the question's framing matters.

Estimate: 5-15% of records are weakened or missed vs ideal-chunked output. Hard to quantify without ground truth.

## Goal

Replace `paragraph-chunks-for-extractor`'s position-only logic with a topic-aware chunker that lands boundaries on natural breaks (topic shifts, Q&A boundaries, scene/entity changes).

## Non-goals

- Don't change the rest of `extract-task!` — chunker is just a different way to produce the `chunks` vector.
- Don't change the cache-key format — chunks still have `:chunk-id` and `:focus-range-label`; the contents vary but the keys are still per-chunk.
- Don't add a new agent step type to Datapotamus — this is internal to `podcast.cc`.

## Design

A small pre-pass agent call between scout and the extractor loop, dedicated to producing a chunking plan.

```
scout (existing — full transcript → registry.json)
  ↓
chunker (NEW — full transcript → chunk-plan.json with [{start-pid end-pid topic-label} ...])
  ↓
extractor loop (existing — one call per chunk, but chunks come from the chunker's plan)
  ↓
aggregate (existing)
```

### Chunker call contract

- **Input:** `paragraphs.txt` in agent-cwd (already there). Optional `:target-chunk-size` (default 100 paragraphs ± 30% slack).
- **System prompt:** "Read paragraphs.txt. Identify natural break points where the conversation pivots — meta-questions, scene changes, topic shifts, end of an anecdote. Emit a list of chunk ranges where each chunk covers ~`target-chunk-size` paragraphs but the boundaries land at natural breaks. Each chunk has a one-line topic label for traceability."
- **Tools:** `["Read" "Grep" "TodoWrite"]` — same as scout/extractor.
- **Output schema:**

```clojure
{:chunks [{:chunk-id "x-0"
           :start-paragraph-id "t0-0"
           :end-paragraph-id "t1099-97"
           :topic "Cold plunge / sauna routine, mental discipline"}
          ...]}
```

- Validation: ranges must be contiguous (`end[i] + 1 == start[i+1]` by paragraph index), cover all paragraphs, have non-empty topic.

### Caching

The chunker call is its own LMDB entry, key `(call-tag :cc-chunker, transcript-hash, model, prompt, schema)`. Cheap (~$0.10 on Sonnet, free on local). Re-runs hit cache.

### Extractor changes

`run-chunk!` already takes a chunk map with `:chunk-id`, `:focus-ids`, `:focus-paragraphs`, `:focus-range-label`. Just feed it chunks from the chunker's output instead of from `paragraph-chunks-for-extractor`. The `:focus-range-label` becomes the chunker's `topic` field for richer trace output.

`paragraph-chunks-for-extractor` stays as a fallback when the chunker isn't enabled (e.g., for the test fixture or when `:topic-aware-chunking? false`).

### Config knob

```clojure
{:topic-aware-chunking? true}        ; default true on cloud-cfg
{:topic-aware-chunking? false}       ; default false on local-cfg until validated
```

Local default off because gemma's chunker call would be ~10 min of extra latency for marginal benefit; cloud default on because Sonnet does it cheaply and well.

## Implementation steps

1. New private fn `chunk-plan-via-agent` in `podcast.cc` — wraps a `cached-cc-run!` call with the chunker system prompt and schema. Returns the parsed chunk plan.
2. New private fn `materialize-chunks` — takes the chunk plan + paragraphs, returns the same `[{:chunk-id :focus-ids :focus-paragraphs :focus-range-label} ...]` shape `paragraph-chunks-for-extractor` produces.
3. In `extract-task!`, after scout completes:
   - If `(:topic-aware-chunking? config)` is true, call `chunk-plan-via-agent` then `materialize-chunks`. Otherwise `paragraph-chunks-for-extractor`.
4. Test with `with-redefs` on `cc-step/run!` returning a canned chunk plan; verify `materialize-chunks` produces correct chunk shapes and `extract-task!` runs the extractor over them.

Total LOC: ~80, all in `podcast.cc` + a new test in `cc_test.clj`. No changes to `claude_code.clj` or schemas.

## Critical files

- `/work/src/podcast/cc.clj` — add `chunker-system-sentiment`, `chunker-schema`, `chunk-plan-via-agent`, `materialize-chunks`. Modify `extract-task!` to branch on `:topic-aware-chunking?`.
- `/work/src/podcast/cc_test.clj` — new test for the topic-aware path.

## Verification

After landing:

```clojure
(require '[podcast.cc :as cc] :reload)
(cc/extract! cc/cloud-cfg "stumpf.json")
;; Expect:
;;   trace.edn shows :cc-chunker :run-finished before any extractor chunks fire
;;   chunk-plan.json sits in agent-cwd with topic labels
;;   extractor chunks land on different boundaries than positional partition-all
;;   no chunk crosses a paragraph index that the previous run cut at (99, 199, 299, ...)
```

Manual re-inspection of boundaries: spot-check that 4-5 of 6 land at meta-questions / topic shifts. If still <50% clean, the chunker prompt needs more direction.

Defer Phase 3c **until** there's evidence the bad cuts are limiting output quality — the current pipeline produces clean validated records and quality drop from naive chunking is in the 5-15% range, lower priority than other improvements like:
- Phase 3a: parallelize chunks within a task (real wallclock speedup)
- Phase 3b: stream-json watchdog (defensive infra)
- Phase 3d: chunk the scout call too (for very long transcripts)

This doc is a parking spot, not an immediate work order.
