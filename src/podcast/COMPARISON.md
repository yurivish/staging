# Local-vs-Haiku comparison + improvements log

A cross-cut of three full sentiment-extraction runs over the COVID
slice (`stumpf.json` paragraphs 280–340, 8 chunks). All three pass
trace validation (every record's quote substring/fuzzy-matches the
source paragraph).

```
run  file                       model                  ents  recs   tokens   cost    wall   judge
1    sentiment-slice2.json      claude-haiku-4-5         43    40   51,811  $0.124   ~3m   85% supported
2    sentiment-slice.json       gemma4:26B (Q5_K_XL)    208    60   89,166  local   904s   95% supported
3    sentiment-slice.json       gemma4:26B + metadata   181    53  164,996  local  2111s   98% supported
```

## Headline findings

1. **Local pipeline works end-to-end** against gemma4-26B running on
   llama.cpp with the OpenAI-compatible endpoint. 100% trace
   validation; 95–98% support rate per the auto-judge.

2. **Speaker canonicalisation needs metadata at extraction time.**
   Run 2 (no metadata) left speakers as `'the speaker'`, `'Joe'`,
   etc. — the `:group` resolve strategy can't merge those into proper
   names. Run 3 (with `Host: Joe Rogan. Description: Andy Stumpf...`
   prepended to Stage A) cleanly produces `'Andy Stumpf'` (with `'I'`,
   `'You'`, `'you'` aliases) and `'Joe Rogan'` (with `'I'`, `'mine'`).
   This is the single biggest quality improvement of the session.

3. **Reasoning models can't do Stage B's full LLM resolve at this
   scale.** With 348 mentions, gemma4-26B blew past the 32k slot
   context on its scratchpad before emitting valid JSON. Switched
   the default to a deterministic `:group` strategy (pure-Clojure
   exact-match clustering) for the local config. The trade-off:
   cross-form merges like "Trump"/"the president" don't happen in
   `:group`; metadata-aware Stage A reduces this cost dramatically by
   canonicalising at extraction time.

4. **4-way parallelism (auto-detected from llama.cpp `/props.total_slots`)
   roughly halved Stage C wall-clock.** Per-call concurrency is
   ~2× theoretical (4 slots, ~2× speedup) — bottlenecked by the
   mac mini's compute or memory bandwidth, not by the slot count.

5. **Quality auditor (`podcast.judge`) catches genuine errors.** Across
   the three runs, judge-flagged issues include:
   - Sarcasm misread: "Well, they're doing good" (about CA tax policy).
     Both Haiku and gemma4 read this as positive; the judge correctly
     identifies the sarcasm.
   - Hallucinated rationale entities: claim's rationale mentions
     "engineers and scientists" not in the source.
   - Over-stretched rationale: source says "political system";
     rationale says "pharmaceutical system".
   - Right entity, wrong polarity (sarcasm-adjacent).

   The judge runs on the same local model as extraction; ~12s per
   record at 4-way parallel, ~1500 tokens per judgment.

## What I built / changed (this session)

| Change | File |
|---|---|
| OpenAI-API-compatible adapter (covers OpenAI, Groq, Together, Ollama, llama.cpp, vLLM) | `src/toolkit/llm/openai.clj` + tests |
| `:reasoning` field surfaced in unified response (Ollama-style scratchpad) | `src/toolkit/llm/openai.clj` |
| Local mac-mini probes (hello, structured, random-int trial) | `dev/mac_mini.clj` |
| 4-way parallelism via `bounded-pmap` + auto-detect from `/props.total_slots` | `src/podcast/core.clj` + `src/podcast/llm.clj` |
| Pre-cluster mentions deterministically (`:resolve-strategy :group`) | `src/podcast/llm.clj` |
| Episode-metadata injection into Stage A prompts | `src/podcast/llm.clj`, `src/podcast/core.clj` |
| Per-stage wall-clock + tokens + tok/s in run summary | `src/podcast/core.clj` |
| Versioned `out/N/` outputs with `run.json` companion | `src/podcast/core.clj` |
| Quality auditor (`podcast.judge`) with verdicts + reasons | `src/podcast/judge.clj` |
| Cross-run summary table | `dev/podcast_summary.py` |
| Side-by-side compare with cost + judge integration | `dev/podcast_compare.py` |
| Bumped http-kit idle timeout to 1 hour for slow local generation | `src/toolkit/llm.clj` |
| Architecture README | `src/podcast/README.md` |

## Costs and throughput

- **Haiku v2 (Anthropic)**: $0.124 for the slice / ~$1.73 for the full
  transcript. Wall-clock not preserved but historically ~3 min slice,
  ~13 min full.
- **Local gemma4 (mac mini)**: free per-call. Sentiment slice with
  metadata: 35 min wall-clock, 165k tokens, 78 tok/s wall-clock
  (faster than that net of Stage A's reasoning budget).
- **Speed: Haiku is 7–12× faster per call than gemma4.** The local
  pipeline trades wall-clock for $0 cost.

## Known limitations carried forward

- gemma4-26B over-extracts in Stage A (50–70 mentions per 8-paragraph
  chunk vs Haiku's ~10). Most are valid but redundant; this inflates
  the registry. A non-reasoning model on the local server (e.g.
  `mistral-small3.2:24b`, `qwen2.5:32b`, `llama3.3:70b`) should be
  faster and less over-eager — recommended as the next step when the
  user pulls one.
- `:group` strategy doesn't merge cross-form references. With
  metadata-aware Stage A this matters less but still costs some entity
  unification (e.g. "pharmaceutical drug companies" and "pharmaceutical
  companies" remain separate in run 3).
- Sarcasm detection misses on both Haiku and gemma4. A quality
  audit pass with a stronger model could be a deliberate review step.

## What to try next when you return

1. **Pull a non-reasoning model.** `ollama pull mistral-small3.2:24b`
   or run llama.cpp with `qwen2.5-32b-instruct`. Then re-run with that
   model name in the config — should drop wall-clock to ~8–10 min for
   the slice.
2. **Run the conspiracy slice on local model.** Same pattern as
   sentiment; uncached so ~35 min.
3. **Run the full transcript on local model.** ~3 hours estimated;
   useful as the proof of corpus-scale viability.
4. **Increase llama.cpp `--n-ctx-per-slot` to 64k or 96k** if you want
   to try the `:llm` Stage B strategy on local. Without that, `:group`
   is the right choice for reasoning models.
5. **Cross-model judge.** Currently the judge runs on the same model
   that produced the output (same gemma4 for run 2/3). Pointing it at
   a different model (Haiku or a stronger local) would be more
   independent — and arguably catch more issues. Easy parameter swap.
