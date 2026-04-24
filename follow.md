# Datapotamus — follow-up work and bottleneck summary

Companion to `we-are-building-a-typed-rabbit.md`. That document plans the **measurement suite**; this one summarizes **what the measurements surfaced** and proposes targeted upstream changes. All observations here are grounded in the code as of 2026-04-24 and in the numbers produced by the four runnable scenarios (`hist`, `datapotamus`, `littles`, `slow-sub`).

---

## Executive summary

On this 4-core box, the `:noop` pipeline caps out at **~17K msgs/s with p50 ≈ 100 µs**, and *every* topology tested — `chain-3`, `chain-10`, `workers-4` — hits the same ~17K/s ceiling. That ceiling isn't your handler code. It's the per-event bookkeeping on the publishing thread: **every pipeline event (`:recv`, `:success`, `:send-out`, `:split`, `:merge`, ...) goes through `sp-pub` → `pubsub/pub` → the main subscriber, which does two `swap!`s on shared atoms and appends to an unbounded event vector.** Roughly 5 events per message × 2 CAS-loops per event × ~8 allocations per event = several hundred thousand allocations/sec and several hundred thousand atomic ops/sec under contention, all on the critical path of the proc threads.

The `slow-sub` experiment (17K/s → 996/s → 165/s for 0 → 100 µs → 1 ms subscriber sleep) is the acute form of the same problem: because pubsub dispatch is synchronous on the publisher's thread, anything slow anywhere in the observability stack stalls the pipeline.

The good news: the routing itself is cheap. The **noop subscriber** line (17.22K/s, indistinguishable from the no-subscriber baseline at 17.16K/s) shows that `sl/match` + `pick-one` + handler dispatch cost is in the noise. The entire ceiling is sitting in *what the built-in subscribers do*, not in pubsub's plumbing.

Below is a ranked plan for addressing this, from "finish before lunch" to "needs a design discussion."

---

## Where the time actually goes

Per pipeline event, tracing down through the code:

**`toolkit.datapotamus.trace/sp-pub`** (`trace.clj:107`)
- `(System/currentTimeMillis)` — syscall, ~20–50 ns.
- `subject-for` → `scope->tokens` — `vec (mapcat …)`, allocates a 4–8 element vector **on every event**, even though the scope is fixed for the lifetime of the proc.
- `flow-path-of` — `mapv (filter …)`, allocates another small vector, same story.
- `assoc` adds `:scope :flow-path :at` — new map.
- Then calls `pubsub/pub`.

**`toolkit.pubsub/pub`** (`pubsub.clj:66`)
- `sl/match` — subject-tree lookup, allocates a `{:plain … :groups …}` result.
- `pick-one` — reduces over `:plain` + non-empty queue groups, allocates a collection.
- For each matching subscriber: `(invoke handler …)` — try/catch + call. Fast.

**The main subscriber** registered at `flow.clj:319`:
```clojure
(fn [_ ev _]
  (swap! events-atom conj ev)                       ; CAS loop + new pvec node
  (let [c' (swap! counters trace/update-counters ev)] ; CAS loop + new map
    (when (trace/balanced? c')
      (deliver @done-p :quiescent))))
```

Hot cost:
- `events-atom`: **unbounded** — every event accumulates forever. The bench discovered this as a memory leak (needed a 50 ms drainer just to keep the heap from blowing up). In production a long-running flow will eventually OOM.
- `counters`: every event causes `(update-counters counters ev)` (`trace.clj:65`), which does `case` + `update` + `inc`. That's a new map per event, CAS'd into the atom. With N procs firing events concurrently on N threads, both swaps serialize across the same two atoms.

**Observed impact:** on noop, ~5 events per message × (2 CAS + ~8 allocations) per event = ~50 CAS ops and ~800 small allocations per message. At 17K msg/s that's ~85K CAS and ~1.4 M allocations per second — GC pressure + atomic retries dominate the observed 60 µs/event critical path.

---

## Ranked bottlenecks

Impact × easiness, approximate rough estimates.

| # | Change | Impact | Effort | Risk |
|---|---|---|---|---|
| 1 | Opt-out / cap on `events-atom` accumulation | ★★★★ | 30 min | low |
| 2 | `LongAdder` for counters, not `atom` + `swap!` | ★★★ | 1 hr | low |
| 3 | `System/nanoTime` in event `:at` stamps | ★★ | 15 min | low |
| 4 | Cache scope-tokens / subject-vector per proc | ★★ | 1 hr | low |
| 5 | Expose channel buffer size per conn | ★★★ (for benches) | 2 hr | low |
| 6 | Async-drain subscriber adapter | ★★★★ | half-day | medium |
| 7 | Sharded / per-proc counters, quiescence via ping | ★★★★★ | 1–2 days | high |
| 8 | Pub path: drop sync semantics for built-in subs | ★★★★ | 1 day | medium |

Pick the top three for a first PR. They collectively knock a chunk out of the 17K/s ceiling without touching anyone's public API.

---

## Quick wins

Each of these is self-contained, doesn't change public semantics, and is independently verifiable against the current bench suite.

### 1 — Cap / opt out of `events-atom`

**File:** `src/toolkit/datapotamus/flow.clj:316` and `319`.

The `events-atom` exists for two reasons: user-accessible `events` accessor after `stop!`, and error collection for the final result. Neither justifies unbounded per-event growth.

**Proposal:**
- Add an `opts`-level `:events` mode passed through `start!`:
  - `:all`   — current behavior (keep everything; default for small runs).
  - `:errors` — keep only `:failure` / `:flow-error` events (default for long-running flows).
  - `:none`  — drop everything; `events` accessor returns `[]`.
- Main-sub handler becomes a switch on this mode. Non-`:all` paths skip the `swap! events-atom conj ev` entirely.

Downstream consumers that genuinely want the full event log (visualizer, tests) either set `:all` explicitly or attach their own subscriber. The main-sub stops being a hidden costly observer.

**Verification:** re-run `clojure -M:bench datapotamus` with default `:events :errors`; expect the noop ceiling to move meaningfully above 17K/s. Keep existing datapotamus_test.clj running with `:events :all` to preserve test semantics.

### 2 — `LongAdder` counters

**File:** `src/toolkit/datapotamus/trace.clj:65–76`, and the atom construction in `flow.clj:317`.

Replace `atom {:sent 0 :recv 0 :completed 0}` with three `LongAdder` instances. Replace `update-counters` with direct `.increment` calls keyed by `(:kind ev)`. `balanced?` becomes `(let [s (.sum sent) r (.sum recv) c (.sum completed)] (and (pos? s) (= s r) (= r c)))`.

`LongAdder` is specifically designed for many-writer hot paths — it strips across cache lines so N threads incrementing rarely contend. Our `hist_bench` already validated this pattern (sparse-via-LongAdder hit 162 M/s at 16 threads on the same box, vs 92 M/s for dense-via-AtomicLongArray).

**Caveat:** the quiescence check `balanced?` reads three sums non-atomically. A reader could see `sent=N, recv=N, completed=N-1` momentarily, then `sent=N+1, recv=N+1, completed=N+1` on the next check. This is already the case today — `swap!` on `counters` doesn't make reads across events atomic either. As long as `balanced?` is re-checked on each event (which it is), eventual consistency is fine.

**Verification:** `datapotamus_test.clj` already exercises quiescence heavily; if those pass, semantics are preserved.

### 3 — `nanoTime` for event timestamps

**File:** `src/toolkit/datapotamus/trace.clj:20`.

Change `(defn- now [] (System/currentTimeMillis))` to `(defn- now [] (System/nanoTime))`. Update the single consumer in `flow.clj:342, 347, 413` similarly.

`currentTimeMillis` resolution is 1 ms on most Linux kernels, so *every* intra-ms operation appears instantaneous in the event log. That's the sole reason the harness had to roll its own per-message stamp via payload wrapping. Switching to `nanoTime` is what unblocks the per-stage-service-time histograms the plan calls for.

**Gotcha:** `nanoTime` is monotonic but not wall-clock. Anything that was comparing `:at` across process restarts (I don't think anything does) would break. Anything that was exposing `:at` as a human-readable timestamp (the visualizer?) needs to re-wall-clock it or relabel. Check `viz.clj` for usage and decide on the field name (`:at-ns` is self-documenting).

### 4 — Cache the subject/scope token materialization

**File:** `src/toolkit/datapotamus/trace.clj:82–101`, and the call sites in `sp-pub`.

`scope->tokens` is called on every event, even though the scope is fixed once per proc. Same for `flow-path-of`. Both allocate small vectors.

**Proposal:** at proc-instantiation time (`flow.clj:244`, the `proc-fn` factory), pre-compute the tokens for every `:kind` the proc might emit. Store a map `{:recv <subject-vec> :success <subject-vec> :send-out <subject-vec> ...}` in a closed-over local. `sp-pub` looks up the pre-built vector instead of re-building it.

Alternatively, just intern: `(memoize scope->tokens)` bounded by the fact that scope identity is stable per proc.

Not huge on its own; combined with #1 (skipping the main-sub entirely for non-error events), the scope-tokenization cost is proportionally more visible, so do this one after #1 and measure.

---

## Medium-term work

### 5 — Expose per-conn channel buffer size

**File:** `src/toolkit/datapotamus/flow.clj:268` (`build-graph`).

Currently `flow/create-flow` is called with `:procs` and `:conns` only. `core.async.flow`'s conn spec supports `:buffer` per edge. Plumbing this through:

- Step representation: `:conns` entries get a third element: `[[from] [to] opts]` where `opts` is `{:buf <int>}` or nil.
- `build-graph` forwards the opts to `flow/create-flow`.
- `step/connect` gets a 4-arity variant that takes `opts`.

Needed for benchmarks (the plan calls for queue-depth sweeps, and without buffer control the numbers are a function of `core.async.flow`'s default, which is not a useful independent variable). Also useful to users who want to apply targeted backpressure.

### 6 — Async-drain subscriber adapter

**File:** new, `src/toolkit/pubsub.clj` or a sibling.

The `slow-sub` experiment proves that every user who attaches a subscriber is one `Thread/sleep` away from a 100× throughput collapse. Today the pubsub docstring warns "handlers run synchronously on the publisher's thread" but doesn't offer an alternative.

**Proposal:** an adapter fn `async-handler` that:
- Takes the user's handler + a `{:buf N :drop-policy :block|:drop-new|:drop-oldest}` opts map.
- Returns a sync handler suitable for `pubsub/sub`, plus a stop fn.
- Internally creates a bounded ring buffer (`ArrayBlockingQueue` or `ManyToOneArrayQueue` from JCTools) and a dedicated drainer thread.
- The sync handler does only an `offer` (non-blocking or bounded-wait per policy) and returns.

Callers opt in:
```clojure
(let [[h stop] (pubsub/async-handler my-slow-fn {:buf 4096})]
  (pubsub/sub ps pattern h))
```

The `drop-policy` choice is load-bearing — `:block` gives correctness at the cost of re-introducing the stall; `:drop-oldest` gives smooth publishers at the cost of missing events. The doc should be explicit about the tradeoff.

This does NOT change the built-in main-sub. That's item #1's job.

### 7 — Fix the visualizer's queue-depth poll

Not a bottleneck per se, but `viz.clj:140` pings every 500 ms via `flow/ping`, which itself goes through core.async.flow message dispatch. At bench rates that's fine, but at idle it's steady work. Consider moving to "sample on demand when the user scrolls the UI" or reducing to 2 Hz by default.

Related: if you do #7 in the architectural section below (per-proc counters via ping), this work becomes redundant.

---

## Architectural — needs a design conversation

### 8 — Per-proc counters + ping-based quiescence

The deepest win, and the most invasive. Today, quiescence is derived from an event-stream aggregate on a single shared atom. That's why every event has to hit that atom. An alternative:

- Each proc maintains its own counters in its state (thread-local; no contention).
- Flow-level quiescence = "for every proc, `sent` at that proc's outputs == the sum of `recv` at its downstream consumers' inputs." This can be computed by walking the conns map and summing proc-local counters.
- `await-quiescent!` either polls (cheap; proc state is local reads) or the proc-loop publishes a low-frequency "my counters changed" signal that the quiescence watcher debounces.

Benefit: the proc hot path drops to zero cross-thread synchronization. A proc handles a message, updates its own local counters, and moves on. The pubsub event stream becomes purely optional (for visualizers / debuggers who opt in via `:events :all`).

This is where the 17K/s ceiling gets *actually* removed. Expect a 5–10× throughput improvement on noop after this change.

**Trade-offs:**
- Polling has latency. If users need to know "quiescent right now" within 1 ms, the polling approach needs tuning.
- Signal-based version needs an async pubsub or a dedicated quiescence-watcher thread.
- The event log goes from always-on to opt-in. Tests that rely on full event logs need explicit `:events :all`.

### 9 — Make the pub-path asynchronous for built-in subs

Related to #6 and #8. The built-in main-sub is the only *non-user* subscriber — everything else is attached by application code. If the main-sub moves to an async internal queue (or goes away entirely per #8), the synchronous-pubsub semantics for user subscribers can stay unchanged (fewer surprises), while the hot path is no longer synchronously bound to observability.

---

## Proposed first PR (scope)

Bundle items 1, 2, 3, 4 into one PR. They're all additive, preserve public semantics, and collectively address the biggest cost on the proc hot path without an architecture change.

1. `trace.clj` — `nanoTime` for `now`, cache scope tokens per proc.
2. `flow.clj` — `:events` opt-out mode, `LongAdder` counters.
3. `datapotamus_test.clj` — add `:events :all` where full event streams are asserted.
4. `bench` — re-run `littles` and `datapotamus`; record the new ceiling.

Measurement target: noop p50 below 40 µs, noop throughput at C=8 above 40K/s on a 4-core box. If those don't move, the hypothesis was wrong and the bottleneck is elsewhere (channel contention in core.async.flow, or something in `msg/synthesize`).

Second PR: item 5 (buffer-size per conn) + the harness extensions that use it (queue-depth experiments from the bench plan). Third PR: item 6 (async-drain adapter) + a `slow-sub` re-run that demonstrates the collapse is gone when subscribers opt in to async. After that, the architectural work in 7–9 gets its own design doc.

---

## Verification plan

After each change:
- Full `clojure -X:test` — must remain 317/317.
- `clojure -M:bench datapotamus` — throughput + p50/p99/max; compare against baseline in this document.
- `clojure -M:bench littles` — L should track C more closely, especially in the previously-saturated region (C=8–32).
- `clojure -M:bench slow-sub` — after #6, the "slow (1 ms)" row should stay within 2× of the baseline rather than collapsing to 165/s.

Baseline to beat (4-core box, this run):
- noop C=16: 17.0K/s, p50=101 µs, p99=434 µs, p999=2.49 ms.
- chain-3 C=16: 17.3K/s, p50=190 µs, p99=1.56 ms.
- chain-10 C=16: 10.8K/s, p50=859 µs, p99=3.72 ms.

---

## Open questions

- Should `:events :errors` (not `:all`) be the default for new flows? I'd lean yes — most users don't inspect the event log, and the memory growth is a silent foot-gun. But it changes the return value of `stop!` and would need a version bump.
- Is there appetite for dropping `toolkit.pubsub`'s synchronous guarantee entirely and making async the default? It would require touching every subscriber site in `viz.clj` / tests. Probably not worth it for this round.
- `core.async.flow`'s internal scheduling model assumes one VT per proc. If `workers-W` is the main scaling primitive, proc count grows with W, so VT count grows with W × pipeline-depth. On a very wide pipeline (say workers-64 × chain-10) we're asking for 640 VTs. That's fine on JDK 21+ in principle, but worth measuring — not today's scope.
