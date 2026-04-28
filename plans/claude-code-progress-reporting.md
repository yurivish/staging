# Streaming progress events from `cc-step/run!`

## Context

`toolkit.datapotamus.claude-code/run!` invokes the `claude` CLI with `--output-format json` (`claude_code.clj:99`), which buffers the entire agent loop and emits a single JSON document on stdout when the run terminates. We slurp it, parse it, emit one result map on the data port `:out`. Between "subprocess started" and "subprocess finished" the rest of Datapotamus sees nothing — humans tailing a long run can't tell whether the agent is making progress, looping on a tool call, or wedged on llama.cpp.

The CLI also supports `--output-format stream-json`, which emits NDJSON: one JSON event per line as the agent runs (session init, assistant turns, tool calls, tool results), terminating with a `result` event whose shape matches today's buffered output. Switching to streaming gives us per-turn observability without changing the data-port contract — we still emit exactly one final result on `:out`. The intermediate events go on the **existing** trace pubsub channel, where the recorder already serializes them into `trace.edn` (`recorder.start-recorder!` at `recorder.clj` is unchanged by this plan).

This change is plumbing — for the podcast pipeline (the immediate consumer) and any future Claude-Code-driven step that wants live status while a long agent run is in flight.

## Goal

Make `cc-step/run!` optionally drive `claude` in stream-json mode and publish per-turn `trace/emit` events on the scoped pubsub passed via `:ctx`. Default behavior (buffered, current invariants) unchanged for callers that don't opt in.

## Non-goals

- **Don't change the data-port contract.** `:out` still emits exactly one result per `:in`. Streaming events go on the side-band trace channel, not `:out`.
- **Don't replace buffered mode.** Stream mode is opt-in; buffered remains the default. The namespace docstring at `claude_code.clj:6-15` calls out the single-emission invariant — we preserve it in both modes.
- **Don't surface per-token deltas.** Stream-json's natural granularity is per-message (one event per assistant turn / per tool call). Per-token deltas live below the CLI; out of scope.
- **Don't add a UI / live viewer.** We publish events; downstream consumers (recorder, terminal observers, etc.) are unchanged.
- **Don't change caching.** The cached value is still the final result map only; per-turn events are not in the cache key or value.

## Stream-json event taxonomy (what `claude` emits)

`claude --print --output-format stream-json` emits NDJSON with these top-level event types. Field shapes are best-effort from docs + buffered output structure; treat as informative, parse defensively.

| `type`      | meaning                                                                  | shape (selected fields)                                                                                |
|-------------|--------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `system`    | session init, emitted once at start                                      | `subtype` (`"init"`), `session_id`, `model`, `cwd`, `tools` (whitelisted names), `mcp_servers`, …      |
| `assistant` | one event per assistant turn                                             | `message: {content: [{type: "text"\|"thinking"\|"tool_use", …}]}`, `parent_tool_use_id`                |
| `user`      | one per "user-side" turn (= echoed tool_results from the harness)        | `message: {content: [{type: "tool_result", tool_use_id, content, is_error}]}`                          |
| `result`    | final summary, emitted once at end                                       | same shape as today's buffered JSON: `result`, `stop_reason`, `total_cost_usd`, `usage`, `num_turns`, `is_error`, `permission_denials`, `modelUsage`, `terminal_reason`, `session_id`, `uuid` |

Deltas (per-token streaming) are NOT in stream-json — that level is inside the Anthropic SDK, not the CLI's `--print` mode.

**Empirical questions, to verify on implementation** (each is a small unknown that would shift parsing details, none invalidates the design):

1. Does `--output-format stream-json` co-exist with `--bare`? Likely yes.
2. Does it co-exist with `--json-schema`? Specifically: does the `result` event still include `structured_output` when `--json-schema` was passed?
3. Is line-buffering reliable in practice? Verify the CLI flushes after each event (no need for a PTY wrapper, no additional buffering on our side).
4. Are tool_use events emitted as content blocks inside an `assistant` message, or as separate top-level events? This determines whether `:tool-use` is a sub-projection of `:assistant-message` or a peer phase.
5. Exact field path for tool name/input. `message.content[].input` vs `message.content[].tool_input` vs other.
6. Do `permission_denials` arrive incrementally during the stream, or only as part of the final `result`? If incremental, add a `:permission-denied` phase.
7. Latency cost of per-event `trace/emit` for chatty runs. Should be negligible — sanity-check on a 50-turn run.
8. Behavior on llama.cpp's Anthropic shim. The shim is best-effort per llama.cpp's docs; if streaming is broken there but works on the cloud, we keep `:stream? false` for `local-cfg` and document.

## Trace event projection

Each stream-json event becomes a small, stable trace event. Goal: useful for live observation, compact enough that `trace.edn` doesn't explode (a 50-turn run with 100KB tool_results in raw form would balloon trace.edn well past anything human-readable). Subscribers can filter on `:phase`.

| stream-json event                | trace `:phase`        | extracted fields                                                                                              |
|----------------------------------|-----------------------|---------------------------------------------------------------------------------------------------------------|
| `system` (subtype `init`)        | `:session-init`       | `:session-id`, `:model`, `:cwd`, `:tools` (count), `:mcp-servers` (count)                                     |
| `assistant` text content block   | `:assistant-text`     | `:length` (chars), `:preview` (first 200 chars), `:turn-index`                                                |
| `assistant` thinking block       | `:assistant-thinking` | `:length`, `:turn-index`                                                                                      |
| `assistant` `tool_use` block     | `:tool-use`           | `:tool` (name), `:tool-use-id`, `:input-preview` (truncated to 500 chars), `:turn-index`                      |
| `user` `tool_result` block       | `:tool-result`        | `:tool-use-id`, `:is-error`, `:content-length`, `:content-preview` (first 500 chars)                          |
| `result`                         | `:run-finished`       | preserves today's fields (stop-reason, num-turns, total-cost-usd, is-error) + new `:permission-denials-count` |
| (synthetic) malformed line       | `:stream-parse-error` | `:line-preview` (first 200 chars), `:exception-message`                                                       |

Existing `:run-started` (emitted at process spawn, `claude_code.clj:176-180`) is unchanged.

**Redaction policy**: previews only. Tool_result bodies (e.g. a 100KB Read of paragraphs.txt) are truncated to 500 chars + length. The full content is recoverable from claude's own session logs if absolutely needed; trace.edn stays lightweight. If someone wants raw stream events for debugging, that's a future feature (`:raw-stream-out <writer>`) — out of scope here.

`:turn-index` is a monotonic counter we maintain locally — claude doesn't expose it directly. Increment on each `assistant` event.

## API surface

Single new opt on `cc-step/run!` and `cc-step/step`:

```clojure
{:stream? boolean   ;; default false; when true, --output-format stream-json
                    ;; and per-turn trace/emit events on the scoped pubsub.
}
```

Other public surface unchanged. `:cancel`, `:env`, `:cwd`, `:ctx`, `:bare?`, `:allowed-tools`, etc. all work the same.

If `:ctx` is nil and `:stream? true`, we still consume the stream (so the subprocess can finish and exit cleanly) but skip emit calls — same as today's "no ctx, no events" behavior on `:run-started`/`:run-finished`.

## Internal structure

Three small private helpers land in `toolkit.datapotamus.claude-code`:

```
parse-stream-line  [line]              → {:type :assistant :data {...}} | {:type :parse-error :line line :ex ex}
project-event      [event turn-index]  → trace-event-data-map  (one of :session-init :assistant-text :tool-use
                                                                :tool-result :assistant-thinking, or nil to skip)
consume-stream!    [reader ctx]        → walks line-seq, emits trace events via trace/emit, returns the
                                         final :result event's data map (kebab-cased like today's json->kebab)
```

`->args` gets one tweak — switch on `:stream?` for the `--output-format` choice:

```clojure
(cond-> ["claude" "--print"
         "--output-format" (if stream? "stream-json" "json")]
  ;; ...rest unchanged
  )
```

`run!` branches on `:stream?`:

```clojure
(defn run! [{:keys [stream?] :as opts}]
  (let [args  (->args opts')
        proc  (bp/process args ...)
        _     (when ctx (trace/emit ctx (run-started-event opts')))
        _     (watch-cancel! cancel proc)
        _     (drain-stderr! (:err proc) stderr-sb)
        result (if stream?
                 (consume-stream! (io/reader (:out proc)) ctx)
                 (try (json->kebab (slurp (:out proc)))
                      (catch Throwable ex (throw (ex-info "non-JSON on stdout" ...)))))
        exit   (:exit @proc)]
    (when (not (zero? exit))
      (throw (ex-info "claude exited non-zero" {:exit exit :stderr (str stderr-sb) :args args})))
    (when ctx (trace/emit ctx (run-finished-event result)))
    result))
```

Cancellation, stderr drain, exit-code handling, error message: ALL UNCHANGED.

`consume-stream!` returns the final `result` event's data and emits everything else on the way:

```clojure
(defn- consume-stream! [^BufferedReader rdr ctx]
  (loop [final nil
         turn-index 0]
    (if-let [line (.readLine rdr)]
      (let [parsed (parse-stream-line line)
            ;; advance turn-index on each assistant event
            next-idx (cond-> turn-index
                       (= "assistant" (-> parsed :data :type)) inc)]
        (when ctx
          (when-let [te (project-event parsed turn-index)]
            (trace/emit ctx te)))
        (recur (if (= "result" (-> parsed :data :type)) (:data parsed) final)
               next-idx))
      (or final
          (throw (ex-info "claude stream ended without :result event" {}))))))
```

## Cancellation, errors, timeouts

| scenario                                          | buffered (today)                       | stream (proposed)                                            |
|---------------------------------------------------|----------------------------------------|--------------------------------------------------------------|
| process exits 0 with valid JSON                   | parse, return                          | consume stream, find `result` event, return its data         |
| process exits 0 but no `result` event             | n/a                                    | throw `ex-info "claude stream ended without :result event"`  |
| process exits non-zero                            | throw `ex-info "claude exited non-zero"` | same — accumulated trace events still got emitted           |
| process exits 0 with non-JSON stdout              | throw `ex-info "non-JSON on stdout"`   | each bad line → `:stream-parse-error` event; no `result` → throw above |
| `:cancel` delivered mid-run                       | destroy proc → non-zero exit → throw   | same — `.readLine` returns nil after stdout closes; throw    |
| `:ctx` is nil                                     | no `:run-started`/`:run-finished`      | no per-turn events either (consume but skip emit)            |

**Single-emission invariant on `:out` is preserved in every case.** On any failure, the data port emits ZERO results. Status events on the trace channel are NOT rolled back — observers see what the agent did up to the failure, which is the right behavior for debugging.

## Files to change

- **`/work/src/toolkit/datapotamus/claude_code.clj`** — add `:stream?` to `->args` destructure, add `parse-stream-line` / `project-event` / `consume-stream!` private helpers, branch `run!` on `:stream?`. ~80 new lines, no removed lines. Update the namespace docstring (the "Single-emission invariant" section already correctly explains why we hardwire `--output-format json` — extend it to document opt-in stream mode and reaffirm that the data-port invariant holds in both modes).

- **`/work/src/toolkit/datapotamus/claude_code_test.clj`** — add tests for stream mode. Existing tests stay; new ones are additive. See **Testing** below.

- **`/work/src/podcast/cc.clj`** — set `:stream? true` in both `local-cfg` and `cloud-cfg`. One-line change per cfg. The recorder will then write per-turn events into `out-cc/N/trace.edn` automatically.

That's it. No new files.

## Testing

### Unit (no subprocess)

Refactor the line consumption into `consume-stream!` taking a `BufferedReader` makes this straightforward. Tests inject an in-memory reader over a fixture string of canned NDJSON. Verify:

1. Each event-type line produces the expected `trace/emit` call (use a recording pubsub stub that captures the emit-fn calls).
2. The final `result` event's data is returned, kebab-cased, matching the buffered path's output for the same canned input.
3. Malformed lines emit `:stream-parse-error` and don't abort the stream.
4. Stream ending without a `result` event throws `ex-info "claude stream ended without :result event"`.
5. With `:ctx nil`, no emits happen but the final result is still returned.
6. `:turn-index` increments correctly across multiple `assistant` events.

Fixture: a 6-line NDJSON file representing `system → assistant-with-text → assistant-with-tool_use → user-with-tool_result → assistant-with-text → result`. Asserts cover every projection in the table above.

### Integration (subprocess; mark `^:slow`)

One end-to-end test that actually launches `claude --bare --print --output-format stream-json --max-turns 1 --model haiku-or-equivalent "say hi"` against the real binary, with `:stream? true` and a recording pubsub. Asserts:

- At least one `:assistant-text` event landed.
- Exactly one `:run-finished` event landed.
- The returned result map has `:stop-reason`, `:num-turns`, `:usage`.
- Same shape as the buffered-mode integration test that already exists at `claude_code_test.clj:34-54`.

Requires `ANTHROPIC_API_KEY` in env (skipped otherwise, like today's existing integration test).

### Manual / observational

After landing, re-run the in-flight stumpf-1445 trial with `:stream? true`. The recorder writes per-turn events into `out-cc/N/trace.edn`; inspect with `clojure -e "(map :phase (clojure.edn/read-string (slurp \"out-cc/N/trace.edn\")))"`.

## Verification

```clojure
(require '[podcast.cc :as cc] :reload)
(cc/extract! cc/local-cfg "stumpf.json" :slice [280 340])
;; While it runs:
;;   tail -f out-cc/N/trace.edn   ← :assistant-text / :tool-use / :tool-result events arrive live
;; After it finishes:
;;   - out-cc/N/sentiment.json byte-identical to a buffered-mode run on the same input
;;     (the data port hasn't moved)
```

Passing run shows:

- `out-cc/N/trace.edn` has many entries with `:phase :tool-use`, `:phase :tool-result`, `:phase :assistant-text`, and exactly one `:phase :run-started` and one `:phase :run-finished` per `cc/run!` call.
- `:tool-result` events for `Read` on paragraphs.txt show `:content-length` ≈ 50000 chars and a 500-char `:content-preview`.
- `out-cc/N/sentiment.json` is identical to the buffered-mode equivalent for the same `transcript-hash` (cache hit semantics unchanged — see Caching note below).

## Caching note

`podcast.cc/cached-cc-run!` (`cc.clj:107-119`) keys on `(call-tag, model, system, schema, allowed-tools, inputs-hash, prompt-text)`. **The cache key does NOT include `:stream?`** — flipping it on doesn't bust the cache, because the underlying agent work and final result are identical regardless of output format. Cache hits return the buffered final result, no streaming events emitted (same as today). Cache misses with `:stream? true` emit per-turn events live as the call runs.

Implication: a subsequent re-run that hits cache won't show streaming events. That's fine — there's nothing to stream when the work was already done.

## Critical files

- `/work/src/toolkit/datapotamus/claude_code.clj` — modify (add `:stream?`, three new private helpers, branch in `run!`, update docstring)
- `/work/src/toolkit/datapotamus/claude_code_test.clj` — add unit + slow integration tests
- `/work/src/podcast/cc.clj` — opt in to streaming on both cfgs (one line each)

No new files; no other modifications.
