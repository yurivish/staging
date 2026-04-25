# Conventions

## OS guard

The app expects a Linux container — lmdbjava, brotli4j, and parquet/hadoop
each pull in native libraries and JVM `--add-opens` flags — and we don't
want a JVM accidentally started on the macOS host to exercise the wrong
native-lib variant or pick up stray state. `src/toolkit/os_guard.clj` is a
top-level side effect that calls `System/exit 1` when `os.name` starts with
`Mac`.

Any new entry-point namespace (anything named by `-m` / `:exec-fn`, or
auto-loaded by an alias) must put `[toolkit.os-guard]` at the top of its
`:require` vector. That's the only way the guard runs before the entry
point's code does.

### Tests

The `:test` alias goes through `toolkit.test-runner/test`, which requires
`toolkit.os-guard` before delegating to `cognitect.test-runner.api/test`.
Don't point `:exec-fn` at `cognitect.test-runner.api/test` directly — that
bypasses the guard, because tests like `toolkit.frng-test` don't transitively
require any app namespace. If you add a new test-running alias (a focused
runner, a property-test alias, etc.), either route it through
`toolkit.test-runner/test` or add `[toolkit.os-guard]` to its exec-fn's
namespace.

## LLM calls

LLM calls go through langchain4j (already in `deps.edn`:
`dev.langchain4j/langchain4j` plus per-provider adapters). There is no
project-internal LLM client; do not reintroduce one.

Reference pattern: `/work/src/doublespeak/llm.clj` — Anthropic via
`AnthropicChatModel`, structured output via `JsonObjectSchema` inside a
`ToolSpecification`, per-model `:provider-extra` mapped to typed builder
methods (`thinkingType` / `thinkingBudgetTokens`) and `customParameters`
for raw passthrough. The `chat-tool!` helper there is the one-call
wrapper to copy. For plain text completion (no tools), `/work/src/hn/core.clj`
shows the inline 5-line pattern.
