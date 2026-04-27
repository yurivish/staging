# Conventions

## Design & Scope Discipline

- Prefer the smallest, least invasive change that solves the problem; do not bundle unrelated refactors.
- When the user pushes back on an approach, STOP and re-plan rather than continuing in the original direction.
- When asked to verify a claim or limitation, actually check the code before answering — never speculate about what a combinator/API can or can't do.

## Communication Style

- Keep explanations concise; do not produce wall-of-text design docs unless explicitly asked.
- For Clojure code, prefer Feynman/Hickey-style clarity: name things for what they mean, not how they're implemented.
- When writing runnable command examples in docstrings, avoid escaped quotes that break copy-paste.

## Testing

- Always run the full test suite after refactors and report the pass count (e.g., '337 tests passing').
- Use the `:slow` test marker (not `:costly`) for expensive tests.
- Never run tests that hit real LLM APIs without explicit confirmation; prefer mock/stub LLMs (e.g., fake Haiku stub) by default.

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

Two paths exist for calling LLMs. Pick by capability, not by preference.

**Default — langchain4j**: `dev.langchain4j/langchain4j` plus per-provider
adapters in `deps.edn`. Use this when the feature you need is supported
by the adapter — text/tool completion, prompt caching markers,
provider-specific knobs reachable via the typed builder methods or its
`customParameters` passthrough.

Reference pattern: `/work/src/doublespeak/llm.clj` — Anthropic via
`AnthropicChatModel`, structured output via `JsonObjectSchema` inside a
`ToolSpecification`, per-model `:provider-extra` mapped to typed builder
methods (`thinkingType` / `thinkingBudgetTokens`) and `customParameters`
for raw passthrough. The `chat-tool!` helper there is the one-call
wrapper to copy. For plain text completion (no tools),
`/work/src/hn/core.clj` shows the inline 5-line pattern.

**Fallback — `toolkit.llm` + provider adapters**: when langchain4j's
adapter doesn't expose the wire feature you need (e.g. the Anthropic
documents/citations API as of langchain4j 1.13), reach for
`/work/src/toolkit/llm.clj` (a small unified-request HTTP driver) and
its provider adapters under `/work/src/toolkit/llm/`. Don't write a new
one-off HTTP client — extend `toolkit.llm` (or the relevant adapter)
with the missing capability so the next caller benefits.

Both paths can share the response cache; see `toolkit.llm.cache` (LMDB-
backed, request → response).
