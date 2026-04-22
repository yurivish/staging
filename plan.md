# Minimal edits from a toolkit audit

## Context — why this work exists

The user ran a fresh-eyes audit of the Clojure code in this repo (`/work`) —
a curated `toolkit` of small composable tools (~28 files, ~5013 LOC) plus a
demo webapp — after a burst of productive porting from Go.

Out of scope for the audit (do NOT touch): `toolkit.sqlite`,
`toolkit.datapotamus.*`, `toolkit.stree`, `toolkit.sublist`, and their tests
or bench files. Note that `toolkit.filewatcher` is a real consumer
(used by `datapotamus.daemon`) so it is in-scope if touched — but it isn't
touched here.

The audit surfaced six judgment-call items for the user. The user approved
**five of six**, and only three of those five require code changes. That is
the entire scope of this plan. Everything from the original broad plan
(README rewrites, demo/server cleanup, datastar/core trims, pubsub
docstring trims, new test files, "testing philosophy" header) is deferred
and out of scope.

## User preferences (load-bearing — apply to every edit)

From the user's memory and in-session feedback:

- **Sparse, minimal, idiomatic Clojure.** No scaffolding, no defensive
  branches for cases that can't happen, no speculative "for future use"
  fields, no verbose docstrings. A one-sentence docstring is fine; a
  three-paragraph block is not.
- **Minimal infrastructure.** Don't invent helpers or extract code unless
  it earns its keep; don't replicate files for symmetry.
- **Preserve multiplicity in assertions.** Use `frequencies` / vectors /
  counters when the spec says "exactly once" — never a set.
- **Stay in scope.** Do only what was asked. Ask before adding adjacent
  cleanups. A prior session over-ran this instruction and had its changes
  reverted; this plan exists because the user trimmed the work back to
  exactly the approved items.

## The five decisions

| # | Area | Decision | Code change? |
|---|------|----------|-------------|
| 1 | `demo/server.clj` `/stream` route + `stream-handler` | keep as-is | no |
| 2 | `demo/server.clj` `:sqlite` component dep | keep as-is | no |
| 3 | `toolkit.llm.anthropic/build-body` & `/parse-response` | keep public, add one-line docstrings | **yes** |
| 4 | `pubsub_property_test` `defspec` | delete, keep frng simulation | **yes** |
| 5 | `toolkit.llm/Request :messages` + `validate-rejects-empty-messages` | tighten schema to require `:min 1`; flip the test's assertion | **yes** |

## The three edits

### Edit A — `/work/src/toolkit/llm/anthropic.clj` — add two docstrings

Both functions are already public and tested; just add a single-line
docstring to each.

- `build-body` (at line ~35) — proposed: `"Translate a unified request to
  the Anthropic wire body."`
- `parse-response` (at line ~50) — proposed: `"Translate an Anthropic wire
  response to the unified response shape."`

Leave the function bodies untouched. Do NOT rename, privatize, or change
arities.

### Edit B — `/work/src/toolkit/pubsub_property_test.clj` — delete the defspec

Delete:

- The four `test.check` imports from `:require`: `clojure.test.check.clojure-test :refer [defspec]`, `clojure.test.check.generators :as gen`, `clojure.test.check.properties :as prop`.
- `def gen-op` (around line 16-20)
- `def gen-ops` (line 22)
- `defn- run-ops` (lines 24-49)
- `defn- valid-delivery?` (lines 51-68)
- `defspec delivery-set-valid` (lines 70-72)

Keep:

- The frng-driven simulation at lines 74+ (`frng-literal`, `frng-subject`,
  `frng-pattern`, `frng-queue`, `pubsub-sim`, `run-frng-sim!`,
  `deftest delivery-invariants-frng`).
- The other `:require`s (clojure.string, clojure.test, toolkit.frng,
  toolkit.pubsub, toolkit.sublist-property-test).

Update the namespace docstring to describe the frng-only shape (the current
docstring mentions "random interleavings of sub/unsub/pub ops" and
"Multiplicity-aware — `:fired` is a vector in call order" — those remain
true of the frng simulation and can stay or be trimmed; either is fine).

The frng simulation already covers the same invariants as the defspec and
strictly more (it runs longer interleavings, localizes failures per-pub,
and verifies unsubbed handlers stay silent). Removing the defspec is
principled, not a capability loss.

### Edit C — `/work/src/toolkit/llm.clj` + `llm_test.clj` — tighten `:messages`

In `/work/src/toolkit/llm.clj`, change the `Request` schema's `:messages`
entry from:

```clojure
   [:messages
    [:vector
     [:map
      [:role [:enum :user :assistant :tool]]
      ...
```

to:

```clojure
   [:messages
    [:vector {:min 1}
     [:map
      [:role [:enum :user :assistant :tool]]
      ...
```

In `/work/src/toolkit/llm_test.clj`, replace:

```clojure
(deftest validate-rejects-empty-messages
  ;; malli's :vector defaults allow []; the original spec required non-empty.
  ;; The current schema admits []; keep this test ready for when we tighten.
  (is (nil? (validate! {:model "m" :max-tokens 1 :messages []}))))
```

with:

```clojure
(deftest validate-rejects-empty-messages
  (is (thrown-with-msg?
       Exception #"invalid request"
       (validate! {:model "m" :max-tokens 1 :messages []}))))
```

The test's name always said "rejects" but its body asserted acceptance;
this closes the gap.

## Verification

```
clojure -X:test
```

Expected: all tests pass. Pay specific attention to `toolkit.llm-test`
(it's what exercises the schema change and the flipped assertion) and
`toolkit.pubsub-property-test` (should still run and pass — just without
the removed defspec).

Also make sure nothing else in the repo references the deleted defs.
Quick grep:

```
rg -n "gen-op|gen-ops|run-ops|valid-delivery\?|delivery-set-valid" src
```

Should show zero hits after Edit B.

## Out of scope (do not do)

- Do NOT edit `/work/README.md` or `/work/src/toolkit/README.md`.
- Do NOT touch `/work/src/demo/server.clj` (including `bg-color`, the
  langchain4j `#_(:import ...)`, the commented-out model def, the
  conventions comment block, or the `/stream` route).
- Do NOT touch `/work/src/toolkit/datastar/core.clj` (the unused
  `sse-response`/`sse-send!` re-exports stay; the perf-notes comment block
  stays; the duplicated `execute-script` docstring stays).
- Do NOT touch `/work/src/toolkit/datastar/sse.clj` docstrings.
- Do NOT touch `/work/src/toolkit/pubsub.clj` docstrings.
- Do NOT touch `/work/src/toolkit/filewatcher.clj` (`find-recent-hit` /
  `find-chunk-hit` stay as two functions).
- Do NOT touch `/work/src/toolkit/watcher.clj`.
- Do NOT delete the 16-line comment block in `/work/src/toolkit/llm.clj`
  (lines 74-89) — keep it.
- Do NOT delete `assistant-role-passes-through` in `llm_test.clj` — keep it.
- Do NOT add new test files.
- Do NOT add an error-path test for `llm/query`.

If during execution something else catches your eye, note it and ask;
don't just fix it.

## File paths in scope

- `/work/src/toolkit/llm/anthropic.clj`
- `/work/src/toolkit/llm.clj`
- `/work/src/toolkit/llm_test.clj`
- `/work/src/toolkit/pubsub_property_test.clj`
