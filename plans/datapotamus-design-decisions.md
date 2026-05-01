# Datapotamus Design Decisions

Running log of non-obvious design choices in the Datapotamus
runtime — what we picked, what we rejected, and what's still open
for reconsideration. Each entry should answer: *why this and not
the alternative*, and *what would change our mind*.

---

## #1 — `c/recursive-pool`'s coordinator has two input ports (`:in` + `:rec`) instead of one

**Status:** provisional. Working as of commit `845bb0b`. Worth
reconsidering once we have time to think deeply about closure
semantics in the framework.

**Decision.** The coordinator proc declares two input ports:
- `:in` — fed only by the ext-wrap proc (external work entry).
- `:rec` — fed by all K wrapped workers' `:to-coord` outputs
  (recursive work + forwarded results, multiplexed and tagged with
  `::class :work | :result`).

**Why it's non-obvious.** From a *data-flow* perspective the split
buys nothing. Coord dispatches on `(::class msg)` — work goes to the
queue, result frees the worker — and that logic is identical
regardless of which port the message arrived on. We could equally
collapse to a single `:in` port (have ext-wrap also tag with
`::class :work`) and dispatch purely on the tag.

**What we'd lose.** The framework's `:on-all-closed` hook fires at
the moment `::closed-ins` equals the declared `:ins` set — it
tracks closure *per declared port*, not per channel/conn. With a
single port:

1. External upstream cascades input-done into `:in`.
2. `::closed-ins = #{:in}`, equal to all declared ins.
3. `:on-all-closed` fires *immediately* — auto-appends input-done
   on `:out` — downstream aggregators flush *while the recursive
   cycle is still mid-drain*. Incorrect.

With two ports, `:on-all-closed` waits for both `:in` (external
cascade) and `:rec` (worker feedback cascade after coord closes the
to-w-chans at terminal). The cascade downstream fires at the right
moment.

**Why this isn't necessarily fundamental.** Two cleaner paths
collapse to a single port:

1. **Suppress `:on-all-closed`'s default auto-append via
   `msg/drain`** when only the external port has closed, then
   manually emit `:input-done` on `:out` from the terminal branch in
   `:on-data`. Requires the framework's `coerce-data` to pass
   `:input-done` envelopes through synthesis cleanly — today it
   wraps non-pending msgs as children of the input-msg, mangling
   raw input-done envelopes. (Noted as an open gap in earlier
   discussion.)

2. **Per-conn closure tracking** in the framework instead of
   per-port. A single `:in` port fed by N conns could then
   distinguish "first conn closed" from "all conns closed."
   `:on-all-closed` would fire only when every conn has signaled.
   Larger framework change — `::closed-ins` becomes a richer data
   structure, and there are likely interactions with how
   `core.async.flow` allocates and reads chans for multi-producer
   ports.

**Why we picked two ports.** Path of least framework change. The
existing `::closed-ins`-per-port mechanism handles "wait for both
sides to exhaust" by construction once we declare two ports.
Nothing in the framework changes; the cost is one extra declared
port that's used purely as a closure sentinel, not for data
dispatch.

**What would change our mind.**

- If a future combinator wants a similar shape and we end up
  declaring even more "phantom" ports purely for `:on-all-closed`
  timing, the per-port mechanism is leaking and per-conn (or some
  other primitive) is worth introducing.
- If `coerce-data` is extended to pass through `:input-done`
  envelopes (small, justifiable change — see the related TODO
  about `::closed-ins`), the manual cascade option (#1 above)
  becomes attractive: single port, explicit `:input-done` emission
  at terminal, no special-case timing.
- If we discover that closing `:to-w` chans + cascading isn't the
  right shape for some richer recursive pattern (e.g., one that
  needs to keep some workers alive after others have terminated),
  the whole closure mechanism needs revisiting and this decision is
  moot.

**Related work / cross-references.**

- `plans/datapotamus-todos.md` — note about reconsidering
  `::closed-ins` (rename or eliminate via token-group balance).
- `c/recursive-pool` docstring in `combinators.clj` — describes
  the two-port shape; should be updated if we collapse later.
