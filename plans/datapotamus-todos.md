# Datapotamus TODOs

## Aggregator efficiency

I noticed that Claude just moved the aggregator to emit partial results with every input rather than waiting for on input done. I think we should still wait for on input done while collecting all inputs into a batch and then process that batch once, efficiently. We should explore this when we have things settled down at the end.

## Replace `::closed-ins` with something honestly-named (and possibly drop it entirely)

The framework's `run-input-done` tracks which input ports of a proc
have received an `:input-done` envelope in a state key called
`::closed-ins` (see `flow.clj`). The name is a leftover from when we
thought of `:input-done` as a "close" event. After the rename to
`:input-done` and the documentation update making clear that ports
are not closed by lifecycle signals, the name is misleading.

Two paths:

1. **Rename to `::input-dones-seen`** (or similar). Keep the data
   structure; just give it an accurate name. Cheap.
2. **Eliminate it.** The only uses of the set are:
   - Determining when all declared ports have been input-done'd, so
     `:on-all-closed` fires.
   - Making redundant input-dones idempotent.

   Both of these could be expressed via token-group balance instead
   (`c/fan-in`-style: declare a group at injection, wait for it to
   XOR-balance to zero before flushing). That's a bigger conceptual
   change but would reduce the framework's lifecycle vocabulary —
   `:on-all-closed` becomes "wait for this declared group to balance,"
   and `::closed-ins` goes away.

The narrow-rename version is a 10-minute change. The
group-balance-as-lifecycle version is a several-hour redesign that
also affects the three user-code aggregator call sites
(`hn_density`, `hn_typing`).

Also drop the comment in `combinators.clj`'s `c/recursive-pool`
docstring that references `::closed-ins` once this is settled.

## Replace `c/stealing-workers` with `c/recursive-pool`'s coordinator design

`c/recursive-pool` (commit `f4a10ac`) is the coordinator-owned
work-stealing implementation. Its non-recursive code path is a clean
drop-in replacement for the existing `c/stealing-workers` (which uses
the shared-q + K-input-done auto-append shape). Once we've validated
`c/recursive-pool` against more real pipelines (`hn_shape`,
`hn_density`, etc.), retire the shared-q implementation entirely and
fold its callers onto the new combinator.

Constraint: the existing `c/stealing-workers` accepts multi-proc step
inners; `c/recursive-pool` requires a handler-map. Either lift that
restriction (wrap the step's entry-point cleanly) or document it as
the new contract.
