# Combinator Design Notes — Candidates for Future Work

Sketches and open questions for four combinators identified by comparing
datapotamus against n8n's flow-logic vocabulary. Not committed to
implementation — captured here so the open questions can be revisited
without losing context.

Existing combinators (for reference): `serial`, `merge-steps`, `connect`,
`passthrough`, `as-step`, `sink`, `fan-out`, `fan-in`, `router`, `retry`.

The four candidates:

1. **`for-each`** — map an inner step over a runtime-determined collection.
2. **`join-by-key`** — combine streams from multiple inputs by a data field
   (not by token lineage).
3. **`loop-until`** — package the existing back-edge pattern as a named
   combinator. (Cycles already work — see the `agent-loop` test.)
4. **`try-catch`** — turn exceptions into routable data on an `:err` port
   (Jepsen's `fcatch` lifted into the graph).

---

## Cross-cutting prereq: 4-arity handler form

`join-by-key` (and any future multi-input combinator) needs to know
**which port** a message arrived on. Today `handler-factory` ignores the
in-port:

```clojure
;; current — datapotamus.clj line 240
([s _ m] (handler ctx s m))
```

Cheap extension: a sibling factory that exposes the in-port to a 4-arg
handler. Existing 3-arg callers (`fan-out`, `router`, etc.) are
unaffected.

```clojure
(defn- handler-factory*
  "Like handler-factory but exposes the in-port to a 4-arg handler."
  [ports handler]
  (let [ins  (:ins  ports {:in  ""})
        outs (:outs ports {:out ""})]
    (fn [ctx]
      (fn ([]            {:params {} :ins ins :outs outs})
          ([_]           {})
          ([s _]         s)
          ([s in-port m] (handler ctx s in-port m))))))
```

---

## 1. `for-each` — map over a dynamic collection

**Problem.** `fan-out` requires a fixed N at construction time. To
process a vector of unknown length you currently write a custom step
that calls `tok/split-value` and emits children manually. This exact
pattern appears in `examples_test.clj` (the `named-port-fan-out-fan-in`
test) with a "promote to combinator later" comment.

**Sketch.** Two layers — a low-level `fan-out-coll` that emits one child
per element of `(coll-fn data)`, and a high-level `for-each` that wraps
an inner step between `fan-out-coll` and `fan-in`.

```clojure
(defn fan-out-coll
  "Emit one msg per element of (coll-fn data) on :out, bound by a fresh
   zero-sum token group. Pair with (fan-in group-id)."
  ([group-id]                    (fan-out-coll group-id group-id identity))
  ([group-id coll-fn]            (fan-out-coll group-id group-id coll-fn))
  ([id group-id coll-fn]
   (let [group-prefix (str (name group-id) "-")]
     (proc id
       (handler-factory
        (fn [_ctx s m]
          (let [items (vec (coll-fn (:data m)))
                n     (count items)]
            (if (zero? n)
              [s {::merges [{:msg-id (random-uuid) :parents [(:msg-id m)]}]}] ; see Q1
              (let [gid    (str group-prefix (:msg-id m))
                    values (tok/split-value 0 n)
                    kids   (mapv (fn [item v]
                                   (-> (child-with-data m item)
                                       (assoc :tokens (assoc (:tokens m) gid v))))
                                 items values)]
                [s {:out kids}])))))))))

(defn for-each
  "Apply inner-step to each element of (coll-fn data); emit one msg
   on :out whose :data is the vector of inner outputs."
  ([inner-step]               (for-each (gen-id :for-each) identity inner-step))
  ([id inner-step]            (for-each id identity inner-step))
  ([id coll-fn inner-step]
   (serial
    (fan-out-coll (keyword (str (name id) ".split")) id coll-fn)
    (as-step      (keyword (str (name id) ".body"))  inner-step)
    (fan-in       (keyword (str (name id) ".join"))  id))))
```

**Open questions.**

- **Q1 — empty collection.** `fan-in` only fires when XOR=0, but XOR of
  zero things never starts. If `(coll-fn data)` is empty: (a) synthesize
  an empty merge and emit `[]` immediately, (b) drop the message
  silently, or (c) treat as an error. (a) preserves "every input
  produces an output" which is useful for downstream join-style
  pipelines, but the merge has no body to attach to — would need a
  small `fan-in` extension that emits an empty-vector merge when seeded
  with N=0.
- **Q2 — ordering. DECIDED: unordered.** Merged results are in arrival
  order, not input/collection order. Tokens conserve cardinality (a
  multiset invariant), not ordinality — a deliberate design choice
  documented in the `fan-out`, `fan-in`, and `emit` docstrings. Pipelines
  that need positional semantics must encode position into the data
  payload before fan-out and reconstruct after fan-in. `for-each` will
  inherit the same property; its docstring should say so explicitly.
- **Q3 — id namespacing.** Sketch uses dotted-keyword suffixes
  (`:for-each.split`, etc.). Worth checking how this interacts with
  `instrument-flow`'s prefixing of subflows.

---

## 2. `join-by-key` — correlate streams by data field

**Problem.** `fan-in` joins by token-group lineage — sibling messages
from a shared `fan-out` ancestor. There's no way to merge two
independently-arriving streams by a key in the data (e.g. "match each
enrichment result to its original user by `user-id`"). Not currently
used in the test suite, but it's the only shape that's structurally
inexpressible today.

**Sketch.** Stateful, multi-input. Buffer messages per
`(key-fn data)` per input port. When all ports have arrived for a key,
emit one merged msg and drop the key from state.

```clojure
(defn join-by-key
  "Buffer inputs from N named ports, keyed by (key-fn data). When every
   port has a msg for some key k, emit one msg on :out whose :data is
   [(data-from-port-1) … (data-from-port-N)] in `ports` order, with
   merged tokens and lineage. No timeout — unmatched keys pile up
   forever (use Limit upstream if needed)."
  [id ports key-fn]
  (let [n (count ports)]
    (proc id
      (handler-factory*
       {:ins  (zipmap ports (repeat ""))
        :outs {:out ""}}
       (fn [_ctx s in-port m]
         (let [k    (key-fn (:data m))
               s'   (assoc-in s [:pending k in-port] m)
               row  (get-in s' [:pending k])]
           (if (= n (count row))
             (let [msgs    (mapv #(get row %) ports)
                   parents (mapv :msg-id msgs)
                   data    (mapv :data msgs)
                   tokens  (reduce tok/merge-tokens {} (map :tokens msgs))
                   mid     (random-uuid)
                   out-msg {:msg-id (random-uuid)
                            :data-id (random-uuid)
                            :data data
                            :tokens tokens
                            :parent-msg-ids [mid]}]
               [(update s' :pending dissoc k)
                {:out [out-msg]
                 ::merges [{:msg-id mid :parents parents}]}])
             [s' {}])))))))
```

**Open questions.**

- **Q4 — needs the 4-arity handler form** described in the prereq above.
- **Q5 — unbounded buffer.** Honest defect: if one port never produces a
  matching key, that key's partial row sits in state forever. n8n
  explicitly documents the same property ("items can be kept
  unpaired"). Two future escapes: (a) a `:timeout` arity that drops
  stale rows, (b) emit an "incomplete join" event to pubsub so
  observers can detect it. Probably ship without these and document the
  leak.
- **Q6 — token semantics.** When messages from two upstream `fan-out`s
  meet at a `join-by-key`, the merged tokens carry both groups.
  Downstream `fan-in`s on either group should still close correctly.
  Worth verifying with a test.

---

## 3. `loop-until` — package the back-edge pattern

**Problem.** Cycles already work (the `agent-loop` test does this
manually with `connect`). The combinator just gives the pattern a name
and shields users from wiring back-edges by hand.

**Sketch.** Body step → router that dispatches `:done`/`:again` →
connect `:again` back to body's input.

```clojure
(defn loop-until
  "Apply body-step repeatedly. After each iteration, (pred? data)
   decides: true → emit on :out and exit; false → feed back into body.
   Termination is the caller's responsibility."
  ([body-step pred?]    (loop-until (gen-id :loop) body-step pred?))
  ([id body-step pred?]
   (let [body-id (keyword (str (name id) ".body"))
         test-id (keyword (str (name id) ".test"))]
     (-> (merge-steps
          (as-step body-id body-step)
          (router test-id [:done :again]
                  (fn [d] [{:data d :port (if (pred? d) :done :again)}])))
         (connect [body-id :out] test-id)
         (connect [test-id :again] body-id)
         (input-at  body-id)
         (output-at [test-id :done])))))
```

**Open questions.**

- **Q7 — trace explosion.** Each iteration produces fresh msg-ids; long
  loops create long lineage chains. Acceptable since that's truthful,
  but worth noting in the docstring.
- **Q8 — token accumulation across iterations.** Each iteration's
  router-split splits tokens N=1 (passthrough). XOR-zero-sum still
  holds, but a token group that fans-out *inside* the body would need
  to fan-in *inside* the body too (can't escape across the loop
  boundary cleanly). Worth a docstring warning.
- **Q9 — backpressure / channel sizing.** Cycles in core.async.flow are
  workable but can deadlock if buffers fill. The `agent-loop` test
  passes; haven't stress-tested it. May need to surface a buffer-size
  knob later.

---

## 4. `try-catch` — exceptions as routable data

**Problem.** Today an exception inside a step becomes a `:failure` trace
event and the message is dropped (`wrap-proc`, datapotamus.clj lines
267-269). It's observable but not routable. Jepsen's `fcatch` pattern —
convert thrown exceptions into return values — fits naturally onto a
graph: send the exception out a different port.

```clojure
;; Reference: Jepsen's fcatch
(defn fcatch
  [f]
  (fn wrapper [& args]
    (try (apply f args)
         (catch Exception e e))))
```

**Sketch.** Two ports: `:out` for success, `:err` for failure.

```clojure
(defn try-catch
  "Wrap (data -> data) f. On success, emit (f data) on :out.
   On exception, emit {:error <ex> :data <input-data>} on :err.
   The exception is *not* rethrown and does NOT produce a :failure event."
  [id f]
  (proc id
    (handler-factory
     {:ins {:in ""} :outs {:out "" :err ""}}
     (fn [_ctx s m]
       (let [r (try {:ok (f (:data m))}
                    (catch Throwable t {:err t}))]
         (if (contains? r :ok)
           [s {:out [(child-with-data m (:ok r))]}]
           [s {:err [(child-with-data m {:error (:err r)
                                         :data  (:data m)})]}]))))))
```

**Open questions.**

- **Q10 — pure-fn-only vs. step-wrapping.** This sketch wraps a
  `(data -> data)` fn, mirroring `retry`. A more ambitious version
  would wrap an entire inner step and route its `:failure` events
  to `:err`. That requires either suppressing `wrap-proc`'s catch or
  subscribing to the inner step's pubsub — both are invasive.
  Recommend shipping the pure-fn version first and revisiting if a
  real use case appears.
- **Q11 — relationship to `retry`.** `retry` is "swallow N times then
  propagate"; `try-catch` is "redirect on failure, never propagate".
  Composable: `(try-catch :foo (fn [d] ((retry-fn 3 f) d)))`. Worth
  showing in a docstring example.
- **Q12 — error data shape.** `{:error <ex> :data <input>}` is a guess.
  Could also be `{:exception ... :input ...}` or just emit the
  throwable directly. Pick a convention and stick to it; matters for
  downstream pattern-matching.

---

## Deliberately excluded

Three things from the n8n comparison that don't earn a combinator:

- **Wait / time delays** — better as a regular step that parks on
  `(:cancel ctx)`.
- **Limit / take-N** — a five-line stateful step.
- **Merge by position / cartesian product** — niche and code-smell-adjacent.

---

## Decisions to make before implementing

The 12 questions above, but the load-bearing ones:

- **Q1** (empty-collection semantics for `for-each`) — pick a convention.
- ~~**Q2** (collection-order vs arrival-order for `for-each`)~~ — **decided**
  above: unordered, documented in docstrings.
- **Q5** (unbounded buffer for `join-by-key`) — accept and document, or
  add timeout.
- **Q10** (pure-fn vs step-wrapping for `try-catch`) — scope decision.
- **Q12** (error data shape for `try-catch`) — pick a convention.

---

## Pending: `fan-in` behavior on `done` receipt

`done` messages (dataless, tokenless port-closure signals) now cascade
through `wrap-proc` automatically — user handlers are bypassed, and done
emerges on all declared output ports once all declared input ports have
closed. That's the foundation; `fan-in` still needs explicit integration
to decide what happens to **pending groups** when its input port closes
with the XOR not yet at zero.

Three candidates, to be picked when `fan-in` integrates `done`:

- **(a) Partial merge.** Emit each pending group as a merge msg tagged
  `:partial? true` on both the merge event and the data message. Data-
  preserving, additive to existing merge semantics, consumers who don't
  care see no diff.
- **(b) Drop silently.** Simplest; loses information about groups that
  never closed. Easy to mis-diagnose.
- **(c) Failure event only.** Emit `:incomplete-group` on pubsub so an
  observer can see it, but drop the group from the data graph. Observable
  but not routable.

**Current lean**: (a). The project doesn't have a first-class error-
routing convention yet (`fcatch` handles exceptions-as-data but not
structural "group never closed" errors), and partial merges let the
consumer decide. Belongs with the `fan-in` update, not the `done`
foundation.

Other deferred `done` work:

- **Upstream done propagation** (back-pressure: sink says "done, stop
  sending" to sources). Not needed for current use cases.
- **done-aware user handlers.** Currently `wrap-proc` absorbs done
  without calling the handler. When finalization hooks are needed
  (flushing, closing resources), add an opt-in convention on step
  factories.

---

## Nesting fan-out/fan-in — resolved

An earlier open question was whether nested fan-out/fan-in composes
correctly. **It does, post-fix** — and the "restriction" turned out to
be a latent bug in `fan-out`, not a fundamental limitation.

**The bug**: `fan-out` used to duplicate the incoming message's tokens
across all N children (`(:tokens m)` was copied whole per child, with
only the new group's slice assoc'd on top). For a child flowing from a
nested topology — where its parent already carried an upstream group
token — the outer group's token value was therefore duplicated across
the children. Pairs of children from the same outer branch XORed to
zero prematurely at the first downstream fan-in that matched, and the
outer fan-in's group accumulated `0`s (producing N single-msg merges
instead of 1 N-msg merge).

**The fix**: `fan-out` now uses `tok/split-tokens` on the parent's
tokens before adding the new group's zero-sum slice — same shape as
`emit`'s earlier fix. Every upstream zero-sum group's total XOR is now
conserved across children, not inflated, so nested fan-out/fan-in pairs
compose correctly. The algebra always supported this; the implementation
now matches.

**Data-shape note that remains**: even with the fix, *interleaving*
fan-out/fan-in pairs (e.g. `FO1 → FO2 → FI1 → FI2` where the pairs
don't nest) produces algebraically correct tokens but merged data
vectors whose nesting reflects fire-order at each fan-in rather than
user structure. Well-nesting is the recommended shape for predictable
`[[a b] [c d]]`-style results. This is captured in the `fan-out`
docstring.

The `nested-fan-out-fan-in-composes` test in `examples_test.clj`
exercises the nesting case end-to-end and pins the invariant: outer
fan-in fires exactly once with three parents.
