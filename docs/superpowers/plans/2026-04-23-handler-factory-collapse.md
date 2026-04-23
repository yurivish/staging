# handler-factory Collapse Implementation Plan

**Goal:** Collapse `handler-factory` to a single 2-arg arity and remove `pure-fn-handler`, matching the concrete example in proposal A3.

**Scope:** Internal refactor of `/work/src/toolkit/datapotamus.clj`. No API change to `step`. All callers are in this one file.

**Out of scope:** Changing the handler arity from 3-arg to 4-arg (proposal prose mentions this, but the concrete example keeps 3-arg handlers). Revisit when `join-by-key` or another multi-input combinator actually needs in-port as a distinct param — and when that happens, extend `handler-factory` directly, do not add a `handler-factory*` sibling.

**Test command:** `clojure -X:test`

---

## Change surface

All hits in `/work/src/toolkit/datapotamus.clj`:

| Line | Form | Action |
|------|------|--------|
| 519–542 | `handler-factory` defn (2 arities) | Collapse to single 2-arg arity; nil ports ⇒ defaults |
| 544–548 | `pure-fn-handler` defn | Delete |
| 570 | `step` 2-arity: `(handler-factory (pure-fn-handler f))` | Inline pure-fn shape with `nil` ports |
| 578 | `sink`: 2-arg call | No change |
| 584 | `passthrough`: 1-arg call | Add explicit `nil` ports |
| 660 | `fan-out`: 1-arg call | Add explicit `nil` ports |
| 675 | `fan-in`: 1-arg call | Add explicit `nil` ports |
| 701 | `router`: 2-arg call | No change |
| 717 | `retry`: 1-arg call | Add explicit `nil` ports |
| 1073 | `collector-step`: 2-arg call | No change |

---

## Task 1: Collapse factory, delete pure-fn-handler, update callers

**File:** `/work/src/toolkit/datapotamus.clj`

- [ ] **Step 1: Replace `handler-factory` with a single 2-arg arity**

Replace lines 519–542 with:

```clojure
(defn- handler-factory
  "Lift a 3-arg message handler `(fn [ctx s d] -> [s' outputs])` into a
   full factory `(fn [ctx] step-fn)` that satisfies core.async.flow's
   4-arity proc-fn shape.

   `ports` is `{:ins {port-kw \"\"} :outs {port-kw \"\"}}`; pass `nil` for
   the default `{:in \"\"} / {:out \"\"}`.

   `d` is the input data (shorthand for `(:data (:msg ctx))`). The
   full envelope is on `(:msg ctx)` if needed (tokens, lineage, etc.).

   `outputs` is a seq of `[port msg-or-data]` pairs. A ledger is
   created per 4-arity invocation and attached to ctx; bare data in
   outputs is coerced via `child` during synthesis."
  [ports handler]
  (let [ins  (:ins  ports {:in  ""})
        outs (:outs ports {:out ""})]
    (fn [factory-ctx]
      (fn ([]             {:params {} :ins ins :outs outs})
        ([_]            {})
        ([s _]          s)
        ([s in-port m]
         (let [ledger   (new-ledger m)
               ctx      (assoc factory-ctx :in-port in-port :msg m :ledger ledger)
               [s' raw] (handler ctx s (:data m))]
           [s' (process-ledger! ctx raw)]))))))
```

- [ ] **Step 2: Delete `pure-fn-handler`**

Delete lines 544–548 (the entire `(defn- pure-fn-handler ...)` form, including its docstring and trailing blank line separator).

- [ ] **Step 3: Update `step`'s 2-arity to inline the pure-fn shape**

In `step` (currently line 556), replace the 2-arity body:

```clojure
  ([id f]
   (proc id (handler-factory nil (fn [_ s d] [s [[:out (f d)]]]))))
```

(The 3-arity stays as `(proc id (handler-factory ports handler))` — already correct.)

- [ ] **Step 4: Add explicit `nil` to the four 1-arg callers**

- `passthrough` (L584): `(handler-factory nil (fn [ctx s _d] [s [[:out (pass ctx)]]]))`
- `fan-out` (L660): `(handler-factory nil (fn [ctx s d] ...))`
- `fan-in` (L675): `(handler-factory nil (fn [ctx s _d] ...))`
- `retry` (L717): `(handler-factory nil (fn [_ctx s d] ...))`

- [ ] **Step 5: Run the test suite**

```
clojure -X:test
```

Expected: all existing datapotamus tests pass. No test code changes are required — this is a pure internal refactor, and `pure-fn-handler` / `handler-factory` are both private (`defn-`).

- [ ] **Step 6: Commit**

```
git add src/toolkit/datapotamus.clj
git commit -m "datapotamus: collapse handler-factory to one arity, inline pure-fn into step"
```

---

## Self-review

- Spec coverage: proposal's example is fully reproduced — step's 2-arity matches the proposal verbatim; `pure-fn-handler` is gone; handler-factory is a single arity.
- No placeholders.
- No type/signature drift: `handler-factory` still returns the same `(fn [ctx] step-fn)` shape; handler still gets `(ctx s data)`.
- Forward guidance for 4-arg extension is captured in the "Out of scope" note so it isn't lost.
