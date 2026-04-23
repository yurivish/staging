# A4. `await-quiescent!` promise-driven completion

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 10 ms busy-poll + O(|events|) error scan in `await-quiescent!` with a single "done" promise delivered from exactly two places.

**Architecture:** Rename `::quiescent-p` → `::done-p` so the same atom-of-promise can carry either `:quiescent` (from the `main-sub` subscriber when counters balance) or `[:failed err]` (from the existing `err-done` `a/io-thread` when a message drains off `error-chan`). `await-quiescent!` collapses to a single `deref` with optional timeout. The reset-on-inject pattern is preserved verbatim.

**Tech Stack:** Clojure 1.12, core.async(.flow), JDK virtual threads via `a/io-thread`. No new concurrency primitives.

---

## File structure

Only one source file is modified; the test file gets one new `deftest`.

- Modify: `/work/src/toolkit/datapotamus.clj`
  - `start!` (L903–963): rename the promise atom, deliver from the io-thread on error.
  - `inject!` (L965–1008): rename the destructured key and the `swap!`.
  - `await-quiescent!` (L1025–1042): replace the `loop`/`find-err`/10 ms deref with a single `deref`.
- Modify: `/work/src/toolkit/datapotamus_test.clj`
  - Add one `deftest` proving `await-quiescent!` returns `[:failed err]` promptly when a handler throws.

Callers (`run!` L1059, `run-seq` L1104) already branch on `(= :quiescent signal)` / `(vector? signal)`; the new return shape is identical, so no caller changes are needed.

---

## Task 1: Failing test — `await-quiescent!` reports flow errors via the done-promise

**Why first:** The error path (`:flow-error` → done-p) is new wiring. Writing the test first proves the refactor is load-bearing; without it the test suite wouldn't distinguish busy-polled error discovery from promise delivery.

**Files:**
- Modify: `/work/src/toolkit/datapotamus_test.clj` (append at end of file)

- [ ] **Step 1: Add the failing test**

Append to `/work/src/toolkit/datapotamus_test.clj`:

```clojure
(deftest await-quiescent-returns-failed-on-flow-error
  (let [bad (dp/step :bad (fn [_d] (throw (ex-info "boom" {:cause :intentional}))))
        h   (dp/start! (dp/serial bad (dp/sink)))]
    (try
      (dp/inject! h {:data 1})
      (let [signal (dp/await-quiescent! h 5000)]
        (testing "signal is [:failed err], not :timeout"
          (is (vector? signal))
          (is (= :failed (first signal)))
          (is (= "boom" (-> signal second :message)))
          (is (= {:cause :intentional} (-> signal second :data)))))
      (finally (dp/stop! h)))))
```

- [ ] **Step 2: Run it; confirm it passes OR times out in a way that proves the refactor is needed**

Run: `clojure -X:test :nses '[toolkit.datapotamus-test]' :vars '[toolkit.datapotamus-test/await-quiescent-returns-failed-on-flow-error]'`

Two acceptable outcomes here:
1. Test **passes** — means today's `find-err` scan already catches the error. That's fine; the test still serves as a regression guard for the new wiring.
2. Test **times out / fails** — confirms the current path is brittle and the refactor is necessary.

Do NOT commit yet. If the test passes, note that the old code already covered this via linear scan; the plan's value is the polling removal, not a brand-new capability.

---

## Task 2: Refactor `start!`, `inject!`, `await-quiescent!`

**Files:**
- Modify: `/work/src/toolkit/datapotamus.clj`

- [ ] **Step 1: Replace the promise-atom binding in `start!`**

In `start!`, at L923, change:

```clojure
quiescent-p  (atom (promise))
```

to:

```clojure
done-p       (atom (promise))
```

- [ ] **Step 2: Update the quiescence subscriber in `start!`**

In `start!`, at L924–929, change:

```clojure
main-sub     (pubsub/sub raw-ps (scope->glob scope)
                         (fn [_ ev _]
                           (swap! events conj ev)
                           (let [c' (swap! counters update-counters ev)]
                             (when (balanced? c')
                               (deliver @quiescent-p :quiescent)))))
```

to:

```clojure
main-sub     (pubsub/sub raw-ps (scope->glob scope)
                         (fn [_ ev _]
                           (swap! events conj ev)
                           (let [c' (swap! counters update-counters ev)]
                             (when (balanced? c')
                               (deliver @done-p :quiescent)))))
```

- [ ] **Step 3: Deliver `[:failed err]` from the `err-done` io-thread in `start!`**

In `start!`, at L933–947, change:

```clojure
err-done     (a/io-thread
               (loop []
                 (when-let [m (a/<!! error-chan)]
                   (let [ex (:clojure.core.async.flow/ex m)]
                     (pubsub/pub raw-ps
                                 (run-subject-for scope :flow-error)
                                 {:kind      :flow-error
                                  :pid       (:clojure.core.async.flow/pid m)
                                  :cid       (:clojure.core.async.flow/cid m)
                                  :msg-id    (get-in m [:clojure.core.async.flow/msg :msg-id])
                                  :error     {:message (ex-message ex) :data (ex-data ex)}
                                  :scope     scope
                                  :flow-path [fid]
                                  :at        (now)}))
                   (recur))))
```

to:

```clojure
err-done     (a/io-thread
               (loop []
                 (when-let [m (a/<!! error-chan)]
                   (let [ex  (:clojure.core.async.flow/ex m)
                         err {:message (ex-message ex) :data (ex-data ex)}]
                     (pubsub/pub raw-ps
                                 (run-subject-for scope :flow-error)
                                 {:kind      :flow-error
                                  :pid       (:clojure.core.async.flow/pid m)
                                  :cid       (:clojure.core.async.flow/cid m)
                                  :msg-id    (get-in m [:clojure.core.async.flow/msg :msg-id])
                                  :error     err
                                  :scope     scope
                                  :flow-path [fid]
                                  :at        (now)})
                     (deliver @done-p [:failed err]))
                   (recur))))
```

- [ ] **Step 4: Rename the handle key in `start!`**

In `start!`, at L959, change:

```clojure
::quiescent-p quiescent-p
```

to:

```clojure
::done-p     done-p
```

- [ ] **Step 5: Update `inject!` destructuring and reset**

In `inject!`, at L968, change:

```clojure
(let [{::keys [step graph pubsub scope fid counters quiescent-p port-index]} handle
```

to:

```clojure
(let [{::keys [step graph pubsub scope fid counters done-p port-index]} handle
```

And at L998, change:

```clojure
(swap! quiescent-p (fn [p] (if (realized? p) (promise) p)))
```

to:

```clojure
(swap! done-p (fn [p] (if (realized? p) (promise) p)))
```

- [ ] **Step 6: Simplify `await-quiescent!`**

Replace the entire `await-quiescent!` (L1025–1042) with:

```clojure
(defn await-quiescent!
  "Block until quiescence or error. Returns :quiescent, [:failed err], or :timeout."
  ([handle] (await-quiescent! handle nil))
  ([handle timeout-ms]
   (let [p @(::done-p handle)]
     (if timeout-ms
       (deref p timeout-ms :timeout)
       @p))))
```

- [ ] **Step 7: Verify nothing else references the old key**

Run: `rg -n 'quiescent-p|::quiescent-p' /work/src/toolkit/datapotamus.clj /work/src/toolkit/datapotamus_test.clj`

Expected: no matches. If any remain, rename them to `done-p` / `::done-p`.

- [ ] **Step 8: Run the error-path test from Task 1**

Run: `clojure -X:test :nses '[toolkit.datapotamus-test]' :vars '[toolkit.datapotamus-test/await-quiescent-returns-failed-on-flow-error]'`

Expected: PASS within well under 5000 ms (ideally sub-100 ms — no more 10 ms polling).

- [ ] **Step 9: Run the full datapotamus test suite**

Run: `clojure -X:test :nses '[toolkit.datapotamus-test]'`

Expected: all previously-passing tests still pass. `run!` and `run-seq` should be unaffected since they dispatch on `(= :quiescent signal)` / `(vector? signal)` — both shapes are unchanged.

- [ ] **Step 10: Commit**

```bash
git add src/toolkit/datapotamus.clj src/toolkit/datapotamus_test.clj
git commit -m "$(cat <<'EOF'
replace await-quiescent! busy-poll with a done-promise

Previously await-quiescent! polled @quiescent-p every 10ms and linearly
rescanned @events for :flow-error on each iteration. The promise atom is
now ::done-p, delivered from exactly two places: the main-sub quiescence
subscriber (:quiescent) and the err-done io-thread ([:failed err]).
await-quiescent! becomes a single deref. No new concurrency primitives.
EOF
)"
```

---

## Self-review notes

- **Spec coverage:** The prompt calls for (a) removing the poll, (b) making error detection O(1), (c) delivering from exactly the two sites named (quiescence subscriber + error-chan consumer), (d) no new concurrency primitives. Task 2 steps 2, 3, and 6 cover these respectively; the existing `a/io-thread` is reused per the virtual-thread preference.
- **Caller compatibility:** `run!` (L1064) and `run-seq` (L1121) consume the return value; both already handle `:quiescent` and `[:failed err]`. The new `:timeout` value only appears when the caller passes `timeout-ms`, and neither caller does.
- **Reset semantics:** The atom-of-promise + `(if (realized? p) (promise) p)` reset in `inject!` is preserved; multi-inject patterns (including `run-seq`'s doseq) continue to work.
- **Race:** Both delivery sites race into one promise, and `deliver` is idempotent — whichever signal arrives first wins, matching the old "first error OR first balanced counters" semantics. A post-quiescence error still lands in `@events` and is observable via `stop!`, matching today's behavior.
