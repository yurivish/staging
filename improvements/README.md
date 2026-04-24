# Improvements — menu

> **Status: applied.** All 9 items below have been landed on the working tree. Baseline tests (317) and the 12 new tests added for this pass all continue to pass (329 tests, 919,165 assertions, 0 failures). See the "Tests added" section at the bottom.

A pruned list of **mechanical substitutions** across the Clojure codebase — pure idiom swaps where the new form is shorter, uses a named stdlib operation, and behaves identically (or identically under a condition you can verify in seconds). Each item below is written as a **standalone prompt** that you can paste into a new LLM session with no further context.

Filtered against:

- **No code deletion.** What looks dead may be load-bearing.
- **No documentation changes.** Docstrings and comments left alone.
- **No judgment calls.** If the win depends on taste (naming, helper granularity, where-to-put-it), it’s out.
- **`io-thread` is already the virtual-thread affordance.** Not a target.

What remains is a set of substitutions that are uniformly good — no "probably better" qualifiers, no "if you prefer X style" caveats. Apply any in any order. Each is independent.

## Menu

| # | Where | What changes | Sites |
|---|---|---|---|
| 1 | `toolkit/zorder.clj` | `(bit-shift-left 1 (- 63 (Long/numberOfLeadingZeros diff)))` → `(Long/highestOneBit diff)` | 1 |
| 2 | `toolkit/spline.clj` | `(Math/sqrt (+ (* dx dx) (* dy dy)))` → `(Math/hypot dx dy)` | 2 |
| 3 | `toolkit/{sublist,frng,datapotamus/viz}.clj` | Drop the redundant `0` seed in `(reduce + 0 …)` | 4 |
| 4 | `toolkit/stree.clj` | `(nth parts (dec (count parts)))` → `(peek parts)` | 4 |
| 5 | `toolkit/datapotamus/flow.clj` | `(filter #(not (contains? outs %)) …)` → `(remove #(contains? outs %) …)` | 1 |
| 6 | `toolkit/hist.clj` | `(first (first bs))` → `(ffirst bs)` | 1 |
| 7 | `toolkit/sublist.clj` | Hoist `(or grp [])` out of the body using `fnil` | 1 |
| 8 | `toolkit/{stree,sublist}.clj` | `(zero? (count x))` / `(zero? (.length s))` / `(or (nil? s) (zero? (.length s)))` → `(empty? x)` | 6 |
| 9 | `toolkit/datastar/sse.clj` | `(into #{} coll)` → `(set coll)` (no transducer involved) | 1 |

Total: **9 items across 21 sites.** Longer reads below. Per-package files (`toolkit_*.md`) repeat each item in context so you can work a single package at a time.

---

## 1. `Long/highestOneBit` in zorder.clj:120

In `/work/src/toolkit/zorder.clj`, inside the function `litmax-bigmin-2d`, line 120 currently reads:

```clojure
diff-msb (bit-shift-left 1 (unchecked-subtract 63 (Long/numberOfLeadingZeros diff)))
```

Replace with:

```clojure
diff-msb (Long/highestOneBit diff)
```

These compute the same value when `diff > 0`. `Long/highestOneBit` is the JDK’s named primitive for "isolate the most significant bit" (it returns a `long` with only the MSB of the input set); the current form reconstructs the same value from `numberOfLeadingZeros` and a shift, which forces the reader to recognize the pattern.

**Verified safe.** Line 114 early-exits with `(= min max) [min max]` before reaching line 120, and the swap at line 116 gives `min < max` unsigned thereafter. `diff = (bit-xor min max)` at line 119 is therefore always positive by the time line 120 runs. `Long/highestOneBit` and the current form agree on every positive long. No edge case.

## 2. `Math/hypot` in spline.clj (2 sites)

In `/work/src/toolkit/spline.clj`:

- **line 58**, inside `subdivisions`:

  ```clojure
  dd  (Math/sqrt (+ (* dx dx) (* dy dy)))
  ```

- **line 109**, inside the private helper `dist`:

  ```clojure
  (Math/sqrt (+ (* dx dx) (* dy dy)))
  ```

Both become:

```clojure
(Math/hypot dx dy)
```

(Adjust the local-binding name at line 58 so the single expression replaces `(Math/sqrt (+ …))`.)

`Math/hypot(x, y)` is the JDK primitive for `sqrt(x² + y²)` and is numerically more stable (it avoids intermediate overflow/underflow by scaling the larger operand). For typical 2D-point distances in this file the double results will be identical; the substitution is pure in practice.

Leave `spline.clj:160` alone — that’s `(Math/sqrt (double %))` on a single value, not sum-of-squares.

## 3. Drop the `0` seed in `(reduce + 0 …)` (4 sites)

In four places the code writes `(reduce + 0 coll)` where `coll` is always numeric. Remove the `0`:

- `/work/src/toolkit/sublist.clj:214` — `(reduce + 0 (map count (vals (:qsubs node))))`
- `/work/src/toolkit/sublist.clj:220` — `(reduce + 0 (map count-tree (vals (:children node))))`
- `/work/src/toolkit/frng.clj:122` — `total (reduce + 0 (map val entries))`
- `/work/src/toolkit/datapotamus/viz.clj:259` — `(reduce + 0 (map #(aggregate-queue % queues) (:children node)))`

Rewrite each as `(reduce + …)` without the `0`. `reduce` with no seed on an empty collection returns `(+)`, which for `+` is `0`; `reduce` with an explicit `0` seed on an empty collection also returns `0`. On a non-empty collection both forms produce the sum. The `0` is the identity element of `+` and is already implied.

Purely identical behavior. Apply without further verification.

## 4. `peek` over `(nth parts (dec (count parts)))` in stree.clj (4 sites)

In `/work/src/toolkit/stree.clj`, the vector `parts` (constructed by `gen-parts` via `(loop [… parts []] … (conj parts …))` at lines 310–358 — so always a vector) is repeatedly accessed at its last index:

- **line 434** in `has-fwc?`:

  ```clojure
  (let [n (count parts)]
    (and (pos? n)
         (let [^String last-p (nth parts (dec n))]
           …)))
  ```

- **line 460** in `do-match`:

  ```clojure
  (let [^String lp (nth parts (dec (count parts)))]
    …)
  ```

- **line 463** in `do-match`:

  ```clojure
  [(nth parts (dec (count parts)))]
  ```

- **line 482** in `do-match`:

  ```clojure
  (let [nparts' [(nth parts (dec (count parts)))] …] …)
  ```

Replace each `(nth parts (dec (count parts)))` with `(peek parts)`. Keep every surrounding structure intact — in particular at line 434, keep the outer `(let [n (count parts)] (and (pos? n) …))` guard. Don’t attempt to collapse it using `when-let`: the shape change from `false` to `nil` on the empty case is falsy-equivalent but produces a different value, and that crosses into judgment.

`peek` on a vector returns the last element in O(1) (or `nil` on an empty vector). The arithmetic `(dec (count parts))` is the index form of the same operation; `peek` names it. The call sites at 460, 463, 482 are always reached with non-empty `parts` (each is inside a `match-parts` consumer that has already read elements), so the `nil`-on-empty behavior of `peek` is irrelevant there.

**Verify first:** grep `parts` in the file and confirm every construction is via `conj` on an initial `[]`. `gen-parts` is the sole producer; its loop-accumulator seed at line 315 is `[]`. Safe.

**Important:** do **not** remove the `(pos? (count parts))` guard at line 459 as part of this change. It’s load-bearing for the `(.length lp)` call on the peek result at that site — although `(peek [])` is `nil`, `(.length nil)` throws. The guard stays; only the inner `(nth parts (dec (count parts)))` becomes `(peek parts)`.

## 5. `remove` over `(filter #(not …))` in datapotamus/flow.clj:41

In `/work/src/toolkit/datapotamus/flow.clj`, inside `validate-ports!`, line 41:

```clojure
(let [unknown (seq (filter #(not (contains? outs %)) (keys port-map)))]
  …)
```

Replace the `filter` expression with `remove`:

```clojure
(let [unknown (seq (remove #(contains? outs %) (keys port-map)))]
  …)
```

`(remove pred coll)` is defined exactly as `(filter (complement pred) coll)` on any seqable collection; the transformation is a named stdlib equivalent. No edge cases.

## 6. `ffirst` in hist.clj:191

In `/work/src/toolkit/hist.clj`, inside `summary`, line 191:

```clojure
first-idx (first (first bs))
```

Replace with:

```clojure
first-idx (ffirst bs)
```

`(ffirst x)` is literally `(first (first x))`. Identical behavior. While you’re there, line 192 also reads `last-idx (first (last bs))` — that one is **not** a candidate for reduction (no shorthand for `first-of-last`); leave it.

## 7. Hoist `(or grp [])` with `fnil` in sublist.clj:85-93

In `/work/src/toolkit/sublist.clj`, inside `add-to-bucket`:

```clojure
(defn- add-to-bucket [bucket q v]
  (if q
    (update-in bucket [:qsubs q]
               (fn [grp]
                 (let [grp (or grp [])]
                   (if (some #(= v %) grp)
                     grp
                     (conj grp v)))))
    (update bucket :psubs conj v)))
```

Rewrite the `(fn [grp] …)` to lift the nil-default out via `fnil`:

```clojure
(defn- add-to-bucket [bucket q v]
  (if q
    (update-in bucket [:qsubs q]
               (fnil (fn [grp]
                       (if (some #(= v %) grp) grp (conj grp v)))
                     []))
    (update bucket :psubs conj v)))
```

`fnil` wraps the function so that a `nil` input is replaced by `[]` before the function sees it. The behavior is the same as the inner `(let [grp (or grp [])] …)`. The rewrite names the pattern ("nil-default at the boundary") and removes one `let` binding from the body.

No semantic change. No judgment.

## 8. `(empty? x)` over `(zero? (count/.length x))` (6 sites)

`(zero? (count x))` on a counted collection and `(zero? (.length s))` on a string are both equivalent to `(empty? x)`, which is the named predicate for "is this collection empty?". Replace at:

- **`/work/src/toolkit/stree.clj:286`** in `delete!`, combined with a nil guard:

  ```clojure
  (if (or (nil? subject) (zero? (.length subject))) …)
  ```

  →

  ```clojure
  (if (empty? subject) …)
  ```

  This also absorbs the `(nil? subject)` branch: `(empty? nil)` is `true` (since `(seq nil)` is `nil`), so the nil case is covered. Identical truth table.

- **`/work/src/toolkit/stree.clj:448`** in `do-match`:

  ```clojure
  (when (or (zero? (count nparts)) (and hasfwc? (= 1 (count nparts)))) …)
  ```

  →

  ```clojure
  (when (or (empty? nparts) (and hasfwc? (= 1 (count nparts)))) …)
  ```

  `nparts` is always a vector (see item 4 notes); `empty?` is O(1) for vectors.

- **`/work/src/toolkit/stree.clj:458`** in `do-match`:

  ```clojure
  (and (zero? (count nparts)) (not hasfwc?))
  ```

  →

  ```clojure
  (and (empty? nparts) (not hasfwc?))
  ```

- **`/work/src/toolkit/stree.clj:471`** in `do-match`:

  ```clojure
  (zero? (.length s))
  ```

  → `(empty? s)` (`s` is a `^String`; `empty?` works on `CharSequence`).

- **`/work/src/toolkit/stree.clj:481`** in `do-match`:

  ```clojure
  (and hasfwc? (zero? (count nparts)))
  ```

  →

  ```clojure
  (and hasfwc? (empty? nparts))
  ```

- **`/work/src/toolkit/sublist.clj:486`** in `walk-intersect`:

  ```clojure
  (let [prefix (if (zero? (count subj)) subj (str subj "."))]
    …)
  ```

  →

  ```clojure
  (let [prefix (if (empty? subj) subj (str subj "."))]
    …)
  ```

  `subj` is a `^String`; `empty?` is O(1) for strings.

(Six sites total. Feel free to apply by function if that’s easier — `delete!` owns 286; `do-match` owns 448/458/471/481; `walk-intersect` in `sublist.clj` owns 486.)

**Do not** extend this to the `(pos? (count x))` / `(pos? (.length s))` sites elsewhere in these files: those are used in boolean contexts where the natural complement is `(seq x)`, which returns a seq or nil rather than a boolean. That conversion is fine in pure truthy/falsy contexts but crosses into judgment territory when the result is bound. Leave them alone for this pass.

## 9. `(set coll)` over `(into #{} coll)` in datastar/sse.clj:154

In `/work/src/toolkit/datastar/sse.clj`, inside `content-encoding`, line 154:

```clojure
first-match (some (into #{} encodings) ranked-prefs)
```

Replace with:

```clojure
first-match (some (set encodings) ranked-prefs)
```

`(set coll)` and `(into #{} coll)` produce the same result when no transducer is involved. `(set nil)` and `(into #{} nil)` both return `#{}`. Pure substitution.

**Do not** apply this to the other `(into #{} …)` sites in the codebase — `sqlite.clj:60`, `datapotamus/msg.clj:156`, `datapotamus/msg.clj:211` all use **transducers** (a second arg like `(map …)` or `(mapcat …)` before the collection), and `set` does not accept a transducer. Leave those as `into #{}`.

---

## Notes on items that were *not* kept

The earlier version of this directory contained per-package lists with ~100 items, scored by `importance × confidence / locality`. After the user’s feedback that only "uniformly good, no-judgment" changes qualify, most items were removed:

- **Docstring trims and comment deletions** — user said no.
- **Renaming** (`pwc`/`fwc` → `partial-wildcard`, etc.) — judgment.
- **Extracting helpers** (codec-access in lmdb, envelope-event in trace, `wildcard-part?`, etc.) — judgment about where and how.
- **Replacing `io-thread` with virtual-thread constructors** — `io-thread` already uses virtual threads.
- **Replacing core.async channels with blocking queues** — judgment.
- **Consolidating cross-file duplicates** (`auto-signal-outputs` across step.clj/flow.clj) — judgment.
- **Defensive-branch removal** (`nil` stubs, defensive catches) — may be load-bearing.
- **Type-hint additions** — would need `*warn-on-reflection* true` runs to verify.

If any of those feel worth revisiting, they’re still findable by re-reading the relevant source files; the list above is the distilled intersection of "clearly a win" and "no context needed".

---

## Tests added

Before applying the 9 changes, three test gaps were filled so the menu had a way to fail loudly on regression:

- **`toolkit/frng_test.clj`** (new file, 8 deftests): `int-inclusive` bounds, `range-inclusive` bounds, `weighted` with single key / zero weights / zero total / rough distribution, and `swarm-weights` bounds. This covers the `(reduce + …)` substitution in `frng.clj:122` (`weighted`), which previously had no tests.
- **`toolkit/datastar/sse_test.clj`** (new file, 4 deftests): `parse-accept-encoding` basics, `preferred-content-encoding` pref-ordering, no-overlap fallback, nil-accept fallback. This covers the `(set …)` substitution in `sse.clj:154`, which previously had no tests.
- **`toolkit/stree_test.clj`** (1-line addition in `delete-edge-cases`): `(delete! t nil)` must return `[nil false]`. The existing test covered `""` but not `nil`; after the `empty?` substitution folds both branches, this guards against accidentally letting a `(.length nil)` NPE slip in.

`toolkit/datapotamus/viz.clj:259` has no test file, but the change there — dropping a `0` seed from a `reduce +` — is identical semantics on every input. No test was added; the risk is zero.

Baseline tests (pre-change, pre-new-tests): **317 tests, 918,594 assertions, 0 failures.**
After adding the 12 new tests and applying all 9 changes: **329 tests, 919,165 assertions, 0 failures.**
