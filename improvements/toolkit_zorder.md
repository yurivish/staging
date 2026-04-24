# `toolkit/zorder.clj` — menu

One mechanical substitution. Apply independently.

## Menu

1. [Line 120: `Long/highestOneBit`](#1)

---

## 1

In `/work/src/toolkit/zorder.clj`, inside `litmax-bigmin-2d`, line 120 reads:

```clojure
diff-msb (bit-shift-left 1 (unchecked-subtract 63 (Long/numberOfLeadingZeros diff)))
```

Replace with:

```clojure
diff-msb (Long/highestOneBit diff)
```

These compute the same value when `diff > 0`. `Long/highestOneBit` is the JDK’s named primitive for "isolate the most significant bit"; the current form reconstructs it from `numberOfLeadingZeros` and a shift.

**Verified safe.** Line 114 has an early-exit `(= min max) [min max]`, so by the time line 120 executes, `min ≠ max`. After the swap at line 116, `min < max` unsigned, so `diff = (bit-xor min max)` at line 119 is strictly positive. `highestOneBit` and the current form agree on all positive longs. No edge case.
