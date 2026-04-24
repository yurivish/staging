# `toolkit/frng.clj` — menu

One mechanical substitution.

## Menu

1. [Line 122: drop `0` seed in `(reduce + 0 …)`](#1)

---

## 1

In `/work/src/toolkit/frng.clj`, line 122:

```clojure
total (reduce + 0 (map val entries))
```

Rewrite as:

```clojure
total (reduce + (map val entries))
```

`reduce +` on an empty seq returns `(+)` which is `0`; on a non-empty seq it sums. The explicit `0` is the identity of `+` and is already implied.

Purely identical behavior.
