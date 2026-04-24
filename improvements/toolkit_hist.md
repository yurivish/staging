# `toolkit/hist.clj` — menu

One mechanical substitution.

## Menu

1. [Line 191: `ffirst`](#1)

---

## 1

In `/work/src/toolkit/hist.clj`, inside `summary`, line 191:

```clojure
first-idx (first (first bs))
```

Replace with:

```clojure
first-idx (ffirst bs)
```

`(ffirst x)` is defined as `(first (first x))`. Identical behavior.

**Do not touch line 192** — it reads `last-idx (first (last bs))`. There is no `flast` shorthand; leave it.
