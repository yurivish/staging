# `toolkit/datapotamus/` — menu

Two mechanical substitutions, one in `flow.clj` and one in `viz.clj`.

## Menu

1. [`flow.clj:41`: `remove` over `(filter #(not …))`](#1)
2. [`viz.clj:259`: drop `0` seed in `(reduce + 0 …)`](#2)

---

## 1

In `/work/src/toolkit/datapotamus/flow.clj`, inside `validate-ports!`, line 41:

```clojure
(let [unknown (seq (filter #(not (contains? outs %)) (keys port-map)))]
  …)
```

Replace the `filter` with `remove`:

```clojure
(let [unknown (seq (remove #(contains? outs %) (keys port-map)))]
  …)
```

`(remove pred coll)` is defined as `(filter (complement pred) coll)`. The substitution is a named stdlib equivalent.

---

## 2

In `/work/src/toolkit/datapotamus/viz.clj`, line 259, inside `aggregate-queue`:

```clojure
(reduce + 0 (map #(aggregate-queue % queues) (:children node)))
```

Drop the `0`:

```clojure
(reduce + (map #(aggregate-queue % queues) (:children node)))
```

`reduce +` on an empty collection returns `0` by the identity of `+`; on a non-empty collection it sums. The explicit seed is redundant.
