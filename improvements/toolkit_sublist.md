# `toolkit/sublist.clj` — menu

Three mechanical substitutions.

## Menu

1. [Lines 214, 220: drop `0` seed in `(reduce + 0 …)`](#1)
2. [Lines 85–93 `add-to-bucket`: hoist `(or grp [])` with `fnil`](#2)
3. [Line 486: `empty?` over `(zero? (count subj))`](#3)

---

## 1

In `/work/src/toolkit/sublist.clj`:

- **line 214** in `count-node`:
  ```clojure
  (reduce + 0 (map count (vals (:qsubs node))))
  ```
- **line 220** in `count-tree`:
  ```clojure
  (reduce + 0 (map count-tree (vals (:children node))))
  ```

Drop the explicit `0` in each:

- line 214: `(reduce + (map count (vals (:qsubs node))))`
- line 220: `(reduce + (map count-tree (vals (:children node))))`

`reduce` with `+` and no explicit seed returns `(+)` on an empty collection, which is `0` — the same result as the explicit seed. On non-empty collections both forms sum. `0` is the identity of `+` and is already implied.

Purely identical. Apply without further verification.

---

## 2

In `/work/src/toolkit/sublist.clj`, `add-to-bucket` (lines 85–93) currently reads:

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

Rewrite using `fnil` to lift the nil-default out of the function body:

```clojure
(defn- add-to-bucket [bucket q v]
  (if q
    (update-in bucket [:qsubs q]
               (fnil (fn [grp]
                       (if (some #(= v %) grp) grp (conj grp v)))
                     []))
    (update bucket :psubs conj v)))
```

`fnil` wraps the function so that a `nil` first-arg is replaced with `[]` before the function sees it. Equivalent to the inline `(or grp [])` binding. Behavior is identical; one let-binding removed from the body; the nil-handling convention is named.

---

## 3

In `/work/src/toolkit/sublist.clj`, line 486 in `walk-intersect`:

```clojure
(let [prefix (if (zero? (count subj)) subj (str subj "."))]
  …)
```

Replace with:

```clojure
(let [prefix (if (empty? subj) subj (str subj "."))]
  …)
```

`subj` is a `^String`. `(empty? subj)` returns `true` exactly when `(zero? (count subj))` does. `empty?` is the named predicate for "collection is empty" and works on strings via `CharSequence`.

Pure substitution.
