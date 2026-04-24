# `toolkit/stree.clj` — menu

Two mechanical themes. Independent; apply either in any order.

## Menu

1. [Lines 434, 460, 463, 482: `peek` over `(nth parts (dec (count parts)))`](#1)
2. [Lines 286, 448, 458, 471, 481: `empty?` over `(zero? (count x))` / `(zero? (.length s))`](#2)

---

## 1

In `/work/src/toolkit/stree.clj`, the expression `(nth parts (dec (count parts)))` appears at four sites. Each is "last element of the `parts` vector". Replace with `(peek parts)`:

- **line 434** in `has-fwc?`. The minimal change is to replace only the inner access, leaving the `(pos? n)` guard intact:
  ```clojure
  (defn- has-fwc? [parts]
    (let [n (count parts)]
      (and (pos? n)
           (let [^String last-p (nth parts (dec n))]
             (and (pos? (.length last-p)) (= fwc (.charAt last-p 0)))))))
  ```
  →
  ```clojure
  (defn- has-fwc? [parts]
    (let [n (count parts)]
      (and (pos? n)
           (let [^String last-p (peek parts)]
             (and (pos? (.length last-p)) (= fwc (.charAt last-p 0)))))))
  ```

  (You might be tempted to collapse the outer `let`/`and` using `when-let` now that `peek` returns nil on empty; don’t, in this pass — the shape would change the empty-case return from `false` to `nil`, which is a different value even though both are falsy. Keep the guard as-is.)

- **line 460** in `do-match`:
  ```clojure
  (let [^String lp (nth parts (dec (count parts)))]
  ```
  →
  ```clojure
  (let [^String lp (peek parts)]
  ```

- **line 463** in `do-match`:
  ```clojure
  [(nth parts (dec (count parts)))]
  ```
  →
  ```clojure
  [(peek parts)]
  ```

- **line 482** in `do-match`:
  ```clojure
  (let [nparts' [(nth parts (dec (count parts)))] …] …)
  ```
  →
  ```clojure
  (let [nparts' [(peek parts)] …] …)
  ```

`peek` on a vector returns the last element in O(1); the arithmetic form is the manual equivalent. `peek` on an empty vector returns `nil`; sites 460/463/482 are always reached with non-empty `parts` (each is inside a consumer that has already tested `parts`), so the `nil`-on-empty behavior is irrelevant.

**Verification done:** `parts` is always a vector. The single producer is `gen-parts` (lines 310–358), which builds via `(loop [… parts []] … (conj parts …))`. Every call site of `peek` will receive a vector.

**Important:** do **not** remove the `(pos? (count parts))` guard at line 459 as part of this change. It’s load-bearing for the `(.length lp)` call on the peek result at that site. The guard stays; only the inner `(nth parts (dec (count parts)))` becomes `(peek parts)`.

---

## 2

`(zero? (count x))` on a counted collection and `(zero? (.length s))` on a string are both equivalent to `(empty? x)`, which is the named predicate for "collection is empty". Replace at five sites:

- **line 286** in `delete!`:
  ```clojure
  (if (or (nil? subject) (zero? (.length subject))) …)
  ```
  →
  ```clojure
  (if (empty? subject) …)
  ```
  `(empty? nil)` is `true` (since `(seq nil)` is `nil`), so the nil-guard is absorbed. Identical truth table; one less branch.

- **line 448** in `do-match`:
  ```clojure
  (when (or (zero? (count nparts)) (and hasfwc? (= 1 (count nparts)))) …)
  ```
  →
  ```clojure
  (when (or (empty? nparts) (and hasfwc? (= 1 (count nparts)))) …)
  ```

- **line 458** in `do-match`:
  ```clojure
  (and (zero? (count nparts)) (not hasfwc?))
  ```
  →
  ```clojure
  (and (empty? nparts) (not hasfwc?))
  ```

- **line 471** in `do-match`, inside the nested cond on leaf-children:
  ```clojure
  (zero? (.length s))
  ```
  →
  ```clojure
  (empty? s)
  ```
  (`s` is `^String`; `empty?` works on `CharSequence`.)

- **line 481** in `do-match`:
  ```clojure
  (and hasfwc? (zero? (count nparts)))
  ```
  →
  ```clojure
  (and hasfwc? (empty? nparts))
  ```

`nparts` is always a vector (see item 1); `empty?` is O(1) for vectors. `subject` and `s` are strings; `empty?` is O(1) for strings.

**Do not** extend this to the `(pos? (count x))` / `(pos? (.length s))` sites at lines 435, 459, 514 of this file. Those are used in boolean contexts where the natural complement is `(seq x)`, which returns a seq or nil rather than a boolean. Leave them alone for this pass.
