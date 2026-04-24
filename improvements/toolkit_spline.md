# `toolkit/spline.clj` — menu

One mechanical substitution at two sites.

## Menu

1. [Lines 58, 109: `Math/hypot`](#1)

---

## 1

In `/work/src/toolkit/spline.clj`, at two sites:

- **line 58**, inside `subdivisions`:
  ```clojure
  dd  (Math/sqrt (+ (* dx dx) (* dy dy)))
  ```
- **line 109**, inside the private helper `dist`:
  ```clojure
  (Math/sqrt (+ (* dx dx) (* dy dy)))
  ```

Replace the RHS with `(Math/hypot dx dy)`:

- line 58: `dd  (Math/hypot dx dy)`
- line 109: `(Math/hypot dx dy)`

`Math/hypot(x, y)` is the JDK primitive for `sqrt(x² + y²)` and is numerically more stable (it scales the larger operand to avoid overflow/underflow). For typical 2D-point distances here the double result is identical; the substitution is a pure win.

**Do not change line 160** — it reads `(Math/sqrt (double %))` on a single value, not sum-of-squares.
