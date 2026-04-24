# `toolkit/datastar/sse.clj` — menu

One mechanical substitution.

## Menu

1. [Line 154: `set` over `(into #{} …)`](#1)

---

## 1

In `/work/src/toolkit/datastar/sse.clj`, inside `content-encoding`, line 154:

```clojure
first-match (some (into #{} encodings) ranked-prefs)
```

Replace with:

```clojure
first-match (some (set encodings) ranked-prefs)
```

`(set coll)` and `(into #{} coll)` produce the same set when no transducer is involved. Pure substitution.

**Do not apply this** to the other `(into #{} …)` sites in the codebase (`sqlite.clj:60`, `datapotamus/msg.clj:156`, `datapotamus/msg.clj:211`) — those use transducers as the middle argument, which `set` does not accept. Only this one site qualifies.
