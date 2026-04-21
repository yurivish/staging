(ns toolkit.stree
  "Adaptive Radix Trie (ART) for NATS-style subjects, ported from
   nats-server's internal/stree package.

   Supports literal insert/lookup/delete plus wildcard `match`:
   `*` matches a single token, `>` matches the rest of the subject.
   Path compression keeps common prefixes on internal nodes and pushes
   unique tails onto leaves, so lookup is O(k) in the subject length and
   independent of the number of entries.

   Go's adaptive node hierarchy (node4/10/16/48/256) is collapsed into a
   single node shape backed by a persistent map keyed by `char`.
   Clojure's HAMT already gives O(log32) ≈ O(1) child lookup, so the
   adaptive sizing — a constant-factor cache optimization in Go — buys
   nothing here and is removed. Big-O matches Go.

   Node shape:
     internal: {:prefix \"...\" :children {\\c1 child1, \\c2 child2}}
     leaf:     {:suffix \"...\" :value v}

   Leaves are discriminated by the presence of a `:suffix` key. The full
   subject of any entry is reconstructed by concatenating the prefixes
   down the traversal path with the terminal leaf's suffix. Subjects are
   stored as Clojure strings throughout (NATS subjects are ASCII).

   The `\\u007f` (DEL) byte is reserved as the \"no pivot\" sentinel:
   inserts containing it are rejected, matching Go. It is, however, a
   legal child-map key when one inserted subject is a strict prefix of
   another (e.g. inserting both \"foo\" and \"foobar\").

   State lives in an atom: `(make)` yields `(atom {:root nil :size 0})`.
   Mutating operations use `swap!` with pure recursive helpers; the
   helpers return `[new-node old-value updated?]` tuples which the swap
   wrapper unpacks to update size."
  (:refer-clojure :exclude [])
  (:require [clojure.string :as str]))

;; --- constants (match stree/util.go:18-20,48) ---

(def ^:private pwc      \*)
(def ^:private fwc      \>)
(def ^:private tsep     \.)
(def ^:private no-pivot \u007f)

(def ^:private tsep-int     (int tsep))
(def ^:private no-pivot-int (int no-pivot))

;; --- low-level helpers ---

(defn- leaf? [n] (contains? n :suffix))

(defn- pivot
  "Char at `pos` in `s`, or no-pivot if `pos` is past the end.
   Mirrors stree/util.go:52-57 — subjects that are strict prefixes of
   one another resolve to a `no-pivot`-keyed child rather than triggering
   infinite recursion."
  ^Character [^String s ^long pos]
  (if (>= pos (.length s)) no-pivot (.charAt s pos)))

(defn- common-prefix-len
  "Length of the common prefix of `s1` and `s2`. stree/util.go:24-33."
  ^long [^String s1 ^String s2]
  (let [lim (min (.length s1) (.length s2))]
    (loop [i 0]
      (if (or (>= i lim) (not= (.charAt s1 i) (.charAt s2 i)))
        i
        (recur (inc i))))))

;; --- public: make / size / clear! ---

(defn make
  "Returns a fresh subject tree (atom wrapping the root + size)."
  []
  (atom {:root nil :size 0}))

(defn size
  "Number of entries in the tree."
  [st]
  (:size @st))

(defn clear!
  "Empties `st` in place and returns it. Named `clear!` rather than
   `empty!` to avoid confusion with `clojure.core/empty`; Go calls this
   `Empty`."
  [st]
  (reset! st {:root nil :size 0})
  st)

;; --- insert ---

;; `ins` is pure: `(ins node subject value si)` returns
;; `[new-node old-value updated?]`. `si` is the subject index already
;; matched down the path. Matches the recursion in stree.go:148-229.

(declare ^:private ins)

(defn- ins-leaf [node ^String subject ^String value ^long si]
  (let [ln-suffix   ^String (:suffix node)
        subj-suffix (.substring subject si)]
    (if (= ln-suffix subj-suffix)
      ;; Exact suffix match — replace. stree.go:156-161.
      [(assoc node :value value) (:value node) true]
      ;; Divergent — split. stree.go:163-180.
      (let [cpi         (common-prefix-len ln-suffix subj-suffix)
            shared      (.substring subj-suffix 0 cpi)
            ln'         (assoc node :suffix (.substring ln-suffix cpi))
            si'         (+ si cpi)
            p-leaf      (pivot (:suffix ln') 0)]
        (if (and (pos? cpi)
                 (< si' (.length subject))
                 (= p-leaf (.charAt subject si')))
          ;; Same-pivot edge case (stree.go:168-172): the demoted leaf
          ;; and the new subject would collide on the same pivot char
          ;; after sharing `shared`. Recurse into the demoted leaf so it
          ;; gets re-split further down.
          (let [[inner old upd?] (ins ln' subject value si')]
            [{:prefix shared :children {p-leaf inner}} old upd?])
          ;; Normal sibling case. stree.go:174-179.
          (let [new-leaf {:suffix (.substring subject si') :value value}
                p-new    (pivot (:suffix new-leaf) 0)]
            [{:prefix shared
              :children {p-new new-leaf, p-leaf ln'}}
             nil false]))))))

(defn- ins-internal [node ^String subject ^String value ^long si]
  (let [prefix ^String (:prefix node)
        plen   (.length prefix)]
    (if (pos? plen)
      (let [subj-suffix (.substring subject si)
            cpi         (common-prefix-len prefix subj-suffix)]
        (cond
          ;; Full prefix match — descend / attach child. stree.go:188-200.
          (>= cpi plen)
          (let [si'      (+ si plen)
                p        (pivot subject si')
                children (:children node)]
            (if-let [child (get children p)]
              (let [[child' old upd?] (ins child subject value si')]
                [(assoc-in node [:children p] child') old upd?])
              (let [new-leaf {:suffix (.substring subject si') :value value}]
                [(assoc-in node [:children p] new-leaf) nil false])))

          ;; Partial match — split node. stree.go:202-215.
          :else
          (let [shared (.substring prefix 0 cpi)
                si'    (+ si cpi)
                node'  (assoc node :prefix (.substring prefix cpi))
                p-old  (pivot (:prefix node') 0)
                new-leaf {:suffix (.substring subject si') :value value}
                p-new  (pivot (:suffix new-leaf) 0)]
            [{:prefix shared
              :children {p-old node', p-new new-leaf}}
             nil false])))
      ;; No prefix — direct child lookup. stree.go:217-225.
      (let [p        (pivot subject si)
            children (:children node)]
        (if-let [child (get children p)]
          (let [[child' old upd?] (ins child subject value si)]
            [(assoc-in node [:children p] child') old upd?])
          (let [new-leaf {:suffix (.substring subject si) :value value}]
            [(assoc-in node [:children p] new-leaf) nil false]))))))

(defn- ins [node ^String subject ^String value ^long si]
  (cond
    (nil? node)  [{:suffix (.substring subject si) :value value} nil false]
    (leaf? node) (ins-leaf node subject value si)
    :else        (ins-internal node subject value si)))

(defn insert!
  "Inserts `value` at `subject`. Returns `[old-value updated?]`: if the
   subject was already present, `old-value` is the previous value and
   `updated?` is true; otherwise `[nil false]`.

   Rejects subjects containing the `\\u007f` sentinel byte (matches
   Go). Nil subjects are a no-op returning `[nil false]`. Wildcards
   (`*`, `>`) are not interpreted at insert time — they are stored as
   ordinary bytes, only `match` treats them as wildcards."
  [st ^String subject value]
  (if (or (nil? subject)
          (>= (.indexOf subject no-pivot-int) 0))
    [nil false]
    (let [result (volatile! nil)]
      (swap! st
             (fn [{:keys [root size]}]
               (let [[root' old upd?] (ins root subject value 0)]
                 (vreset! result [old upd?])
                 {:root root' :size (if upd? size (inc size))})))
      @result)))

;; --- lookup ---

(defn lookup
  "Returns `[value found?]` for the literal `subject`. No wildcards.
   stree.go:71-100."
  [st ^String subject]
  (if (nil? subject)
    [nil false]
    (let [n-len (.length subject)]
      (loop [n (:root @st) si 0]
        (cond
          (nil? n) [nil false]

          (leaf? n)
          (let [suf ^String (:suffix n)]
            (if (= suf (.substring subject si))
              [(:value n) true]
              [nil false]))

          :else
          (let [prefix ^String (:prefix n)
                plen   (.length prefix)]
            (if (pos? plen)
              (let [end (min (+ si plen) n-len)]
                (if (not= (.substring subject si end) prefix)
                  [nil false]
                  (let [si' (+ si plen)
                        p   (pivot subject si')]
                    (if-let [c (get (:children n) p)]
                      (recur c si')
                      [nil false]))))
              (if-let [c (get (:children n) (pivot subject si))]
                (recur c si)
                [nil false]))))))))

;; --- delete with prefix absorption ---

(defn- absorb-prefix
  "When a node shrinks to a single remaining child, the sole child must
   absorb the collapsing parent's prefix. Stree.go:273-283. For leaves
   the prefix prepends to `:suffix`; for internal nodes it prepends to
   `:prefix`."
  [^String pre sole]
  (if (leaf? sole)
    (update sole :suffix #(str pre %))
    (update sole :prefix #(str pre %))))

(declare ^:private del)

(defn- del-internal [node ^String subject ^long si]
  (let [prefix ^String (:prefix node)
        plen   (.length prefix)]
    (cond
      ;; Guard: subject shorter than prefix. stree.go:247-250.
      (and (pos? plen) (< (.length subject) (+ si plen)))
      [node nil false]

      ;; Guard: prefix mismatch.
      (and (pos? plen)
           (not= prefix (.substring subject si (+ si plen))))
      [node nil false]

      :else
      (let [si'      (+ si plen)
            p        (pivot subject si')
            children (:children node)
            child    (get children p)]
        (if (nil? child)
          [node nil false]
          (let [[child' old deleted?] (del child subject si')]
            (if-not deleted?
              [node nil false]
              (let [children' (if (nil? child')
                                (dissoc children p)
                                (assoc children p child'))]
                (if (= 1 (count children'))
                  ;; Collapse: absorb this node's prefix into the sole
                  ;; remaining child. stree.go:268-285.
                  (let [sole (val (first children'))]
                    [(absorb-prefix prefix sole) old true])
                  [(assoc node :children children') old true])))))))))

(defn- del [node ^String subject ^long si]
  (cond
    (nil? node) [nil nil false]

    (leaf? node)
    (if (= ^String (:suffix node) (.substring subject si))
      [nil (:value node) true]
      [node nil false])

    :else (del-internal node subject si)))

(defn delete!
  "Removes the entry at `subject` and returns `[deleted-value deleted?]`.
   Rejects nil or empty subject (matches Go stree.go:233)."
  [st ^String subject]
  (if (or (nil? subject) (zero? (.length subject)))
    [nil false]
    (let [result (volatile! nil)]
      (swap! st
             (fn [{:keys [root size]}]
               (let [[root' val deleted?] (del root subject 0)]
                 (vreset! result [val deleted?])
                 {:root root' :size (if deleted? (dec size) size)})))
      @result)))

;; --- parts (port of stree/parts.go) ---

;; `gen-parts` breaks a wildcard filter into chunks bounded by `*` / `>`
;; wildcards. Wildcards are only honored at token boundaries (start or
;; preceded by `.`; end or followed by `.`). Examples (from the Go
;; reference):
;;   "foo.bar.baz" -> ["foo.bar.baz"]
;;   "foo.*.baz"   -> ["foo." "*" "baz"]
;;   "foo.bar.>"   -> ["foo.bar." ">"]
;;   "*.foo.>"     -> ["*" "foo." ">"]
;; Tseps immediately after a wildcard are consumed by that wildcard; any
;; leading tsep on the final chunk is eaten so chunks don't double up
;; separators (stree/parts.go:64-70).

(defn- gen-parts
  "Transliteration of parts.go:23-72."
  [^String filter]
  (let [n (.length filter)
        e (dec n)]
    (loop [i 0 start 0 parts []]
      (cond
        (>= i n)
        (if (< start n)
          (let [start' (if (= tsep (.charAt filter start)) (inc start) start)]
            (conj parts (.substring filter start' n)))
          parts)

        ;; tsep then pwc, with next-next being tsep or end-of-string
        (and (= tsep (.charAt filter i))
             (< i e)
             (= pwc (.charAt filter (inc i)))
             (or (and (<= (+ i 2) e) (= tsep (.charAt filter (+ i 2))))
                 (= (inc i) e)))
        (let [parts  (cond-> parts
                       (> i start) (conj (.substring filter start (inc i))))
              parts  (conj parts (.substring filter (inc i) (+ i 2)))
              i2     (inc i)                     ; skip pwc
              i3     (if (<= (+ i2 2) e) (inc i2) i2)] ; skip following tsep if any
          (recur (inc i3) (inc i3) parts))

        ;; tsep then fwc at end of filter
        (and (= tsep (.charAt filter i))
             (< i e)
             (= fwc (.charAt filter (inc i)))
             (= (inc i) e))
        (let [parts (cond-> parts
                      (> i start) (conj (.substring filter start (inc i))))
              parts (conj parts (.substring filter (inc i) (+ i 2)))
              i2    (inc i)]                     ; skip fwc
          (recur (inc i2) (inc i2) parts))

        ;; Bare wildcard at position i — valid only at start-or-after-tsep
        ;; AND at-end-or-before-tsep.
        (and (or (= pwc (.charAt filter i)) (= fwc (.charAt filter i)))
             (or (zero? i) (= tsep (.charAt filter (dec i))))
             (not (or (= (inc i) e)
                      (and (< (inc i) e) (not= tsep (.charAt filter (inc i)))))))
        (let [parts (conj parts (.substring filter i (inc i)))
              i2    (if (<= (inc i) e) (inc i) i)] ; skip next tsep if present
          (recur (inc i2) (inc i2) parts))

        :else
        (recur (inc i) start parts)))))

(defn- match-parts
  "Transliteration of parts.go:75-144. Matches a sequence of wildcard-
   delimited `parts` against a single `frag` (a node prefix or leaf
   suffix) and returns `[remaining-parts matched?]`.

   When a part is only partially consumed by the fragment (fragment
   ends mid-part), the remaining first part is replaced with its tail
   so the caller can continue matching against the next fragment."
  [parts ^String frag]
  (let [lf  (.length frag)
        lpi (dec (count parts))]
    (if (zero? lf)
      [parts true]
      (loop [i 0 si 0 parts parts]
        (cond
          (>= i (count parts))
          [parts false]

          (>= si lf)
          [(subvec parts i) true]

          :else
          (let [^String part (nth parts i)
                lp           (.length part)]
            (cond
              ;; PWC — consume up to next tsep in frag.
              (and (= 1 lp) (= pwc (.charAt part 0)))
              (let [idx (.indexOf frag tsep-int (int si))]
                (if (neg? idx)
                  (if (= i lpi) [nil true] [(subvec parts i) true])
                  (recur (inc i) (+ idx 1) parts)))

              ;; FWC — matches everything from here.
              (and (= 1 lp) (= fwc (.charAt part 0)))
              [nil true]

              :else
              (let [end          (min (+ si lp) lf)
                    part-to-cmp  ^String (if (> (+ si lp) end)
                                           (.substring part 0 (- end si))
                                           part)]
                (cond
                  (not= part-to-cmp (.substring frag si end))
                  [parts false]

                  (< end lf)
                  (recur (inc i) end parts)

                  ;; end == lf. Did we fully consume `part`?
                  (< end (+ si lp))
                  ;; Partial — trim consumed prefix off the first part
                  ;; and return. The original (untruncated) part is at
                  ;; (nth parts i); `part-to-cmp` was truncated locally.
                  [(-> parts
                       (assoc i (.substring ^String (nth parts i) (- lf si)))
                       (subvec i))
                   true]

                  (= i lpi)
                  [nil true]

                  :else
                  ;; Fully consumed part and fragment at the same time,
                  ;; but more parts remain. Next iter's `si >= lf` check
                  ;; will return [parts[i+1:] true].
                  (recur (inc i) (+ si lp) parts))))))))))

;; --- match ---

(declare ^:private do-match)

(defn- has-fwc? [parts]
  (let [n (count parts)]
    (and (pos? n)
         (let [^String last-p (nth parts (dec n))]
           (and (pos? (.length last-p)) (= fwc (.charAt last-p 0)))))))

(defn- do-match
  "Recursive matcher mirroring stree.go:296-382. `pre` accumulates the
   subject path. `hasfwc?` caches whether the filter ends in `>`."
  [n parts ^String pre hasfwc? cb]
  (loop [n n parts parts pre pre]
    (when (some? n)
      (let [frag (if (leaf? n) ^String (:suffix n) ^String (:prefix n))
            [nparts matched?] (match-parts parts frag)]
        (when matched?
          (cond
            (leaf? n)
            (when (or (zero? (count nparts))
                      (and hasfwc? (= 1 (count nparts))))
              (cb (str pre (:suffix n)) (:value n)))

            :else
            (let [pre' (str pre (:prefix n))]
              (cond
                ;; All parts consumed, no trailing fwc — check for
                ;; terminal-pwc case and for children whose suffix
                ;; completes an exact match (stree.go:326-354).
                (and (zero? (count nparts)) (not hasfwc?))
                (let [term-pwc? (and (pos? (count parts))
                                     (let [^String lp (nth parts (dec (count parts)))]
                                       (and (= 1 (.length lp)) (= pwc (.charAt lp 0)))))
                      nparts'   (if term-pwc?
                                  [(nth parts (dec (count parts)))]
                                  nparts)]
                  (doseq [[_ cn] (:children n)]
                    (when (some? cn)
                      (cond
                        (leaf? cn)
                        (let [^String s (:suffix cn)]
                          (cond
                            (zero? (.length s))
                            (cb (str pre' s) (:value cn))

                            (and term-pwc? (neg? (.indexOf s tsep-int)))
                            (cb (str pre' s) (:value cn))))

                        term-pwc?
                        (do-match cn nparts' pre' hasfwc? cb)))))

                ;; Trailing fwc consumed — restore it and continue.
                (and hasfwc? (zero? (count nparts)))
                (let [nparts' [(nth parts (dec (count parts)))]
                      fp      ^String (nth nparts' 0)]
                  (if (and (= 1 (.length fp))
                           (or (= pwc (.charAt fp 0)) (= fwc (.charAt fp 0))))
                    (doseq [[_ cn] (:children n)]
                      (when (some? cn) (do-match cn nparts' pre' hasfwc? cb)))
                    ;; (unreachable given hasfwc? true and len=1)
                    nil))

                :else
                (let [fp ^String (nth nparts 0)
                      p  (pivot fp 0)]
                  (if (and (= 1 (.length fp))
                           (or (= pwc (.charAt fp 0)) (= fwc (.charAt fp 0))))
                    ;; Wildcard at head of remaining parts — iterate all.
                    (doseq [[_ cn] (:children n)]
                      (when (some? cn) (do-match cn nparts pre' hasfwc? cb)))
                    ;; Literal — find next child and continue loop.
                    (when-let [cn (get (:children n) p)]
                      (recur cn nparts pre'))))))))))))

(defn match
  "Walks every entry whose subject is matched by the wildcard filter and
   invokes `(cb subject value)` for each. `*` matches a single token,
   `>` matches one or more trailing tokens. Callback return value is
   ignored (no early termination, matches Go stree.go:116-125)."
  [st ^String filter cb]
  (let [root (:root @st)]
    (when (and root filter (pos? (.length filter)) cb)
      (let [parts (gen-parts filter)]
        (do-match root parts "" (has-fwc? parts) cb)))))

;; --- iter ---

(defn- path-of
  "A child's path for sort purposes: its `:suffix` if a leaf, its
   `:prefix` otherwise. Matches Go's `node.path()` (node.go:32), which
   the iter uses as the sort key."
  ^String [n]
  (if (leaf? n) (:suffix n) (:prefix n)))

(defn- do-iter
  "Returns boolean: true to continue iteration, false to abort. Mirrors
   stree.go:384-424. Ordered mode sorts siblings by full path (the
   child's first-byte alone isn't enough: a child with an empty
   prefix/suffix keyed under no-pivot must sort before a dot-keyed
   sibling, since \"\" < \".\")."
  [n ^String pre ordered? cb]
  (if (leaf? n)
    (boolean (cb (str pre (:suffix n)) (:value n)))
    (let [pre'     (str pre (:prefix n))
          children (:children n)
          entries  (if ordered?
                     (sort-by (fn [[_ v]] (path-of v)) children)
                     children)]
      (loop [es (seq entries)]
        (cond
          (nil? es) true
          (not (do-iter (val (first es)) pre' ordered? cb)) false
          :else (recur (next es)))))))

(defn iter-ordered
  "Walks every entry in lexicographic subject order. `(cb subject value)`
   returns truthy to continue, falsey to terminate."
  [st cb]
  (when-let [r (:root @st)] (do-iter r "" true cb) nil))

(defn iter-fast
  "Walks every entry in unspecified order (HAMT iteration). Same
   callback contract as `iter-ordered` but without the per-level sort."
  [st cb]
  (when-let [r (:root @st)] (do-iter r "" false cb) nil))

;; --- lazy-intersect ---

(defn lazy-intersect
  "Iterates the smaller of the two trees, looks up each subject in the
   larger, and invokes `(cb subject value-left value-right)` for hits.
   stree.go:430-450."
  [tl tr cb]
  (when (and tl tr (pos? (size tl)) (pos? (size tr)) cb)
    (if (<= (size tl) (size tr))
      (iter-fast tl (fn [k v1]
                      (let [[v2 ok?] (lookup tr k)]
                        (when ok? (cb k v1 v2))
                        true)))
      (iter-fast tr (fn [k v2]
                      (let [[v1 ok?] (lookup tl k)]
                        (when ok? (cb k v1 v2))
                        true))))))
