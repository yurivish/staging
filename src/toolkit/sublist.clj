(ns toolkit.sublist
  "NATS-style subject routing trie with `:*` (single-token) and `:>` (tail)
   wildcards and queue groups.

   Subjects and patterns are vectors of tokens. Literal tokens are
   strings; wildcards are the keywords `:*` and `:>`. Because wildcards
   are a disjoint type from literals, a string `\"*\"` is a perfectly
   valid literal token — no escaping, no split/join.

   Subscriptions are value-deduped on (subject, value, queue) — inserting
   the same triple twice leaves one copy. A pubsub layer that wants Go-style
   multi-delivery from a single `sub` call should wrap its stored value with
   a unique token (e.g. a gensym inside a map field). That stays in the
   pubsub layer; the sublist stays generic.

   `match` returns {:plain #{value ...} :groups {queue [value ...]}}. The
   full structure (not a pre-picked delivery set) is load-bearing for
   debug/tracing subscribers that want to see every match. Queue-group
   values are vectors so that a random pick (one-per-group delivery) is
   O(1); dedup of `(subject, value, queue)` is enforced on insert.

   `match` results are cached by subject inside the atom's state
   (`{:root node :cache {subject result}}`). Any real write clears the
   cache; no-op writes (duplicate insert, absent remove) preserve state
   identity and the cache alongside it. Root identity is the freshness
   token — a computed result installs only if the atom's root hasn't
   moved in the meantime."
  (:require [toolkit.stree :as stree])
  (:import [clojure.lang ExceptionInfo]))

(def ^:private empty-node
  {:psubs #{} :qsubs {} :children {} :pwc nil :fwc nil})

;; fwc is a leaf-like bucket (it can hold psubs/qsubs but never children/pwc/fwc).
(def ^:private empty-fwc
  {:psubs #{} :qsubs {}})

;; --- validation ---

(defn- bad [reason s & {:as extra}]
  (throw (ex-info (name reason) (merge {:reason reason :subject s} extra))))

(defn- validate-pattern [v]
  (when-not (and (vector? v) (seq v))
    (bad :empty-subject v))
  (let [n (count v)]
    (dotimes [i n]
      (let [t (nth v i)]
        (cond
          (identical? t :>)
          (when (not= i (dec n))
            (bad :fwc-not-final v :index i))

          (identical? t :*)
          nil

          (not (string? t))
          (bad :bad-token v :index i)

          (= t "")
          (bad :empty-token v :index i)

          (re-find #"[\s\p{Cntrl}]" t)
          (bad :whitespace-in-token v :index i))))
    v))

(defn- validate-subject [v]
  (let [v' (validate-pattern v)]
    (when (some #(or (identical? % :*) (identical? % :>)) v')
      (bad :wildcard-in-subject v))
    v'))

(defn valid-pattern? [v]
  (try (validate-pattern v) true (catch ExceptionInfo _ false)))

(defn valid-subject? [v]
  (try (validate-subject v) true (catch ExceptionInfo _ false)))

;; --- insert ---

;; Queue-group members are stored as a vector so `pick-one` can pick via
;; O(1) indexed `rand-nth`. We enforce the (subject, value, queue) dedup
;; contract with a linear scan on insert; queue groups are typically small.

(defn- add-to-bucket [bucket q v]
  (if q
    (update-in bucket [:qsubs q]
               (fnil (fn [grp]
                       (if (some #(= v %) grp) grp (conj grp v)))
                     []))
    (update bucket :psubs conj v)))

(defn- ins [node [t & rst] v q]
  (cond
    (nil? t)          (add-to-bucket node q v)
    (identical? t :*) (update node :pwc #(ins (or % empty-node) rst v q))
    (identical? t :>) (update node :fwc #(add-to-bucket (or % empty-fwc) q v))
    :else             (update-in node [:children t] #(ins (or % empty-node) rst v q))))

;; --- remove with pruning ---

;; `disj-from-bucket` and `rm` are identity-preserving on no-op: if the
;; target value isn't present, they return the input unchanged (same
;; object). `remove!` exploits this to compute `:removed?` via a single
;; `identical?` check instead of a second tree walk.

(defn- disj-from-bucket [bucket q v]
  (if q
    (let [grp (get-in bucket [:qsubs q])]
      (cond
        (or (nil? grp) (not (some #(= v %) grp)))
        bucket

        :else
        (let [grp' (filterv #(not= v %) grp)]
          (if (empty? grp')
            (update bucket :qsubs dissoc q)
            (assoc-in bucket [:qsubs q] grp')))))
    (let [ps (:psubs bucket)]
      (if (contains? ps v)
        (assoc bucket :psubs (disj ps v))
        bucket))))

(defn- dead? [node]
  (and (empty? (:psubs node))
       (empty? (:qsubs node))
       (empty? (:children node))
       (nil? (:pwc node))
       (nil? (:fwc node))))

(defn- fwc-dead? [fwc]
  (and (empty? (:psubs fwc)) (empty? (:qsubs fwc))))

(defn- rm [node [t & rst] v q]
  (cond
    (nil? t)
    (disj-from-bucket node q v)

    (identical? t :*)
    (if-let [child (:pwc node)]
      (let [child' (rm child rst v q)]
        (cond
          (identical? child child') node
          (dead? child')            (assoc node :pwc nil)
          :else                     (assoc node :pwc child')))
      node)

    (identical? t :>)
    (if-let [fwc (:fwc node)]
      (let [fwc' (disj-from-bucket fwc q v)]
        (cond
          (identical? fwc fwc') node
          (fwc-dead? fwc')      (assoc node :fwc nil)
          :else                 (assoc node :fwc fwc')))
      node)

    :else
    (if-let [child (get-in node [:children t])]
      (let [child' (rm child rst v q)]
        (cond
          (identical? child child') node
          (dead? child')            (update node :children dissoc t)
          :else                     (assoc-in node [:children t] child')))
      node)))

;; --- match ---

;; Single-pass walker: instead of building intermediate `{:plain :groups}`
;; maps at each recursion level and merging them, we descend depth-first
;; and fold each visited node's psubs/qsubs directly into shared volatile
;; accumulators. This avoids per-level map allocation.

(defn- collect-node! [!plain !groups node]
  (when-some [ps (:psubs node)] (vswap! !plain into ps))
  (when-some [qs (:qsubs node)]
    (vswap! !groups
            (fn [gs]
              (reduce-kv (fn [acc q members] (update acc q (fnil into []) members))
                         gs qs)))))

(defn- walk-match [!plain !groups node [t & rst]]
  (if (nil? t)
    ;; Terminal: `>` and `*` both require at least one more token, so no
    ;; fwc/pwc contribution here — only exact subs at this node.
    (collect-node! !plain !groups node)
    (do
      (when-let [fwc (:fwc node)] (collect-node! !plain !groups fwc))
      (when-let [pwc (:pwc node)] (walk-match !plain !groups pwc rst))
      (when-let [c   (get-in node [:children t])]
        (walk-match !plain !groups c rst)))))

(defn- mtch [root tokens]
  (let [!plain  (volatile! #{})
        !groups (volatile! {})]
    (walk-match !plain !groups root tokens)
    {:plain @!plain :groups @!groups}))

(defn- has-interest*
  "Short-circuiting walk: returns true as soon as any matching sub
   (plain or queue) is found. Mirrors `walk-match` but without
   accumulation."
  [node [t & rst]]
  (if (nil? t)
    (or (seq (:psubs node)) (seq (:qsubs node)))
    (or (when-let [fwc (:fwc node)]
          (or (seq (:psubs fwc)) (seq (:qsubs fwc))))
        (when-let [pwc (:pwc node)] (has-interest* pwc rst))
        (when-let [c   (get-in node [:children t])] (has-interest* c rst)))))

(defn- count-node [node]
  (+ (count (:psubs node))
     (reduce + (map count (vals (:qsubs node))))))

(defn- count-tree [node]
  (if (nil? node)
    0
    (+ (count-node node)
       (reduce + (map count-tree (vals (:children node))))
       (if-let [pwc (:pwc node)] (count-tree pwc) 0)
       (if-let [fwc (:fwc node)] (count-node fwc) 0))))

;; --- public api ---

;; Cache bounds: at `cache-max` entries we evict down to `cache-sweep` by
;; dropping a random subset. Matches the NATS Go impl; random drop avoids
;; per-entry LRU bookkeeping.
(def ^:private cache-max 1024)
(def ^:private cache-sweep 256)

(defn- shrink-cache [cache]
  (if (< (count cache) cache-max)
    cache
    (into {} (take cache-sweep (shuffle (seq cache))))))

(def ^:private empty-state {:root empty-node :cache {}})

(defn- apply-write
  "`swap!` fn that applies a trie-mutating `f` to the current root. If
   the new root is `identical?` to the old (a no-op write), returns the
   input state unchanged — preserving both state identity and the
   cache. Otherwise returns a fresh state with a cleared cache."
  [state f]
  (let [{:keys [root]} state
        root' (f root)]
    (if (identical? root root')
      state
      {:root root' :cache {}})))

(defn make
  "Returns a fresh sublist."
  []
  (atom empty-state))

(defn insert!
  "Registers `value` under `subject`. `subject` is a pattern vector and
   may contain `:*` or `:>` wildcards. An optional `{:queue name}` places
   the subscription in a queue group."
  ([sl subject value] (insert! sl subject value nil))
  ([sl subject value {:keys [queue]}]
   (let [tokens (validate-pattern subject)]
     (swap! sl apply-write #(ins % tokens value queue))
     nil)))

(defn remove!
  "Removes a previously-inserted subscription. Returns `{:removed? bool}`.
   Removing an absent subscription is a no-op — the atom's value is
   structurally and identity-preserved."
  ([sl subject value] (remove! sl subject value nil))
  ([sl subject value {:keys [queue]}]
   (let [tokens    (validate-pattern subject)
         [old new] (swap-vals! sl apply-write #(rm % tokens value queue))]
     {:removed? (not (identical? old new))})))

(defn match
  "Looks up subscriptions matching `subject`, which must be a literal
   vector (no `:*` or `:>`). Returns `{:plain #{...} :groups {queue [...]}}`.

   Results are memoized per subject inside the atom's state. A concurrent
   writer that moves the root between the snapshot and the post-compute
   install skips caching this call's result — the next call will recompute
   against the new root."
  [sl subject]
  (let [tokens (validate-subject subject)
        {:keys [root cache]} @sl]
    (or (get cache subject)
        (let [result (mtch root tokens)]
          (swap! sl
                 (fn [{curr-root :root curr-cache :cache :as s}]
                   (if (identical? curr-root root)
                     {:root curr-root
                      :cache (assoc (shrink-cache curr-cache) subject result)}
                     s)))
          result))))

(defn has-interest?
  "Fast boolean: is there any subscriber matching `subject`? Walks the
   trie and returns at the first match, without materializing a result."
  [sl subject]
  (boolean (has-interest* (:root @sl) (validate-subject subject))))

(defn count-subs
  "Total number of subscriptions stored in `sl`, across plain and all
   queue groups. Walks the trie."
  [sl]
  (count-tree (:root @sl)))

(defn stats
  "Snapshot of sublist state useful at the REPL and in cache tests:
   `:count` subscriptions and `:cache-size` memoized match results."
  [sl]
  (let [{:keys [root cache]} @sl]
    {:count (count-tree root) :cache-size (count cache)}))

;; --- reverse-match / subject collision (ports of Go helpers) ---

(defn- analyze-tokens [toks]
  (reduce (fn [acc t]
            (cond
              (identical? t :*) (assoc acc :pwc? true)
              (identical? t :>) (assoc acc :fwc? true)
              :else acc))
          {:pwc? false :fwc? false}
          toks))

(defn- tokens-can-match? [t1 t2]
  (cond
    (or (identical? t1 :*) (identical? t1 :>)
        (identical? t2 :*) (identical? t2 :>)) true
    :else (= t1 t2)))

(defn- subset-match-tokenized?
  "Tokens-vs-tokens subset match. Both may contain wildcards. So `[foo :*]`
   is a subset of `[[:>] [:* :*] [foo :*]]`, but not of `[foo bar]`. Port
   of Go's isSubsetMatchTokenized."
  [tokens test-toks]
  (let [n-tok  (count tokens)
        n-test (count test-toks)]
    (loop [i 0]
      (if (>= i n-test)
        (= n-tok n-test)
        (let [t2 (nth test-toks i)]
          (cond
            (>= i n-tok) false  ; `tokens` exhausted — no match regardless of t2
            (identical? t2 :>) true
            :else
            (let [t1 (nth tokens i)]
              (cond
                (identical? t1 :>) false
                (identical? t1 :*)
                (if (identical? t2 :*) (recur (inc i)) false)
                (and (not (identical? t2 :*)) (not= t1 t2)) false
                :else (recur (inc i))))))))))

(defn subject-matches-filter?
  "Returns true iff `subject` is a subset match of `filter` — i.e. every
   literal a subscription on `subject` would receive is also one `filter`
   would receive. Both may contain wildcards. Port of Go's
   SubjectMatchesFilter."
  [subject filter]
  (subset-match-tokenized? (validate-pattern subject) (validate-pattern filter)))

(defn subjects-collide?
  "Returns true iff `subj1` and `subj2` — both possibly wildcard patterns
   — could both match some single literal subject. Port of Go's
   SubjectsCollide."
  [subj1 subj2]
  (if (= subj1 subj2)
    true
    (let [toks1 (validate-pattern subj1)
          toks2 (validate-pattern subj2)
          {pwc1? :pwc? fwc1? :fwc?} (analyze-tokens toks1)
          {pwc2? :pwc? fwc2? :fwc?} (analyze-tokens toks2)
          lit1? (not (or pwc1? fwc1?))
          lit2? (not (or pwc2? fwc2?))]
      (cond
        (and lit1? lit2?)
        (= subj1 subj2)

        (and lit1? (not lit2?))
        (subset-match-tokenized? toks1 toks2)

        (and lit2? (not lit1?))
        (subset-match-tokenized? toks2 toks1)

        :else
        (let [lt1 (count toks1)
              lt2 (count toks2)]
          (cond
            (and (not fwc1?) (not fwc2?) (not= lt1 lt2)) false
            (and (< lt1 lt2) (not fwc1?))                false
            (and (< lt2 lt1) (not fwc2?))                false
            :else
            (let [stop (min lt1 lt2)]
              (loop [i 0]
                (cond
                  (>= i stop) true
                  (not (tokens-can-match? (nth toks1 i) (nth toks2 i))) false
                  :else (recur (inc i)))))))))))

;; --- reverse-match ---

(defn- collect-all-subs
  "Adds every sub in the subtree rooted at `node` (including `node`'s
   own subs and its fwc bucket)."
  [!plain !groups node]
  (when node
    (collect-node! !plain !groups node)
    (when-let [fwc (:fwc node)] (collect-node! !plain !groups fwc))
    (when-let [pwc (:pwc node)] (collect-all-subs !plain !groups pwc))
    (doseq [c (vals (:children node))]
      (collect-all-subs !plain !groups c))))

(defn- collect-descendants
  "Adds every sub strictly below `node`: its fwc bucket, pwc subtree,
   and children subtrees. Excludes `node`'s own psubs/qsubs. Used by the
   `:>` query token, which requires 1+ more tokens beyond the current
   position."
  [!plain !groups node]
  (when node
    (when-let [fwc (:fwc node)] (collect-node! !plain !groups fwc))
    (when-let [pwc (:pwc node)] (collect-all-subs !plain !groups pwc))
    (doseq [c (vals (:children node))]
      (collect-all-subs !plain !groups c))))

(defn- walk-reverse-match
  "Walks the trie collecting subs whose subject-pattern is a subset of
   the remaining query tokens. Subset semantics: literal query only
   accepts literal subs at that position; `:*` query accepts literals and
   `:*` subs but not `:>` subs; `:>` query accepts any non-empty tail."
  [!plain !groups node [t & rst]]
  (when node
    (cond
      (nil? t)
      (collect-node! !plain !groups node)

      (identical? t :>)
      (collect-descendants !plain !groups node)

      (identical? t :*)
      (do
        (doseq [c (vals (:children node))]
          (walk-reverse-match !plain !groups c rst))
        (when-let [pwc (:pwc node)]
          (walk-reverse-match !plain !groups pwc rst)))

      :else
      (walk-reverse-match !plain !groups (get-in node [:children t]) rst))))

(defn reverse-match
  "Given `subject-pattern` (which may contain `:*` / `:>`), returns the
   subs whose stored subject is a subset of `subject-pattern` — every
   literal matched by the stored subject is also matched by the query.
   For a sublist of literal subjects (the typical use case) this is
   equivalently 'the subs that would fire under any literal covered by
   the query'. Shape matches `match`: {:plain #{...} :groups {queue [...]}}."
  [sl subject-pattern]
  (let [tokens  (validate-pattern subject-pattern)
        !plain  (volatile! #{})
        !groups (volatile! {})]
    (walk-reverse-match !plain !groups (:root @sl) tokens)
    {:plain @!plain :groups @!groups}))

;; --- intersect-stree ---

;; Walks the sublist and queries the stree at every point of interest.
;; A visited-subject set dedupes across overlapping subs. See Go's
;; `IntersectStree` (pubsub/internal/sublist/sublist.go) for the
;; reference; we deliberately skip two of its structural prune rules
;; because both can miss entries on overlapping sub/sibling patterns
;; (see the plan for concrete counter-examples).

;; The stree API is string-based, so this bridge builds dot-joined
;; strings from the sublist's string children tokens. Wildcard keywords
;; never appear in `:children` (they go into `:pwc`/`:fwc`), so the
;; string construction is unambiguous.

(defn- sl-node-interest? [n]
  (or (seq (:psubs n)) (seq (:qsubs n))))

(defn- sl-node-desc? [n]
  (or (seq (:children n)) (some? (:pwc n)) (some? (:fwc n))))

(defn- walk-intersect [st node subj wc? cb]
  (let [prefix (if (empty? subj) subj (str subj "."))]
    (if (:fwc node)
      (stree/match st (str prefix ">") cb)
      (do
        (when-let [pwc (:pwc node)]
          (let [pwc-subj (str prefix "*")]
            (when (sl-node-interest? pwc)
              (stree/match st pwc-subj cb))
            (when (sl-node-desc? pwc)
              (walk-intersect st pwc pwc-subj true cb))))
        (doseq [[t child] (:children node)]
          (let [subj' (str prefix t)]
            (when (sl-node-interest? child)
              (if wc?
                (stree/match st subj' cb)
                (let [[v found?] (stree/lookup st subj')]
                  (when found? (cb subj' v)))))
            (when (sl-node-desc? child)
              (walk-intersect st child subj' wc? cb))))))))

(defn intersect-stree
  "For every entry in the stree `st` whose subject is matched by some
   subscription in the sublist `sl`, invokes `(cb subject value)` exactly
   once. Overlapping sublist subs do not cause double-fires — a visited-
   subject set enforces uniqueness. `cb`'s return value is ignored (no
   early termination).

   `subject` handed to `cb` is a dot-joined string, matching the stree's
   subject format."
  [st sl cb]
  (when (and st sl cb)
    (let [seen    (volatile! #{})
          wrapped (fn [subj v]
                    (when-not (contains? @seen subj)
                      (vswap! seen conj subj)
                      (cb subj v)))]
      (walk-intersect st (:root @sl) "" false wrapped))))
