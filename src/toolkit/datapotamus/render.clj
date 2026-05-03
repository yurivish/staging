(ns toolkit.datapotamus.render
  "Render a shape-decomposed pipeline as an indented nested-list view.

   Walks a `(shape/decompose topology)` tree once. The output uses three
   notation slots per line:

     [left ] [indent + body] [right annotations]

   - Left slot (2 chars at column 0):
       `↓ ` — this line falls through to its successor in the same shape's
              `:order`; that pair is a real edge.
       `  ` — no fall-through.
   - Body — name, optionally with a `│ ` branch-root marker for
            scatter-gather arms or a `K× …` compression header.
   - Right slot (after the name):
       `⮥ <name>`        — back-edge to a line above (always named).
       `⮧ <name>`        — forward off-spine to a non-adjacent line below.

   Two layers:
   - `render` — pure, returns a seq of lines (strings, no newlines).
   - `print-pipeline` — terminal entry point; prints to *out*."
  (:require [clojure.string :as str]
            [toolkit.datapotamus.shape :as shape]
            [toolkit.datapotamus.step :as step]))

(defn- indent [depth]
  (apply str (repeat (* 2 depth) \space)))

(defn- last-sid [path]
  (when (seq path)
    (let [s (last path)]
      (if (keyword? s) (name s) (str s)))))

(defn- humanize-sid
  "Humanize a step id the same way `step/topology` humanizes leaf
   names — keyword `:format-check` → `format check`. Keeps annotation
   text consistent with the displayed leaf name."
  [s]
  (when s (str/replace s "-" " ")))

(defn- elt-name
  "Short name for an :order element used in inline annotations."
  [elt]
  (cond
    (vector? elt) (or (humanize-sid (last-sid elt)) "")

    (and (map? elt) (= :cycle (:kind elt)))
    (str "↻" (when-let [m (first (:order elt))] (elt-name m)))

    :else (pr-str elt)))

(defn- classified-annotations
  "Off-spine edges classified by direction relative to `:order` position.
   Returns map src-elt → vec of [kind target] tuples:
     [:back v]  back-edge to v (always named, even when adjacent)
     [:fwd v]   forward off-spine to v"
  [shape]
  (let [order     (:order shape)
        positions (zipmap order (range))
        consec    (set (map vec (partition 2 1 order)))
        off       (remove (fn [[u v]] (consec [u v])) (:internal-edges shape))]
    (-> (group-by first off)
        (update-vals
         (fn [edges]
           (mapv (fn [[u v]]
                   (let [up (positions u)
                         vp (positions v)]
                     (if (and up vp (< vp up))
                       [:back v]
                       [:fwd v])))
                 edges))))))

(defn- annotation-string [annot]
  (cond
    (and (vector? annot) (= :back (first annot))) (str "⮥ " (elt-name (second annot)))
    (and (vector? annot) (= :fwd (first annot)))  (str "⮧ " (elt-name (second annot)))))

(defn- attach-annotations
  "Append right-side annotations to the first line of `tagged`."
  [tagged annots]
  (if (and (seq tagged) (seq annots))
    (let [{:keys [line fall-through? paths]} (first tagged)
          suffix (str "  " (str/join ", " (map annotation-string annots)))]
      (cons {:line (str line suffix) :fall-through? fall-through? :paths paths} (rest tagged)))
    tagged))

(defn- with-fall-through
  "Set :fall-through? on both the FIRST and LAST line of `tagged`. The
   first is the element's header in its parent shape; the last is its
   rendered exit point — for a leaf they're the same line; for a
   container with internal chain they differ. Setting both means the
   element's outer fall-through visually propagates from its header
   down through nested content to its exit, so the reader sees a
   continuous ↓ trail across container boundaries."
  [tagged fall-through?]
  (let [v (vec tagged)
        n (count v)
        ft (boolean fall-through?)]
    (cond
      (zero? n) v
      (= 1 n)   (assoc-in v [0 :fall-through?] ft)
      :else     (-> v
                    (assoc-in [0 :fall-through?] ft)
                    (assoc-in [(dec n) :fall-through?] ft)))))

(defn- apply-bracket-rail
  "Decorate a flat seq of branch lines with the parallel-section
   `⎢` rail and absorb in-branch chain fall-through into the rail
   itself. Each line gets a 2-character prefix `⎢<↓ or space>`:
     - `⎢↓name` — in-branch fall-through to next line.
     - `⎢ name` — no in-branch fall-through.
   Corners (`⎡⎣`) are deliberately omitted — they were ambiguous
   about whether the parent line above belongs to the bracket. A
   uniform `⎢` rail says \"these lines are siblings under the parent
   above\" without that ambiguity. Last line never has fall-through
   (nothing inside the bracket after it).
   Leading whitespace on the input line is fully replaced — all
   bracket-wrapped content sits at the same column relative to the
   rail, regardless of nesting depth. Inner brackets re-introduce
   visual offset where needed. Output `:fall-through?` is cleared so
   `finalize` doesn't also emit a left-column `↓` for the same line."
  [tagged-lines]
  (let [total (count tagged-lines)]
    (vec
     (map-indexed
      (fn [i {:keys [line fall-through? paths]}]
        (let [last? (= (dec total) i)
              fall-through-mark (if (and fall-through? (not last?)) "↓" " ")
              rest-line (str/replace line #"^\s+" "")
              new-line (str "⎢" fall-through-mark rest-line)]
          {:line new-line :fall-through? false :paths paths}))
      tagged-lines))))

(defn- apply-collapsed-rail
  "Decorate the lines of a collapsed `K× …` block with a single-row
   `⎢` rail at col 2. The K× block sits at the same level as
   source/sink (it's their visual successor in the chain), so its
   fall-through belongs in the LEFT column — unlike multi-row
   `⎡⎢⎣` brackets, which absorb in-branch fall-through into the rail
   itself. Leaves `:fall-through?` intact so `finalize` emits `↓` at
   col 0; the rail just adds `⎢ ` at col 2 (replacing the existing
   2-space indent), preserving any deeper indent for inner lines."
  [tagged]
  (vec
   (map (fn [{:keys [line fall-through? paths]}]
          (let [rest-line (if (>= (count line) 2) (subs line 2) line)
                new-line (str "⎢ " rest-line)]
            {:line new-line :fall-through? fall-through? :paths paths}))
        tagged)))

(declare render-shape)

(declare render-inline-sg compute-branch-lines)

(defn- render-elt
  "Render one :order element (a path, a nested :cycle record, or a
   nested :scatter-gather record). Returns a seq of
   {:line :fall-through? :paths} maps. `:paths` carries the step paths
   the line represents — usually a single path; for a nested record's
   header line, the members."
  [elt children-map depth]
  (cond
    (vector? elt)
    (if-let [child (get children-map elt)]
      (case (:kind child)
        :leaf
        [{:line (str (indent depth) (:name child)) :fall-through? false :paths [elt]}]

        :container
        (cons {:line (str (indent depth) (:name child)
                          (when-let [c (:combinator child)]
                            (str " (" (name c) ")")))
               :fall-through? false :paths [elt]}
              (render-shape (:shape child)
                            (into {} (for [c (:children child)] [(:path c) c]))
                            (inc depth))))
      [{:line (str (indent depth) (str (last-sid elt))) :fall-through? false :paths [elt]}])

    (and (map? elt) (= :cycle (:kind elt)))
    (cons {:line (str (indent depth) "(cycle)")
           :fall-through? false
           :paths (vec (filter vector? (:order elt)))}
          (render-shape elt children-map (inc depth)))

    (and (map? elt) (= :scatter-gather (:kind elt)))
    ;; Inline scatter-gather record inside a parent chain's :order.
    ;; The sg's :source and :sink are the chain neighbors and are
    ;; rendered by the chain — so we emit only the branches under a
    ;; rail at depth+1.
    (render-inline-sg elt children-map (inc depth))

    :else
    [{:line (str (indent depth) (pr-str elt)) :fall-through? false :paths []}]))

;; ---- Compression of identical scatter-gather branches ----

(defn- branch-rendered
  "Render a single branch shape at `branch-depth`. Branches are full
   shape records (chain / scatter-gather / cycle / prime / empty); for
   chain branches, each :order element renders one indent deeper so
   name-at-root vs. name-at-deeper-indent reads as new-branch vs.
   continuation."
  [branch children-map branch-depth]
  (case (:kind branch)
    :chain
    (let [order (:order branch)
          n (count order)]
      (apply concat
             (map-indexed
              (fn [i elt]
                (with-fall-through (render-elt elt children-map (+ branch-depth i))
                         (< i (dec n))))
              order)))

    :empty []

    ;; Non-chain branch (scatter-gather, cycle, prime as a sub-shape).
    ;; Recurse via render-shape; outer bracket rail wraps the result.
    (render-shape branch children-map branch-depth)))

(defn- branch-signature
  "Normalize a rendered branch so first-line names get a placeholder.
   Two branches share a signature iff they render identically up to
   the wrapper name on their first line. Excludes `:paths` from the
   signature — different branches have different paths by definition."
  [tagged]
  (when (seq tagged)
    (let [{:keys [line fall-through?]} (first tagged)
          leading (re-find #"^\s*" line)
          first-norm {:line (str leading "<<wrapper>>") :fall-through? fall-through?}]
      (cons first-norm
            (mapv #(select-keys % [:line :fall-through?]) (rest tagged))))))

(defn- compress-branches
  "If all branches share a signature AND have non-trivial inner content,
   return [k :collapsed sample all-paths]; otherwise return [k
   :uncompressed per-branch-lines]. `all-paths` is the union of every
   branch's first-line paths so the K× block can carry them forward
   for downstream stats lookup."
  [rendered-branches]
  (let [k (count rendered-branches)
        sigs (mapv branch-signature rendered-branches)
        first-sig (first sigs)
        all-same? (and (seq rendered-branches)
                       (every? #(= % first-sig) sigs))
        nontrivial? (and all-same? (> (count first-sig) 1))]
    (if (and (>= k 2) nontrivial?)
      (let [all-paths (vec (mapcat (fn [branch]
                                     (when (seq branch) (:paths (first branch))))
                                   rendered-branches))]
        [k :collapsed (first rendered-branches) all-paths])
      [k :uncompressed rendered-branches])))

(declare displayify-line)

(defn- render-collapsed
  "Render `k` identical branches as a `K× ...` block. `sample` is one
   branch's tagged lines; for multi-line samples the wrapper name is
   carried onto the `K× <name>` header so the reader can see what the
   block contains. `all-paths` is the union of branch paths so
   downstream stats consumers know which members the block represents."
  [k sample all-paths branch-depth]
  (cond
    (empty? sample)
    []

    (= 1 (count sample))
    ;; Single-leaf branches: inline the leaf body after `K× `.
    (let [{:keys [line]} (first sample)
          leading (re-find #"^\s*" line)
          body (subs line (count leading))]
      [{:line (str (indent branch-depth) (str k "× ") body)
        :fall-through? false :paths all-paths}])

    :else
    ;; Multi-line: header `K× <wrapper>` line at branch-depth, inner
    ;; body lines preserved as-is. The wrapper name is displayified so
    ;; per-instance digit suffixes (`worker 0`/`worker 1`/...) collapse
    ;; to a generic `worker K`.
    (let [{:keys [line]} (first sample)
          leading (re-find #"^\s*" line)
          body (-> (subs line (count leading)) displayify-line)]
      (cons {:line (str (indent branch-depth) (str k "× ") body)
             :fall-through? false :paths all-paths}
            (rest sample)))))

(defn- compute-branch-lines
  "Render the branches of a scatter-gather shape and apply the
   appropriate rail (collapsed K× or per-branch ⎢). Returns the
   tagged lines for the bracket — does NOT include source/sink lines."
  [branches children-map branch-depth]
  (let [rendered (mapv #(branch-rendered % children-map branch-depth) branches)
        result (compress-branches rendered)
        [k mode] result]
    (case mode
      :collapsed
      (let [[_ _ payload all-paths] result
            collapsed (render-collapsed k payload all-paths branch-depth)]
        (apply-collapsed-rail (with-fall-through collapsed true)))
      :uncompressed
      (apply-bracket-rail (apply concat (nth result 2))))))

(defn- render-inline-sg
  "Render a scatter-gather record that's nested inside a parent chain's
   :order. The sg's source/sink are atomic chain neighbors (rendered
   by the chain), so we emit only the bracketed branches at
   `branch-depth`."
  [sg children-map branch-depth]
  (compute-branch-lines (:branches sg) children-map branch-depth))

;; ---- Pattern compression for cycle / prime :order ----
;;
;; Detects runs of repeated patterns in pre-rendered cycle/prime
;; output. A pattern is a window of W consecutive `:order` elements
;; whose normalized rendering matches the next W consecutive elements
;; (and so on). Names with a letter-prefix and trailing digits are
;; normalized to letter-prefix+`K`, so `w0`/`w1`/...`w15` all match.

(defn- displayify-line
  "Replace per-instance digit suffixes with a generic `K` placeholder
   so K identical workers all normalize to the same string. Handles
   two patterns:
     - letters immediately followed by digits (e.g. `w0`, `s10`) → `wK`
     - one or more letter-words then a final space-digits group at end
       of line (e.g. `worker 0`, `to worker 0`) → `worker K`
   Names without a trailing-digit suffix pass through unchanged."
  [line]
  (-> line
      (str/replace #"\b([a-zA-Z]+)\d+\b" "$1K")
      (str/replace #"(\b[a-zA-Z]+(?:\s[a-zA-Z]+)*)\s(\d+)$" "$1 K")))

(defn- normalize-tagged [tagged]
  (mapv (fn [{:keys [line fall-through?]}]
          {:line (displayify-line line) :fall-through? fall-through?})
        tagged))

(defn- find-pattern-at
  "Look for the largest repeating pattern starting at `start` in
   `pre-rendered` (a vec of seq-of-tagged-lines, one per :order elt).
   Tries window sizes 1..max-size. Returns {:size W :count N} for the
   most-compressing match (largest N, then largest W), or nil."
  [pre-rendered start max-size]
  (let [n (count pre-rendered)]
    (->> (range 1 (inc max-size))
         (keep
          (fn [size]
            (when (<= (+ start (* 2 size)) n)
              (let [template (mapv normalize-tagged
                                   (subvec pre-rendered start (+ start size)))
                    rep-count
                    (loop [c 1]
                      (let [ns (+ start (* c size))
                            ne (+ ns size)]
                        (if (and (<= ne n)
                                 (= template
                                    (mapv normalize-tagged
                                          (subvec pre-rendered ns ne))))
                          (recur (inc c))
                          c)))]
                (when (>= rep-count 2)
                  {:size size :n rep-count})))))
         (sort-by (fn [{:keys [size n]}] [(- n) (- size)]))
         first)))

(defn- compress-pattern-block
  "Take the first instance (size elts) of a detected pattern and emit
   a compressed block: name-normalized lines with the K× prefix on
   the first line. The K× line carries `:paths` for ALL N instances
   so downstream stats consumers can aggregate across them."
  [pre-rendered start {:keys [size n]}]
  (let [first-instance (apply concat
                              (subvec pre-rendered start (+ start size)))
        ;; Union of paths across all N repetitions of this pattern.
        all-paths (vec
                   (mapcat (fn [elt-tagged] (mapcat :paths elt-tagged))
                           (subvec pre-rendered start (+ start (* n size)))))
        normalized (mapv (fn [{:keys [line fall-through? paths]}]
                           {:line (displayify-line line)
                            :fall-through? fall-through?
                            :paths paths})
                         first-instance)]
    (if (seq normalized)
      (let [{:keys [line fall-through?]} (first normalized)
            leading (re-find #"^\s*" line)
            body (subs line (count leading))
            new-line (str leading n "× " body)]
        (cons {:line new-line :fall-through? fall-through? :paths all-paths}
              (rest normalized)))
      normalized)))

(defn- compress-cycle-order
  "Walk `pre-rendered` (a vec of per-elt tagged-line seqs) and emit a
   seq of tagged-line seqs where repeating patterns have been
   collapsed. Maximum pattern window: 4 elements."
  [pre-rendered]
  (loop [start 0
         out []]
    (if (>= start (count pre-rendered))
      out
      (if-let [pattern (find-pattern-at pre-rendered start 4)]
        (recur (+ start (* (:size pattern) (:n pattern)))
               (conj out (compress-pattern-block pre-rendered start pattern)))
        (recur (inc start)
               (conj out (nth pre-rendered start)))))))

;; ---- Block aggregation (Option A): WL refinement + faithfulness gate ----
;;
;; A `:cycle` or `:prime` shape is a small graph. Some of its members
;; can be structurally interchangeable — `c/round-robin-workers` makes
;; 16 workers wired identically; `c/stealing-workers` makes 16 worker
;; + 16 emitter members in matching positions. When members are
;; interchangeable, presenting them individually is misleading
;; (Eades-derived ordering arbitrarily promotes one to a special
;; spine position). This section partitions members into equivalence
;; classes via 1-WL color refinement, verifies the inter-class edges
;; can be summarized faithfully (fan-in, fan-out, bijection,
;; complete), and renders each class as a `K× <representative>` block.
;;
;; Constraint not enforced in V1: port-class symmetry. The
;; `:internal-edges` data preserves only `[from-path to-path]`, not
;; port labels. For our pipelines, name-normalization (digit suffix →
;; K) suffices: round-robin/stealing-workers use digit-suffixed names
;; that normalize identically; `cc/parallel` heterogeneous specialists
;; use distinct names that do not. If a real case ever needs port
;; labels, lift them into `:internal-edges` in shape.clj.

(def ^:dynamic *aggregate?*
  "When true, `:cycle`/`:prime` rendering attempts block aggregation
   first; falls back to per-member if the faithfulness gate bails."
  true)

(declare render-shape)

(defn- intrinsic-color
  "Color of `member` reflecting only its intrinsic structure: leaf
   name (displayified) for leaves, recursive shape signature for
   containers. Two members with equal intrinsic-color are
   indistinguishable in isolation."
  [member children-map]
  (when-let [child (get children-map member)]
    (case (:kind child)
      :leaf
      [:leaf (displayify-line (or (:name child) ""))]

      :container
      (let [sub-children (into {} (for [c (:children child)] [(:path c) c]))
            sub-rendering (binding [*aggregate?* true]
                            (render-shape (:shape child) sub-children 0))]
        [:container
         (mapv (fn [{:keys [line fall-through?]}]
                 [(displayify-line line) fall-through?])
               sub-rendering)]))))

(defn- canonicalize-coloring
  "Replace `coloring`'s values with small integer IDs preserving the
   partition. Critical for WL: without this, color values grow
   exponentially each round and equality comparisons blow up. Uses
   first-occurrence-in-`members` order for deterministic IDs."
  [coloring members]
  (loop [seen {}
         next-id 0
         out {}
         [m & ms] members]
    (if (nil? m)
      out
      (let [v (get coloring m)
            existing (get seen v)
            id (or existing next-id)
            seen' (if existing seen (assoc seen v id))
            next-id' (if existing next-id (inc next-id))]
        (recur seen' next-id' (assoc out m id) ms)))))

(defn- wl-refine
  "1-WL color refinement on the SCC. Iterates color = (old-color, sorted
   multiset of (direction, neighbor-color)) until colors stabilize.
   Colors are canonicalized to integer IDs after every round so that
   per-round work and equality stay O(N+E). Returns map: member →
   final integer-ID color. Members with equal final color are
   recursively indistinguishable (= automorphism orbit, modulo
   WL-pathological graphs that don't arise in pipeline topologies)."
  [members internal-edges children-map]
  (let [member-set (set members)
        intrinsic (into {} (for [m members]
                             [m (intrinsic-color m children-map)]))
        out (reduce (fn [m [u v]]
                      (cond-> m
                        (and (member-set u) (member-set v))
                        (update u (fnil conj []) v)))
                    {} internal-edges)
        in  (reduce (fn [m [u v]]
                      (cond-> m
                        (and (member-set u) (member-set v))
                        (update v (fnil conj []) u)))
                    {} internal-edges)
        max-rounds (count members)]
    (loop [coloring (canonicalize-coloring intrinsic members)
           rounds 0]
      (let [raw-new
            (into {}
                  (for [m members]
                    [m [(get coloring m)
                        (vec (sort
                              (concat
                               (for [v (get out m)] [:out (get coloring v)])
                               (for [u (get in m)]  [:in  (get coloring u)]))))]]))
            new-coloring (canonicalize-coloring raw-new members)]
        (if (or (= coloring new-coloring) (>= rounds max-rounds))
          coloring
          (recur new-coloring (inc rounds)))))))

(defn- bipartite-pattern
  "Classify the bipartite edge multiset from class `Ci` to class `Cj`
   under directed edges `edges`. Returns one of:

     :fan-in     — |Cj|=1, every member of Ci has an edge to the singleton
     :fan-out    — |Ci|=1, the singleton has edges to every member of Cj
     :bijection  — |Ci|=|Cj|=K, edges form a perfect matching
     :complete   — every Ci-member has edges to every Cj-member
     :partial    — none of the above"
  [Ci Cj edges]
  (let [ij (set (for [[u v] edges :when (and (Ci u) (Cj v))] [u v]))
        |ci| (count Ci)
        |cj| (count Cj)
        sources (set (map first ij))
        targets (set (map second ij))]
    (cond
      ;; Both singletons — treat as fan-in (1 → 1 fits the rendering rule).
      (and (= 1 |ci|) (= 1 |cj|) (seq ij))
      :fan-in

      (and (= 1 |cj|)
           (= sources Ci)
           (every? #(ij [% (first Cj)]) Ci))
      :fan-in

      (and (= 1 |ci|)
           (= targets Cj)
           (every? #(ij [(first Ci) %]) Cj))
      :fan-out

      ;; Bijection: |Ci|=|Cj|=K, exactly K edges, each Ci-member has
      ;; exactly one out-edge and each Cj-member has exactly one in-edge.
      (and (= |ci| |cj|)
           (= |ci| (count ij))
           (= sources Ci)
           (= targets Cj))
      :bijection

      ;; Complete bipartite: |ij| = |Ci| * |Cj|.
      (= (count ij) (* |ci| |cj|))
      :complete

      :else :partial)))

(defn- class-display-name
  "How a class shows up as a target in another class's annotation.
   `pattern` is the bipartite pattern from the source side."
  [color class-map pattern]
  (let [members (get class-map color)
        rep (first (sort members))
        rep-name (-> (last-sid rep) humanize-sid displayify-line)]
    (case pattern
      ;; Singleton target: just the name.
      :fan-in    rep-name
      ;; Bijection: suffix-K convention; same template name.
      :bijection rep-name
      ;; Source is singleton, target is K-class: explicit "K× rep".
      :fan-out   (str (count members) "× " rep-name)
      ;; Complete bipartite: explicit count too.
      :complete  (str (count members) "× " rep-name)
      ;; Default.
      rep-name)))

(defn- aggregate-cycle
  "Try to render `shape` (a `:cycle` or `:prime`) as block-aggregated.
   Returns a seq of tagged lines on success, or nil if the
   faithfulness gate bailed (caller falls back to per-member)."
  [shape children-map depth]
  (let [members  (:order shape)
        edges    (:internal-edges shape)
        coloring (wl-refine members edges children-map)
        class-map (group-by coloring members)
        class-order (vec (distinct (map coloring members)))
        class-pos (zipmap class-order (range))
        class-edge-set (set (for [[u v] edges
                                  :when (not= (coloring u) (coloring v))]
                              [(coloring u) (coloring v)]))
        patterns (into {}
                       (for [[ci cj] class-edge-set]
                         [[ci cj]
                          (bipartite-pattern (set (class-map ci))
                                             (set (class-map cj))
                                             edges)]))
        any-partial? (some #(= :partial %) (vals patterns))]
    (when-not any-partial?
      (apply concat
             (map-indexed
              (fn [i ci]
                (let [class-members (get class-map ci)
                      k (count class-members)
                      rep (first (sort class-members))
                      next-ci (when (< (inc i) (count class-order))
                                (nth class-order (inc i)))
                      spine-edge? (boolean (and next-ci
                                                (class-edge-set [ci next-ci])))
                      ;; off-spine class-edges leaving ci
                      off-spine (for [[a b] class-edge-set
                                      :when (and (= a ci)
                                                 (or (not next-ci)
                                                     (not= b next-ci)))]
                                  [b (get patterns [a b])])
                      ;; Render representative. Apply displayify only when
                      ;; the class has > 1 member — for singletons there's
                      ;; no per-instance index to collapse, and forcing the
                      ;; K-placeholder would lie about the real step names.
                      tagged (render-elt rep children-map depth)
                      tagged' (if (> k 1)
                                (mapv (fn [{:keys [line fall-through? paths]}]
                                        {:line (displayify-line line)
                                         :fall-through? fall-through?
                                         :paths paths})
                                      tagged)
                                tagged)
                      ;; Prepend K× to first line when k > 1; the line
                      ;; represents ALL k members, so :paths is the
                      ;; full class member list.
                      tagged'' (if (and (> k 1) (seq tagged'))
                                 (let [{:keys [line fall-through?]} (first tagged')
                                       leading (re-find #"^\s*" line)
                                       body (subs line (count leading))]
                                   (cons {:line (str leading k "× " body)
                                          :fall-through? fall-through?
                                          :paths (vec class-members)}
                                         (rest tagged')))
                                 tagged')
                      ;; Build annotation suffix from off-spine class-edges
                      annot-strs (for [[cj pattern] off-spine]
                                   (let [pos-source (class-pos ci)
                                         pos-target (class-pos cj)
                                         tname (class-display-name cj class-map pattern)]
                                     (if (< pos-target pos-source)
                                       (str "⮥ " tname)
                                       (str "⮧ " tname))))
                      tagged''' (if (seq annot-strs)
                                  (let [{:keys [line fall-through? paths]} (first tagged'')]
                                    (cons {:line (str line "  " (str/join ", " annot-strs))
                                           :fall-through? fall-through?
                                           :paths paths}
                                          (rest tagged'')))
                                  tagged'')
                      ;; Set fall-through based on class-level spine
                      final (with-fall-through tagged''' spine-edge?)]
                  final))
              class-order)))))

;; ---- Shape-level rendering ----

(defn render-shape
  "Walk a shape (one of `:empty` / `:chain` / `:scatter-gather` /
   `:cycle` / `:prime`) under a child path → child-node lookup.
   Returns a seq of {:line :fall-through?} maps."
  [shape children-map depth]
  (case (:kind shape)
    :empty
    []

    :chain
    (let [order (:order shape)
          n (count order)]
      (apply concat
             (map-indexed
              (fn [i elt]
                (let [rendered (render-elt elt children-map depth)]
                  ;; Inline scatter-gather records inside a chain
                  ;; represent a parallel sub-shape whose source/sink
                  ;; ARE the chain neighbors. The chain neighbors carry
                  ;; the fall-through arrow; the rail rows themselves
                  ;; should stay un-↓ (matches heterogeneous parallel
                  ;; rendering). Skip with-fall-through propagation.
                  (if (and (map? elt) (= :scatter-gather (:kind elt)))
                    rendered
                    (with-fall-through rendered (< i (dec n))))))
              order)))

    :scatter-gather
    ;; Top-level scatter-gather (e.g., as the inner shape of a
    ;; container like cc/parallel). Source has ↓ (real edge to every
    ;; branch). Branches render via the compute-branch-lines helper.
    ;; Sink's ↓ comes from the outer chain's with-fall-through
    ;; propagation if the parent has a chain successor.
    (let [source-lines (with-fall-through
                        (render-elt (:source shape) children-map depth)
                        true)
          branch-lines (compute-branch-lines (:branches shape) children-map depth)
          sink-lines (with-fall-through
                      (render-elt (:sink shape) children-map depth)
                      false)]
      (concat source-lines branch-lines sink-lines))

    (:cycle :prime)
    (or (when *aggregate?*
          (aggregate-cycle shape children-map depth))
        (let [order (:order shape)
              n (count order)
              edges (set (:internal-edges shape))
              anns (classified-annotations shape)
              pre-rendered (mapv
                            (fn [i]
                              (let [elt (nth order i)
                                    next-elt (when (< (inc i) n) (nth order (inc i)))
                                    fall-through? (and next-elt (contains? edges [elt next-elt]))
                                    tagged (render-elt elt children-map depth)
                                    with-anns (attach-annotations tagged (anns elt))]
                                (with-fall-through with-anns fall-through?)))
                            (range n))]
          (apply concat (compress-cycle-order pre-rendered))))))

;; ---- Top-level entry points ----

(defn- finalize
  "Convert tagged lines to printable strings: prepend `↓ ` on fall-through lines,
   `  ` otherwise."
  [tagged]
  (mapv (fn [{:keys [line fall-through?]}]
          (str (if fall-through? "↓ " "  ") line))
        tagged))

(defn- tree->tagged
  "Like tree->lines but returns the {:line :fall-through? :paths} structures
   without finalizing to plain strings. Stats consumers use this to
   look up per-path stats by line."
  [tree]
  (let [children-map (into {} (for [c (:children tree)] [(:path c) c]))]
    (vec (render-shape (:shape tree) children-map 0))))

(defn- tree->lines [tree]
  (finalize (tree->tagged tree)))

(defn- shape-tree? [x]
  (and (map? x) (contains? x :shape) (contains? x :children)))

(defn- topology? [x]
  (and (map? x) (contains? x :nodes) (contains? x :edges)))

(defn- stepmap? [x]
  (and (map? x) (map? (:procs x))))

(defn ->tree
  "Coerce input to a shape tree. Accepts a stepmap, a topology, or a
   shape tree (the output of `shape/decompose`)."
  [x]
  (cond
    (shape-tree? x) x
    (topology? x)   (shape/decompose x)
    (stepmap? x)    (shape/decompose (step/topology x))
    :else (throw (ex-info "render/->tree: unrecognized input"
                          {:type (type x)}))))

(defn render
  "Render a pipeline to a seq of lines. `x` may be a stepmap, a
   topology, or a shape tree.

   Opts:
     :aggregate?  when true (default), `:cycle`/`:prime` are
                  block-aggregated via WL refinement; when false, each
                  member renders individually (with consecutive-pattern
                  compression still applied)."
  ([x] (render x nil))
  ([x opts]
   (binding [*aggregate?* (get opts :aggregate? true)]
     (tree->lines (->tree x)))))

(defn render-tagged
  "Like `render` but returns a vec of `{:line :fall-through? :paths}` maps
   instead of finalized strings. Each line carries the step paths it
   represents (typically one; K for block-aggregated lines). Used by
   stats and live-view consumers that look up per-path data per line."
  ([x] (render-tagged x nil))
  ([x opts]
   (binding [*aggregate?* (get opts :aggregate? true)]
     (tree->tagged (->tree x)))))

;; ---- Stats columns: render with per-step counters/latency ----

(defn- humanize-count
  "Compact number → string. 1024 → '1.0k'; 1500000 → '1.5M'."
  [^long n]
  (cond
    (< n 1000) (str n)
    (< n 1000000) (format "%.1fk" (double (/ n 1000.0)))
    (< n 1000000000) (format "%.1fM" (double (/ n 1000000.0)))
    :else (format "%.1fG" (double (/ n 1000000000.0)))))

(defn- humanize-ns
  "Latency in ns → human-readable string."
  [v]
  (when (some? v)
    (let [v (long v)]
      (cond
        (< v 1000) (str v "ns")
        (< v 1000000) (format "%dµs" (long (/ v 1000)))
        (< v 1000000000) (format "%dms" (long (/ v 1000000)))
        :else (format "%.1fs" (double (/ v 1000000000.0)))))))

(defn- pad-right [s n]
  (let [len (count s)]
    (if (>= len n) s (str s (apply str (repeat (- n len) \space))))))

(defn- pad-left [s n]
  (let [len (count s)]
    (if (>= len n) s (str (apply str (repeat (- n len) \space)) s))))

(defn- merged-record
  "Merge K stats records via inline sum + histogram merge."
  [records]
  (if (= 1 (count records))
    (first records)
    (let [sum-key (fn [k] (apply + (map #(or (k %) 0) records)))
          snaps (keep :latency records)
          merged-snap (when (seq snaps)
                        (reduce
                         (fn [a b]
                           ((requiring-resolve 'toolkit.hist/merge-snapshots) a b))
                         snaps))]
      {:recv (sum-key :recv)
       :sent (sum-key :sent)
       :completed (sum-key :completed)
       :queued (sum-key :queued)
       :latency merged-snap})))

(defn- resolve-line-stats
  "For each tagged line, compute the displayable strings for its
   stats cells. Returns nil for lines with no associated path records,
   else `{:queued-str :inflight-str :done-str :mean-str :p50-str
   :p99-str :p100-str}`. Inflight = recv − completed. K-path lines
   merge first; quantiles + max derive from the merged histogram via
   `hist/summary`."
  [tagged stats-map]
  (mapv (fn [{:keys [paths]}]
          (let [paths (or paths [])
                records (keep #(get stats-map %) paths)]
            (when (seq records)
              (let [m   (merged-record records)
                    sum (when-let [snap (:latency m)]
                          ((requiring-resolve 'toolkit.hist/summary) snap))
                    inflight (- (long (:recv m 0)) (long (:completed m 0)))
                    ;; Use ASCII "-" for "no data"; em-dash has
                    ;; ambiguous display width in some terminals,
                    ;; which breaks column alignment.
                    no-data "-"]
                {:queued-str   (humanize-count (:queued m 0))
                 :inflight-str (humanize-count (max 0 inflight))
                 :done-str     (humanize-count (:completed m 0))
                 :mean-str     (or (humanize-ns (some-> sum :mean long)) no-data)
                 :p50-str      (or (humanize-ns (:p50 sum))  no-data)
                 :p99-str      (or (humanize-ns (:p99 sum))  no-data)
                 :p100-str     (or (humanize-ns (:max sum))  no-data)}))))
        tagged))

(def ^:private stats-columns
  [[:queued-str   "queued"]
   [:inflight-str "inflight"]
   [:done-str     "done"]
   [:mean-str     "mean"]
   [:p50-str      "p50"]
   [:p99-str      "p99"]
   [:p100-str     "p100"]])

(defn- finalize-with-stats
  "Like `finalize`, but each line gets stats columns appended with
   each column right-aligned to the widest entry across all rows.
   Columns: recv, done, mean, p50, p99, p100 (max). Latency stats
   derive on demand from the per-record histogram. Lines with no
   associated path records get blank but width-matching cells so the
   column separators (`│`) line up vertically across all rows."
  [tagged stats-map]
  (let [base-strs (mapv (fn [{:keys [line fall-through?]}]
                          (str (if fall-through? "↓ " "  ") line))
                        tagged)
        tree-w (apply max 0 (map count base-strs))
        cells  (resolve-line-stats tagged stats-map)
        col-w  (fn [k] (apply max 0 (map #(count (k %)) (filter some? cells))))
        widths (into {} (for [[k _] stats-columns] [k (col-w k)]))
        ;; Pre-build a blank-but-aligned cells string for path-less lines.
        empty-cells (apply str
                           (for [[k label] stats-columns]
                             (str " │ " label " "
                                  (apply str (repeat (get widths k) " ")))))]
    (mapv (fn [bstr resolved]
            (let [tree-padded (pad-right bstr tree-w)
                  stats-str
                  (cond
                    resolved
                    (apply str
                           (for [[k label] stats-columns]
                             (str " │ " label " "
                                  (pad-left (k resolved) (get widths k)))))
                    (some some? cells) empty-cells
                    :else "")]
              (str tree-padded stats-str)))
          base-strs
          cells)))

(defn render-with-stats
  "Render a pipeline with stats columns appended. `stats-map` is
   `{path → stats-record}` (see `render.stats`). Lines whose paths
   have no entry in the stats-map render with no stats columns.

   Single-path lines look up directly; multi-path lines (K× blocks)
   sum counters and merge histograms before computing percentiles, so
   the displayed numbers reflect the pooled distribution."
  ([x stats-map] (render-with-stats x stats-map nil))
  ([x stats-map opts]
   (binding [*aggregate?* (get opts :aggregate? true)]
     (let [tagged (tree->tagged (->tree x))]
       (finalize-with-stats tagged stats-map)))))

(defn print-pipeline
  "Print an indented nested-list view of `x` to *out*. `x` may be a
   stepmap, a topology, or a shape tree. See `render` for opts."
  ([x] (print-pipeline x nil))
  ([x opts]
   (run! println (render x opts))
   nil))
