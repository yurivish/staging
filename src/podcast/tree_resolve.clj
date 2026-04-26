(ns podcast.tree-resolve
  "Hierarchical merge-sort Stage B as a STATIC Datapotamus graph.

   The graph topology never changes. The recursion lives on a single
   loop edge — the reducer's `:loop` output feeds back into its own
   `:in`. Each pass through the loop processes one tree level: pair up
   the current registries, run a merge LLM call per pair, hand the
   resulting vector back to the loop. When the vector is down to one
   registry, the reducer emits on `:final`.

   Wiring:

       chunks-vec ─→ explode ─→ leaves (workers k) ─→ gather-all ─→ reducer ─┐
                                                                    ↑   :loop│
                                                                    └────────┘
                                                                    │
                                                                  :final
                                                                    ↓
                                                                  output

     • `explode` mints one child msg per chunk, each carrying the
       per-chunk slice the leaf needs (chunk + chunk-mentions +
       local→global map + forward context).
     • `leaves` is a `c/workers` pool — k parallel copies of the leaf
       handler; the local llama.cpp slot pool throttles real concurrency.
     • `gather-all` is a handler-map that accumulates leaf outputs and
       emits the collected vec from `:on-all-closed` via `msg/merge`,
       carrying tokens forward from every leaf parent.
     • `reducer` does one tree level per invocation; its self-loop is
       the recursion. Token mass converges into the final emission."
  (:require [clojure.string :as str]
            [podcast.llm :as llm]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

;; ============================================================================
;; Prompts and schemas.
;; ============================================================================

(def ^:private leaf-system
  "You are clustering raw entity mentions extracted from one chunk of a podcast transcript.

You see THREE regions in the transcript:
  - CONTEXT (preceding) — paragraphs immediately before the focus, for resolving anaphora that reaches back.
  - FOCUS — the paragraphs whose mentions you must cluster.
  - CONTEXT (following) — a few paragraphs after, for resolving forward references.

Cluster ONLY the mentions listed under \"MENTIONS TO CLUSTER\" — they all come from the FOCUS. The CONTEXT regions exist solely to disambiguate references; do not invent mentions from them.

CLUSTERING RULES — read these carefully:

  CLUSTER TOGETHER:
    - All mentions of the same person/place/organization, including pronouns and noun-phrase paraphrases (\"Andy\" / \"Andy Stumpf\" / \"he\" / \"the guest\").
    - A compound name and its components when the components are introduced as the expansion (\"DTP vaccine\" + \"diphtheria\" + \"tetanus\" + \"pertussis\" = ONE entity, canonical \"DTP vaccine\", with the component names as aliases — UNLESS each component is independently discussed as its own topic).
    - A category and its examples when the speaker is listing instances of that category (\"essential businesses including restaurants, fast food, and media\" = ONE entity \"essential businesses\", with the listed items as aliases — UNLESS one of the listed items becomes its own subject of discussion).
    - Plural / singular / abbreviation / expansion variants of the same referent.

  KEEP SEPARATE:
    - Distinct people, places, organizations, or concepts that share a topic but aren't the same referent.
    - A category and an instance when the instance is independently discussed (\"pharmaceutical companies\" stays separate from \"Pfizer\" if Pfizer comes up as its own topic).

  DO NOT CREATE AN ENTITY FOR:
    - Incidental possessions or body parts of a person already in your registry (\"his teeth\", \"her hand\", \"my keys\") — drop these unless they're the SUBJECT of sustained discussion.
    - Generic stage props or ambient referents mentioned in passing without sustained attention (\"this table\", \"the road\", \"a note\", \"a bag of masks\" if just mentioned once as a prop).
    - Pure rhetorical framings (\"an inconvenient fact\", \"the situation\", \"the claim that X\") — these are descriptions of speech acts, not entities. Drop them.

For each canonical entity you DO output:
  - entity_id: a stable id you assign (e_001, e_002, ...).
  - canonical: the most specific full name available, or a short noun phrase if no name.
  - summary: ONE sentence (≤ 25 words) describing what this entity IS in the transcript — concrete and grounded in the surrounding paragraphs. Downstream merge steps will read this in place of the transcript.
  - mention_indices: the integer indices (the leftmost number on each MENTIONS row) of the input mentions that belong to this entity. It is FINE to leave a mention unassigned if it's an incidental referent that doesn't deserve its own entity — not every mention must be clustered.

If a referent is genuinely ambiguous between two entities you've created, give it its own entity rather than guessing.

Respond with a JSON object conforming to the supplied schema. Output nothing else.")

(def ^:private leaf-schema
  {:type "object"
   :properties
   {:entities
    {:type "array"
     :items
     {:type "object"
      :properties
      {:entity_id       {:type "string"
                         :description "Stable id you assign (e_001, e_002, ...)."}
       :canonical       {:type "string"
                         :description "Most specific full name, or a short noun phrase."}
       :summary         {:type "string"
                         :description "One sentence (≤25 words) grounded in the chunk."}
       :mention_indices {:type "array"
                         :description "Indices (1-based) of input mentions belonging to this entity."
                         :items {:type "integer"}}}
      :required ["entity_id" "canonical" "summary" "mention_indices"]}}}
   :required ["entities"]})

(def ^:private merge-system
  "You are merging two sibling registries of canonical entities from a podcast transcript.

Each side has already been deduplicated within itself — your only job is to find LEFT/RIGHT pairs (or small groups) that refer to the SAME real-world entity.

You see, for each entry, an id, a canonical name, the surface aliases observed, and a one-sentence summary written when local transcript context was available. There is NO raw transcript here — rely on the names, aliases, and summaries.

KEY PRINCIPLE: same REFERENT means merge, even if the two sides emphasized different aspects of it. A specific person mentioned with different roles, a vaccine described with different concerns — these are still ONE entity. The summaries describing different aspects is normal and EXPECTED, not a reason to keep them apart. The merged summary should combine both perspectives.

MERGE WHEN any of these hold:
  - One side's canonical (or any alias) matches the other side's canonical (or any alias) verbatim, ignoring case, plural/singular, articles ('the', 'a'), or trivial punctuation. EXAMPLES: 'Trump' = 'the president' if summaries both name him; 'Andy' = 'Andy Stumpf'; 'mRNA vaccine' = 'mRNA vaccines'; 'LA' = 'Los Angeles'; 'Joe' = 'Joe Rogan'.
  - One side's canonical is a clear abbreviation or expansion of the other ('DTP' / 'Diphtheria, tetanus, and pertussis vaccine'; 'mRNA' / 'messenger RNA').
  - The summaries describe the same person/place/organization, even if from different angles (e.g., 'Joe Rogan, the host' + 'the host of the podcast' → merge).

DO NOT MERGE WHEN:
  - The two sides are different people who share a name component ('Andy Stumpf' vs 'Andy Stumpf's friend' — different people).
  - One is a category, the other an instance ('pharmaceutical companies' vs 'Pfizer').
  - Generic noun phrases ('people', 'they', 'some people') with anything specific.
  - Semantic neighbours that are clearly distinct entities ('Cybertruck' vs 'electric car').
  - The two sides are different things that happen to share a topic ('mRNA vaccines linked to cancer' vs 'vaccine injuries' — related concerns but distinct entities).

For each merge group, output:
  - ids: ≥ 2 distinct ids drawn from LEFT and RIGHT (mixed).
  - canonical: the merged canonical name (most specific full form available).
  - summary: ONE sentence (≤ 25 words) reflecting what BOTH sides said about this entity — combine the two perspectives.

If no merges are warranted, output an empty list. Do NOT list entries that should carry through unchanged — they're handled automatically.

Respond with a JSON object conforming to the supplied schema. Output nothing else.")

(def ^:private merge-schema
  {:type "object"
   :properties
   {:merges
    {:type "array"
     :description "Merge groups. Each group has ≥2 distinct ids. Empty list if no merges."
     :items
     {:type "object"
      :properties
      {:ids       {:type "array"
                   :description "Ids from LEFT and/or RIGHT to merge into one entity."
                   :items {:type "string"}
                   :minItems 2
                   :uniqueItems true}
       :canonical {:type "string"}
       :summary   {:type "string"
                   :description "One sentence (≤25 words) for the merged entity."}}
      :required ["ids" "canonical" "summary"]}}}
   :required ["merges"]})

;; ============================================================================
;; Pure helpers.
;; ============================================================================

(defn- compute-aliases [all-mentions indices]
  (let [forms (for [i indices
                    :when (and (integer? i) (< -1 i (count all-mentions)))
                    :let [m (nth all-mentions i)]
                    s [(:surface_form m) (:mention_text m)]
                    :when (and (string? s) (not (str/blank? s)))]
                s)]
    (vec (distinct forms))))

(defn- well-formed-entity? [e]
  (and (string? (:entity_id e))
       (not (str/blank? (:entity_id e)))
       (string? (:canonical e))
       (not (str/blank? (:canonical e)))))

(defn- render-paragraph [{:keys [id timestamp text]}]
  (str "[" id (when timestamp (str " @ " timestamp)) "] " text))

(defn- render-paragraphs [paragraphs]
  (str/join "\n\n" (map render-paragraph paragraphs)))

(defn- render-numbered-mentions [mentions]
  (str/join "\n"
            (map-indexed (fn [i {:keys [paragraph_id surface_form mention_text]}]
                           (format "%d. [%s] surface=%s  guess=%s"
                                   (inc i) paragraph_id
                                   (pr-str surface_form) (pr-str mention_text)))
                         mentions)))

(defn- forward-context-paragraphs
  "Up to k paragraphs after the chunk's focus, drawn from the global
   paragraphs vec by matching the last focus paragraph's id."
  [paragraphs focus k]
  (when (and (seq focus) (pos? k))
    (let [paragraphs (vec paragraphs)
          last-id    (:id (peek focus))
          last-idx   (->> paragraphs
                          (map-indexed vector)
                          (some (fn [[i p]] (when (= (:id p) last-id) i))))]
      (when last-idx
        (subvec paragraphs
                (min (count paragraphs) (inc last-idx))
                (min (count paragraphs) (+ 1 last-idx k)))))))

(defn- render-leaf-text [{:keys [context focus]} fwd-context mentions]
  (str
   (when (seq context)
     (str "=== CONTEXT (preceding) ===\n"
          (render-paragraphs context)
          "\n\n"))
   "=== FOCUS — cluster mentions in these paragraphs ===\n"
   (render-paragraphs focus)
   (when (seq fwd-context)
     (str "\n\n=== CONTEXT (following) ===\n"
          (render-paragraphs fwd-context)))
   "\n\n=== MENTIONS TO CLUSTER (focus only) ===\n"
   (render-numbered-mentions mentions)))

(defn- render-registry-side [side-label registry]
  (str "=== " side-label " ===\n"
       (str/join "\n"
                 (for [[id e] (sort-by key registry)]
                   (format "[%s] canonical=%s  aliases=%s\n        summary: %s"
                           id
                           (pr-str (:canonical e))
                           (str/join "; " (:aliases e))
                           (or (:summary e) "(none)"))))))

(defn- render-merge-text [left right]
  (str (render-registry-side "LEFT"  left)
       "\n\n"
       (render-registry-side "RIGHT" right)))

(defn- prefix-leaf-ids
  "Make leaf-assigned ids globally unique by prepending the chunk-id."
  [registry chunk-id]
  (into (sorted-map)
        (for [[id e] registry]
          (let [new-id (str (name chunk-id) ":" id)]
            [new-id (assoc e :entity_id new-id)]))))

;; ============================================================================
;; LLM call wrappers.
;; ============================================================================

(defn- cluster-leaf-call!
  [model-cfg id-key chunk fwd-context chunk-mentions local->global all-mentions]
  (let [content [{:type :text
                  :text (render-leaf-text chunk fwd-context chunk-mentions)}]
        {:keys [value tokens cache]}
        (llm/cached-chat! :tree-leaf model-cfg leaf-system content leaf-schema)
        registry
        (->> (:entities value)
             (filter well-formed-entity?)
             (map (fn [e]
                    (let [globals (->> (:mention_indices e)
                                       (keep #(get local->global %))
                                       distinct
                                       vec)
                          aliases (compute-aliases all-mentions globals)
                          eid     (:entity_id e)]
                      [eid (cond-> {:entity_id       eid
                                    :canonical       (:canonical e)
                                    :summary         (:summary e)
                                    :aliases         aliases
                                    :mention_indices globals}
                             (not= id-key :entity_id) (assoc id-key eid))])))
             (into (sorted-map)))]
    {:registry (prefix-leaf-ids registry (:chunk-id chunk))
     :tokens   tokens
     :cache    cache}))

(defn- merge-pair-call!
  "One LLM cross-merge call. Returns {:registry :tokens :cache}.
   Carry-through entries (not in any returned merge group) keep their
   ids and content unchanged."
  [model-cfg id-key left right]
  (let [content [{:type :text :text (render-merge-text left right)}]
        {:keys [value tokens cache]}
        (llm/cached-chat! :tree-merge model-cfg merge-system content merge-schema)
        all       (clojure.core/merge left right)
        valid
        (->> (or (:merges value) [])
             (keep (fn [m]
                     (let [members (keep #(when-let [e (get all %)] [% e])
                                         (distinct (:ids m)))]
                       (when (>= (count members) 2)
                         (assoc m :members members))))))
        consumed (set (mapcat (fn [m] (map first (:members m))) valid))
        carry    (apply dissoc all consumed)
        merged
        (into {}
              (for [m valid
                    :let [members (mapv second (:members m))
                          ids     (mapv first (:members m))
                          keep-id (first (sort ids))
                          aliases (vec (distinct (mapcat :aliases members)))
                          mis     (vec (distinct (mapcat :mention_indices members)))
                          canon   (or (not-empty (:canonical m))
                                      (->> members (map :canonical) (remove str/blank?)
                                           (sort-by #(- (count %))) first))
                          summ    (or (not-empty (:summary m))
                                      (->> members (map :summary) (remove str/blank?) first))]]
                [keep-id (cond-> {:entity_id       keep-id
                                  :canonical       canon
                                  :summary         summ
                                  :aliases         aliases
                                  :mention_indices mis}
                           (not= id-key :entity_id) (assoc id-key keep-id))]))]
    {:registry (into (sorted-map) (clojure.core/merge carry merged))
     :tokens   tokens
     :cache    cache}))

;; ============================================================================
;; Step constructors.
;; ============================================================================

(defn- explode-step
  "One chunks-vec in → N leaf-input msgs out, one per chunk. Each
   leaf-input carries the per-chunk slice the leaf needs and (in
   `::expected`) the total count, so the downstream gather step knows
   how many siblings to wait for without depending on done-cascade
   timing."
  [config all-mentions paragraphs k-fwd]
  (let [model-cfg  (:resolve-model config)
        id-key     (case (:task config)
                     :sentiment  :entity_id
                     :conspiracy :theory_id)
        global-idx (into {} (map-indexed (fn [i m] [m i])) all-mentions)
        by-chunk   (group-by :chunk-id all-mentions)]
    (step/step
     :tree-explode
     {:ins {:in ""} :outs {:out ""}}
     (fn [_ctx _state chunks]
       (let [n (count chunks)
             leaf-inputs
             (vec
              (for [chunk chunks]
                (let [cid (:chunk-id chunk)
                      cm  (vec (get by-chunk cid []))
                      l->g (into {}
                                 (map-indexed (fn [i m] [(inc i) (get global-idx m)]))
                                 cm)
                      fwd  (forward-context-paragraphs paragraphs (:focus chunk) k-fwd)]
                  {:chunk          chunk
                   :chunk-mentions cm
                   :local->global  l->g
                   :fwd-context    fwd
                   :all-mentions   all-mentions
                   :model-cfg      model-cfg
                   :id-key         id-key
                   ::expected      n})))]
         {:out leaf-inputs})))))

(def ^:private leaf-step
  (step/step
   :tree-leaf
   (fn [{:keys [chunk chunk-mentions local->global fwd-context
                all-mentions model-cfg id-key ::expected]}]
     (let [out (if (empty? chunk-mentions)
                 {:registry (sorted-map) :tokens 0 :cache :pure}
                 (cluster-leaf-call! model-cfg id-key chunk fwd-context
                                     chunk-mentions local->global all-mentions))]
       ;; Forward the expected-count tag so gather-all can close on count.
       (assoc out ::expected expected)))))

(defn- gather-all-step
  "Buffer leaf outputs. Each leaf input carries `::expected` — the total
   number of siblings produced by `explode` — so we know exactly when
   the level has fully arrived without depending on done-cascade timing.

   On every leaf arrival except the last, return `msg/drain` (stash
   parent ref, suppress auto-signal). On the last, emit one merged msg
   via `msg/merge` listing all stashed parents — synthesis carries
   every leaf's tokens forward into that one merged output."
  []
  {:procs
   {:tree-gather-all
    (step/handler-map
     {:ports   {:ins {:in ""} :outs {:out ""}}
      :on-init (fn [] {:parents [] :registries []})
      :on-data
      (fn [ctx {:keys [parents registries]} d]
        (let [parents'    (conj parents (:msg ctx))
              registries' (conj registries (dissoc d ::expected))
              expected    (::expected d)]
          (if (and expected (= (count parents') expected))
            ;; Last sibling: emit merged.
            [{:parents [] :registries []}
             {:out [(msg/merge ctx parents' (vec registries'))]}]
            ;; Still waiting: stash, suppress auto-signal so tokens
            ;; ride forward via the eventual msg/merge.
            [{:parents parents' :registries registries'} msg/drain])))})}
   :conns []
   :in :tree-gather-all
   :out :tree-gather-all})

(defn- vthread-mapv
  "Parallel mapv on a virtual-thread executor, bounded by `n` in-flight
   tasks (the local server's slot count). Order-preserving."
  [n f coll]
  (let [items (vec coll)]
    (if (empty? items)
      []
      (let [sem (java.util.concurrent.Semaphore. (int n))
            ex  (java.util.concurrent.Executors/newVirtualThreadPerTaskExecutor)]
        (try
          (let [futs (mapv (fn [x]
                             (.acquire sem)
                             (.submit ex
                                      ^java.util.concurrent.Callable
                                      (fn [] (try (f x) (finally (.release sem))))))
                           items)]
            (mapv #(.get ^java.util.concurrent.Future %) futs))
          (finally (.shutdown ex)))))))

(defn- reducer-step
  "One pass per invocation = one tree level. Pair up registries; merge
   each pair via an LLM call; hand the resulting vec back to `:in` via
   the self-loop edge until exactly one registry remains, at which
   point emit on `:final`.

   Within-level parallelism: pairs at the same level run concurrently
   on a virtual-thread executor, capped at `:tree-workers` (default 4
   = local llama.cpp slot count)."
  [config]
  (let [model-cfg (:resolve-model config)
        id-key    (case (:task config)
                    :sentiment  :entity_id
                    :conspiracy :theory_id)
        workers   (or (:tree-workers config) 4)]
    (step/step
     :tree-reducer
     {:ins {:in ""} :outs {:loop "" :final ""}}
     (fn [_ctx _state regs]
       (let [regs (vec regs)]
         (cond
           (<= (count regs) 1)
           {:final [(or (first regs)
                        {:registry (sorted-map) :tokens 0 :cache :tree})]}

           :else
           (let [pairs (partition-all 2 regs)
                 ;; Tokens accumulate up the tree: each merge keeps the
                 ;; sum of its two children's prior tokens plus its own.
                 merged
                 (vthread-mapv
                  workers
                  (fn [p]
                    (if (= 1 (count p))
                      (first p)
                      (let [a (first p) b (second p)
                            r (merge-pair-call! model-cfg id-key
                                                (:registry a) (:registry b))]
                        (assoc r :tokens
                               (+ (or (:tokens a) 0)
                                  (or (:tokens b) 0)
                                  (or (:tokens r) 0))))))
                  pairs)]
             {:loop [merged]})))))))

;; ============================================================================
;; Static graph wiring.
;; ============================================================================

(defn- build-graph [config all-mentions paragraphs k-fwd workers]
  (-> (step/beside
       (explode-step config all-mentions paragraphs k-fwd)
       (c/workers :tree-leaves workers leaf-step)
       (gather-all-step)
       (reducer-step config))
      (step/connect [:tree-explode :out]    [:tree-leaves :in])
      (step/connect [:tree-leaves :out]     [:tree-gather-all :in])
      (step/connect [:tree-gather-all :out] [:tree-reducer :in])
      ;; The recursion: reducer's :loop output feeds back into its own :in.
      (step/connect [:tree-reducer :loop]   [:tree-reducer :in])
      (step/input-at  :tree-explode)
      (step/output-at [:tree-reducer :final])))

;; ============================================================================
;; Entry point.
;; ============================================================================

(defn tree-resolve!
  "Datapotamus-driven `:tree` Stage B. Returns
   `{:registry … :rejected [] :tokens n :cache :tree}`."
  [config all-mentions paragraphs chunks]
  (if (empty? chunks)
    {:registry (sorted-map) :rejected [] :tokens 0 :cache :tree}
    (let [k-fwd   (or (:tree-fwd-context config) 2)
          workers (or (:tree-workers      config) 4)
          graph   (build-graph config all-mentions paragraphs k-fwd workers)
          {:keys [state outputs error]} (flow/run-seq graph [(vec chunks)])]
      (when (= :failed state)
        (throw (ex-info "tree-resolve flow failed" {:error error})))
      (let [final (-> outputs first first)]
        {:registry (or (:registry final) (sorted-map))
         :rejected []
         :tokens   (or (:tokens final) 0)
         :cache    :tree}))))
