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
     • `leaves` is a `c/round-robin-workers` pool — k parallel copies of the leaf
       handler; the local llama.cpp slot pool throttles real concurrency.
     • `gather-all` is a handler-map that accumulates leaf outputs and
       emits the collected vec from `:on-all-input-done` via `msg/merge`,
       carrying tokens forward from every leaf parent.
     • `reducer` does one tree level per invocation; its self-loop is
       the recursion. Token mass converges into the final emission."
  (:require [clojure.string :as str]
            [podcast.llm :as llm]
            [toolkit.datapotamus.combinators.workers :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]))

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

PRONOUN HANDLING — be explicit:
  - \"I\" / \"my\" / \"me\" → cluster under the speaker named in Stage A's `guess` field.
  - \"you\" / \"your\" → cluster under the addressed speaker named in `guess`.
  - \"we\" / \"us\" / \"our\" — these are COLLECTIVE pronouns referring to multiple speakers at once. Stage A may canonicalise them as \"Joe Rogan and Andy Stumpf\" or similar; this is a Stage A artefact, NOT a real entity. When you see such a guess: assign the mention to whichever speaker held the floor in the surrounding paragraphs. If you genuinely cannot tell, drop the mention.
  - \"the show\" / \"your show\" / \"the podcast\" / \"this podcast\" — these refer to the PROGRAM, a SEPARATE entity from the speakers. Cluster them under a \"the podcast\" entity if they appear repeatedly, or drop them as incidental otherwise. NEVER lump them in with Joe Rogan or Andy Stumpf.

WRONG ENTITY EXAMPLE — do not produce this:
  canonical: \"Joe Rogan and Andy Stumpf\"
  aliases: [\"us\", \"Joe Rogan and Andy Stumpf\", \"your show\", \"Joe Rogan's show\"]
This entity conflates two speakers AND the podcast program. The correct output is THREE separate entities: Joe Rogan, Andy Stumpf, and (optionally) the podcast.

ALWAYS CLUSTER the speakers and named persons:
  - Stage A's `guess` column gives a canonical name for each mention (often resolving \"I\" / \"you\" / \"he\" to the actual person using episode metadata). If `guess` is a named person, place, or organization, that mention IS for an entity — cluster it under that name even if no literal name appears in the FOCUS paragraphs. The host and guest of the podcast almost always appear as \"I\" / \"my\" / \"you\" mentions and MUST appear in your registry.
  - The HOST and the GUEST are TWO DIFFERENT people. \"I\" alternates between them depending on whose turn it is in the conversation; the FOCUS paragraphs make this clear by attribution. Stage A's `guess` field disambiguates per-mention — if mention A has guess='Joe Rogan' and mention B has guess='Andy Stumpf', they belong to TWO different entities. NEVER produce a single entity called \"Joe Rogan and Andy Stumpf\" — output Joe Rogan as one entity and Andy Stumpf as another, even when both appear in the same chunk.

For each canonical entity you DO output:
  - entity_id: a stable id you assign (e_001, e_002, ...).
  - canonical: the most specific full name available, or a short noun phrase if no name.
  - summary: ONE sentence (≤ 25 words) describing what this entity IS in the transcript — concrete and grounded in the surrounding paragraphs. Downstream merge steps will read this in place of the transcript.
  - mention_indices: the integer indices (the leftmost number on each MENTIONS row) of the input mentions that belong to this entity. Leave a mention unassigned ONLY when it falls under \"DO NOT CREATE AN ENTITY FOR\" above; every other mention must be clustered into some entity.

If a referent is genuinely ambiguous between two entities you've created, give it its own entity rather than guessing.

Respond with a JSON object conforming to the supplied schema. Output nothing else.")

;; `max-leaf-entities` is paired with `loop-buffer-size` below: it bounds
;; how many entity-msgs a single leaf can dump on the merger's :in port,
;; which (transitively, summed across both bins of the worst-case root
;; pair) bounds the merger's per-pair-process emission count. The loop
;; buffer must be ≥ that emission count or the self-loop deadlocks.
(def ^:private max-leaf-entities 64)

(def ^:private leaf-schema
  {:type "object"
   :properties
   {:entities
    {:type "array"
     :maxItems max-leaf-entities
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

KEY PRINCIPLE: merge requires the SAME REFERENT. Similar behaviour, similar role, or similar archetype is NOT enough. Two entities that are 'both profit-driven' or 'both controlling media' are not the same entity if their canonical names point at different things. Read the rules below before deciding.

DO NOT MERGE WHEN:
  - The canonical names refer to DIFFERENT named people, even if they share a role. The host and guest of a podcast are two different people; both may have 'I', 'you', or 'me' as aliases (because Stage A canonicalised pronouns per chunk to whichever speaker was talking), but if LEFT canonical is 'Joe Rogan' and RIGHT canonical is 'Andy Stumpf', they are DIFFERENT entities — never merge them, no matter how similar the summaries sound. Pronoun-alias overlap is NOT a merge signal.
  - The two sides are different people who share a name component ('Andy Stumpf' vs 'Andy Stumpf's friend' — different people).
  - One is a category, the other an instance ('pharmaceutical companies' vs 'Pfizer').
  - One canonical names a SPECIFIC industry, organisation, or category ('pharmaceutical companies', 'media corporations', 'tech companies'), and the other names a BROADER ABSTRACTION ('corporations', 'the system', 'the establishment', 'the elite'). These are different LEVELS OF GRANULARITY. Even if both summaries describe similar behaviour (profit-driven, controlling media, etc.), the REFERENTS are different. The speakers may rhetorically conflate them — keep them separate so downstream analysis preserves the distinction.
  - Rhetorical similarity is NOT a merge signal. 'Both are profit-driven', 'both control media', 'both are bad actors' describe a SHARED ARCHETYPE, not the SAME REFERENT.
  - Generic noun phrases ('people', 'they', 'some people') with anything specific.
  - Semantic neighbours that are clearly distinct entities ('Cybertruck' vs 'electric car').
  - The two sides are different things that happen to share a topic ('mRNA vaccines linked to cancer' vs 'vaccine injuries' — related concerns but distinct entities).

WRONG MERGE EXAMPLE — do not merge these:
  LEFT  canonical='pharmaceutical companies'
        summary='Profit-driven companies pushing vaccines'
  RIGHT canonical='corporations'
        summary='Profit-driven entities controlling media'
  → DO NOT MERGE. These are different levels of abstraction (one industry vs all companies). The summaries describe similar BEHAVIOUR but the REFERENTS are different. Keep both, separate.

MERGE WHEN any of these hold:
  - One side's canonical (or any alias) matches the other side's canonical (or any alias) verbatim, ignoring case, plural/singular, articles ('the', 'a'), or trivial punctuation. EXAMPLES: 'Trump' = 'the president' if summaries both name him; 'Andy' = 'Andy Stumpf'; 'mRNA vaccine' = 'mRNA vaccines'; 'LA' = 'Los Angeles'; 'Joe' = 'Joe Rogan'.
  - One side's canonical is a clear abbreviation or expansion of the other ('DTP' / 'Diphtheria, tetanus, and pertussis vaccine'; 'mRNA' / 'messenger RNA').
  - The summaries describe the same person/place/organization, even if from different angles (e.g., 'Joe Rogan, the host' + 'the host of the podcast' → merge).

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

;; ============================================================================
;; LLM call wrappers.
;; ============================================================================

(defn- cluster-leaf-call!
  "Return {:entities [entity-record ...] :tokens n :cache c}.
   Entities are returned as a vec (so they can be emitted as separate
   msgs by the leaf step), prefix-id'd with the chunk-id."
  [model-cfg id-key chunk fwd-context chunk-mentions local->global all-mentions]
  (let [content [{:type :text
                  :text (render-leaf-text chunk fwd-context chunk-mentions)}]
        {:keys [value tokens cache]}
        (llm/cached-chat! :tree-leaf model-cfg leaf-system content leaf-schema)
        chunk-id (:chunk-id chunk)
        entities
        (->> (:entities value)
             (filter well-formed-entity?)
             (map (fn [e]
                    (let [globals (->> (:mention_indices e)
                                       (keep #(get local->global %))
                                       distinct
                                       vec)
                          aliases (compute-aliases all-mentions globals)
                          eid     (str (name chunk-id) ":" (:entity_id e))]
                      (cond-> {:entity_id       eid
                               :canonical       (:canonical e)
                               :summary         (:summary e)
                               :aliases         aliases
                               :mention_indices globals}
                        (not= id-key :entity_id) (assoc id-key eid)))))
             vec)]
    {:entities entities :tokens tokens :cache cache}))

(defn- merge-pair-call!
  "One LLM cross-merge call across two registries (id → entity).
   Returns:
     {:outputs       [entity-record ...]   ; merged + carry-through, in order
      :provenance    {output-id [contributing-input-ids]}
      :tokens n :cache c}
   Provenance lets the caller wire `msg/merge` parents per output entity."
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
                           (not= id-key :entity_id) (assoc id-key keep-id))]))
        provenance
        (clojure.core/merge
         ;; Carry-through: each output id has itself as sole contributor.
         (into {} (for [[id _] carry] [id [id]]))
         ;; Merged: keep-id's contributors are all merged ids.
         (into {} (for [m valid
                        :let [ids     (mapv first (:members m))
                              keep-id (first (sort ids))]]
                    [keep-id ids])))
        outputs (vec (vals (clojure.core/merge carry merged)))]
    {:outputs    outputs
     :provenance provenance
     :tokens     tokens
     :cache      cache}))

;; ============================================================================
;; Step constructors.
;; ============================================================================

(defn- explode-step
  "One chunks-vec in → N leaf-input msgs out, one per chunk. Each
   leaf-input carries the per-chunk slice the leaf needs."
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
       (let [leaf-inputs
             (vec
              (for [[pos chunk] (map-indexed vector chunks)]
                (let [cid (:chunk-id chunk)
                      cm  (vec (get by-chunk cid []))
                      l->g (into {}
                                 (map-indexed (fn [i m] [(inc i) (get global-idx m)]))
                                 cm)
                      fwd  (forward-context-paragraphs paragraphs (:focus chunk) k-fwd)]
                  {:chunk          chunk
                   :chunk-pos      pos
                   :chunk-mentions cm
                   :local->global  l->g
                   :fwd-context    fwd
                   :all-mentions   all-mentions
                   :model-cfg      model-cfg
                   :id-key         id-key})))]
         {:out leaf-inputs})))))

(defn- task-from-id-key [id-key]
  (case id-key :entity_id :sentiment :theory_id :conspiracy))

(def ^:private leaf-step
  "Per-chunk leaf clustering; emits one msg per output entity via
   `msg/children`. The last entity carries `:leaf-meta` for tokens/cache.
   An empty chunk emits a single sentinel msg with `:empty? true`.

   Each emitted entity-msg's data shape:
     {:entity {entity-record}      ; canonical, summary, aliases, mention_indices, ...
      :bin-id [0 chunk-pos]        ; level + position; merger pairs by position
      :is-last?  bool              ; true on the last entity for this leaf
      :empty?    bool              ; true if no entities emitted (sentinel)
      :leaf-meta {:tokens n :cache c}  ; only on the last/sentinel msg}"
  (step/step
   :tree-leaf
   {:ins {:in ""} :outs {:out ""}}
   (fn [ctx _state {:keys [chunk chunk-pos chunk-mentions local->global fwd-context
                           all-mentions model-cfg id-key]}]
     (let [t0 (System/nanoTime)
           {:keys [entities tokens cache]}
           (if (empty? chunk-mentions)
             {:entities [] :tokens 0 :cache :pure}
             (cluster-leaf-call! model-cfg id-key chunk fwd-context
                                 chunk-mentions local->global all-mentions))
           ms (long (/ (- (System/nanoTime) t0) 1e6))
           bin-id [0 chunk-pos]
           leaf-meta {:tokens (or tokens 0) :cache cache}]
       (trace/emit ctx
                   {:llm-call {:stage    :tree-leaf
                               :task     (task-from-id-key id-key)
                               :chunk-id (:chunk-id chunk)
                               :model    (:model model-cfg)
                               :tokens   tokens
                               :cache    cache
                               :ms       ms}
                    :inputs   {:chunk-id   (:chunk-id chunk)
                               :n-mentions (count chunk-mentions)}
                    :outputs  {:entity-ids (mapv :entity_id entities)
                               :n-entities (count entities)}})
       (if (empty? entities)
         {:out [(msg/child ctx
                           {:bin-id    bin-id
                            :is-last?  true
                            :empty?    true
                            :leaf-meta leaf-meta})]}
         (let [n (count entities)
               datas (vec
                      (for [[i e] (map-indexed vector entities)]
                        (cond-> {:entity   e
                                 :bin-id   bin-id
                                 :is-last? (= i (dec n))}
                          (= i (dec n)) (assoc :leaf-meta leaf-meta))))]
           {:out (msg/children ctx datas)}))))))

;; ============================================================================
;; Pair-merger: stateful per-bin accumulation, level-aware pairing,
;; per-entity msg/merge or msg/pass on output.
;; ============================================================================

(defn- level-bin-counts
  "Sequence of bin counts per level for a binary merge tree starting
   at N leaves. Always ends with 1.
     N=8  → [8 4 2 1]
     N=3  → [3 2 1]
     N=1  → [1]"
  [n]
  (loop [out [n]]
    (let [cur (peek out)]
      (if (<= cur 1)
        out
        (recur (conj out (quot (inc cur) 2)))))))

(defn- pass-msg
  "Build a pending msg that carries forward `data`, parented on
   `parent-msg`, with the parent's data-id preserved (same logical
   datum). Hand-rolled because msg/pass uses (:msg ctx) and we need
   to pass-through a stashed msg from state."
  [parent-msg data]
  {:msg-id         (random-uuid)
   :data-id        (:data-id parent-msg)
   :msg-kind       :data
   :parent-msg-ids [(:msg-id parent-msg)]
   :data           data
   ::msg/parents   [parent-msg]})

(defn- to-registry
  "Turn a vec of entity records into the id→entity map shape that
   merge-pair-call! consumes. nil-safe (empty vec → empty map)."
  [entities]
  (into (sorted-map)
        (for [e entities] [(:entity_id e) e])))

(defn- emit-output-entity
  "Build one output entity msg for the merger, given:
     parents      — the source-entity msgs that contribute to this output
     entity       — the new entity record
     output-port  — :loop or :final
     bin-id       — next-level bin-id (carry-through pair-id at next level)
     is-last?     — true if this is the last emission for this bin
     leaf-meta    — token/cache metadata to attach if last

   For carry-through (single parent) we preserve `:data-id` via pass-msg.
   For merged (multiple parents) we use `msg/merge` (fresh `:data-id`,
   parents listed)."
  [ctx parents entity bin-id is-last? leaf-meta]
  (let [data (cond-> {:entity   entity
                      :bin-id   bin-id
                      :is-last? is-last?}
               leaf-meta (assoc :leaf-meta leaf-meta))]
    (cond
      (= 1 (count parents))
      (pass-msg (first parents) data)
      :else
      (msg/merge ctx parents data))))

(defn- process-pair
  "Run one merge call (or carry-through if odd singleton). Returns
   `[state' {output-port [output-msgs]}]`."
  [ctx state model-cfg id-key bin-counts left-bin-id right-bin-id has-right?]
  (let [left-bin    (get-in state [:bins left-bin-id])
        right-bin   (when has-right? (get-in state [:bins right-bin-id]))
        next-level  (inc (first left-bin-id))
        next-pos    (quot (second left-bin-id) 2)
        next-bin-id [next-level next-pos]
        is-root?    (= 1 (nth bin-counts next-level))
        port        (if is-root? :final :loop)
        ;; Pop processed bins from state.
        state' (-> state
                   (update :bins dissoc left-bin-id)
                   (update :done-bins disj left-bin-id)
                   (cond-> has-right?
                     (-> (update :bins dissoc right-bin-id)
                         (update :done-bins disj right-bin-id))))
        left-by-id  (zipmap (map :entity_id (:entities left-bin))  (:msgs left-bin))
        right-by-id (when has-right?
                      (zipmap (map :entity_id (:entities right-bin)) (:msgs right-bin)))
        ;; Tokens accumulated at the level we're processing (children).
        accum-tokens (+ (or (:tokens left-bin) 0)
                        (or (:tokens right-bin) 0))]
    (if-not has-right?
      ;; Odd singleton: forward each entity to next level unchanged.
      (let [entities (:entities left-bin)
            msgs     (:msgs left-bin)
            n        (count entities)
            outs (if (zero? n)
                   ;; Empty bin propagates as a sentinel.
                   [(emit-output-entity ctx msgs nil next-bin-id true
                                        {:tokens accum-tokens})]
                   (vec
                    (for [[i [e parent]] (map-indexed vector
                                                     (map vector entities msgs))]
                      (emit-output-entity ctx [parent] e next-bin-id
                                          (= i (dec n))
                                          (when (= i (dec n))
                                            {:tokens accum-tokens})))))]
        [state' {port outs}])
      ;; Both sides present: run LLM merge.
      (let [left-reg  (to-registry (:entities left-bin))
            right-reg (to-registry (:entities right-bin))
            t0 (System/nanoTime)
            {:keys [outputs provenance tokens cache]}
            (merge-pair-call! model-cfg id-key left-reg right-reg)
            ms (long (/ (- (System/nanoTime) t0) 1e6))
            _ (trace/emit ctx
                          {:llm-call {:stage  :tree-merge
                                      :task   (task-from-id-key id-key)
                                      :model  (:model model-cfg)
                                      :tokens tokens
                                      :cache  cache
                                      :ms     ms}
                           :inputs   {:left-bin   left-bin-id
                                      :right-bin  right-bin-id
                                      :left-ids   (mapv :entity_id (:entities left-bin))
                                      :right-ids  (mapv :entity_id (:entities right-bin))}
                           :outputs  {:entity-ids (mapv :entity_id outputs)
                                      :provenance provenance
                                      :n-outputs  (count outputs)}})
            total-tokens (+ accum-tokens (or tokens 0))
            n (count outputs)
            outs (if (zero? n)
                   [(emit-output-entity ctx
                                        (concat (:msgs left-bin) (:msgs right-bin))
                                        nil next-bin-id true
                                        {:tokens total-tokens})]
                   (vec
                    (for [[i e] (map-indexed vector outputs)
                          :let [contrib-ids (get provenance (:entity_id e))
                                parent-msgs (vec (keep #(or (left-by-id %)
                                                            (right-by-id %))
                                                       contrib-ids))]]
                      (emit-output-entity ctx parent-msgs e next-bin-id
                                          (= i (dec n))
                                          (when (= i (dec n))
                                            {:tokens total-tokens})))))]
        [state' {port outs}]))))

(defn- pair-merger-step
  "Stateful pair-merger. Buffers entity-msgs per bin-id. When both
   bins of a pair are complete (both have signaled `:is-last?`), runs
   the merge LLM and emits per-entity outputs on `:loop` or `:final`
   depending on whether the next level is the root.

   Lineage: per-entity outputs use `msg/merge` (multiple parents) when
   the LLM grouped them, or hand-rolled `pass-msg` (data-id preserved)
   for carry-through and odd-singleton."
  [config initial-n]
  (let [model-cfg  (:resolve-model config)
        id-key     (case (:task config)
                     :sentiment  :entity_id
                     :conspiracy :theory_id)
        bin-counts (level-bin-counts initial-n)]
    {:procs
     {:tree-merger
      (step/handler-map
       {:ports   {:ins {:in ""} :outs {:loop "" :final ""}}
        :on-init (fn [] {:bins {} :done-bins #{}})
        :on-data
        (fn [ctx state d]
          (let [in-msg (:msg ctx)
                {:keys [entity bin-id is-last? empty? leaf-meta]} d
                [level position] bin-id
                bin-entry (-> (get-in state [:bins bin-id]
                                      {:msgs [] :entities [] :tokens 0})
                              (update :msgs conj in-msg))
                bin-entry (cond-> bin-entry
                            (and entity (not empty?)) (update :entities conj entity)
                            leaf-meta (update :tokens + (or (:tokens leaf-meta) 0)))
                state-1 (assoc-in state [:bins bin-id] bin-entry)
                state-2 (cond-> state-1
                          is-last? (update :done-bins conj bin-id))]
            (if-not is-last?
              [state-2 msg/drain]
              (let [expected (nth bin-counts level)]
                (if (= 1 expected)
                  ;; This bin IS the root — only bin at its level.
                  ;; Emit its entities directly to :final.
                  (let [bin (get-in state-2 [:bins bin-id])
                        msgs (:msgs bin)
                        ents (:entities bin)
                        n    (count ents)
                        toks (:tokens bin)
                        outs (if (zero? n)
                               [(emit-output-entity ctx msgs nil bin-id true
                                                    {:tokens (or toks 0)})]
                               (vec
                                (for [[i [e parent]]
                                      (map-indexed vector
                                                   (map vector ents msgs))]
                                  (emit-output-entity ctx [parent] e bin-id
                                                      (= i (dec n))
                                                      (when (= i (dec n))
                                                        {:tokens (or toks 0)})))))
                        state' (-> state-2
                                   (update :bins dissoc bin-id)
                                   (update :done-bins disj bin-id))]
                    [state' {:final outs}])
                  ;; Normal case: check if pair partner is also done.
                  (let [pair-pos     (quot position 2)
                        left-pos     (* 2 pair-pos)
                        right-pos    (inc left-pos)
                        has-right?   (< right-pos expected)
                        left-id      [level left-pos]
                        right-id     [level right-pos]
                        left-done?   (contains? (:done-bins state-2) left-id)
                        right-done?  (or (not has-right?)
                                         (contains? (:done-bins state-2) right-id))]
                    (if (and left-done? right-done?)
                      (process-pair ctx state-2 model-cfg id-key
                                    bin-counts left-id right-id has-right?)
                      [state-2 msg/drain])))))))})}
     :conns []
     :in :tree-merger
     :out :tree-merger}))

;; ============================================================================
;; Static graph wiring.
;; ============================================================================

;; Self-loop deadlock guard: the merger emits up to (left-bin + right-bin)
;; entity-msgs in a single pair-process invocation, and those msgs go back
;; into its own :in via the loop edge. If the loop channel buffer can't
;; hold them all at once, the proc thread blocks emitting and (being
;; single-threaded) is the only thing that could drain :in — classic
;; self-loop deadlock. See `self-loop-default-buffer-deadlocks` test for
;; the minimal reproduction.
;;
;; Bound: max single-invocation emissions ≤ sum of all leaf entities,
;; which is bounded by `max-leaf-entities × initial-n`. Even on the full
;; 677-paragraph transcript (~85 chunks) at the schema cap of 64
;; entities/leaf, the worst-case root pair emits ≤ 5440 — `loop-buffer-size`
;; must exceed that. We pick 8192 for headroom.
(def ^:private loop-buffer-size 8192)

(defn- build-graph [config all-mentions paragraphs k-fwd workers initial-n]
  (-> (step/beside
       (explode-step config all-mentions paragraphs k-fwd)
       (c/round-robin-workers :tree-leaves workers leaf-step)
       (pair-merger-step config initial-n))
      (step/connect [:tree-explode :out]  [:tree-leaves :in])
      (step/connect [:tree-leaves :out]   [:tree-merger :in]
                    {:buf-or-n loop-buffer-size})
      ;; The recursion: merger's :loop output feeds back into its own :in.
      (step/connect [:tree-merger :loop]  [:tree-merger :in]
                    {:buf-or-n loop-buffer-size})
      (step/input-at  :tree-explode)
      (step/output-at [:tree-merger :final])))

;; ============================================================================
;; Entry point.
;; ============================================================================

(defn tree-resolve!
  "Datapotamus-driven `:tree` Stage B.

   Final output is a stream of per-entity msgs at `:tree-merger :final`.
   We collect them and assemble the registry by entity id. Per-entity
   token metadata accumulates on the last sibling of each merged bin; the
   sum across all final-port emissions is the total-tokens figure.

   `opts` may carry `:pubsub` (shared raw pubsub for the run) and
   `:flow-id` (string, becomes the outer `[:scope <fid>]` segment so
   subscribers can filter per-task). Both default to a fresh pubsub /
   random uuid when omitted.

   Returns `{:registry … :rejected [] :tokens n :cache :tree}`."
  ([config all-mentions paragraphs chunks]
   (tree-resolve! config all-mentions paragraphs chunks {}))
  ([config all-mentions paragraphs chunks opts]
  (if (empty? chunks)
    {:registry (sorted-map) :rejected [] :tokens 0 :cache :tree}
    (let [k-fwd     (or (:tree-fwd-context config) 2)
          workers   (or (:tree-workers      config) 4)
          initial-n (count chunks)
          graph     (build-graph config all-mentions paragraphs k-fwd workers initial-n)
          {:keys [state outputs error]}
          (flow/run-seq graph [(vec chunks)]
                        (cond-> {}
                          (:pubsub opts)  (assoc :pubsub  (:pubsub opts))
                          (:flow-id opts) (assoc :flow-id (:flow-id opts))))]
      (when (= :failed state)
        (throw (ex-info "tree-resolve flow failed" {:error error})))
      (let [final-msgs (or (first outputs) [])
            entities   (keep :entity final-msgs)
            tokens     (transduce (keep (comp :tokens :leaf-meta)) + 0 final-msgs)
            registry   (into (sorted-map)
                             (for [e entities] [(:entity_id e) e]))]
        {:registry registry
         :rejected []
         :tokens   tokens
         :cache    :tree})))))
