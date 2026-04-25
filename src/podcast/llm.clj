(ns podcast.llm
  "LLM glue for the podcast extraction pipeline.

   Three public roles, called once per chunk (mentions, records) or once
   per pipeline (resolve):

     `extract-mentions!`   — list every entity referenced in a chunk's
                             focus paragraphs; context paragraphs are for
                             pronoun resolution only.
     `resolve-entities!`   — cluster the flat mention list across all
                             chunks into a canonical registry. Receives
                             the full transcript as a side document so
                             it can disambiguate speaker-vs-third-party
                             references with ground truth.
     `extract-records!`    — extract task-specific records (sentiment or
                             conspiracy stance) from each chunk's focus,
                             with the registry available as a side input.

   Mechanics:
     - LLM calls go through `toolkit.llm` + provider adapters.
       Provider-neutral, no forced tool-calls — structured output via
       `:response-schema` (Anthropic's `output_config.format`,
       Gemini's `responseMimeType + responseSchema`, etc.).
     - Cache: `toolkit.llm.cache` (LMDB) keyed on
       (stage, model, system, content-parts, schema). Editing one
       role's prompt only invalidates that role's entries.
     - Validation: `paragraph_id` exact-match, `entity_id` set
       membership, `quote` approximate-match (Sellers' algorithm,
       budget proportional to quote length). Records that fail any
       check are dropped, not retried.

   Configs in `podcast.core` parameterise this with sentiment-vs-
   conspiracy prompts/schemas; everything else (model, cache,
   validation, content-part construction) is shared."
  (:require [clojure.string :as str]
            [toolkit.approxmatch :as am]
            [toolkit.llm :as llm]
            [toolkit.llm.anthropic :as anthropic]
            [toolkit.llm.cache :as cache]))

;; ============================================================================
;; 1. Cache + provider client.
;; ============================================================================

(def ^:private cache-store
  (delay
    (let [c (cache/open "cache/podcast/lmdb")]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. ^Runnable (fn [] (cache/close c))))
      c)))

(def ^:private anthropic-client
  (delay (anthropic/client (str/trim (slurp "claude.key")))))

(defn- cache-key [stage model system content-parts schema]
  (cache/key-bytes-of (str (name stage) "\n--\n" model
                           "\n--\n" system
                           "\n--\n" (pr-str content-parts)
                           "\n--\n" (pr-str schema))))

;; ============================================================================
;; 2. Structured-output call. Provider is fixed to Anthropic for now;
;;    swap by replacing `@anthropic-client` (or thread provider through
;;    the config when we want per-stage provider selection).
;; ============================================================================

(defn- usage-tokens [resp]
  (let [u (some-> resp :raw :usage)]
    (long (+ (or (:input_tokens u) 0) (or (:output_tokens u) 0)))))

(defn- chat-structured!*
  [{:keys [model max-tokens]} system content-parts schema]
  (let [resp (llm/query @anthropic-client
                        {:model           model
                         :max-tokens      (or max-tokens 4096)
                         :system          system
                         :messages        [{:role :user :content content-parts}]
                         :response-schema schema})]
    {:value  (or (:structured resp) {})
     :tokens (usage-tokens resp)}))

(defn cached-chat!
  "Cache-wrapped structured chat call. Returns
   `{:value <parsed-json> :tokens n :cache :hit|:miss}`. On a hit,
   `:tokens` is reported as 0 so per-run sums reflect actual spend."
  [stage {:keys [model] :as model-cfg} system content-parts schema]
  (let [k (cache-key stage model system content-parts schema)
        {:keys [value cache]}
        (cache/compute! @cache-store k
                        (fn []
                          (chat-structured!* model-cfg system content-parts schema)))]
    {:value  (:value value)
     :tokens (if (= cache :hit) 0 (:tokens value))
     :cache  cache}))

;; ============================================================================
;; 3. Validation helpers — pure.
;;
;; `quote-matches?` uses Sellers' approximate string matching with a
;; quote-length-proportional budget. The 1/5 ratio is the same heuristic
;; the upstream Go implementation suggests in its `Example`. Lower
;; ratios are stricter; raise the floor to allow trivial typos in short
;; quotes.
;; ============================================================================

(defn quote-matches?
  "True if `quote` appears in `paragraph-text` with edit distance at
   most ~20% of the quote's length (minimum 4 edits to allow short
   typos). Returns false if either input is blank."
  [paragraph-text quote]
  (let [budget (max 4 (quot (count (or quote "")) 5))]
    (am/matches? paragraph-text quote budget)))

(defn- validate-mention [focus-ids m]
  (and (string? (:paragraph_id m))
       (contains? focus-ids (:paragraph_id m))
       (string? (:mention_text m))
       (not (str/blank? (:mention_text m)))))

(defn- validate-record [focus-ids registry-ids paragraphs-by-id id-key m]
  (and (string? (:paragraph_id m))
       (contains? focus-ids (:paragraph_id m))
       (string? (id-key m))
       (contains? registry-ids (id-key m))
       (string? (:quote m))
       (quote-matches? (paragraphs-by-id (:paragraph_id m)) (:quote m))))

(defn- well-formed-entity? [id-key e]
  (and (not (str/blank? (id-key e)))
       (not (str/blank? (:canonical e)))))

(defn- partition-by-pred [pred xs]
  (reduce (fn [acc x] (if (pred x) (update acc :valid conj x) (update acc :rejected conj x)))
          {:valid [] :rejected []} xs))

;; ============================================================================
;; 4. Prompt rendering — paragraphs and mentions get pretty-printed
;;    once each; both configs share these.
;; ============================================================================

(defn- render-paragraph [{:keys [id timestamp text]}]
  (str "[" id " @ " timestamp "] " text))

(defn- render-paragraphs [paragraphs]
  (str/join "\n\n" (map render-paragraph paragraphs)))

(defn- render-chunk-text
  "User text for Stages A and C: focus + (optional) backward context."
  [{:keys [focus context]}]
  (str (when (seq context)
         (str "CONTEXT (do not extract from these — for pronoun resolution only):\n\n"
              (render-paragraphs context)
              "\n\n=====\n\n"))
       "FOCUS (extract from these paragraphs only):\n\n"
       (render-paragraphs focus)))

(defn- render-mentions
  "Compact one-line-per-mention list with stable indices."
  [mentions]
  (str/join "\n"
            (map-indexed (fn [i {:keys [paragraph_id mention_text surface_form]}]
                           (format "%d. [%s] mention=%s surface=%s"
                                   i paragraph_id (pr-str mention_text) (pr-str surface_form)))
                         mentions)))

;; ============================================================================
;; 5. JSON-Schema fragments. Standard shape (`:type "string" :enum [...]`),
;;    so adapters pass through verbatim into provider-native structured
;;    output controls.
;; ============================================================================

(def ^:private mention-schema
  {:type "object"
   :properties
   {:mentions {:type "array"
               :description "All entities referenced IN the focus paragraphs (not the context)."
               :items {:type "object"
                       :properties
                       {:paragraph_id {:type "string"
                                       :description "ID of a FOCUS paragraph (e.g. \"t96-23\"). Must be one of the focus IDs shown."}
                        :mention_text {:type "string"
                                       :description "Canonical name for the entity (e.g. \"Donald Trump\", not \"Trump\" or \"the president\"). If you can't determine a name, use a short noun phrase (\"the unnamed cancer patient\")."}
                        :surface_form {:type "string"
                                       :description "The exact word(s) used in the focus paragraph — e.g. \"him\", \"the president\", \"that thing\"."}}
                       :required ["paragraph_id" "mention_text" "surface_form"]}}}
   :required ["mentions"]})

(defn- resolve-schema [id-key item-noun]
  {:type "object"
   :properties
   {:entities {:type "array"
               :description (str "Canonical " item-noun "s, clustered from the input mentions.")
               :items {:type "object"
                       :properties
                       {id-key {:type "string"
                                :description (str "Stable ID for this " item-noun " (you assign — \"e_001\", \"e_002\", ...).")}
                        :canonical {:type "string"
                                    :description (str "The canonical name (or short noun phrase) for this " item-noun ".")}
                        :mention_indices {:type "array"
                                          :description "Indices (0-based) of input mentions that refer to this entity."
                                          :items {:type "integer"}}}
                       :required [(name id-key) "canonical" "mention_indices"]}}}
   :required ["entities"]})

(defn- record-schema [id-key extra-props extra-required]
  {:type "object"
   :properties
   {:records {:type "array"
              :items {:type "object"
                      :properties
                      (merge
                       {:paragraph_id {:type "string"
                                       :description "ID of the focus paragraph this record is grounded in."}
                        id-key        {:type "string"
                                       :description "ID from the supplied registry."}
                        :quote        {:type "string"
                                       :description "A short verbatim quote from the cited paragraph supporting this record. Must appear in the paragraph text."}}
                       extra-props)
                      :required (into ["paragraph_id" (name id-key) "quote"] extra-required)}}}
   :required ["records"]})

(def ^:private sentiment-record-schema
  (record-schema :entity_id
                 {:polarity {:type "string"
                             :enum ["positive" "negative" "neutral" "mixed"]
                             :description "Speaker's overall valence toward the entity in this passage."}
                  :emotion  {:type "string"
                             :description "Short phrase capturing the specific emotion (e.g. \"contempt\", \"affectionate\", \"worried\")."}
                  :rationale {:type "string"
                              :description "One sentence explaining why this is the appraisal expressed in the quote."}}
                 ["polarity" "emotion" "rationale"]))

(def ^:private conspiracy-record-schema
  (record-schema :theory_id
                 {:stance {:type "string"
                           :enum ["introduces" "asserts" "doubts" "elaborates" "references" "debunks" "neutral"]
                           :description "Speaker's stance toward the theory in this passage."}
                  :summary {:type "string"
                            :description "One sentence summary of what the speaker said about the theory in this passage."}}
                 ["stance" "summary"]))

;; ============================================================================
;; 6. System prompts. Updated to reference the structured-output
;;    contract instead of tool calls.
;; ============================================================================

(def ^:private sentiment-mention-system
  "You are extracting mentions of PEOPLE, PLACES, and THINGS from a podcast transcript chunk.

You will be given a FOCUS region of paragraphs and a CONTEXT region above it. Extract every entity that the speakers reference in the FOCUS paragraphs. The CONTEXT is provided so you can resolve pronouns and noun-phrases that reach back to earlier paragraphs — DO NOT output mentions whose paragraph_id is in the context.

For each mention in the focus:
- paragraph_id: the EXACT id of the focus paragraph where the mention appears.
- mention_text: a canonical name when possible, resolving \"he/she/it/that thing\" using the context. Use a short noun phrase if no name is available.
- surface_form: the exact wording in the focus paragraph (could be a name, a pronoun, or a description).

Be COMPLETE. List every distinct mention, including repeated mentions in different paragraphs. If the same person is mentioned twice in different focus paragraphs, output two mentions.

Respond with a JSON object conforming to the supplied schema. Output nothing else.")

(def ^:private conspiracy-mention-system
  "You are extracting mentions of CONSPIRACY THEORIES, FRINGE CLAIMS, or CONTESTED EXPLANATIONS from a podcast transcript chunk.

What counts: theories about hidden coordination (depopulation, cover-ups), fringe medical/scientific claims (vaccine harms, lab origins), economic-political conspiracies (media-pharma collusion, deep state, election fraud), historical or paranormal claims, and any other narrative the speakers treat as a contested explanation rather than mainstream consensus. Include both theories the speakers ENDORSE and ones they REJECT or DOUBT.

You will be given a FOCUS region and a CONTEXT region above it. Extract theories REFERENCED in the focus. The context helps you resolve back-references like \"that thing they were saying\" or \"this concept\" — DO NOT output mentions whose paragraph_id is in the context.

For each mention:
- paragraph_id: the EXACT id of the focus paragraph.
- mention_text: a canonical short statement of the theory (a noun phrase like \"vaccines used for population reduction\" or \"pharma controls media coverage\"). If the theory is referenced anaphorically, restate it.
- surface_form: the exact wording in the focus paragraph.

Be COMPLETE — list every distinct theory reference, including repeated references in different focus paragraphs.

Respond with a JSON object conforming to the supplied schema. Output nothing else.")

(def ^:private sentiment-resolve-system
  "You are clustering raw entity mentions from a podcast transcript into a canonical registry.

You are given THREE inputs:
1. The full episode transcript as a document — paragraphs are tagged with their IDs (e.g. [t96-23]). The episode metadata in `context` names the host and guest. Use the transcript to verify referents whenever you're uncertain.
2. Episode metadata in the document's `context` field, which names the host and guest.
3. A numbered list of mentions extracted per chunk. Each row has a paragraph_id, a `surface_form` (the literal text from that paragraph — a name, a pronoun, a description) and a `mention_text` (the per-chunk extractor's guess at canonicalisation, which can be inconsistent across mentions of the same person — including hallucinated names or wrong attributions).

Cluster mentions into canonical entities:
- The HOST and the GUEST are the two speakers; the metadata gives their names. \"I\" / \"me\" / \"my\" alternate between them depending on conversational turn — read the surrounding paragraphs in the transcript to decide which speaker each self-reference belongs to.
- A name that appears in the transcript may refer to a third party even if some chunks' mention_text guessed it was the speaker. Verify with the document.
- Pronouns (\"him\", \"that guy\") attach to the most recent named referent, which may sit a few paragraphs above the mention's own paragraph_id.
- \"Trump\", \"the president\", \"him\" (when context indicates Trump) all belong together. \"Bill Gates\" and \"Bezos\" are separate entities even if both are billionaires.

For each canonical entity, output:
- entity_id: a stable ID you assign (e_001, e_002, ...).
- canonical: the most specific full name available, or a noun phrase if no name (e.g. \"the unnamed cancer patient\").
- mention_indices: the integer indices (0-based) of the input mentions that belong to this entity. Aliases will be derived from these — DO NOT include an alias field.

If a mention is genuinely ambiguous (you can't tell from the transcript which of two entities it refers to), give it its own entity. It's better to over-split than to merge wrongly.

Respond with a JSON object conforming to the supplied schema. Output nothing else.")

(def ^:private conspiracy-resolve-system
  "You are clustering raw conspiracy-theory mentions from a podcast transcript into a canonical registry.

You are given THREE inputs:
1. The full episode transcript as a document — paragraphs are tagged with their IDs (e.g. [t96-23]). Use the transcript to disambiguate when two mentions might refer to the same theory.
2. Episode metadata in the document's `context` field.
3. A numbered list of mentions, each with a paragraph_id, a `surface_form` (the literal text from that paragraph) and a `mention_text` (the per-chunk extractor's canonicalisation, which can be inconsistent across mentions of the same theory).

Cluster mentions into canonical theories:
- \"vaccines reduce population\" and \"depopulation through immunisation\" are the same theory.
- \"vaccine injuries hidden by media\" is a DIFFERENT theory than \"vaccines reduce population\" — keep them separate.
- An anaphoric reference (\"that idea\", \"this concept\") attaches to whichever theory was most recently discussed in the surrounding paragraphs — read the transcript to decide which.

For each canonical theory, output:
- theory_id: a stable ID you assign (e_001, e_002, ...).
- canonical: a short noun-phrase statement of the theory.
- mention_indices: the integer indices of the input mentions that belong to this theory. Aliases will be derived from these — DO NOT include an alias field.

When in doubt, prefer over-splitting (two narrow theories) to over-merging (one fuzzy umbrella). It's easier to merge later than to disentangle.

Respond with a JSON object conforming to the supplied schema. Output nothing else.")

(def ^:private sentiment-record-system
  "You are extracting expressed SENTIMENT toward specific entities from a podcast transcript chunk.

You will be given:
- a CONTEXT region (for pronoun resolution; do NOT extract from it),
- a FOCUS region (extract only from these paragraphs),
- a REGISTRY of canonical entities mentioned in the episode (with their aliases).

For each passage in the FOCUS where a speaker expresses an attitude, emotion, or value-judgement about an entity in the REGISTRY, output one record:
- paragraph_id: the focus paragraph (must be a focus id).
- entity_id: the registry id this record is about.
- quote: a SHORT verbatim quote from that paragraph supporting your reading. The quote must appear in the paragraph text — do not paraphrase.
- polarity: positive | negative | neutral | mixed.
- emotion: a short phrase (one or two words) capturing the specific feeling.
- rationale: one sentence explaining the appraisal in your own words.

Match anaphoric references (\"he\", \"that guy\", \"those people\") to the right entity using the CONTEXT and the REGISTRY's aliases. If a passage discusses an entity but expresses no clear attitude, skip it. If the same paragraph contains attitudes toward multiple entities, output one record per entity.

Respond with a JSON object conforming to the supplied schema. Output nothing else.")

(def ^:private conspiracy-record-system
  "You are extracting DISCUSSION of conspiracy theories from a podcast transcript chunk.

You will be given:
- a CONTEXT region (for pronoun resolution; do NOT extract from it),
- a FOCUS region (extract only from these paragraphs),
- a REGISTRY of canonical theories referenced in the episode.

For each passage in the FOCUS where a speaker discusses a theory in the REGISTRY (introduces, asserts, doubts, elaborates, debunks, or references it), output one record:
- paragraph_id: the focus paragraph.
- theory_id: the registry id.
- quote: a SHORT verbatim quote supporting your reading. Must appear in the paragraph text.
- stance: introduces | asserts | doubts | elaborates | references | debunks | neutral.
   - introduces  = first time the theory enters the conversation in this chunk.
   - asserts     = speaker affirms or treats the theory as true.
   - doubts      = speaker pushes back, expresses skepticism, or distances.
   - elaborates  = speaker adds detail, evidence, or implications.
   - references  = passing mention without commitment.
   - debunks     = speaker explicitly calls it wrong / a misconception.
   - neutral     = discussed without a clear stance.
- summary: one sentence in your own words capturing what was said about the theory.

Match anaphoric references (\"that thing\", \"this idea\") to the right theory via the CONTEXT and the REGISTRY's aliases. Output one record per (paragraph, theory) pair when both are present.

Respond with a JSON object conforming to the supplied schema. Output nothing else.")

;; ============================================================================
;; 7. Per-stage call configs — pull the right system + schema +
;;    model-cfg out of the user-supplied config based on :task.
;; ============================================================================

(defn- mention-call-config [{:keys [task] :as config}]
  {:system    (case task :sentiment sentiment-mention-system :conspiracy conspiracy-mention-system)
   :schema    mention-schema
   :model-cfg (:mention-model config)})

(defn- resolve-call-config [{:keys [task] :as config}]
  (let [id-key (case task :sentiment :entity_id :conspiracy :theory_id)
        item   (case task :sentiment "entity"    :conspiracy "theory")]
    {:system    (case task :sentiment sentiment-resolve-system :conspiracy conspiracy-resolve-system)
     :schema    (resolve-schema id-key item)
     :id-key    id-key
     :model-cfg (:resolve-model config)}))

(defn- record-call-config [{:keys [task] :as config}]
  {:system    (case task :sentiment sentiment-record-system :conspiracy conspiracy-record-system)
   :schema    (case task :sentiment sentiment-record-schema :conspiracy conspiracy-record-schema)
   :id-key    (case task :sentiment :entity_id :conspiracy :theory_id)
   :model-cfg (:record-model config)})

;; ============================================================================
;; 8. Public extraction functions. Each returns
;;    `{<output-key> [...] :rejected [...] :tokens n :cache :hit|:miss}`.
;; ============================================================================

(defn extract-mentions!
  "Run Stage A on one chunk."
  [config chunk]
  (let [{:keys [system schema model-cfg]} (mention-call-config config)
        content [{:type :text :text (render-chunk-text chunk)}]
        {:keys [value tokens cache]}
        (cached-chat! :mentions model-cfg system content schema)
        focus-ids (:focus-ids chunk)
        {:keys [valid rejected]}
        (partition-by-pred #(validate-mention focus-ids %) (:mentions value))]
    {:mentions (mapv #(assoc % :chunk-id (:chunk-id chunk)) valid)
     :rejected rejected
     :tokens   tokens
     :cache    cache}))

(defn- compute-aliases
  "Distinct (surface_form ∪ mention_text) strings observed for an
   entity, in stable order."
  [all-mentions indices]
  (let [forms (for [i indices
                    :when (and (integer? i) (< -1 i (count all-mentions)))
                    :let [m (nth all-mentions i)]
                    s [(:surface_form m) (:mention_text m)]
                    :when (and (string? s) (not (str/blank? s)))]
                s)]
    (vec (distinct forms))))

(defn resolve-entities!
  "Run Stage B once over all mentions. Sends the full transcript as a
   side document so the resolver can disambiguate referents with
   ground truth."
  [config all-mentions paragraphs description]
  (let [{:keys [system schema id-key model-cfg]} (resolve-call-config config)
        document-part {:type        :document
                       :source-kind :blocks
                       :blocks      (mapv #(str "[" (:id %) "] " (:text %)) paragraphs)
                       :title       "Episode transcript"
                       :context     (when description (subs description 0 (min (count description) 800)))}
        text-part     {:type :text
                       :text (str "Mentions to cluster (indices match the FOCUS list):\n\n"
                                  (render-mentions all-mentions))}
        {:keys [value tokens cache]}
        (cached-chat! :resolve model-cfg system [document-part text-part] schema)
        {:keys [valid rejected]}
        (partition-by-pred #(well-formed-entity? id-key %) (:entities value))
        registry (into {} (map (fn [e]
                                 [(id-key e)
                                  (assoc e :aliases
                                         (compute-aliases all-mentions
                                                          (:mention_indices e)))]))
                       valid)]
    {:registry registry
     :rejected rejected
     :tokens   tokens
     :cache    cache}))

(defn- render-registry [id-key registry]
  (str/join "\n"
            (for [[id e] (sort-by key registry)]
              (format "- %s: %s  (aliases: %s)"
                      id
                      (:canonical e)
                      (str/join ", " (:aliases e))))))

(defn extract-records!
  "Run Stage C on one chunk with the registry as a side input."
  [config chunk registry]
  (let [{:keys [system schema id-key model-cfg]} (record-call-config config)
        user-text (str "REGISTRY:\n" (render-registry id-key registry)
                       "\n\n=====\n\n"
                       (render-chunk-text chunk))
        content [{:type :text :text user-text}]
        {:keys [value tokens cache]}
        (cached-chat! :records model-cfg system content schema)
        focus-ids        (:focus-ids chunk)
        registry-ids     (set (keys registry))
        paragraphs-by-id (into {} (map (juxt :id :text)) (:focus chunk))
        {:keys [valid rejected]}
        (partition-by-pred #(validate-record focus-ids registry-ids paragraphs-by-id id-key %)
                           (:records value))]
    {:records  (mapv #(assoc % :chunk-id (:chunk-id chunk)) valid)
     :rejected rejected
     :tokens   tokens
     :cache    cache}))
