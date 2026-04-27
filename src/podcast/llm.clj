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
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [org.httpkit.client :as http]
            [toolkit.approxmatch :as am]
            [toolkit.llm :as llm]
            [toolkit.llm.anthropic :as anthropic]
            [toolkit.llm.cache :as cache]
            [toolkit.llm.openai :as openai]))

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

(def local-base-url
  "Server root for the local llama.cpp / Ollama box. The OpenAI
   adapter appends `/v1`; other endpoints (`/props`, `/slots`) sit at
   the root."
  "http://192.168.0.10:8080")

(def ^:private local-client
  "llama.cpp / Ollama-compat server on the user's mac mini. The model
   name is set per-call via the unified request, so the same client
   value works for every model the server has loaded."
  (delay (openai/client "ollama" (str local-base-url "/v1"))))

(def ^:private clients-by-base-url
  "Memoised OpenAI-compat clients keyed by base URL. Lets a model-cfg
   say `:base-url \"http://192.168.0.10:8081\"` to route this one
   stage to a different llama.cpp server without touching the global
   active-client binding."
  (atom {}))

(defn- client-for-base-url [base-url]
  (or (get @clients-by-base-url base-url)
      (get (swap! clients-by-base-url
                  (fn [m]
                    (if (contains? m base-url)
                      m
                      (assoc m base-url
                             (openai/client (str "local-" base-url)
                                            (str base-url "/v1"))))))
           base-url)))

(defn detect-slots
  "Probe llama.cpp's /props endpoint and return total_slots, or nil if
   the server doesn't expose the field (i.e. it's not llama.cpp). Used
   to size the worker pool dynamically — a 4-slot box runs 4 chunks
   in parallel; a 1-slot Anthropic-style API stays sequential."
  ([] (detect-slots local-base-url))
  ([server-root]
   (try
     (let [{:keys [status body]} @(http/get (str server-root "/props")
                                            {:timeout 3000})]
       (when (= status 200)
         (-> (json/read-str body :key-fn keyword) :total_slots)))
     (catch Throwable _ nil))))

;; Active LLM client. Edit this binding to swap providers — every stage
;; goes through whichever value is selected here.
(def ^:private llm-client local-client)

(defn- cache-key [stage model system content-parts schema]
  ;; Per-task cache isolation is implicit: `system` (prompt) and `schema`
  ;; differ between tasks at every stage, so the same chunk under sentiment
  ;; vs. conspiracy hashes to distinct keys without `:task` in the mix. If a
  ;; future change deduplicates prompts/schemas across tasks, add :task here.
  (cache/key-bytes-of (str (name stage) "\n--\n" model
                           "\n--\n" system
                           "\n--\n" (pr-str content-parts)
                           "\n--\n" (pr-str schema))))

;; ============================================================================
;; 2. Structured-output call. Provider is fixed to Anthropic for now;
;;    swap by replacing `@anthropic-client` (or thread provider through
;;    the config when we want per-stage provider selection).
;; ============================================================================

(defn- usage-tokens
  "Total tokens consumed, normalised across provider shapes:
   Anthropic uses input_tokens / output_tokens; OpenAI uses
   prompt_tokens / completion_tokens (and exposes total_tokens)."
  [resp]
  (let [u (or (:usage resp) (some-> resp :raw :usage))]
    (long
     (or (:total_tokens u)
         (+ (or (:input_tokens u) (:prompt_tokens u) 0)
            (or (:output_tokens u) (:completion_tokens u) 0))))))

(defn- chat-structured!*
  [{:keys [model max-tokens base-url]} system content-parts schema]
  (let [client (if base-url (client-for-base-url base-url) @llm-client)
        resp (llm/query client
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

(defn- partition-by-pred [pred xs]
  (reduce (fn [acc x] (if (pred x) (update acc :valid conj x) (update acc :rejected conj x)))
          {:valid [] :rejected []} xs))

;; ============================================================================
;; 4. Prompt rendering — paragraphs get pretty-printed once.
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
  "Run Stage A on one chunk.

   `(:episode-metadata config)` (optional) is prepended verbatim to the
   user prompt so Stage A canonicalises speaker self-references
   (\"I\", \"my\") to the actual host/guest names. This gives Stage B's
   leaf clusterer consistent per-chunk inputs to align across chunks."
  [config chunk]
  (let [{:keys [system schema model-cfg]} (mention-call-config config)
        meta-prefix (when-let [m (:episode-metadata config)]
                      (str "EPISODE METADATA (use to canonicalise self-references like \"I\" / \"my\" to host/guest names; do not add metadata-only entities):\n"
                           m
                           "\n\n=====\n\n"))
        content [{:type :text :text (str meta-prefix (render-chunk-text chunk))}]
        {:keys [value tokens cache]}
        (cached-chat! :mentions model-cfg system content schema)
        focus-ids (:focus-ids chunk)
        {:keys [valid rejected]}
        (partition-by-pred #(validate-mention focus-ids %) (:mentions value))]
    {:mentions (mapv #(assoc % :chunk-id (:chunk-id chunk)) valid)
     :rejected rejected
     :tokens   tokens
     :cache    cache}))

(defn resolve-entities!
  "Cluster Stage A's mentions into a canonical registry via the
   Datapotamus tree merge-sort. Leaves cluster per-chunk with
   transcript context; internal nodes pairwise merge using only
   canonical + aliases + summary. Bounded prompts at every level;
   parallelisable.

   Returns `{:registry {entity_id → {...}} :rejected [] :tokens n :cache :tree}`."
  [config all-mentions paragraphs chunks]
  ;; requiring-resolve breaks the otherwise-circular require:
  ;; podcast.tree-resolve uses cached-chat! from this ns.
  (let [tree-resolve! (requiring-resolve 'podcast.tree-resolve/tree-resolve!)]
    (tree-resolve! config all-mentions paragraphs chunks)))

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
