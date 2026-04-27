(ns podcast.cc
  "Per-step Claude Code variant of the podcast extraction pipeline.

   Two CC calls per task: scout reads the whole transcript and emits a
   canonical registry; extractor reads transcript + registry.json and
   emits task records. Tasks share an agent-cwd inside out-cc/N/.
   Validation (paragraph_id ∈ transcript, entity_id ∈ registry, quote
   matches via Sellers) is the same as podcast.core's; aggregation reuses
   pc/aggregate.

   Defaults to llama.cpp's Anthropic-compatible /v1/messages on the user's
   Mac Mini (no proxy). Cloud Sonnet is a comparison flavor."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [podcast.core :as pc]
            [podcast.llm :as llm]
            [toolkit.datapotamus.claude-code :as cc-step]
            [toolkit.llm.cache :as cache])
  (:import [java.security MessageDigest]
           [java.nio.charset StandardCharsets]))

;; ============================================================================
;; 1. Key conversion. cc-step/run! parses JSON with snake_case → kebab-case
;;    keywords. We rewrite back so the rest of this pipeline (and the
;;    output JSON) match the existing podcast pipeline shape.
;; ============================================================================

(defn- kebab->snake [k]
  (if (keyword? k) (keyword (str/replace (name k) #"-" "_")) k))

(defn- snake-keys
  "Recursively rewrite keyword keys from kebab-case back to snake_case."
  [x]
  (cond
    (map? x)        (into {} (map (fn [[k v]] [(kebab->snake k) (snake-keys v)]) x))
    (sequential? x) (mapv snake-keys x)
    :else           x))

(defn- registry->string-keys
  "Registry keys are entity_ids — keep them as strings (matches the existing
   pipeline's output shape, which uses string ids in JSON keys)."
  [reg]
  (into (sorted-map) (map (fn [[k v]] [(name k) (snake-keys v)]) reg)))

(defn- strip-md-fences
  "Some local models wrap JSON in ```json ... ``` even when told not to.
   Strip leading/trailing fences so we can parse the inner document."
  ^String [^String s]
  (-> s
      (str/replace #"(?s)\A\s*```(?:json|JSON)?\s*\n?" "")
      (str/replace #"(?s)\n?\s*```\s*\z" "")
      str/trim))

(defn- structured-output-or-result
  "Claude Code's `--json-schema` is supposed to populate :structured-output
   on the result map, but with llama.cpp's Anthropic shim the agent can
   end up emitting JSON in :result instead (sometimes wrapped in ```json```
   fences). Prefer :structured-output when present and non-empty; fall
   back to parsing :result.

   :structured-output comes back kebab-cased (cc-step/run! does that).
   The fallback parses gemma's literal output, which uses the snake_case
   keys we asked for in the schema; we run snake-keys at the call site
   either way, so the kebab→snake step here uses cc-step's same
   convention to keep both branches uniform."
  [run-result]
  (let [so (:structured-output run-result)]
    (if (and so (or (and (map? so) (seq so))
                    (and (sequential? so) (seq so))))
      so
      (try
        (-> (or (:result run-result) "")
            strip-md-fences
            ;; Snake_case kept verbatim (no kebab transform). snake-keys
            ;; downstream is a no-op for snake-cased keys but converts
            ;; kebab back to snake for the :structured-output branch.
            (json/read-str :key-fn keyword))
        (catch Throwable _ nil)))))

;; ============================================================================
;; 2. Cache. Separate LMDB store from podcast.llm so they never collide.
;; ============================================================================

(def ^:private cache-store
  (delay
    (let [c (cache/open "cache/podcast-cc/lmdb")]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. ^Runnable #(cache/close c)))
      c)))

(defn- sha256-hex [^String s]
  (let [b (.digest (MessageDigest/getInstance "SHA-256")
                   (.getBytes s StandardCharsets/UTF_8))
        sb (StringBuilder.)]
    (doseq [^Byte by b] (.append sb (format "%02x" (bit-and by 0xff))))
    (str sb)))

(defn- cc-cache-key [call-tag opts inputs-hash]
  (cache/key-bytes-of
    (str (name call-tag) "\n--\n"
         (or (:model opts) "")                  "\n--\n"
         (or (:append-system-prompt opts) "")   "\n--\n"
         (pr-str (or (:json-schema opts) {}))   "\n--\n"
         (pr-str (or (:allowed-tools opts) [])) "\n--\n"
         inputs-hash                            "\n--\n"
         (or (:prompt opts) ""))))

(defn cached-cc-run!
  "Run cc-step/run! with LMDB caching keyed on call-tag + opts + inputs-hash.
   `inputs-hash` is whatever side-input fingerprint the caller wants — for
   scout it's the transcript hash; for extractor it's transcript+registry
   so that a re-run with a different registry.json doesn't hit a stale
   miss from the previous registry. Returns the result map with
   `:cache :hit|:miss` added."
  [call-tag inputs-hash opts]
  (let [k (cc-cache-key call-tag opts inputs-hash)
        {:keys [value cache]}
        (cache/compute! @cache-store k
                        (fn [] (cc-step/run! opts)))]
    (assoc value :cache cache)))

;; ============================================================================
;; 3. Per-run agent scratch dir. Three files: paragraphs.txt (Grep-friendly
;;    flat view), transcript.json (raw), episode.txt (description).
;; ============================================================================

(defn- render-paragraph [{:keys [id timestamp text]}]
  (str "[" id " @ " timestamp "] " text))

(defn prepare-agent-cwd!
  "Write paragraphs.txt + transcript.json + episode.txt into <run-dir>/agent-cwd/.
   Returns [cwd-abs-path transcript-hash]."
  [run-dir transcript-doc paras]
  (let [cwd  (io/file run-dir "agent-cwd")
        _    (.mkdirs cwd)
        ptxt (str/join "\n" (map render-paragraph paras))
        tjson (json/write-str (assoc transcript-doc :paragraphs paras))
        desc (or (:description transcript-doc) "")]
    (spit (io/file cwd "paragraphs.txt") ptxt)
    (spit (io/file cwd "transcript.json") tjson)
    (spit (io/file cwd "episode.txt") desc)
    [(.getAbsolutePath cwd) (sha256-hex ptxt)]))

;; ============================================================================
;; 4. Schemas — registry-only for scout; records-only for extractor.
;; ============================================================================

(def ^:private scout-schema
  {:type "object"
   :required ["registry"]
   :properties
   {:registry
    {:type "object"
     :description "Map from entity_id (use sequential ids: e_001, e_002, ...) to entity record."
     :additionalProperties
     {:type "object"
      :required ["canonical" "aliases" "summary"]
      :properties
      {:canonical {:type "string"
                   :description "Canonical name (e.g. \"Donald Trump\", not \"Trump\" or \"the president\"). For unnamed entities use a short noun phrase."}
       :aliases   {:type "array" :items {:type "string"}
                   :description "Surface forms used in the transcript: pronouns, partial names, descriptions."}
       :summary   {:type "string"
                   :description "≤25 words explaining who or what this entity is."}}}}}})

(def ^:private sentiment-extractor-schema
  {:type "object"
   :required ["records"]
   :properties
   {:records
    {:type "array"
     :items
     {:type "object"
      :required ["paragraph_id" "entity_id" "quote" "polarity" "emotion" "rationale"]
      :properties
      {:paragraph_id {:type "string" :description "Paragraph id where the sentiment is expressed (e.g. \"t96-23\"). Must be a real id from the transcript."}
       :entity_id    {:type "string" :description "Registry id (e.g. \"e_001\")."}
       :quote        {:type "string" :description "Verbatim short quote from the cited paragraph supporting your reading. Must appear in the paragraph text exactly."}
       :polarity     {:type "string" :enum ["positive" "negative" "neutral" "mixed"]}
       :emotion      {:type "string" :description "Short phrase capturing the specific feeling (e.g. \"contempt\", \"affectionate\", \"worried\")."}
       :rationale    {:type "string" :description "One sentence explaining your reading."}}}}}})

;; ============================================================================
;; 5. System prompts.
;; ============================================================================

(def ^:private scout-system-sentiment
  "You are building a registry of every PERSON, PLACE, or THING that any speaker references in a podcast transcript, for later sentiment analysis.

The full transcript is in your working directory:
  - paragraphs.txt  — one paragraph per line, prefixed [<id> @ <timestamp>]
  - transcript.json — same data plus episode metadata (description, previous episodes)
  - episode.txt     — episode description

Walk the transcript using Read and Grep. Use TodoWrite to track which paragraph ranges you have reviewed. Be EXHAUSTIVE — the goal is to catalog every entity any speaker references.

For each distinct entity, emit:
  - canonical: standard name (\"Donald Trump\", not \"Trump\"). For unnamed entities, a short noun phrase.
  - aliases:   every surface form the transcript uses — pronouns, partial names, descriptions. Use Grep to find them.
  - summary:   ≤25 words on who/what this entity is.

Use sequential ids as the registry keys: e_001, e_002, e_003, ...

When you have walked the entire transcript, emit ONLY a raw JSON object with the registry conforming to the supplied schema. Do NOT wrap your output in markdown code fences (no ```json``` blocks). Do NOT include any prose, preamble, or explanation. Output the JSON object and nothing else.")

(def ^:private extractor-system-sentiment
  "You are extracting expressed SENTIMENT toward entities from a podcast transcript.

Your working directory contains:
  - paragraphs.txt  — one paragraph per line, prefixed [<id> @ <timestamp>]
  - transcript.json — same data plus episode metadata
  - registry.json   — canonical entities for this episode (read this FIRST)

For each passage where a speaker expresses an attitude, emotion, or value-judgement about an entity in the registry, emit one record:
  - paragraph_id: the paragraph id where the sentiment appears (must be a real id from paragraphs.txt).
  - entity_id:    the registry id this is about.
  - quote:        a SHORT verbatim quote from that paragraph supporting your reading. Must appear in the paragraph text — do NOT paraphrase.
  - polarity:     positive | negative | neutral | mixed
  - emotion:      short phrase (\"contempt\", \"affectionate\", \"worried\")
  - rationale:    one sentence explaining the reading.

Resolve anaphoric references (\"he\", \"that guy\") to the right entity using the registry's aliases plus surrounding paragraphs (Read or Grep neighbors).

Use TodoWrite to track which paragraph ranges you have processed. Walk the entire transcript. If a paragraph discusses an entity but expresses no clear attitude, skip it. If a paragraph contains attitudes toward multiple entities, emit one record per entity.

Emit ONLY a raw JSON object conforming to the supplied schema. Do NOT wrap your output in markdown code fences (no ```json``` blocks). Do NOT include any prose, preamble, or explanation. Output the JSON object and nothing else.")

;; ============================================================================
;; 6. Validation — same invariants as podcast.llm's per-stage validators.
;; ============================================================================

(defn validate-records
  "Split records into :valid / :rejected:
     paragraph_id ∈ valid-pids
     entity_id    ∈ registry-ids
     quote-matches? against the cited paragraph text"
  [records valid-pids registry-ids paragraphs-by-id]
  (reduce
    (fn [acc r]
      (let [pid (:paragraph_id r)
            eid (:entity_id r)
            q   (:quote r)
            ok? (and (string? pid)
                     (contains? valid-pids pid)
                     (string? eid)
                     (contains? registry-ids eid)
                     (string? q)
                     (llm/quote-matches? (paragraphs-by-id pid) q))]
        (update acc (if ok? :valid :rejected) conj r)))
    {:valid [] :rejected []}
    records))

;; ============================================================================
;; 7. Configs. Local default; cloud comparison flavor.
;; ============================================================================

(def local-cfg
  "Local — gemma via llama.cpp's Anthropic-compatible /v1/messages. No proxy.
   Requires llama-server launched with --jinja (the user already runs it).
   To make `--model gemma` consistent end-to-end, add `--alias gemma` to the
   server command; otherwise pass any Anthropic-style id."
  {:flavor          :local
   :scout-model     {:model "gemma" :max-turns 100}
   :extractor-model {:model "gemma" :max-turns 200}
   :env             {"ANTHROPIC_BASE_URL" "http://192.168.0.10:8080"
                     "ANTHROPIC_API_KEY"  "sk-not-checked-by-llama-server"}
   :bare?           true
   :allowed-tools   ["Read" "Grep" "TodoWrite"]})

(def cloud-cfg
  "Comparison — Anthropic API via user OAuth from ~/.claude/."
  {:flavor          :cloud
   :scout-model     {:model "sonnet" :max-turns 100 :max-budget-usd 2.00}
   :extractor-model {:model "sonnet" :max-turns 200 :max-budget-usd 4.00}
   :env             nil
   :bare?           false
   :allowed-tools   ["Read" "Grep" "TodoWrite"]})

;; ============================================================================
;; 8. Per-task pipeline.
;; ============================================================================

(defn- log-banner [s] (locking *out* (println) (println s) (flush)))

(defn- build-opts [config model-key system schema prompt agent-cwd]
  (let [m (model-key config)]
    (cond-> {:model                (:model m)
             :max-turns            (:max-turns m)
             :append-system-prompt system
             :allowed-tools        (:allowed-tools config)
             :json-schema          schema
             :cwd                  agent-cwd
             :env                  (:env config)
             :bare?                (:bare? config)
             :prompt               prompt}
      (:max-budget-usd m) (assoc :max-budget-usd (:max-budget-usd m)))))

(defn- system-for [task kind]
  (case [task kind]
    [:sentiment :scout]     scout-system-sentiment
    [:sentiment :extractor] extractor-system-sentiment))

(defn- schema-for [task kind]
  (case [task kind]
    [:sentiment :scout]     scout-schema
    [:sentiment :extractor] sentiment-extractor-schema))

(defn extract-task!
  "Run scout then extractor for one task. Writes registry.json into
   agent-cwd between calls. Returns
     {:task :registry :records :rejected :scout-result :extractor-result}."
  [config task agent-cwd transcript-hash paras]
  (let [paragraphs-by-id (into {} (map (juxt :id :text) paras))
        valid-pids       (set (map :id paras))

        scout-prompt   "Walk the entire transcript and build the registry as described in the system prompt. Start by reading paragraphs.txt to see the episode's structure."
        scout-opts     (build-opts config :scout-model
                                   (system-for task :scout)
                                   (schema-for task :scout)
                                   scout-prompt
                                   agent-cwd)
        _ (log-banner (format "Scout (%s) running…" (name task)))
        scout-result   (cached-cc-run! (keyword (str "cc-scout-" (name task)))
                                       transcript-hash scout-opts)
        registry-raw   (or (:registry (structured-output-or-result scout-result)) {})
        registry       (registry->string-keys registry-raw)
        registry-ids   (set (keys registry))
        registry-doc   {:registry registry}
        registry-json  (with-out-str (json/pprint registry-doc))
        _ (spit (io/file agent-cwd "registry.json") registry-json)
        extractor-inputs-hash (sha256-hex (str transcript-hash "|" registry-json))
        _ (log-banner (format "Scout (%s) done: %d entities, turns=%s, stop=%s, cache=%s"
                              (name task) (count registry)
                              (:num-turns scout-result)
                              (:stop-reason scout-result)
                              (:cache scout-result)))

        extractor-prompt "Read registry.json first to see the canonical entities. Then walk paragraphs.txt and emit one sentiment record per (paragraph, entity, expressed attitude) as described in the system prompt."
        extractor-opts   (build-opts config :extractor-model
                                     (system-for task :extractor)
                                     (schema-for task :extractor)
                                     extractor-prompt
                                     agent-cwd)
        _ (log-banner (format "Extractor (%s) running…" (name task)))
        extr-result    (cached-cc-run! (keyword (str "cc-extractor-" (name task)))
                                       extractor-inputs-hash extractor-opts)
        records-raw    (->> (or (:records (structured-output-or-result extr-result)) [])
                            (mapv snake-keys))
        {:keys [valid rejected]}
        (validate-records records-raw valid-pids registry-ids paragraphs-by-id)
        _ (log-banner (format "Extractor (%s) done: %d emitted (%d valid, %d rejected), turns=%s, stop=%s, cache=%s"
                              (name task) (count records-raw)
                              (count valid) (count rejected)
                              (:num-turns extr-result)
                              (:stop-reason extr-result)
                              (:cache extr-result)))]
    {:task             task
     :registry         registry
     :records          valid
     :rejected         rejected
     :scout-result     scout-result
     :extractor-result extr-result}))

;; ============================================================================
;; 9. Output dir + run.json.
;; ============================================================================

(defn- next-run-dir!
  []
  (let [root (io/file "out-cc")]
    (.mkdirs root)
    (let [used (->> (.listFiles root)
                    (keep #(when (.isDirectory %)
                             (try (Long/parseLong (.getName %)) (catch Exception _ nil))))
                    set)
          n (loop [i 1] (if (used i) (recur (inc i)) i))
          d (io/file root (str n))]
      (.mkdirs d)
      d)))

(defn- write-task-json! [run-dir task task-result paras slice flavor]
  (let [id-key   (pc/task-id-key task)
        registry (:registry task-result)
        records  (:records  task-result)
        entities (pc/aggregate registry id-key records)
        out      {:task         (name task)
                  :n-paragraphs (count paras)
                  :slice        slice
                  :flavor       (name flavor)
                  :n-entities   (count registry)
                  :n-records    (count records)
                  :n-rejected   (count (:rejected task-result))
                  :registry     registry
                  :entities     entities}
        path (.getPath (io/file run-dir (str (name task) ".json")))]
    (io/make-parents path)
    (spit path (with-out-str (json/pprint out)))
    path))

(defn- call-summary
  "Project the interesting bits of a cc/run! result map for run.json."
  [r]
  (let [u (:usage r)]
    {:stop-reason     (:stop-reason r)
     :num-turns       (:num-turns r)
     :total-cost-usd  (:total-cost-usd r)
     :duration-ms     (:duration-ms r)
     :duration-api-ms (:duration-api-ms r)
     :input-tokens         (:input-tokens u)
     :output-tokens        (:output-tokens u)
     :cache-read-input-tokens     (:cache-read-input-tokens u)
     :cache-creation-input-tokens (:cache-creation-input-tokens u)
     :is-error        (:is-error r)
     :cache           (:cache r)}))

(defn- run-summary [task r]
  (let [s (call-summary (:scout-result r))
        e (call-summary (:extractor-result r))
        sum-keys [:input-tokens :output-tokens
                  :cache-read-input-tokens :cache-creation-input-tokens]
        sum-fn   (fn [k] (+ (or (k s) 0) (or (k e) 0)))]
    {:task        (name task)
     :out-name    (str (name task) ".json")
     :n-entities  (count (:registry r))
     :n-records   (count (:records r))
     :n-rejected  (count (:rejected r))
     :tokens      (zipmap sum-keys (map sum-fn sum-keys))
     :scout       s
     :extractor   e}))

;; ============================================================================
;; 10. Public entry.
;; ============================================================================

(defn extract!
  "Run the agent pipeline against a transcript. Phase 1 ships :sentiment
   only — multi-task parallelism comes in Phase 2.

   Returns {task → result-map}; writes <run-dir>/<task>.json,
   <run-dir>/run.json, <run-dir>/agent-cwd/.

   Options:
     :slice [start end]    process only paragraphs[start, end). Default: all.
     :run-dir <File|path>  write into this dir instead of out-cc/N.
     :tasks    [:sentiment]  Phase 1 supports :sentiment only."
  [config in-path & {:keys [slice run-dir tasks]
                     :or   {tasks [:sentiment]}}]
  (let [run-t0     (System/currentTimeMillis)
        json-doc   (json/read-str (slurp in-path) :key-fn keyword)
        all-paras  (vec (:paragraphs json-doc))
        paras      (if slice (let [[a b] slice] (subvec all-paras a b)) all-paras)
        run-dir    (or (when run-dir (doto (io/file run-dir) .mkdirs))
                       (next-run-dir!))
        [agent-cwd transcript-hash]
        (prepare-agent-cwd! run-dir json-doc paras)
        _ (log-banner
           (format "Agent extraction (%s) — %d paragraphs%s  flavor=%s  → %s"
                   (str/join "+" (map name tasks))
                   (count paras)
                   (if slice (format ", slice=%s" (pr-str slice)) "")
                   (name (:flavor config))
                   (.getPath run-dir)))
        results    (into {}
                         (for [t tasks]
                           [t (extract-task! config t agent-cwd transcript-hash paras)]))
        elapsed-ms (- (System/currentTimeMillis) run-t0)]
    (doseq [[t r] results]
      (write-task-json! run-dir t r paras slice (:flavor config)))
    (let [meta-path (.getPath (io/file run-dir "run.json"))
          meta {:flavor       (name (:flavor config))
                :tasks        (mapv name tasks)
                :in-path      in-path
                :n-paragraphs (count paras)
                :slice        slice
                :elapsed-ms   elapsed-ms
                :timestamp    (str (java.time.Instant/now))
                :runs         (vec (for [[t r] results] (run-summary t r)))}]
      (spit meta-path (with-out-str (json/pprint meta))))
    (log-banner (format "Done. %.1fs total. → %s"
                        (/ elapsed-ms 1000.0) (.getPath run-dir)))
    results))
