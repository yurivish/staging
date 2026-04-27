(ns podcast.mock-server
  "Deterministic in-process mock for `podcast.llm/cached-chat!`. Lets the
   full extract! pipeline run without a real LLM, with optional simulated
   per-call latency and slot-cap concurrency for wall-clock comparison.

   Usage:

     (with-redefs [llm/cached-chat! (mock/responder {:latency-ms 10})]
       (pc/extract! ...))

   The responder maintains an in-memory cache; pass `:cache <atom>` to
   share state across runs (so reruns hit) or omit it for a fresh cache.

   Each stage's payload is parsed back out of the rendered prompt so
   paragraph_id and quote fields keep alignment with the validators in
   `podcast.llm`."
  (:require [clojure.string :as str]))

;; ============================================================================
;; Prompt parsing — extract focus paragraphs and (for records) the first
;; registry id from the rendered chat content.
;; ============================================================================

(defn- prompt-text [content-parts]
  (->> content-parts (keep :text) (str/join "\n")))

(defn- focus-section
  "The slice of `text` covering the rendered FOCUS paragraphs. Both Stage
   A/C (`FOCUS (extract from ...)`) and tree-leaf (`=== FOCUS — cluster ...`)
   start with the literal word FOCUS — the regex catches both."
  [text]
  (or (-> (re-find #"(?s)FOCUS[^\n]*\n+(.*?)(?:\n\n===|\z)" text) second) ""))

(defn- focus-paragraphs
  "Pairs of `[paragraph_id text]` extracted from a rendered FOCUS region."
  [text]
  (->> (re-seq #"\[([^\]\s]+)\s*@[^\]]*\]\s*([^\n]+)" (focus-section text))
       (mapv (fn [[_ id t]] [id t]))))

(defn- registry-first-id
  "First `<id>:` token from a `REGISTRY:` block. Tree-resolve ids contain
   their own `:` (e.g. `c-0:e_001`), so the regex requires a non-whitespace
   id followed by `: ` (colon + space) — the canonical name's separator."
  [text]
  (some-> (re-find #"(?m)^- (\S+):\s" text) second))

;; ============================================================================
;; Stage payload generators. Deterministic in (focus-paragraphs, registry).
;; ============================================================================

(defn- mentions-payload [focus]
  {:mentions (mapv (fn [[id _]]
                     {:paragraph_id id
                      :mention_text (str "Person-" id)
                      :surface_form id})
                   focus)})

(defn- tree-leaf-payload [focus]
  {:entities
   (if (empty? focus)
     []
     [{:entity_id       "e_001"
       :canonical       (str "Person-" (-> focus first first))
       :summary         (str "subject around " (-> focus first first))
       :mention_indices (vec (range 1 (inc (count focus))))}])})

(def ^:private tree-merge-payload
  ;; Carry-through. Real merge logic isn't exercised; the validator just
  ;; threads inputs to outputs.
  {:merges []})

(defn- records-payload [system focus registry-id]
  (if (or (str/blank? registry-id) (empty? focus))
    {:records []}
    (let [task   (if (re-find #"DISCUSSION of conspiracy" (or system "")) :conspiracy :sentiment)
          id-key (case task :sentiment :entity_id :conspiracy :theory_id)]
      {:records
       (mapv (fn [[id text]]
               (cond-> {:paragraph_id id
                        id-key        registry-id
                        :quote        text}
                 (= task :sentiment)
                 (assoc :polarity "neutral" :emotion "calm" :rationale "mock")
                 (= task :conspiracy)
                 (assoc :stance "references" :summary "mock")))
             focus)})))

(defn- payload-for [stage system content-parts]
  (let [text (prompt-text content-parts)]
    (case stage
      :mentions   (mentions-payload (focus-paragraphs text))
      :tree-leaf  (tree-leaf-payload (focus-paragraphs text))
      :tree-merge tree-merge-payload
      :records    (records-payload system (focus-paragraphs text) (registry-first-id text)))))

(def ^:private stage-tokens
  {:mentions 100 :tree-leaf 150 :tree-merge 50 :records 80})

;; ============================================================================
;; Responder factory.
;; ============================================================================

(defn responder
  "Returns a function with the signature and return shape of
   `podcast.llm/cached-chat!`. Suitable for `with-redefs`.

   Options (all optional):
     :latency-ms — sleep this long on cache miss before responding (default 0).
     :slots      — semaphore-cap on concurrent in-flight calls (default unlimited).
                   Mimics e.g. a 4-slot llama.cpp server.
     :cache      — atom backing the in-memory cache (default fresh atom).
                   Pass a shared atom to test cache-hit behaviour across runs."
  ([] (responder {}))
  ([{:keys [latency-ms slots cache]
     :or   {latency-ms 0 cache (atom {})}}]
   (let [sem (when slots (java.util.concurrent.Semaphore. (int slots)))]
     (fn [stage model-cfg system content-parts schema]
       (let [k [stage (:model model-cfg) system content-parts schema]]
         (if-let [hit (get @cache k)]
           (assoc hit :tokens 0 :cache :hit)
           (do
             (when sem (.acquire sem))
             (try
               (when (pos? latency-ms) (Thread/sleep ^long latency-ms))
               (let [res {:value  (payload-for stage system content-parts)
                          :tokens (stage-tokens stage 0)}]
                 (swap! cache assoc k res)
                 (assoc res :cache :miss))
               (finally
                 (when sem (.release sem)))))))))))
