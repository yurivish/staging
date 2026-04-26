(ns podcast.core
  "Datapotamus pipeline for podcast structured-extraction.

   Two flows in series, three LLM stages, one task config:

     paragraphs ─► chunk ─► A: mentions  (per chunk, sequential)
                            ─► B: resolve (1 call, builds registry)
                            ─► C: records (per chunk, sequential, registry side-input)
                            ─► D: aggregate (pure group-by entity_id)

   The dataflow primitives used are deliberately small: `step/step`,
   `flow/run-seq`. Sequential is fine at this scale (~85 chunks × 2-3s).
   Caching, validation, schemas, prompts — all in `podcast.llm`.

   Two demonstrator configs share the pipeline shape: `sentiment-config`
   extracts (entity, polarity, emotion) records; `conspiracy-config`
   extracts (theory, stance, summary) records. Same Stumpf transcript,
   different lenses.

   Run from a shell:

     clojure -M -e \"(require 'podcast.core) \\
                     (podcast.core/extract! podcast.core/sentiment-config \\
                                            \\\"stumpf.json\\\" \\\"out/sentiment.json\\\" \\
                                            :slice [280 340])\""
  (:require [toolkit.os-guard]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [podcast.llm :as llm]))

;; ============================================================================
;; 1. Chunking — pure. Non-overlapping focus, backward-only context.
;; ============================================================================

(defn paragraph-chunks
  "Split paragraphs into non-overlapping focus windows of N with K-paragraph
   backward context. Returns chunks with `:chunk-id`, `:focus`, `:context`,
   and `:focus-ids` (a set, for validation). The first chunk's context is
   shorter (or empty) since there's nothing before it."
  [paragraphs {:keys [n k]}]
  (let [paragraphs (vec paragraphs)
        n-paras    (count paragraphs)]
    (vec (for [i (range 0 n-paras n)
               :let [end       (min n-paras (+ i n))
                     focus     (subvec paragraphs i end)
                     ctx-start (max 0 (- i k))
                     context   (subvec paragraphs ctx-start i)]]
           {:chunk-id    (keyword (str "c-" (quot i n)))
            :focus       focus
            :context     context
            :focus-ids   (set (map :id focus))}))))

;; ============================================================================
;; 2. Progress logging — print one line per LLM call so we can watch
;;    the run unfold and see token spend in real time.
;; ============================================================================

(defn- log-stage [stage chunk-id n-out tokens cache]
  (locking *out*
    (println (format "  [%-9s] %-6s  out=%-3d  tokens=%-5d  cache=%s"
                     (name stage)
                     (if (keyword? chunk-id) (name chunk-id) (str chunk-id))
                     n-out tokens (name cache)))
    (flush)))

(defn- log-banner [s]
  (locking *out* (println) (println s) (flush)))

;; ============================================================================
;; 3. Flow 1 — chunks → per-chunk mentions → registry.
;;    Stage A is wrapped in a Datapotamus step so each chunk shows up
;;    as a discrete data point in the trace; Stage B is a single LLM
;;    call with all mentions, called directly after fan-in.
;; ============================================================================


(defn- bounded-pmap
  "Like `pmap` but caps concurrency at `n` workers. Order-preserving.
   Designed for I/O-bound chunk processing — the worker pool gates how
   many concurrent LLM calls are in flight."
  [n f coll]
  (let [coll (vec coll)
        sem  (java.util.concurrent.Semaphore. (int n))
        futs (mapv (fn [x]
                     (.acquire sem)
                     (future
                       (try (f x) (finally (.release sem)))))
                   coll)]
    (mapv deref futs)))

(defn- run-flow-1 [config chunks paragraphs description workers]
  (log-banner (format "── Flow 1: %d chunks → mentions → registry  (%d worker%s) ──"
                      (count chunks) workers (if (= 1 workers) "" "s")))
  (let [step-fn        (fn [chunk]
                         (let [{:keys [mentions tokens cache rejected]}
                               (llm/extract-mentions! config chunk)]
                           (log-stage :mentions (:chunk-id chunk) (count mentions) tokens cache)
                           (when (seq rejected)
                             (println (format "    rejected %d mention(s) in %s"
                                              (count rejected) (name (:chunk-id chunk)))))
                           {:chunk-id (:chunk-id chunk) :mentions mentions :tokens tokens}))
        a-t0           (System/currentTimeMillis)
        per-chunk      (bounded-pmap workers step-fn chunks)
        a-elapsed      (- (System/currentTimeMillis) a-t0)
        all-mentions   (vec (mapcat :mentions per-chunk))
        stage-a-tokens (apply + (map :tokens per-chunk))
        _ (log-banner (format "Stage A: %d mentions across %d chunks (%d tokens, %.1fs)"
                              (count all-mentions) (count chunks) stage-a-tokens
                              (/ a-elapsed 1000.0)))
        b-t0           (System/currentTimeMillis)
        {:keys [registry tokens cache rejected]}
        (llm/resolve-entities! config all-mentions paragraphs description chunks)
        b-elapsed      (- (System/currentTimeMillis) b-t0)]
    (log-stage :resolve "—" (count registry) tokens cache)
    (when (seq rejected)
      (println (format "    rejected %d entity record(s)" (count rejected))))
    (log-banner (format "Stage B: %d canonical entities (%d tokens, %.1fs, strategy=%s)"
                        (count registry) tokens (/ b-elapsed 1000.0)
                        (or (:resolve-strategy config) :llm)))
    {:registry          registry
     :all-mentions      all-mentions
     :per-chunk-mentions per-chunk
     :tokens            (+ stage-a-tokens tokens)
     :stage-a-tokens    stage-a-tokens
     :stage-b-tokens    tokens
     :stage-a-ms        a-elapsed
     :stage-b-ms        b-elapsed}))

;; ============================================================================
;; 4. Flow 2 — chunks + registry → per-chunk records.
;;    Registry is closed over in the step's handler.
;; ============================================================================

(defn- run-flow-2 [config chunks registry workers]
  (log-banner (format "── Flow 2: %d chunks + registry → records  (%d worker%s) ──"
                      (count chunks) workers (if (= 1 workers) "" "s")))
  (let [step-fn (fn [chunk]
                  (let [{:keys [records tokens cache rejected]}
                        (llm/extract-records! config chunk registry)]
                    (log-stage :records (:chunk-id chunk) (count records) tokens cache)
                    (when (seq rejected)
                      (println (format "    rejected %d record(s) in %s"
                                       (count rejected) (name (:chunk-id chunk)))))
                    {:chunk-id (:chunk-id chunk) :records records :tokens tokens}))
        c-t0       (System/currentTimeMillis)
        per-chunk  (bounded-pmap workers step-fn chunks)
        c-elapsed  (- (System/currentTimeMillis) c-t0)
        all-records (vec (mapcat :records per-chunk))
        tokens    (apply + (map :tokens per-chunk))]
    (log-banner (format "Stage C: %d records across %d chunks (%d tokens, %.1fs)"
                        (count all-records) (count chunks) tokens (/ c-elapsed 1000.0)))
    {:records          all-records
     :per-chunk-records per-chunk
     :tokens           tokens
     :stage-c-ms       c-elapsed}))

;; ============================================================================
;; 5. Stage D — pure aggregation. Group records by id-key, sort each
;;    entry's occurrences by paragraph_id (timestamp via the t{seconds}-...
;;    prefix would be more correct, but paragraph_id is monotone enough).
;; ============================================================================

(defn- paragraph-seconds
  "Extract the seconds prefix from a paragraph_id like \"t96-23\"."
  [pid]
  (or (some-> pid (subs 1) (str/split #"-") first parse-long) 0))

(defn- aggregate
  "Group records by id-key into `{id → {canonical aliases occurrences}}`."
  [registry id-key records]
  (let [by-id (group-by id-key records)]
    (into (sorted-map)
          (for [[id entry] registry
                :let [occurrences (vec (sort-by #(paragraph-seconds (:paragraph_id %))
                                                (get by-id id [])))]]
            [id (assoc (select-keys entry [:canonical :aliases])
                       :occurrences occurrences)]))))

;; ============================================================================
;; 6. Configs — both default to Haiku 4.5 throughout. Promote a stage
;;    to Sonnet by editing its :model-cfg here.
;; ============================================================================

(def ^:private haiku-cfg
  "Haiku 4.5 default. Mention/record stages need ~1-2k output; resolve over
   the whole transcript can need 10k+ output for a busy episode."
  {:model "claude-haiku-4-5" :max-tokens 4096})

(def ^:private haiku-resolve-cfg
  (assoc haiku-cfg :max-tokens 24576))

;; Local llama.cpp / Ollama. The Gemma 4 quant on the user's mac mini
;; is a reasoning model — needs generous max-tokens to leave room for
;; the chain-of-thought before the visible output.
(def ^:private local-cfg
  {:model "unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q5_K_XL"
   :max-tokens 16384})

(def ^:private local-resolve-cfg
  (assoc local-cfg :max-tokens 32768))

;; :resolve-strategy :group is the default for the local config because
;; gemma4-26B's reasoning blows past the 32k slot context when asked to
;; cluster ≥ ~150 mentions in a single call. Pure-code grouping by
;; normalised mention_text gets us most of the value at zero LLM cost.
(def sentiment-config
  {:task          :sentiment
   :resolve-strategy :group
   :mention-model local-cfg
   :resolve-model local-resolve-cfg
   :record-model  local-cfg})

(def conspiracy-config
  {:task          :conspiracy
   :resolve-strategy :group
   :mention-model local-cfg
   :resolve-model local-resolve-cfg
   :record-model  local-cfg})

;; ============================================================================
;; 7. Public entry — load, slice, chunk, run, aggregate, persist.
;; ============================================================================

(def default-chunking {:n 8 :k 5})

(defn- maybe-slice [paragraphs slice]
  (if slice
    (let [[a b] slice] (subvec (vec paragraphs) a b))
    (vec paragraphs)))

(defn next-run-dir!
  "Find the next available `out/N` directory and create it. Returns the
   `java.io.File` for the new directory. Versioning means runs never
   overwrite each other; comparing two runs is a directory diff."
  []
  (let [root (clojure.java.io/file "out")]
    (.mkdirs root)
    (let [used (->> (.listFiles root)
                    (keep #(when (.isDirectory %)
                             (try (Long/parseLong (.getName %)) (catch Exception _ nil))))
                    set)
          n (loop [i 1] (if (used i) (recur (inc i)) i))
          d (clojure.java.io/file root (str n))]
      (.mkdirs d)
      d)))

(defn extract!
  "Run the full pipeline and write a JSON result. Returns the in-memory
   result map.

   `out-name` is a basename; the file lands in a freshly-allocated
   `out/N/` directory (next available N). A `run.json` companion is
   written alongside with the config metadata (model names, slice, task).

   Options:
     :slice [start end]   — process only paragraphs[start, end). Default: all.
     :chunking {:n :k}    — override chunk sizing. Default {:n 8 :k 5}.
     :run-dir <File|path> — write into this dir instead of allocating a new one."
  [config in-path out-name & {:keys [slice chunking run-dir workers]
                              :or   {chunking default-chunking}}]
  (let [run-t0   (System/currentTimeMillis)
        json-doc (json/read-str (slurp in-path) :key-fn keyword)
        description (:description json-doc)
        paras    (maybe-slice (:paragraphs json-doc) slice)
        chunks   (paragraph-chunks paras chunking)
        run-dir  (or (when run-dir (doto (clojure.java.io/file run-dir) .mkdirs))
                     (next-run-dir!))
        out-path (.getPath (clojure.java.io/file run-dir out-name))
        ;; Auto-detect parallelism from llama.cpp's /props endpoint.
        ;; Fall back to 1 (sequential) for providers that don't expose
        ;; this — Anthropic's rate limits make heavy concurrency a
        ;; foot-gun there anyway.
        workers  (or workers (llm/detect-slots) 1)
        ;; Inject episode metadata into the config so Stage A can
        ;; canonicalise self-references at extraction time.
        ;; (`:episode-metadata` already in the config wins over the
        ;; auto-derived default.)
        config   (cond-> config
                   (and description (not (:episode-metadata config)))
                   (assoc :episode-metadata
                          (str "Host: Joe Rogan. Description: "
                               (subs description 0 (min (count description) 600)))))
        _ (log-banner (format "Podcast extraction (%s) — %d paragraphs → %d chunks (N=%d, K=%d)%s  workers=%d"
                              (name (:task config)) (count paras) (count chunks)
                              (:n chunking) (:k chunking)
                              (if slice (format ", slice=%s" (pr-str slice)) "")
                              workers))
        f1       (run-flow-1 config chunks paras description workers)
        f2       (run-flow-2 config chunks (:registry f1) workers)
        id-key   (case (:task config) :sentiment :entity_id :conspiracy :theory_id)
        entities (aggregate (:registry f1) id-key (:records f2))
        total-tokens (+ (:tokens f1) (:tokens f2))
        run-elapsed-ms (- (System/currentTimeMillis) run-t0)
        timings  {:total-ms   run-elapsed-ms
                  :stage-a-ms (:stage-a-ms f1)
                  :stage-b-ms (:stage-b-ms f1)
                  :stage-c-ms (:stage-c-ms f2)
                  :workers    workers}
        result   {:task         (name (:task config))
                  :n-paragraphs (count paras)
                  :n-chunks     (count chunks)
                  :slice        slice
                  :chunking     chunking
                  :tokens       total-tokens
                  :stage-tokens {:a (:stage-a-tokens f1)
                                 :b (:stage-b-tokens f1)
                                 :c (:tokens f2)}
                  :timings      timings
                  :n-mentions   (count (:all-mentions f1))
                  :n-entities   (count (:registry f1))
                  :n-records    (count (:records f2))
                  :registry     (:registry f1)
                  :entities     entities}]
    (clojure.java.io/make-parents out-path)
    (spit out-path (with-out-str (json/pprint result)))
    ;; Write run metadata alongside the output for posthoc analysis.
    ;; If multiple extract! calls share the same run-dir (e.g. sentiment +
    ;; conspiracy in one bundle), each call appends to the same run.json
    ;; if it exists, otherwise creates it.
    (let [meta-path (.getPath (clojure.java.io/file run-dir "run.json"))
          existing  (when (.exists (clojure.java.io/file meta-path))
                      (try (json/read-str (slurp meta-path) :key-fn keyword)
                           (catch Throwable _ {})))
          this-run  {:task         (name (:task config))
                     :out-name     out-name
                     :n-paragraphs (count paras)
                     :n-chunks     (count chunks)
                     :slice        slice
                     :chunking     chunking
                     :tokens       total-tokens
                     :stage-tokens (:stage-tokens result)
                     :timings      timings
                     :n-mentions   (count (:all-mentions f1))
                     :n-entities   (count (:registry f1))
                     :n-records    (count (:records f2))
                     :config       (-> config
                                       (update :mention-model #(into {} %))
                                       (update :resolve-model #(into {} %))
                                       (update :record-model  #(into {} %))
                                       (dissoc :task))
                     :timestamp    (str (java.time.Instant/now))}]
      (spit meta-path
            (with-out-str (json/pprint (update existing :runs (fnil conj []) this-run)))))
    (let [tps (when (pos? run-elapsed-ms)
                (* 1000.0 (/ total-tokens (double run-elapsed-ms))))]
      (log-banner
       (format "Done. %d entities, %d records, %d tokens, %.1fs total (%.1f tok/s).  Stage A: %.1fs · B: %.1fs · C: %.1fs.  → %s"
               (count (:registry f1)) (count (:records f2)) total-tokens
               (/ run-elapsed-ms 1000.0) tps
               (/ (:stage-a-ms f1) 1000.0)
               (/ (:stage-b-ms f1) 1000.0)
               (/ (:stage-c-ms f2) 1000.0)
               out-path)))
    result))

(defn -main
  "CLI: clojure -M -m podcast.core sentiment|conspiracy <in.json> <out.json> [start end]"
  [& args]
  (let [[task in-path out-path start-s end-s] args
        config (case task
                 "sentiment"  sentiment-config
                 "conspiracy" conspiracy-config
                 (throw (ex-info "task must be 'sentiment' or 'conspiracy'"
                                 {:given task})))
        slice  (when (and start-s end-s) [(parse-long start-s) (parse-long end-s)])]
    (extract! config in-path out-path :slice slice)
    (System/exit 0)))
