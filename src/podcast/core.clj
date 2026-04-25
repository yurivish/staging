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
            [podcast.llm :as llm]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]))

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

(defn- mention-step [config]
  (step/step :mentions
             (fn [chunk]
               (let [{:keys [mentions tokens cache rejected]}
                     (llm/extract-mentions! config chunk)]
                 (log-stage :mentions (:chunk-id chunk) (count mentions) tokens cache)
                 (when (seq rejected)
                   (println (format "    rejected %d mention(s) in %s"
                                    (count rejected) (name (:chunk-id chunk)))))
                 {:chunk-id (:chunk-id chunk)
                  :mentions mentions
                  :rejected rejected
                  :tokens   tokens}))))

(defn- run-flow-1 [config chunks paragraphs description]
  (log-banner (format "── Flow 1: %d chunks → mentions → registry ──"
                      (count chunks)))
  (let [res            (flow/run-seq (mention-step config) chunks)
        per-chunk      (mapv first (:outputs res))
        all-mentions   (vec (mapcat :mentions per-chunk))
        stage-a-tokens (apply + (map :tokens per-chunk))
        _ (log-banner (format "Stage A: %d mentions across %d chunks (%d tokens)"
                              (count all-mentions) (count chunks) stage-a-tokens))
        {:keys [registry tokens cache rejected]}
        (llm/resolve-entities! config all-mentions paragraphs description)]
    (log-stage :resolve "—" (count registry) tokens cache)
    (when (seq rejected)
      (println (format "    rejected %d entity record(s)" (count rejected))))
    (log-banner (format "Stage B: %d canonical entities (%d tokens)"
                        (count registry) tokens))
    {:registry          registry
     :all-mentions      all-mentions
     :per-chunk-mentions per-chunk
     :tokens            (+ stage-a-tokens tokens)}))

;; ============================================================================
;; 4. Flow 2 — chunks + registry → per-chunk records.
;;    Registry is closed over in the step's handler.
;; ============================================================================

(defn- record-step [config registry]
  (step/step :records
             (fn [chunk]
               (let [{:keys [records tokens cache rejected]}
                     (llm/extract-records! config chunk registry)]
                 (log-stage :records (:chunk-id chunk) (count records) tokens cache)
                 (when (seq rejected)
                   (println (format "    rejected %d record(s) in %s"
                                    (count rejected) (name (:chunk-id chunk)))))
                 {:chunk-id (:chunk-id chunk)
                  :records  records
                  :rejected rejected
                  :tokens   tokens}))))

(defn- run-flow-2 [config chunks registry]
  (log-banner (format "── Flow 2: %d chunks + registry → records ──"
                      (count chunks)))
  (let [res         (flow/run-seq (record-step config registry) chunks)
        per-chunk   (mapv first (:outputs res))
        all-records (vec (mapcat :records per-chunk))
        tokens      (apply + (map :tokens per-chunk))]
    (log-banner (format "Stage C: %d records across %d chunks (%d tokens)"
                        (count all-records) (count chunks) tokens))
    {:records          all-records
     :per-chunk-records per-chunk
     :tokens           tokens}))

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

(def sentiment-config
  {:task          :sentiment
   :mention-model haiku-cfg
   :resolve-model haiku-resolve-cfg
   :record-model  haiku-cfg})

(def conspiracy-config
  {:task          :conspiracy
   :mention-model haiku-cfg
   :resolve-model haiku-resolve-cfg
   :record-model  haiku-cfg})

;; ============================================================================
;; 7. Public entry — load, slice, chunk, run, aggregate, persist.
;; ============================================================================

(def default-chunking {:n 8 :k 5})

(defn- maybe-slice [paragraphs slice]
  (if slice
    (let [[a b] slice] (subvec (vec paragraphs) a b))
    (vec paragraphs)))

(defn extract!
  "Run the full pipeline and write a JSON result. Returns the in-memory
   result map.

   Options:
     :slice [start end]   — process only paragraphs[start, end). Default: all.
     :chunking {:n :k}    — override chunk sizing. Default {:n 8 :k 5}."
  [config in-path out-path & {:keys [slice chunking]
                              :or   {chunking default-chunking}}]
  (let [json-doc (json/read-str (slurp in-path) :key-fn keyword)
        description (:description json-doc)
        paras    (maybe-slice (:paragraphs json-doc) slice)
        chunks   (paragraph-chunks paras chunking)
        _ (log-banner (format "Podcast extraction (%s) — %d paragraphs → %d chunks (N=%d, K=%d)%s"
                              (name (:task config)) (count paras) (count chunks)
                              (:n chunking) (:k chunking)
                              (if slice (format ", slice=%s" (pr-str slice)) "")))
        f1       (run-flow-1 config chunks paras description)
        f2       (run-flow-2 config chunks (:registry f1))
        id-key   (case (:task config) :sentiment :entity_id :conspiracy :theory_id)
        entities (aggregate (:registry f1) id-key (:records f2))
        total-tokens (+ (:tokens f1) (:tokens f2))
        result   {:task         (name (:task config))
                  :n-paragraphs (count paras)
                  :n-chunks     (count chunks)
                  :slice        slice
                  :chunking     chunking
                  :tokens       total-tokens
                  :n-mentions   (count (:all-mentions f1))
                  :n-entities   (count (:registry f1))
                  :n-records    (count (:records f2))
                  :registry     (:registry f1)
                  :entities     entities}]
    (clojure.java.io/make-parents out-path)
    (spit out-path (with-out-str (json/pprint result)))
    (log-banner (format "Done. %d entities, %d records, %d total tokens. → %s"
                        (count (:registry f1)) (count (:records f2))
                        total-tokens out-path))
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
