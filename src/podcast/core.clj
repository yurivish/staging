(ns podcast.core
  "Pipeline for podcast structured-extraction. One or more tasks
   (sentiment, conspiracy) run over the same transcript:

     paragraphs ─► chunk ─► A: mentions  (per (task, chunk), shared pool)
                            ─► B: resolve (per task, concurrent tree-resolve)
                            ─► C: records (per (task, chunk), shared pool, registry side-input)
                            ─► D: aggregate (pure group-by id-key)

   Caching, validation, schemas, prompts — all in `podcast.llm`.

   Run from a shell:

     clojure -M -m podcast.core sentiment <in.json>             # one task
     clojure -M -m podcast.core sentiment,conspiracy <in.json>  # both"
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
;; 3. Concurrency primitive — bounded pool for Stages A and C.
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

;; ============================================================================
;; 4. Stage D — pure aggregation. Group records by id-key, sort each
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

(defn- task-id-key [task]
  (case task :sentiment :entity_id :conspiracy :theory_id))

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

(def sentiment-config
  {:task          :sentiment
   :mention-model local-cfg
   :resolve-model local-resolve-cfg
   :record-model  local-cfg})

(def conspiracy-config
  {:task          :conspiracy
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
  "Run the full pipeline for one or more tasks against a single transcript.
   Returns `{task → result}` keyed by task keyword. Each task's result lands
   at `<run-dir>/<task>.json`; `<run-dir>/run.json` accumulates metadata
   across runs sharing the directory.

   `config` must contain `:tasks` (vec of task keywords) — `:task` (singular)
   is auto-promoted to `:tasks [t]` for backwards compatibility.

   Options:
     :slice [start end]   — process only paragraphs[start, end). Default: all.
     :chunking {:n :k}    — override chunk sizing. Default {:n 8 :k 5}.
     :run-dir <File|path> — write into this dir instead of allocating a new one.
     :workers n           — bounded-pmap worker count for Stages A and C
                            (one shared pool serves all tasks). Defaults to
                            `llm/detect-slots`, then 1."
  [config in-path & {:keys [slice chunking run-dir workers]
                     :or   {chunking default-chunking}}]
  (let [tasks    (or (:tasks config) (some-> (:task config) vector))
        _        (assert (and (sequential? tasks) (seq tasks))
                         "extract! requires :tasks (vec) or :task in config")
        run-t0   (System/currentTimeMillis)
        json-doc (json/read-str (slurp in-path) :key-fn keyword)
        description (:description json-doc)
        paras    (maybe-slice (:paragraphs json-doc) slice)
        chunks   (paragraph-chunks paras chunking)
        run-dir  (or (when run-dir (doto (clojure.java.io/file run-dir) .mkdirs))
                     (next-run-dir!))
        ;; Auto-detect parallelism from llama.cpp's /props endpoint. Fall
        ;; back to 1 for providers that don't expose it — Anthropic's rate
        ;; limits make heavy concurrency a foot-gun there anyway.
        workers  (or workers (llm/detect-slots) 1)
        config   (cond-> config
                   (and description (not (:episode-metadata config)))
                   (assoc :episode-metadata
                          (str "Host: Joe Rogan. Description: "
                               (subs description 0 (min (count description) 600)))))
        per-task (fn [t] (-> config (dissoc :tasks) (assoc :task t)))
        _ (log-banner
           (format "Podcast extraction (%s) — %d paragraphs → %d chunks (N=%d, K=%d)%s  workers=%d"
                   (str/join "+" (map name tasks))
                   (count paras) (count chunks)
                   (:n chunking) (:k chunking)
                   (if slice (format ", slice=%s" (pr-str slice)) "")
                   workers))

        ;; ── Stage A: per-(task, chunk) mention extraction, shared pool ──
        a-t0 (System/currentTimeMillis)
        a-results
        (bounded-pmap
         workers
         (fn [[t chunk]]
           (let [{:keys [mentions tokens cache rejected]}
                 (llm/extract-mentions! (per-task t) chunk)
                 tag (str (name t) "/" (name (:chunk-id chunk)))]
             (log-stage :mentions tag (count mentions) tokens cache)
             (when (seq rejected)
               (println (format "    rejected %d mention(s) in %s" (count rejected) tag)))
             {:task t :mentions mentions :tokens tokens}))
         (for [t tasks, c chunks] [t c]))
        a-elapsed (- (System/currentTimeMillis) a-t0)
        a-by-task (group-by :task a-results)
        mentions-by-task (update-vals a-by-task #(vec (mapcat :mentions %)))
        a-tokens-by-task (update-vals a-by-task #(apply + (map :tokens %)))
        _ (log-banner
           (format "Stage A: %s mentions (%d tokens, %.1fs)"
                   (str/join " + "
                             (for [t tasks]
                               (format "%d %s" (count (mentions-by-task t)) (name t))))
                   (apply + (vals a-tokens-by-task))
                   (/ a-elapsed 1000.0)))

        ;; ── Stage B: per-task tree-resolve, concurrent ──
        b-t0 (System/currentTimeMillis)
        b-results
        (into {}
              (mapv deref
                    (mapv (fn [t]
                            (future
                              (let [r (llm/resolve-entities! (per-task t)
                                                             (mentions-by-task t)
                                                             paras chunks)]
                                (log-stage :resolve (name t) (count (:registry r))
                                           (:tokens r) (:cache r))
                                [t r])))
                          tasks)))
        b-elapsed (- (System/currentTimeMillis) b-t0)
        _ (log-banner
           (format "Stage B: %s entities (%d tokens, %.1fs)"
                   (str/join " + "
                             (for [t tasks]
                               (format "%d %s" (count (:registry (b-results t))) (name t))))
                   (apply + (map (comp :tokens val) b-results))
                   (/ b-elapsed 1000.0)))

        ;; ── Stage C: per-(task, chunk) record extraction, shared pool ──
        c-t0 (System/currentTimeMillis)
        c-results
        (bounded-pmap
         workers
         (fn [[t chunk]]
           (let [registry (:registry (b-results t))
                 {:keys [records tokens cache rejected]}
                 (llm/extract-records! (per-task t) chunk registry)
                 tag (str (name t) "/" (name (:chunk-id chunk)))]
             (log-stage :records tag (count records) tokens cache)
             (when (seq rejected)
               (println (format "    rejected %d record(s) in %s" (count rejected) tag)))
             {:task t :records records :tokens tokens}))
         (for [t tasks, c chunks] [t c]))
        c-elapsed (- (System/currentTimeMillis) c-t0)
        c-by-task (group-by :task c-results)
        records-by-task (update-vals c-by-task #(vec (mapcat :records %)))
        c-tokens-by-task (update-vals c-by-task #(apply + (map :tokens %)))
        _ (log-banner
           (format "Stage C: %s records (%d tokens, %.1fs)"
                   (str/join " + "
                             (for [t tasks]
                               (format "%d %s" (count (records-by-task t)) (name t))))
                   (apply + (vals c-tokens-by-task))
                   (/ c-elapsed 1000.0)))

        ;; ── Stage D: per-task aggregation ──
        run-elapsed-ms (- (System/currentTimeMillis) run-t0)
        timings {:total-ms   run-elapsed-ms
                 :stage-a-ms a-elapsed
                 :stage-b-ms b-elapsed
                 :stage-c-ms c-elapsed
                 :workers    workers}
        per-task-result
        (fn [t]
          (let [id-key   (task-id-key t)
                registry (:registry (b-results t))
                records  (records-by-task t)
                a-tok    (or (a-tokens-by-task t) 0)
                b-tok    (:tokens (b-results t))
                c-tok    (or (c-tokens-by-task t) 0)]
            {:task         (name t)
             :n-paragraphs (count paras)
             :n-chunks     (count chunks)
             :slice        slice
             :chunking     chunking
             :tokens       (+ a-tok b-tok c-tok)
             :stage-tokens {:a a-tok :b b-tok :c c-tok}
             :timings      timings
             :n-mentions   (count (mentions-by-task t))
             :n-entities   (count registry)
             :n-records    (count records)
             :registry     registry
             :entities     (aggregate registry id-key records)}))
        results (into {} (for [t tasks] [t (per-task-result t)]))
        total-tokens (apply + (map :tokens (vals results)))]
    (doseq [[t r] results]
      (let [path (.getPath (clojure.java.io/file run-dir (str (name t) ".json")))]
        (clojure.java.io/make-parents path)
        (spit path (with-out-str (json/pprint r)))))
    (let [meta-path (.getPath (clojure.java.io/file run-dir "run.json"))
          existing  (when (.exists (clojure.java.io/file meta-path))
                      (try (json/read-str (slurp meta-path) :key-fn keyword)
                           (catch Throwable _ {})))
          new-runs  (vec
                     (for [[t r] results]
                       {:task         (name t)
                        :out-name     (str (name t) ".json")
                        :n-paragraphs (:n-paragraphs r)
                        :n-chunks     (:n-chunks r)
                        :slice        slice
                        :chunking     chunking
                        :tokens       (:tokens r)
                        :stage-tokens (:stage-tokens r)
                        :timings      timings
                        :n-mentions   (:n-mentions r)
                        :n-entities   (:n-entities r)
                        :n-records    (:n-records r)
                        :config       (-> config
                                          (update :mention-model #(into {} %))
                                          (update :resolve-model #(into {} %))
                                          (update :record-model  #(into {} %))
                                          (dissoc :tasks :task :episode-metadata))
                        :timestamp    (str (java.time.Instant/now))}))]
      (spit meta-path
            (with-out-str (json/pprint (update existing :runs (fnil into []) new-runs)))))
    (let [tps (when (pos? run-elapsed-ms)
                (* 1000.0 (/ total-tokens (double run-elapsed-ms))))]
      (log-banner
       (format "Done. %d tokens, %.1fs total (%.1f tok/s).  Stage A: %.1fs · B: %.1fs · C: %.1fs.  → %s"
               total-tokens
               (/ run-elapsed-ms 1000.0) (or tps 0.0)
               (/ a-elapsed 1000.0)
               (/ b-elapsed 1000.0)
               (/ c-elapsed 1000.0)
               (.getPath run-dir))))
    results))

(defn -main
  "CLI: clojure -M -m podcast.core <task[,task...]> <in.json> [start end]
   Tasks: sentiment, conspiracy. Combine with commas to run both in one
   pass: sentiment,conspiracy."
  [& args]
  (let [[tasks-arg in-path start-s end-s] args
        tasks (mapv (fn [s]
                      (case s
                        "sentiment"  :sentiment
                        "conspiracy" :conspiracy
                        (throw (ex-info "task must be 'sentiment' or 'conspiracy'"
                                        {:given s}))))
                    (str/split (or tasks-arg "") #","))
        config (-> sentiment-config (dissoc :task) (assoc :tasks tasks))
        slice  (when (and start-s end-s) [(parse-long start-s) (parse-long end-s)])]
    (extract! config in-path :slice slice)
    (System/exit 0)))
