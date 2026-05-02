(ns podcast.flow
  "Stages A and C as Datapotamus flows. Each stage is a small graph:

       (task,chunk) pairs ─→ explode ─→ stealing-workers k ─→ output

   The worker calls into `podcast.llm` (`extract-mentions!` /
   `extract-records!`) and emits a `:status` event with the prompt
   metadata, model, tokens, cache hit/miss, wall-clock, and the input/
   output domain refs the trace UI needs.

   Stage B is its own beast (`podcast.tree-resolve`), but uses the same
   pattern of trace/emit at the LLM call site."
  (:require [podcast.llm :as llm]
            [toolkit.datapotamus.combinators.workers :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace]))

;; ============================================================================
;; Steps
;; ============================================================================

(defn- explode-step
  "One vec-of-pairs in → N msgs out, one per pair. Each pair carries
   the per-call params the worker needs."
  []
  (step/step
   :explode
   {:ins {:in ""} :outs {:out ""}}
   (fn [_ctx _state pairs]
     {:out (vec pairs)})))

(defn- log-stage [stage tag n-out tokens cache]
  (locking *out*
    (println (format "  [%-9s] %-30s  out=%-3d  tokens=%-5d  cache=%s"
                     (name stage) tag n-out tokens (name cache)))
    (flush)))

(defn- stage-a-worker [config]
  (step/step
   :worker
   {:ins {:in ""} :outs {:out ""}}
   (fn [ctx _state {:keys [task chunk]}]
     (let [t0 (System/nanoTime)
           per-task (assoc config :task task)
           {:keys [mentions tokens cache rejected]}
           (llm/extract-mentions! per-task chunk)
           ms (long (/ (- (System/nanoTime) t0) 1e6))
           tag (str (name task) "/" (name (:chunk-id chunk)))]
       (trace/emit ctx
                   {:llm-call {:stage     :mentions
                               :task      task
                               :chunk-id  (:chunk-id chunk)
                               :model     (:model (:mention-model config))
                               :tokens    tokens
                               :cache     cache
                               :ms        ms}
                    :inputs   {:chunk-id            (:chunk-id chunk)
                               :focus-paragraph-ids (mapv :id (:focus chunk))}
                    :outputs  {:mention-paragraph-ids (mapv :paragraph_id mentions)
                               :n-mentions             (count mentions)
                               :n-rejected             (count rejected)}})
       (log-stage :mentions tag (count mentions) tokens cache)
       (when (seq rejected)
         (locking *out*
           (println (format "    rejected %d mention(s) in %s" (count rejected) tag))))
       {:out [{:task     task
               :chunk-id (:chunk-id chunk)
               :mentions mentions
               :rejected rejected
               :tokens   tokens
               :cache    cache}]}))))

(defn- stage-c-worker [config registries-by-task]
  (step/step
   :worker
   {:ins {:in ""} :outs {:out ""}}
   (fn [ctx _state {:keys [task chunk]}]
     (let [t0 (System/nanoTime)
           per-task (assoc config :task task)
           registry (get registries-by-task task)
           {:keys [records tokens cache rejected]}
           (llm/extract-records! per-task chunk registry)
           ms (long (/ (- (System/nanoTime) t0) 1e6))
           id-key (case task :sentiment :entity_id :conspiracy :theory_id)
           tag (str (name task) "/" (name (:chunk-id chunk)))]
       (trace/emit ctx
                   {:llm-call {:stage     :records
                               :task      task
                               :chunk-id  (:chunk-id chunk)
                               :model     (:model (:record-model config))
                               :tokens    tokens
                               :cache     cache
                               :ms        ms}
                    :inputs   {:chunk-id            (:chunk-id chunk)
                               :focus-paragraph-ids (mapv :id (:focus chunk))
                               :registry-task       task
                               :registry-size       (count registry)}
                    :outputs  {:record-keys (mapv (fn [r] [(:paragraph_id r) (id-key r)]) records)
                               :n-records   (count records)
                               :n-rejected  (count rejected)}})
       (log-stage :records tag (count records) tokens cache)
       (when (seq rejected)
         (locking *out*
           (println (format "    rejected %d record(s) in %s" (count rejected) tag))))
       {:out [{:task     task
               :chunk-id (:chunk-id chunk)
               :records  records
               :rejected rejected
               :tokens   tokens
               :cache    cache}]}))))

;; ============================================================================
;; Graph builders
;; ============================================================================

(defn- build-graph [worker workers-k]
  (-> (step/beside
       (explode-step)
       (c/stealing-workers :workers workers-k worker))
      (step/connect [:explode :out] [:workers :in])
      (step/input-at  :explode)
      (step/output-at :workers)))

(defn build-flow
  "Representative graph for topology export. Stages A and C share this
   shape — they differ only in the worker handler body. Worker config
   is nil because it is only closed over by the handler, never invoked
   at construction."
  []
  (build-graph (stage-a-worker nil) 4))

(defn- sort-by-input-order
  "Stealing-workers don't preserve order. Restore (task, chunk-id) input
   order on the way out so downstream globals (e.g. mention_indices into
   `all-mentions`) are deterministic across runs."
  [tasks chunks outputs]
  (let [task-idx  (zipmap tasks  (range))
        chunk-idx (zipmap (mapv :chunk-id chunks) (range))]
    (vec (sort-by (fn [r] [(task-idx  (:task r))
                           (chunk-idx (:chunk-id r))])
                  outputs))))

;; ============================================================================
;; Public entry points — one call per phase per run
;; ============================================================================

(defn run-stage-a!
  "Run Stage A across `(task, chunk)` pairs in parallel. Returns a flat
   vec of per-pair results: `[{:task :chunk-id :mentions :rejected :tokens :cache} ...]`,
   sorted in input order so downstream `mention_indices` are deterministic.

   Events are published on `pubsub` under `[:scope flow-id ...]`."
  [config tasks chunks workers-k pubsub]
  (let [pairs (vec (for [t tasks, c chunks] {:task t :chunk c}))]
    (if (empty? pairs)
      []
      (let [graph (build-graph (stage-a-worker config) workers-k)
            {:keys [state outputs error]}
            (flow/run-seq graph [pairs]
                          {:pubsub  pubsub
                           :flow-id "stage-a"})]
        (when (= :failed state)
          (throw (ex-info "Stage A flow failed" {:error error})))
        (sort-by-input-order tasks chunks (or (first outputs) []))))))

(defn run-stage-c!
  "Run Stage C across `(task, chunk)` pairs in parallel. Registry is
   looked up per-task from `registries-by-task`, closed in the worker
   step. Returns a flat vec of per-pair results, sorted in input order:
   `[{:task :chunk-id :records :rejected :tokens :cache} ...]`."
  [config tasks chunks registries-by-task workers-k pubsub]
  (let [pairs (vec (for [t tasks, c chunks] {:task t :chunk c}))]
    (if (empty? pairs)
      []
      (let [graph (build-graph (stage-c-worker config registries-by-task) workers-k)
            {:keys [state outputs error]}
            (flow/run-seq graph [pairs]
                          {:pubsub  pubsub
                           :flow-id "stage-c"})]
        (when (= :failed state)
          (throw (ex-info "Stage C flow failed" {:error error})))
        (sort-by-input-order tasks chunks (or (first outputs) []))))))
