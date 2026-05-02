(ns distill.core
  "Datapotamus flow: iteratively distill a long message to a Pareto
   frontier of (length, fidelity) candidates.

   Cyclic flow shape:

       seed ──► controller ──to-pipeline──► pipeline ──► controller ──► … ──final──►
                   ▲                                         │
                   └────────────── round-results ────────────┘

   `controller` is a stateful step. Each round it consumes K per-port
   results, updates a Pareto frontier, and either re-emits a fresh
   K-payload on `:to-pipeline` (loop) or emits the final result on
   `:final` (terminate). `pipeline` is a `c/parallel` over K ports;
   each port runs `(serial encode decode judge)` against `distill.llm`.

   Key data shapes:

     Round payload (input to one port's encoder, K per round):
       {:original :tips :target-len}

     Candidate (output of one port's judge — what fills the frontier):
       {:original :tips :target-len
        :short :reconstructed
        :score :length :judge-tips
        :encode-tokens :decode-tokens :judge-tokens}

     Controller state:
       {:round :frontier :history :tokens-used :original
        :prev-coords :plateau-count}

     Final result (emitted on `:final`):
       {:frontier :rounds :tokens-used :stop-reason :history}

   Stop conditions, checked in priority order each round:
     :target-hit  — frontier has a (length ≤ target, score ≥ threshold) point
     :budget      — tokens-used ≥ token-budget
     :max-turns   — round count ≥ max-rounds
     :plateau     — frontier coords unchanged for N consecutive rounds

   All Anthropic / langchain4j machinery lives in `distill.llm`; this
   file is the flow plumbing and the runner.

   Run from a shell:

     clojure -M -e \"(require 'distill.core) (distill.core/run-distill! (slurp \\\"input.txt\\\") :out \\\"out.json\\\")\""
  (:require [toolkit.os-guard]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [distill.llm :as llm]
            [toolkit.datapotamus.combinators.core :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]
            [toolkit.pareto :as pareto]
            [toolkit.pubsub :as pubsub]))

;; ============================================================================
;; 1. Loop primitives — small pure functions the controller will compose.
;;
;;    target-lengths    pick K target word-counts for the next round.
;;    frontier-coords   stable identity for plateau detection.
;;    target-hit?       success predicate on the frontier.
;;    best-of           pick a single representative from the frontier
;;                      (used for live progress and the final summary).
;; ============================================================================

(defn- target-lengths
  "K target lengths, log-spaced starting at the soft target. Capped at
   the original length and deduplicated, so we never ask for a 'short'
   version that's longer than the input."
  [{:keys [target-length k]} original-len]
  (->> (iterate #(* 2 %) target-length)
       (take k)
       (mapv #(min % (max 1 original-len)))
       distinct
       vec))

(defn- frontier-coords
  "Stable identity for plateau detection — coord set, ignoring text."
  [frontier]
  (set (map (juxt :length :score) frontier)))

(defn- target-hit?
  "True if any frontier point hits the (length ≤ target, score ≥ threshold)
   corner — the success criterion that ends the loop early."
  [{:keys [target-length quality-threshold]} frontier]
  (boolean
    (some (fn [{:keys [length score]}]
            (and (<= length target-length)
                 (>= score quality-threshold)))
          frontier)))

(defn best-of
  "Pick the 'best' point from a frontier: the shortest one whose score
   meets the threshold, falling back to the highest-score point if none
   does. Public so callers can reproduce the same selection over a saved
   result's frontier."
  [{:keys [quality-threshold]} frontier]
  (or (->> frontier
           (filter #(>= (:score %) (or quality-threshold 0.0)))
           (sort-by :length)
           first)
      (->> frontier
           (sort-by :score >)
           first)))

;; ============================================================================
;; 2. Console output — what the user sees while the loop runs.
;;
;;    print-start!   one-line banner before round 0 fires.
;;    print-round!   per-round line + best-message-so-far.
;;    print-final!   end-of-run summary with the chosen best.
;;    print-event    verbose per-flow-event tracer (opt-in via :trace?).
;; ============================================================================

(defn- print-start! [config input]
  (let [{:keys [k max-rounds target-length quality-threshold token-budget]} config]
    (locking *out*
      (println)
      (println (format "Distill — %d-word input. K=%d, max-rounds=%d, target=%d words, threshold=%.2f, budget=%d tokens."
                       (llm/word-count input) k max-rounds target-length
                       (double quality-threshold) token-budget))
      (flush))))

(defn- print-round!
  [config round best frontier-size tokens-used stop-reason]
  (locking *out*
    (println)
    (println (format "Round %d  —  frontier %d, tokens %d%s"
                     round frontier-size tokens-used
                     (if stop-reason (str "  [STOP: " (name stop-reason) "]") "")))
    (when best
      (println (format "  Best: %d words, score %.2f%s"
                       (:length best) (double (:score best))
                       (if (>= (:score best) (or (:quality-threshold config) 0.0))
                         "" "  (below threshold)")))
      (println (format "  \"%s\""
                       (str/replace (or (:short best) "") #"\s+" " "))))
    (flush)))

(defn- print-final! [config result]
  (locking *out*
    (println)
    (cond
      result
      (let [best (best-of config (:frontier result))]
        (println (format "Done — %d rounds, %d tokens used, stopped on :%s."
                         (:rounds result) (:tokens-used result)
                         (name (:stop-reason result))))
        (when best
          (println (format "Best: %d words, score %.2f  (frontier has %d point%s)"
                           (:length best) (double (:score best))
                           (count (:frontier result))
                           (if (= 1 (count (:frontier result))) "" "s")))
          (println)
          (println (str/replace (or (:short best) "") #"\s+" " "))))
      :else
      (println "Run did not complete."))
    (flush)))

(defn- print-event
  "Pubsub subscriber that prints every flow event in a compact line —
   wired in by `run-distill!` when `:trace? true`."
  [_subject ev _match]
  (let [preview (fn [v] (let [s (pr-str v)]
                          (if (> (count s) 80) (str (subs s 0 77) "...") s)))]
    (locking *out*
      (println (format "[%-8s %-6s] %-32s %s"
                       (name (:kind ev))
                       (some-> (:msg-kind ev) name (or ""))
                       (str (:step-id ev) (when-let [p (:port ev)] (str " → " p)))
                       (cond-> ""
                         (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                         (contains? ev :tokens) (str "tokens=" (preview (:tokens ev)))))))))

;; ============================================================================
;; 3. Controller — the stateful step that drives the loop.
;;
;;    build-payload         shape one round's K per-port inputs.
;;    handle-round-results  fold a finished round into state and decide
;;                          loop-or-stop.
;;    controller            the Datapotamus step (wraps the above with
;;                          the `:seed` and `:round-results` ports).
;; ============================================================================

(defn- build-payload
  "One round's per-port payload map: `{port {:original :tips :target-len}}`,
   sized to the number of distinct target lengths we want this round."
  [original tips ports targets]
  (into {} (map (fn [port tlen]
                  [port {:original original :tips tips :target-len tlen}])
                ports targets)))

(defn- handle-round-results
  "Update the frontier with K results, check stop conditions, and either
   emit the next round's payload (`:to-pipeline`) or the final result
   (`:final`). Returns Datapotamus's `[state' port-map]` tuple."
  [config state by-port]
  (let [{:keys [round frontier history tokens-used original
                prev-coords plateau-count]} state
        {:keys [token-budget max-rounds plateau-rounds quiet?]} config
        results       (vec (vals by-port))
        frontier'     (reduce pareto/insert frontier results)
        round-tokens  (apply + (map llm/result-tokens results))
        tokens-used'  (+ tokens-used round-tokens)
        history'      (conj history {:round round :results results :tokens round-tokens})
        coords'       (frontier-coords frontier')
        plateau-count' (if (= coords' prev-coords)
                         (inc plateau-count)
                         0)
        next-round    (inc round)
        stop-reason   (cond
                        (target-hit? config frontier')        :target-hit
                        (>= tokens-used' token-budget)        :budget
                        (>= next-round max-rounds)            :max-turns
                        (>= plateau-count' plateau-rounds)    :plateau)]
    (when-not quiet?
      (print-round! config round (best-of config frontier')
                    (count frontier') tokens-used' stop-reason))
    (if stop-reason
      [state
       {:final [{:frontier    (pareto/sort-by-dim frontier' 0
                                                  :order :asc
                                                  :key-fn (juxt :length :score))
                 :rounds      (count history')
                 :tokens-used tokens-used'
                 :stop-reason stop-reason
                 :history     history'}]}]
      (let [tips    (str/join "\n\n---\n\n"
                              (for [r results]
                                (format "(target=%d, got %d, score=%.2f)\n%s"
                                        (:target-len r) (:length r)
                                        (double (:score r)) (:judge-tips r))))
            targets (target-lengths config (llm/word-count original))
            ports   (mapv #(keyword (str "c" %)) (range (count targets)))]
        [{:round next-round :frontier frontier' :history history'
          :tokens-used tokens-used' :original original
          :prev-coords coords' :plateau-count plateau-count'}
         {:to-pipeline [(build-payload original tips ports targets)]}]))))

(defn- controller
  "The `:controller` step. `:seed` fires once per run with the original
   input; `:round-results` fires once per completed round with the K
   per-port results. Outputs go to `:to-pipeline` (next round) or
   `:final` (terminate)."
  [config]
  (step/step :controller
             {:ins  {:seed "" :round-results ""}
              :outs {:to-pipeline "" :final ""}}
             (fn [ctx state data]
               (case (:in-port ctx)
                 :seed
                 (let [original (str data)
                       targets  (target-lengths config (llm/word-count original))
                       ports    (mapv #(keyword (str "c" %)) (range (count targets)))]
                   [{:round 0
                     :frontier      (pareto/empty-frontier
                                     :directions [:min :max]
                                     :key-fn     (juxt :length :score))
                     :history       []
                     :tokens-used   0
                     :original      original
                     :prev-coords   #{}
                     :plateau-count 0}
                    {:to-pipeline [(build-payload original "" ports targets)]}])

                 :round-results
                 (handle-round-results config state data)))))

;; ============================================================================
;; 4. Pipeline — the K-way scatter that runs encode → decode → judge per
;;    candidate. Lives entirely between the controller's `:to-pipeline`
;;    and `:round-results` ports.
;;
;;    per-port-pipeline   one port's serial chain.
;;    pipeline-step       K-way `c/parallel` over named per-port chains.
;; ============================================================================

(defn- per-port-pipeline
  "One port's serial chain: encode → decode → judge. Each step calls
   into distill.llm and threads the augmented payload forward."
  [port-id {:keys [role-models]}]
  (let [{:keys [encoder decoder judge]} role-models
        suf  (name port-id)
        encs (keyword (str "encode-" suf))
        decs (keyword (str "decode-" suf))
        jds  (keyword (str "judge-"  suf))]
    (step/serial
      (step/step encs (fn [d] (llm/encode! encoder d)))
      (step/step decs (fn [d] (llm/decode! decoder d)))
      (step/step jds  (fn [d] (llm/judge!  judge   d))))))

(defn- pipeline-step
  "K parallel per-port chains. The controller's payload is already a
   `{port payload}` map, so `c/parallel`'s `:select` is `identity`."
  [{:keys [k] :as config}]
  (let [ports (mapv #(keyword (str "c" %)) (range k))]
    (c/parallel :pipeline
                (into {} (for [p ports] [p (per-port-pipeline p config)]))
                :select identity)))

;; ============================================================================
;; 5. Wiring — drop the controller next to the pipeline, connect the loop
;;    edge, and designate the public input/output boundaries.
;; ============================================================================

(declare default-config)

(defn build-flow
  ([] (build-flow default-config))
  ([config]
   (-> (step/beside (controller config)
                    (pipeline-step config))
       (step/input-at  [:controller :seed])
       (step/connect   [:controller :to-pipeline] [:pipeline :in])
       (step/connect   [:pipeline :out]           [:controller :round-results])
       (step/output-at [:controller :final]))))

;; ============================================================================
;; 6. Public entry — settings template + `run-distill!`.
;;
;;    `default-config` is a settings template (no `:original`); the caller
;;    supplies input as the first positional arg to `run-distill!`.
;;    `with-derived-budget` fills in `:token-budget` proportional to the
;;    loop size if the caller didn't pin one.
;; ============================================================================

(def default-config
  "Settings template. Pair with an input via `run-distill!`. No `:original`
   — input is supplied as the first positional arg to `run-distill!`.
   `:token-budget` is intentionally absent — `run-distill!` derives it
   from `:k × :max-rounds × 8000` so the cap scales with the loop size.
   Override `:token-budget` explicitly if you want a different cap."
  {:role-models       {:encoder {:model "claude-opus-4-7" :max-tokens 1024}
                       :decoder {:model "claude-opus-4-7" :max-tokens 4096}
                       :judge   {:model "claude-opus-4-7" :max-tokens 1024}}
   :k                 3
   :max-rounds        5
   :target-length     25
   :quality-threshold 0.85
   :plateau-rounds    2})

(defn- with-derived-budget
  "If `:token-budget` is unset, derive it from `:k × :max-rounds × 8000`
   (a per-port-round estimate). Keeps the budget proportional to how
   much work the loop is actually being asked to do."
  [config]
  (update config :token-budget
          #(or % (* (:k config) (:max-rounds config) 8000))))

(defn run-distill!
  "Run the distill loop on `input` (a long message string). `input` is
   required — the loop will not run without an explicit input.

   Options:
     :config    Settings map; defaults to `default-config`. Override
                specific fields with `(assoc default-config :k 2 ...)`.
     :out       Output path for the final JSON; default nil (no file).
     :trace?    Print every flow event (very verbose). Default false.
     :quiet?    Suppress per-round progress output. Default false.
     :return?   Return the result map (for REPL/test use). Default false
                — the function returns nil so shell `clojure -M -e` runs
                don't dump the result to the terminal.

   If `:token-budget` isn't set in `:config`, it's derived from
   `:k × :max-rounds × 8000`."
  [input & {:keys [config out trace? quiet? return?]
            :or   {config default-config quiet? false return? false}}]
  (when (or (nil? input) (and (string? input) (str/blank? input)))
    (throw (ex-info "distill: input must be a non-empty string"
                    {:input input})))
  (let [config (-> config
                   (assoc :quiet? quiet?)
                   with-derived-budget)
        ps     (when trace? (pubsub/make))
        unsub  (when trace? (pubsub/sub ps [:>] print-event))
        opts   (cond-> {} ps (assoc :pubsub ps))]
    (when-not quiet? (print-start! config input))
    (let [res    (flow/run-seq (build-flow config) [input] opts)
          result (ffirst (:outputs res))]
      (when unsub (unsub))
      (when (and out (= :completed (:state res)) result)
        (spit out (with-out-str (json/pprint result))))
      (when-not quiet?
        (print-final! config result)
        (when (not= :completed (:state res))
          (locking *out*
            (println " state:" (:state res))
            (println " error:" (pr-str (:error res)))
            (flush))))
      (when return? result))))
