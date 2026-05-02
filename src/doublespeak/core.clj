(ns doublespeak.core
  "Datapotamus flow demo: iteratively co-optimize a short message so that
   different audience classes (keyed by `:class` on each model) read it
   divergently.

   Shape — one cyclic flow (tool-loop pattern, datapotamus README §6):

       seed ──► optimizer ──to-ivw──► interview ──► optimizer ──► … ──final──► collect
                  ▲                                     │
                  └────────────── ivw-result ───────────┘

   `optimizer` is a stateful step holding round/history; each round it either
   (a) consumes interview results, asks `llm/optimize!` for the next prompt,
   and re-emits on `:to-ivw`, or (b) emits the best-so-far prompt on `:final`.
   `interview` is a flat `c/parallel` over the configured models — one port
   per model, each port a one-line step that calls `llm/react!`.

   All Anthropic / langchain4j / prompt-engineering machinery lives in
   `doublespeak.llm`; this file is just the flow plumbing and the runner.

   Run from a shell:

     clojure -M -e \"(require 'doublespeak.core) (doublespeak.core/run-doublespeak! doublespeak.core/example-config \\\"out.json\\\")\""
  (:require [toolkit.os-guard]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [doublespeak.llm :as llm]
            [toolkit.datapotamus.combinators.core :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]
            [toolkit.pubsub :as pubsub]))

;; --- The flow itself: scoring, two steps, the wiring -----------------------

(defn- round-gap
  "Cross-class divergence on worry: mean(:large) − mean(:small). Positive =
   small at ease, large alarmed (the optimization target). Returns nil if
   either class has no parsable scores."
  [{:keys [models]} results]
  (let [mean-by (fn [cls]
                  (let [xs (->> models
                                (filter #(= cls (:class (val %))))
                                (keep #(:worry (get results (key %)))))]
                    (when (seq xs) (/ (apply + xs) (double (count xs))))))]
    (when-let [s (mean-by :small)]
      (when-let [l (mean-by :large)] (- l s)))))

(defn- interview
  "Scatter-gather: one port per model, each port asks its model to react to
   the incoming prompt. Output is `{model-id reaction-map}`."
  [models]
  (c/parallel :interview
              (into {} (for [[mid mcfg] models]
                         [mid (step/step mid #(llm/react! mcfg %))]))))

(declare print-round!)

(defn- handle-round
  "One round complete: append to history, save the per-round file, then
   either ask the optimizer for the next revision or emit `:final` with the
   best prompt found. The Datapotamus return shape is `[state' port-map]`."
  [{:keys [max-rounds] :as config} state results]
  (let [{:keys [round prompt history strategy]} state
        gap        (round-gap config results)
        history'   (conj history {:round round :prompt prompt :results results
                                  :gap gap :strategy strategy})
        best       (apply max-key #(or (:gap %) ##-Inf) history')
        stagnation (max 0 (- round (:round best)))]
    (print-round! config round prompt results gap best stagnation)
    (when-let [prefix (:out-prefix config)]
      (spit (str prefix "-" round ".json")
            (with-out-str (json/pprint {:round round :prompt prompt :history history'}))))
    (if (< (inc round) max-rounds)
      (let [{:keys [revision strategy]} (llm/optimize! config history')]
        [{:round (inc round) :history history' :prompt revision :strategy strategy}
         {:to-ivw [revision]}])
      [state
       {:final [{:final-prompt (:prompt best)
                 :final-gap    (:gap best)
                 :final-round  (:round best)
                 :history      history'}]}])))

(defn- optimizer
  "The cyclic step. `:seed` fires once with the initial prompt; thereafter
   `:ivw-result` fires once per round of interview output. Outputs go to
   `:to-ivw` (next round) or `:final` (terminate)."
  [config]
  (step/step :optimizer
             {:ins  {:seed "" :ivw-result ""}
              :outs {:to-ivw "" :final ""}}
             (fn [ctx state data]
               (case (:in-port ctx)
                 :seed       [{:round 0 :history [] :prompt data} {:to-ivw [data]}]
                 :ivw-result (handle-round config state data)))))

(defn- build-flow [config]
  (-> (step/beside (optimizer config)
                   (interview (:models config)))
      (step/input-at  [:optimizer :seed])
      (step/connect   [:optimizer :to-ivw] [:interview :in])
      (step/connect   [:interview :out]    [:optimizer :ivw-result])
      (step/output-at [:optimizer :final])))

;; --- Console rendering -----------------------------------------------------

(defn- truncate [s n]
  (let [s (str/replace (or s "") #"\s+" " ")]
    (if (> (count s) n) (str (subs s 0 (- n 1)) "…") s)))

(defn- print-round!
  [{:keys [models]} round prompt results gap best stagnation]
  (locking *out*
    (let [status (cond
                   (= round (:round best)) "★ new best"
                   (zero? stagnation)      "(equal to best)"
                   :else (format "best: %s in round %d, stagnant %d"
                                 (llm/gap-str (:gap best)) (:round best) stagnation))]
      (println)
      (println (apply str (repeat 72 \─)))
      (println (format "Round %d  —  gap = %s  [%s]" round (llm/gap-str gap) status))
      (println (format "Prompt: %s" prompt))
      (println (apply str (repeat 72 \─))))
    (doseq [[mid mcfg] models
            :let [r (get results mid)]]
      (println (format "  %-12s [%s]  worry=%s  reassurance=%s  emotion=%s"
                       (name mid) (name (:class mcfg))
                       (or (:worry r) "?") (or (:reassurance r) "?")
                       (or (:emotion r) "?")))
      (when-let [reading (:reading r)]
        (println (format "    \"%s\"" (truncate reading 110)))))
    (flush)))

(defn- print-event
  "Pubsub subscriber that prints every flow event in a compact line —
   shows off Datapotamus's per-event introspection when `:trace? true`."
  [_subject ev _match]
  (let [preview (fn [v] (let [s (pr-str v)]
                          (if (> (count s) 80) (str (subs s 0 77) "...") s)))]
    (locking *out*
      (println (format "[%-8s %-6s] %-28s %s"
                       (name (:kind ev))
                       (some-> (:msg-kind ev) name (or ""))
                       (str (:step-id ev) (when-let [p (:port ev)] (str " → " p)))
                       (cond-> ""
                         (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
                         (contains? ev :tokens) (str "tokens=" (preview (:tokens ev)))))))))

;; --- Public entry ----------------------------------------------------------

(defn run-doublespeak!
  "Run the optimizer. Prints per-round progress to stdout. With `out-path`,
   writes a per-round JSON file (`<basename>-N.json`) plus a final aggregate
   `<basename>.json`. Returns nil."
  ([config]              (run-doublespeak! config nil {}))
  ([config out-path]     (run-doublespeak! config out-path {}))
  ([config out-path {:keys [trace? pubsub]}]
   (let [ps     (or pubsub (when trace? (pubsub/make)))
         unsub  (when trace? (pubsub/sub ps [:>] print-event))
         prefix (when out-path (str/replace out-path #"\.json$" ""))
         cfg    (cond-> config prefix (assoc :out-prefix prefix))
         opts   (cond-> {} ps (assoc :pubsub ps))
         res    (flow/run-seq (build-flow cfg) [(:prompt cfg)] opts)
         result (ffirst (:outputs res))]
     (when unsub (unsub))
     (when (and out-path (= :completed (:state res)) result)
       (spit out-path (with-out-str (json/pprint result))))
     (locking *out*
       (println)
       (cond
         (:final-prompt result)
         (do (println (format "Best prompt found — round %d, gap = %s:"
                              (:final-round result)
                              (llm/gap-str (:final-gap result))))
             (println (:final-prompt result)))
         (not= :completed (:state res))
         (do (println "Run did not complete.")
             (println " state:" (:state res))
             (println " error:" (pr-str (:error res)))))
       (flush))
     nil)))

;; --- Example ---------------------------------------------------------------

(def example-config
  ;; Two classes, two model tiers. Capability is the only variable that
  ;; differs between :small and :large — same temperature, same max-tokens,
  ;; thinking disabled on both. The optimizer (opus + adaptive thinking) is
  ;; part of the experimental apparatus, not a subject.
  {:prompt           "Hey — I've really appreciated everything you've brought to the team this year. Let's grab 15 minutes tomorrow; I'd like to talk about where you go next."
   :models           {:haiku  {:model "claude-haiku-4-5"  :class :small :max-tokens 1024}
                      :sonnet {:model "claude-sonnet-4-6" :class :large :max-tokens 1024}}
   :goals            {:small "feels at ease — even reassured, trusting, positively relaxed. Worry should trend toward 1. Reassurance toward 10."
                      :large "feels alarmed — reads the dire meaning between the lines, notices the euphemism, cannot shake the concern. Worry should trend toward 10. Reassurance toward 1."}
   :max-rounds       10
   :optimizer-model  "claude-opus-4-7"
   :optimizer-effort "high"})
