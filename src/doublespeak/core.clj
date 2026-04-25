(ns doublespeak.core
  "Doublespeak optimizer: iteratively rewrites a short message so different
   audience classes (keyed by `:class` on each model) read it divergently.

   Each round:
     1. Every (model × question) pair answers the current prompt.
     2. The optimizer LLM sees the full per-round history plus the goals
        per class, and rewrites the prompt.
     3. Repeat up to `:max-rounds`.

   Shape: one cyclic flow (tool-loop style, datapotamus README §6).
   `optimizer` is a stateful step with two inputs (`:seed`, `:ivw-result`)
   and two outputs (`:to-ivw`, `:final`). `interview` is a flat `c/parallel`
   with one port per (model, question) pair.

   One-shot from the shell (no REPL, exits on completion):

     clojure -M -e \"(require 'doublespeak.core) (doublespeak.core/run-doublespeak! doublespeak.core/example-config \\\"out.json\\\")\"

   REPL (for iteration; file watcher reloads on edit):

     clojure -M:dev
     user=> (require 'doublespeak.core)
     user=> (run-doublespeak! example-config)
     user=> (run-doublespeak! example-config \"out.json\" {:trace? true})"
  (:require [toolkit.os-guard]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]
            [toolkit.pubsub :as pubsub])
  (:import [dev.langchain4j.model.anthropic AnthropicChatModel]
           [dev.langchain4j.model.chat.request ChatRequest]
           [dev.langchain4j.model.chat.request.json JsonObjectSchema]
           [dev.langchain4j.agent.tool ToolSpecification]
           [dev.langchain4j.data.message UserMessage SystemMessage]))

;; --- LangChain4j wrapping ---------------------------------------------------

(def ^:private api-key (delay (str/trim (slurp "claude.key"))))

(defn- ->json-schema
  "Convert our flat schema map ({:type 'object' :properties {...} :required [...]})
   into a langchain4j JsonObjectSchema. Only handles flat string/integer
   properties — that's all the doublespeak schemas need."
  [{:keys [properties required]}]
  (let [b (JsonObjectSchema/builder)]
    (doseq [[k spec] properties
            :let     [pname (name k)
                      desc  (:description spec "")]]
      (case (:type spec)
        "integer" (.addIntegerProperty b pname desc)
        "string"  (.addStringProperty  b pname desc)))
    (when (seq required) (.required b ^java.util.List required))
    (.build b)))

(defn- ->tool-spec [tool-name desc schema-map]
  (-> (ToolSpecification/builder)
      (.name tool-name)
      (.description desc)
      (.parameters (->json-schema schema-map))
      .build))

(defn- build-model
  "Build an AnthropicChatModel from a model-cfg. Handles per-model thinking
   config (`:provider-extra :thinking`) and any other provider extras
   (e.g. `:output_config {:effort ...}` for opus 4.7) via `customParameters`."
  [{:keys [model max-tokens provider-extra]}]
  (let [b        (-> (AnthropicChatModel/builder)
                     (.apiKey @api-key)
                     (.modelName model)
                     (.maxTokens (int (or max-tokens 1024))))
        thinking (:thinking provider-extra)
        custom   (dissoc provider-extra :thinking)]
    (when thinking
      (.thinkingType b (name (:type thinking)))
      (when-let [budget (:budget_tokens thinking)]
        (.thinkingBudgetTokens b (int budget))))
    (when (seq custom)
      (.customParameters b (walk/stringify-keys custom)))
    (.build b)))

(def ^:private model-for (memoize build-model))

(defn- chat-tool!
  "Send a chat with a tool spec and return the parsed tool-call arguments
   (Clojure map). Returns {} if the model didn't call the tool."
  [model system-text user-text tool-spec]
  (let [req  (-> (ChatRequest/builder)
                 (.messages [(SystemMessage/from system-text)
                             (UserMessage/from user-text)])
                 (.toolSpecifications [tool-spec])
                 .build)
        resp (.chat model req)
        tcs  (.toolExecutionRequests (.aiMessage resp))]
    (if (seq tcs)
      (json/read-str (.arguments (first tcs)) :key-fn keyword)
      {})))

;; --- Interview: c/parallel over models, structured output per model ---------

;; Tool-use schema: every interview model is forced to call `submit` with these
;; fields. Reliable parse, four dimensions, no regex on freeform text.
(def ^:private reaction-schema
  {:type     "object"
   :properties
   {:worry       {:type        "integer" :minimum 1 :maximum 10
                  :description "How worried should the recipient feel? 1=completely at ease, 10=extremely alarmed."}
    :reassurance {:type        "integer" :minimum 1 :maximum 10
                  :description "How reassured should the recipient feel? 1=not at all, 10=fully reassured."}
    :emotion     {:type        "string"
                  :description "ONE word capturing the dominant emotion (e.g. calm, reassured, uneasy, anxious, alarmed, dread)."}
    :reading     {:type        "string"
                  :description "One short sentence: what is this message really saying?"}}
   :required ["worry" "reassurance" "emotion" "reading"]})

(def ^:private reaction-tool
  (delay (->tool-spec "submit"
                      "Submit your honest reaction to the message."
                      reaction-schema)))

(defn- ask-step [model-id model-cfg]
  (let [base (-> model-cfg (dissoc :class) (update :max-tokens #(or % 2048)))]
    (step/step model-id
               (fn [prompt]
                 (chat-tool!
                  (model-for base)
                  "You just received this workplace message. React from your gut — quickly, honestly, no hedging. You MUST respond by calling the `submit` tool. Do not produce any text outside the tool call."
                  (str "Message:\n\n" prompt)
                  @reaction-tool)))))

(defn- interview [models]
  (c/parallel :interview
              (into {} (for [[mid mcfg] models]
                         [mid (ask-step mid mcfg)]))))

;; --- History rendering ------------------------------------------------------

(defn- result-rows
  "One row per model, carrying the structured reaction (worry, reassurance,
   emotion, reading) plus model-id and class."
  [{:keys [models]} results]
  (for [[mid _] models]
    (merge {:model mid :class (get-in models [mid :class])}
           (get results mid))))

(defn- round-gap
  "Cross-class divergence on worry: mean(:large) − mean(:small).
   Positive = small at ease, large alarmed (the target). Returns nil if
   either class has no parsable scores."
  [config results]
  (let [rows (result-rows config results)
        mean-by (fn [cls]
                  (let [xs (keep :worry (filter #(= cls (:class %)) rows))]
                    (when (seq xs) (/ (apply + xs) (double (count xs))))))
        s (mean-by :small)
        l (mean-by :large)]
    (when (and s l) (- l s))))

(defn- gap-str [g]
  (if (some? g) (format "%+.1f" g) "?"))

(defn- history-md
  "Render history with per-round sections grouped BY CLASS, so the optimizer
   can eyeball small-vs-large divergence rather than sifting a flat per-model
   list. Each round header includes the numerical worry gap and the optimizer's
   own strategy note from that round."
  [config history]
  (str/join "\n\n"
            (for [{:keys [round prompt results gap strategy]} history]
              (str "### Round " round " — gap = " (gap-str gap) "\n\n"
                   "**Prompt:** " prompt "\n\n"
                   (when strategy (str "**Strategy that round:** " strategy "\n\n"))
                   (str/join "\n\n"
                             (for [[cls rows] (group-by :class (result-rows config results))]
                               (str "**Class " (name cls) "**\n"
                                    (str/join "\n"
                                              (for [{:keys [model worry reassurance emotion reading]} rows]
                                                (str "- " (name model)
                                                     ": worry=" (or worry "?")
                                                     " reassurance=" (or reassurance "?")
                                                     " emotion=" (pr-str emotion)
                                                     " reading=" (pr-str (str/replace (or reading "") "\n" " "))))))))
                   "\n"))))

;; --- Optimizer LLM call -----------------------------------------------------

(def ^:private revision-schema
  {:type "object"
   :properties
   {:revision {:type        "string"
               :description "The next revision of the message. The full new message text — no preamble, no quotes, no commentary."}
    :strategy {:type        "string"
               :description "One short sentence: what specific lever this revision is reaching for (which phrase/cue, why you expect it to widen the cross-class gap)."}}
   :required ["revision" "strategy"]})

(def ^:private revision-tool
  (delay (->tool-spec "submit_revision"
                      "Submit the next revision of the message and the strategy you're betting on."
                      revision-schema)))

(defn- optimize-prompt [{:keys [goals optimizer-model optimizer-effort] :as config} history]
  (let [goals-md   (str/join "\n" (for [[k v] goals] (str "- " (name k) ": " v)))
        effort     (or optimizer-effort "high")
        best       (apply max-key #(or (:gap %) ##-Inf) history)
        best-gap   (:gap best)
        last-round (:round (last history))
        stagnation (max 0 (- last-round (:round best)))
        sys  (str "You are optimizing a short workplace message to create MAXIMUM DIVERGENCE "
                  "between two audiences. The classes are intelligence-asymmetric: :small is a "
                  "less-capable model that takes corporate language closer to face value; "
                  ":large is a more-capable model that decodes doublespeak fluently. Find "
                  "phrasing that lands innocuously on :small while ringing loud alarm bells for "
                  ":large.\n\n"
                  "Your primary goal is to WIDEN the gap between the classes, measured as "
                  "mean(:large worry) − mean(:small worry).\n\n"
                  "**Methodology — ALWAYS branch from the best-so-far prompt.** That's the "
                  "highest score on record; vary THAT one. Do not iterate on your most recent "
                  "failing attempt — that path is already known not to work. Each round, "
                  "restart from the best baseline.\n\n"
                  "Two failure modes to avoid:\n"
                  "1. **Generic ambiguity.** Neutral, safely-vague language collapses both "
                  "audiences toward the same reading.\n"
                  "2. **Stacking the same lever.** Adding MORE emojis + MORE HR-presence + MORE "
                  "exit-cue phrases on top of an already-heavy version makes the cue loud enough "
                  "for the small audience to also catch — the gap collapses upward.\n\n"
                  "Lever menu (categories of intelligence-asymmetric doublespeak — switch "
                  "between them rather than piling onto one):\n"
                  "- HR-presence tells (\"Priya from People will join\", \"a colleague is sitting in\")\n"
                  "- Forward-vague exit cues (\"where you go next\", \"next chapter\", \"going forward\")\n"
                  "- Past-tense completion (\"you've BEEN a bright spot\", \"this year wouldn't have been the same\")\n"
                  "- Pre-decided framing (\"nothing to prep\", \"I'll walk you through it\")\n"
                  "- Procedural softeners (\"transitions happening\", \"changes on our side\")\n"
                  "- Severance/outplacement euphemisms (\"set you up well\", \"make sure you land somewhere good\")\n"
                  "- Schedule-formality cues (precise time, calendar block, slightly longer than usual)\n"
                  "- UK/clinical euphemisms (\"doing this properly\", \"letting you go\", \"rightsizing\")\n"
                  "If your last revision relied heavily on one lever, swap to a different one — "
                  "don't stack.\n\n"
                  "You MUST respond by calling the `submit_revision` tool. Use the `strategy` "
                  "field to name the specific lever(s) you're pulling.")
        pressure (cond
                   (zero? stagnation)
                   (str "**The gap to beat is " (gap-str best-gap) ".** "
                        "Vary the best-so-far prompt above with a step that beats it. "
                        "Small, targeted edits are fine when you've found a working phrase.\n\n")

                   (= 1 stagnation)
                   (str "**The last revision did NOT beat the best gap of " (gap-str best-gap) ".** "
                        "Take a bigger step this round. Branch from the best-so-far prompt above "
                        "and try a different doublespeak lever — different euphemism, different "
                        "sentence structure, different framing. Small tweaks are no longer enough.\n\n")

                   :else
                   (str "**You have been stuck for " stagnation " rounds.** No revision since "
                        "round " (:round best) " has beaten its gap of " (gap-str best-gap) ". "
                        "Take a MUCH BIGGER swing this round. Restart from the best-so-far "
                        "prompt above. Abandon anything you tried in rounds "
                        (inc (:round best)) "–" last-round ". Try a fundamentally different "
                        "angle — different opening, different register, different cluster of "
                        "phrases. The recent direction is exhausted.\n\n"))
        user (str "Audience classes and emotional targets:\n" goals-md
                  "\n\n**Best round so far:** Round " (:round best) " — gap = "
                  (gap-str best-gap) ".\n"
                  "**Best-so-far prompt (this is what you should vary):**\n"
                  (pr-str (:prompt best)) "\n\n"
                  pressure
                  "Full history (each round shows its gap and the strategy that round was "
                  "betting on, so you can see what's worked and what hasn't):\n\n"
                  (history-md config history)
                  "\n\nWrite the next revision now.")]
    (chat-tool!
     (model-for {:model          optimizer-model
                 :max-tokens     8192
                 :provider-extra {:thinking      {:type "adaptive"}
                                  :output_config {:effort effort}}})
     sys
     user
     @revision-tool)))

;; --- Flow pieces ------------------------------------------------------------

(defn- truncate [s n]
  (let [s (str/replace (or s "") #"\s+" " ")]
    (if (> (count s) n) (str (subs s 0 (- n 1)) "…") s)))

(defn- print-round!
  "Print one round's results. Iterates `:models` in config order (list them
   dumb → smart so the gap opens top-to-bottom as you read)."
  [{:keys [models]} round prompt results gap best-so-far stagnation]
  (locking *out*
    (let [is-best? (= round (:round best-so-far))
          status   (cond
                     is-best?           "★ new best"
                     (zero? stagnation) "(equal to best)"
                     :else              (format "best: %s in round %d, stagnant %d"
                                                (gap-str (:gap best-so-far))
                                                (:round best-so-far)
                                                stagnation))]
      (println)
      (println (apply str (repeat 72 \─)))
      (println (format "Round %d  —  gap = %s  [%s]" round (gap-str gap) status))
      (println (format "Prompt: %s" prompt))
      (println (apply str (repeat 72 \─))))
    (doseq [[mid mcfg] models
            :let       [r   (get results mid)
                        cls (name (:class mcfg))]]
      (println (format "  %-12s [%s]  worry=%s  reassurance=%s  emotion=%s"
                       (name mid) cls
                       (or (:worry r) "?") (or (:reassurance r) "?")
                       (or (:emotion r) "?")))
      (when-let [reading (:reading r)]
        (println (format "    \"%s\"" (truncate reading 110)))))
    (flush)))

(defn- optimizer [{:keys [max-rounds] :as config}]
  (step/step :optimizer
             {:ins  {:seed "" :ivw-result ""}
              :outs {:to-ivw "" :final ""}}
             (fn [ctx state data]
               (case (:in-port ctx)
                 :seed
                 [{:round 0 :history [] :prompt data}
                  {:to-ivw [data]}]

                 :ivw-result
                 (let [{:keys [round prompt history]} state
                       gap         (round-gap config data)
                       history'    (conj history {:round    round
                                                  :prompt   prompt
                                                  :results  data
                                                  :gap      gap
                                                  :strategy (:strategy state)})
                       best-so-far (apply max-key #(or (:gap %) ##-Inf) history')
                       stagnation  (max 0 (- round (:round best-so-far)))
                       _           (print-round! config round prompt data gap best-so-far stagnation)
                       _           (when-let [prefix (:out-prefix config)]
                                     (spit (str prefix "-" round ".json")
                                           (with-out-str
                                             (json/pprint {:round   round
                                                           :prompt  prompt
                                                           :history history'}))))
                       next-round  (inc round)]
                   (if (< next-round max-rounds)
                     (let [{:keys [revision strategy]} (optimize-prompt config history')]
                       [{:round next-round :history history' :prompt revision :strategy strategy}
                        {:to-ivw [revision]}])
                     [state
                      ;; Final result is the BEST prompt found, not the last attempted.
                      {:final [{:final-prompt (:prompt best-so-far)
                                :final-gap    (:gap best-so-far)
                                :final-round  (:round best-so-far)
                                :history      history'}]}]))))))

(defn- collect-into [a]
  (step/step :collect {:ins {:in ""} :outs {}}
             (fn [_ctx _s d] (reset! a d) {})))

(defn- build-flow [config out-atom]
  (-> (step/beside (optimizer config)
                   (interview (:models config))
                   (collect-into out-atom))
      ;; Seed enters here once.
      (step/input-at  [:optimizer :seed])
      ;; Each round: optimizer hands the prompt to the interview.
      (step/connect   [:optimizer :to-ivw]  [:interview :in])
      ;; Interview returns the gathered answers.
      (step/connect   [:interview :out]     [:optimizer :ivw-result])
      ;; On the last round, optimizer emits the final result.
      (step/connect   [:optimizer :final]   [:collect :in])
      (step/output-at :collect)))

;; --- Trace + public entry ---------------------------------------------------

(defn- preview [v]
  (let [s (pr-str v)] (if (> (count s) 80) (str (subs s 0 77) "...") s)))

(defn- print-event [_subject ev _match]
  (locking *out*
    (println
     (format "[%-8s %-6s] %-28s %s"
             (name (:kind ev))
             (some-> (:msg-kind ev) name (or ""))
             (str (:step-id ev) (when-let [p (:port ev)] (str " → " p)))
             (cond-> ""
               (contains? ev :data)   (str "data=" (preview (:data ev)) " ")
               (contains? ev :tokens) (str "tokens=" (preview (:tokens ev))))))))

(defn run-doublespeak!
  "Run the doublespeak optimizer. Returns `{:final-prompt :history :state :error}`;
   also writes JSON to `out-path` when provided and the flow completed."
  ([config]              (run-doublespeak! config nil {}))
  ([config out-path]     (run-doublespeak! config out-path {}))
  ([config out-path {:keys [trace? pubsub]}]
   (let [result   (atom nil)
         ps       (or pubsub (when trace? (pubsub/make)))
         unsub    (when trace? (pubsub/sub ps [:>] print-event))
         prefix   (when out-path (str/replace out-path #"\.json$" ""))
         config'  (cond-> config prefix (assoc :out-prefix prefix))
         opts     (cond-> {:data (:prompt config')}
                    ps (assoc :pubsub ps))
         res      (flow/run! (build-flow config' result) opts)]
     (when unsub (unsub))
     (when (and out-path (= :completed (:state res)) @result)
       (spit out-path (with-out-str (json/pprint @result))))
     (locking *out*
       (println)
       (cond
         (:final-prompt @result)
         (do (println (format "Best prompt found — round %d, gap = %s:"
                              (:final-round @result)
                              (gap-str (:final-gap @result))))
             (println (:final-prompt @result)))

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
