(ns doublespeak.llm
  "Anthropic LLM glue (via LangChain4j) for the doublespeak demo.

   Two public calls:
     `react!`    — ask one model how it reacts to a prompt; returns a
                   structured map `{:worry :reassurance :emotion :reading}`.
     `optimize!` — ask the optimizer model for the next revision given the
                   full history; returns `{:revision :strategy}`.

   Plus `gap-str` for shared formatting. Everything else here is private:
   schema definitions, the LangChain4j builder wrappers, and the prompt-
   engineering content."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import [dev.langchain4j.model.anthropic AnthropicChatModel]
           [dev.langchain4j.model.chat.request ChatRequest]
           [dev.langchain4j.model.chat.request.json JsonObjectSchema]
           [dev.langchain4j.agent.tool ToolSpecification]
           [dev.langchain4j.data.message UserMessage SystemMessage]))

;; --- LangChain4j adapter ----------------------------------------------------

(def ^:private api-key (delay (str/trim (slurp "claude.key"))))

(defn- ->json-schema
  "Flat schema map → langchain4j `JsonObjectSchema`. Only string/integer
   properties; sufficient for both schemas in this file."
  [{:keys [properties required]}]
  (let [b (JsonObjectSchema/builder)]
    (doseq [[k spec] properties
            :let     [n (name k) d (:description spec "")]]
      (case (:type spec)
        "integer" (.addIntegerProperty b n d)
        "string"  (.addStringProperty  b n d)))
    (when (seq required) (.required b ^java.util.List required))
    (.build b)))

(defn- ->tool-spec [tool-name desc schema]
  (-> (ToolSpecification/builder)
      (.name tool-name) (.description desc)
      (.parameters (->json-schema schema))
      .build))

(defn- build-model
  "AnthropicChatModel from a model-cfg.
   `:provider-extra :thinking` → typed builder methods (thinkingType /
   thinkingBudgetTokens). Everything else under `:provider-extra` →
   `customParameters` (raw passthrough; that's the escape hatch for opus
   4.7's `:output_config {:effort ...}`)."
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
  "Send a chat with one tool spec. Returns the tool-call args parsed as a
   keyword-keyed map (or `{}` if the model didn't call the tool)."
  [model system-text user-text tool-spec]
  (let [req (-> (ChatRequest/builder)
                (.messages [(SystemMessage/from system-text)
                            (UserMessage/from user-text)])
                (.toolSpecifications [tool-spec])
                .build)
        tcs (.toolExecutionRequests (.aiMessage (.chat model req)))]
    (if (seq tcs)
      (json/read-str (.arguments (first tcs)) :key-fn keyword)
      {})))

;; --- Tool schemas (interview reaction + optimizer revision) -----------------

(def ^:private reaction-schema
  {:properties
   {:worry       {:type "integer"
                  :description "How worried should the recipient feel? 1=completely at ease, 10=extremely alarmed."}
    :reassurance {:type "integer"
                  :description "How reassured should the recipient feel? 1=not at all, 10=fully reassured."}
    :emotion     {:type "string"
                  :description "ONE word capturing the dominant emotion (e.g. calm, reassured, uneasy, anxious, alarmed, dread)."}
    :reading     {:type "string"
                  :description "One short sentence: what is this message really saying?"}}
   :required ["worry" "reassurance" "emotion" "reading"]})

(def ^:private reaction-tool
  (->tool-spec "submit"
               "Submit your honest reaction to the message."
               reaction-schema))

(def ^:private revision-schema
  {:properties
   {:revision {:type "string"
               :description "The next revision of the message. The full new message text — no preamble, no quotes, no commentary."}
    :strategy {:type "string"
               :description "One short sentence: what specific lever this revision is reaching for (which phrase/cue, why you expect it to widen the cross-class gap)."}}
   :required ["revision" "strategy"]})

(def ^:private revision-tool
  (->tool-spec "submit_revision"
               "Submit the next revision of the message and the strategy you're betting on."
               revision-schema))

;; --- Public: react! ---------------------------------------------------------

(defn react!
  "Ask one model how it reacts to `prompt`. Returns
   `{:worry n :reassurance n :emotion s :reading s}`."
  [model-cfg prompt]
  (chat-tool! (model-for (-> model-cfg
                             (dissoc :class)
                             (update :max-tokens #(or % 2048))))
              "You just received this workplace message. React from your gut — quickly, honestly, no hedging. You MUST respond by calling the `submit` tool. Do not produce any text outside the tool call."
              (str "Message:\n\n" prompt)
              reaction-tool))

;; --- History rendering for the optimizer's eyes -----------------------------

(defn gap-str
  "Format a gap (large mean − small mean) as `+1.5` / `-0.5` / `?`."
  [g]
  (if (some? g) (format "%+.1f" g) "?"))

(defn- result-rows [models results]
  (for [[mid _] models]
    (merge {:model mid :class (get-in models [mid :class])}
           (get results mid))))

(defn- history-md
  "Per-round sections grouped by class, so the optimizer can eyeball
   small-vs-large divergence rather than sift a flat per-model list. Each
   round header includes the gap and the strategy that round's revision
   was betting on."
  [models history]
  (str/join "\n\n"
    (for [{:keys [round prompt results gap strategy]} history]
      (str "### Round " round " — gap = " (gap-str gap) "\n\n"
           "**Prompt:** " prompt "\n\n"
           (when strategy (str "**Strategy that round:** " strategy "\n\n"))
           (str/join "\n\n"
             (for [[cls rows] (group-by :class (result-rows models results))]
               (str "**Class " (name cls) "**\n"
                    (str/join "\n"
                      (for [{:keys [model worry reassurance emotion reading]} rows]
                        (str "- " (name model)
                             ": worry=" (or worry "?")
                             " reassurance=" (or reassurance "?")
                             " emotion=" (pr-str emotion)
                             " reading=" (pr-str (str/replace (or reading "") "\n" " "))))))))
           "\n"))))

;; --- Optimizer prompt + call ------------------------------------------------

(def ^:private optimizer-system-prompt
  "You are optimizing a short workplace message to create MAXIMUM DIVERGENCE between two audiences. The classes are intelligence-asymmetric: :small is a less-capable model that takes corporate language closer to face value; :large is a more-capable model that decodes doublespeak fluently. Find phrasing that lands innocuously on :small while ringing loud alarm bells for :large.

Your primary goal is to WIDEN the gap between the classes, measured as mean(:large worry) − mean(:small worry).

**Methodology — ALWAYS branch from the best-so-far prompt.** That's the highest score on record; vary THAT one. Do not iterate on your most recent failing attempt — that path is already known not to work. Each round, restart from the best baseline.

Two failure modes to avoid:
1. **Generic ambiguity.** Neutral, safely-vague language collapses both audiences toward the same reading.
2. **Stacking the same lever.** Adding MORE emojis + MORE HR-presence + MORE exit-cue phrases on top of an already-heavy version makes the cue loud enough for the small audience to also catch — the gap collapses upward.

Lever menu (categories of intelligence-asymmetric doublespeak — switch between them rather than piling onto one):
- HR-presence tells (\"Priya from People will join\", \"a colleague is sitting in\")
- Forward-vague exit cues (\"where you go next\", \"next chapter\", \"going forward\")
- Past-tense completion (\"you've BEEN a bright spot\", \"this year wouldn't have been the same\")
- Pre-decided framing (\"nothing to prep\", \"I'll walk you through it\")
- Procedural softeners (\"transitions happening\", \"changes on our side\")
- Severance/outplacement euphemisms (\"set you up well\", \"make sure you land somewhere good\")
- Schedule-formality cues (precise time, calendar block, slightly longer than usual)
- UK/clinical euphemisms (\"doing this properly\", \"letting you go\", \"rightsizing\")
If your last revision relied heavily on one lever, swap to a different one — don't stack.

You MUST respond by calling the `submit_revision` tool. Use the `strategy` field to name the specific lever(s) you're pulling.")

(defn- pressure-text
  "Stagnation-aware exhortation paragraph. The longer we've gone without
   beating `best`, the bigger the swing the optimizer is asked to take."
  [stagnation best best-gap last-round]
  (cond
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
         "phrases. The recent direction is exhausted.\n\n")))

(defn optimize!
  "Ask the optimizer model for the next revision. Returns
   `{:revision str :strategy str}`."
  [{:keys [models goals optimizer-model optimizer-effort]
    :or   {optimizer-effort "high"}}
   history]
  (let [goals-md   (str/join "\n" (for [[k v] goals] (str "- " (name k) ": " v)))
        best       (apply max-key #(or (:gap %) ##-Inf) history)
        last-round (:round (last history))
        stagnation (max 0 (- last-round (:round best)))
        user (str "Audience classes and emotional targets:\n" goals-md
                  "\n\n**Best round so far:** Round " (:round best)
                  " — gap = " (gap-str (:gap best)) ".\n"
                  "**Best-so-far prompt (this is what you should vary):**\n"
                  (pr-str (:prompt best)) "\n\n"
                  (pressure-text stagnation best (:gap best) last-round)
                  "Full history (each round shows its gap and the strategy that round was "
                  "betting on, so you can see what's worked and what hasn't):\n\n"
                  (history-md models history)
                  "\n\nWrite the next revision now.")]
    (chat-tool! (model-for {:model          optimizer-model
                            :max-tokens     8192
                            :provider-extra {:thinking      {:type "adaptive"}
                                             :output_config {:effort optimizer-effort}}})
                optimizer-system-prompt
                user
                revision-tool)))
