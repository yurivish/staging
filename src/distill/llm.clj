(ns distill.llm
  "Anthropic LLM glue (via LangChain4j) for the distill pipeline.

   Three public roles, each a thin call into LangChain4j. They run in
   sequence per candidate (encode → decode → judge):

     `encode!` — shorten an :original to roughly :target-len words,
                 guided by accumulated :tips.
     `decode!` — expand a :short back into a faithful reconstruction.
     `judge!`  — compare :original against :reconstructed and emit
                 a structured {:score 0..1 :judge-tips} pair.

   Each role returns its input map augmented with the role's outputs
   plus a per-call token count (`:encode-tokens`, etc.). `result-tokens`
   sums those for budget tracking in `distill.core`.

   Length is measured in whitespace-delimited words — model-independent,
   deterministic, sufficient for a frontier objective."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import [dev.langchain4j.model.anthropic AnthropicChatModel]
           [dev.langchain4j.model.chat.request ChatRequest]
           [dev.langchain4j.model.chat.request.json JsonObjectSchema]
           [dev.langchain4j.agent.tool ToolSpecification]
           [dev.langchain4j.data.message UserMessage SystemMessage]))

;; ============================================================================
;; 1. Length helper — what every role and the controller mean by "words".
;;    Defined first because `encode!` targets a length, `judge!` records
;;    it, and `distill.core` checks it against the budget / target.
;; ============================================================================

(defn word-count
  "Whitespace-delimited word count. Pragmatic, model-independent proxy
   for length; swap for a real tokenizer if needed."
  [s]
  (->> (str/split (or s "") #"\s+")
       (remove str/blank?)
       count))

;; ============================================================================
;; 2. LangChain4j adapter — two primitives the roles below build on:
;;
;;      `chat-text!`   plain text completion (encode, decode use this).
;;      `chat-tool!`   structured-output completion (judge uses this).
;;
;;    Both share `model-for` (a memoized AnthropicChatModel builder)
;;    and `usage` (extracts total token count from the response).
;;    `->json-schema` and `->tool-spec` are helpers callers use to build
;;    the tool spec that `chat-tool!` consumes — used by the judge below.
;; ============================================================================

(def ^:private api-key (delay (str/trim (slurp "claude.key"))))

(defn- build-model
  "Build an AnthropicChatModel from a role config. `:provider-extra :thinking`
   maps to typed builder methods (thinkingType / thinkingBudgetTokens);
   everything else under `:provider-extra` becomes raw `customParameters`."
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

(def ^:private model-for
  "Memoized: same role-config → same AnthropicChatModel instance."
  (memoize build-model))

(defn- usage [^dev.langchain4j.model.chat.response.ChatResponse resp]
  (when-let [u (.tokenUsage resp)]
    (long (.totalTokenCount u))))

(defn- chat-text!
  "Plain text completion (no tools). Returns `{:text s :tokens n}`."
  [model system-text user-text]
  (let [req  (-> (ChatRequest/builder)
                 (.messages [(SystemMessage/from system-text)
                             (UserMessage/from user-text)])
                 .build)
        resp (.chat model req)]
    {:text   (.text (.aiMessage resp))
     :tokens (or (usage resp) 0)}))

(defn- ->json-schema
  "Flat map → langchain4j JsonObjectSchema. Supports string / integer /
   number property types; sufficient for the judge's schema."
  [{:keys [properties required]}]
  (let [b (JsonObjectSchema/builder)]
    (doseq [[k spec] properties
            :let     [n (name k) d (:description spec "")]]
      (case (:type spec)
        "integer" (.addIntegerProperty b n d)
        "number"  (.addNumberProperty  b n d)
        "string"  (.addStringProperty  b n d)))
    (when (seq required) (.required b ^java.util.List required))
    (.build b)))

(defn- ->tool-spec [tool-name desc schema]
  (-> (ToolSpecification/builder)
      (.name tool-name) (.description desc)
      (.parameters (->json-schema schema))
      .build))

(defn- chat-tool!
  "Structured-output call. Returns `{:args {...} :tokens n}` (args parsed
   as a keyword-keyed map, or `{}` if the model didn't call the tool)."
  [model system-text user-text tool-spec]
  (let [req  (-> (ChatRequest/builder)
                 (.messages [(SystemMessage/from system-text)
                             (UserMessage/from user-text)])
                 (.toolSpecifications [tool-spec])
                 .build)
        resp (.chat model req)
        tcs  (.toolExecutionRequests (.aiMessage resp))]
    {:args   (if (seq tcs)
               (json/read-str (.arguments (first tcs)) :key-fn keyword)
               {})
     :tokens (or (usage resp) 0)}))

;; ============================================================================
;; 3. The three roles — each role is a (system-prompt + public function)
;;    pair. The function takes the role's model config plus the running
;;    payload map, calls the right primitive above, and returns the
;;    payload augmented with its outputs and a per-call token count.
;;
;;    Order below matches the order in which a candidate visits them:
;;      encode! → decode! → judge!
;; ============================================================================

;; --- encode! ------------------------------------------------------------

(def ^:private encode-system
  "You are an expert text compressor. Rewrite the user's message to be SHORTER while preserving the maximum amount of meaning, intent, tone, and concrete detail.

You will be given a target length (in whitespace-delimited words). Aim close to that target — slightly under is fine, going significantly over defeats the purpose.

Output ONLY the shortened message. No preamble, no quotes, no commentary, no markdown. The first character of your reply should be the first character of the message.

If the user provides tips from a critic about what was missed in prior attempts, prioritize preserving the items the critic flagged.")

(defn encode!
  "Shorten `:original` to roughly `:target-len` words, guided by `:tips`.
   Returns the input map merged with `{:short s :encode-tokens n}`."
  [role-cfg {:keys [original target-len tips] :as data}]
  (let [user (str "Target length: ~" target-len " words.\n\n"
                  (when (seq tips)
                    (str "Critic's tips from prior attempts:\n" tips "\n\n"))
                  "Original message:\n" original)
        {:keys [text tokens]} (chat-text! (model-for role-cfg) encode-system user)]
    (assoc data
           :short         (str/trim text)
           :encode-tokens tokens)))

;; --- decode! ------------------------------------------------------------

(def ^:private decode-system
  "You are an expert text reconstructor. The user will give you a SHORTENED message that was distilled from a longer original. Your job is to reconstruct the most likely full message — restore phrasing, tone, transitions, and detail consistent with what the short version implies.

Output ONLY the reconstructed message. No preamble, no quotes, no commentary, no markdown. The first character of your reply should be the first character of the message.

You don't have access to the original. Reconstruct in the spirit and register the short version suggests; don't pad with content that the short version doesn't justify.")

(defn decode!
  "Expand `:short` back into a faithful reconstruction. Returns the input
   map merged with `{:reconstructed s :decode-tokens n}`."
  [role-cfg {:keys [short] :as data}]
  (let [{:keys [text tokens]} (chat-text! (model-for role-cfg)
                                          decode-system
                                          (str "Shortened message:\n" short))]
    (assoc data
           :reconstructed (str/trim text)
           :decode-tokens tokens)))

;; --- judge! -------------------------------------------------------------
;; The judge is the only role that returns structured output (a numeric
;; score plus actionable tips), hence the JsonObjectSchema + Tool spec.

(def ^:private judge-schema
  {:properties
   {:score      {:type "number"
                 :description "Reconstruction fidelity, 0.0 to 1.0. 1.0 = the reconstruction recovers all meaning, intent, tone, and concrete details. 0.0 = the reconstruction is unrelated or wrong on key points."}
    :judge-tips {:type "string"
                 :description "One short paragraph of concrete tips for the encoder: what specific items were missed, distorted, or lost. Cite phrases when useful. If the reconstruction is excellent, say so briefly and identify the smallest remaining gap."}}
   :required ["score" "judge-tips"]})

(def ^:private judge-tool
  (->tool-spec "submit_judgment"
               "Submit your fidelity score and concrete tips for the encoder."
               judge-schema))

(def ^:private judge-system
  "You are a judge for a text-compression pipeline. You will be given an ORIGINAL message and a RECONSTRUCTION produced by expanding a shortened version of it.

Score reconstruction fidelity from 0.0 to 1.0:
- 1.0: every meaningful claim, intent, tone cue, and concrete detail of the original is recovered.
- 0.7-0.9: most meaning preserved; minor losses or rephrasings.
- 0.4-0.6: gist preserved but major details lost or distorted.
- 0.0-0.3: substantially unfaithful.

Then write tips for the encoder — ACTIONABLE pointers to what was missed or distorted. Cite specific phrases or facts. The encoder will use these tips to improve its next shortening attempt.

You MUST respond by calling the `submit_judgment` tool. Do not produce any text outside the tool call.")

(defn judge!
  "Compare `:original` and `:reconstructed`. Returns the input map merged
   with `{:score :judge-tips :length :judge-tokens}` — the four fields
   that the controller's frontier and stop-conditions read."
  [role-cfg {:keys [original reconstructed short] :as data}]
  (let [user (str "ORIGINAL:\n" original "\n\nRECONSTRUCTION:\n" reconstructed)
        {:keys [args tokens]} (chat-tool! (model-for role-cfg)
                                          judge-system user judge-tool)]
    (assoc data
           :score        (double (or (:score args) 0.0))
           :judge-tips   (or (:judge-tips args) "")
           :length       (word-count short)
           :judge-tokens tokens)))

;; ============================================================================
;; 4. Per-candidate token aggregation — the controller calls this once per
;;    candidate per round, sums the result across the K candidates, and
;;    checks the running total against `:token-budget`.
;; ============================================================================

(defn result-tokens
  "Sum the token counts across one candidate's three calls."
  [{:keys [encode-tokens decode-tokens judge-tokens] :or {encode-tokens 0
                                                          decode-tokens 0
                                                          judge-tokens 0}}]
  (+ encode-tokens decode-tokens judge-tokens))
