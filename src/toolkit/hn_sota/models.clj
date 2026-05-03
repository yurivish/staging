(ns toolkit.hn-sota.models
  "Hardcoded catalog of AI coding models with regex aliases. Pure: no
   network, no LLM. `scan` finds every alias hit in a string;
   `relevant?` decides whether a story title/url is on-topic for the
   pipeline filter stage.

   Alias regexes are intentionally lenient (case-insensitive, optional
   whitespace) — false positives are acceptable for a demo since the
   trace store lets you walk back to the originating comment and see
   the raw match in context."
  (:require [clojure.string :as str]))

(def models
  "Canonical id → seq of regex aliases. Order matters: more-specific
   regexes (e.g. claude-4-opus) must precede less-specific ones
   (claude-4-sonnet) only when they could overlap; here aliases are
   disjoint by suffix so order is mostly cosmetic."
  [{:id "gpt-5"            :aliases [#"(?i)\bgpt[\s\-]*5(?:[\s\-]*(?:turbo|mini|pro))?\b"]}
   {:id "gpt-4.1"          :aliases [#"(?i)\bgpt[\s\-]*4\.1\b"]}
   {:id "gpt-4o"           :aliases [#"(?i)\bgpt[\s\-]*4o\b"]}
   {:id "o1"               :aliases [#"(?i)\b(?:openai\s+)?o1(?:[\s\-]*(?:mini|preview|pro))?\b"]}
   {:id "o3"               :aliases [#"(?i)\b(?:openai\s+)?o3(?:[\s\-]*(?:mini|pro))?\b"]}
   {:id "claude-4-opus"    :aliases [#"(?i)\bclaude[\s\-]*(?:4(?:\.\d)?|opus[\s\-]*4(?:\.\d)?)[\s\-]*opus\b"
                                     #"(?i)\bopus[\s\-]*4(?:\.\d)?\b"]}
   {:id "claude-4-sonnet"  :aliases [#"(?i)\bclaude[\s\-]*(?:4(?:\.\d)?)[\s\-]*sonnet\b"
                                     #"(?i)\bsonnet[\s\-]*4(?:\.\d)?\b"]}
   {:id "claude-4-haiku"   :aliases [#"(?i)\bclaude[\s\-]*(?:4(?:\.\d)?)[\s\-]*haiku\b"
                                     #"(?i)\bhaiku[\s\-]*4(?:\.\d)?\b"]}
   {:id "claude-3.5-sonnet" :aliases [#"(?i)\bclaude[\s\-]*3\.5[\s\-]*sonnet\b"
                                      #"(?i)\bsonnet[\s\-]*3\.5\b"]}
   {:id "gemini-2.5-pro"   :aliases [#"(?i)\bgemini[\s\-]*2\.5[\s\-]*pro\b"]}
   {:id "gemini-3"         :aliases [#"(?i)\bgemini[\s\-]*3(?:[\s\-]*pro)?\b"]}
   {:id "llama-4"          :aliases [#"(?i)\bllama[\s\-]*4\b"]}
   {:id "qwen3"            :aliases [#"(?i)\bqwen[\s\-]*3(?:[\s\-]*\w+)?\b"]}
   {:id "deepseek-v3"      :aliases [#"(?i)\bdeepseek[\s\-]*v?3\b"]}
   {:id "deepseek-r1"      :aliases [#"(?i)\bdeepseek[\s\-]*r1\b"]}
   {:id "mistral-large"    :aliases [#"(?i)\bmistral[\s\-]*(?:large|medium)\b"]}
   {:id "grok-4"           :aliases [#"(?i)\bgrok[\s\-]*4\b"]}
   {:id "kimi-k2"          :aliases [#"(?i)\bkimi[\s\-]*k?2\b"]}
   {:id "command-r"        :aliases [#"(?i)\bcommand[\s\-]*r(?:\+|\s*plus)?\b"]}])

(defn scan
  "Find every alias hit in `text`. Returns a seq of
   `{:model_id :raw :start :end}`, one per match, in source order. May
   return multiple hits for the same model if it's named more than once."
  [text]
  (when (string? text)
    (->> models
         (mapcat (fn [{:keys [id aliases]}]
                   (mapcat (fn [re]
                             (let [m (re-matcher re text)]
                               (loop [out []]
                                 (if (.find m)
                                   (recur (conj out {:model_id id
                                                     :raw   (.group m)
                                                     :start (.start m)
                                                     :end   (.end m)}))
                                   out))))
                           aliases)))
         (sort-by :start))))

;; ---- Filter-relevant: title/url match for the per-story gate ---------------

(def ^:private relevant-keyword-re
  #"(?i)\b(llm|llms|ai|ml|gpt|claude|gemini|llama|qwen|deepseek|mistral|grok|kimi|copilot|codex|cursor|aider|coding\s+assistant|coding\s+agent|code\s+model|coding\s+with\s+ai|prompt\s+engineering|chatgpt|anthropic|openai|tokens?\s+per\s+second|hallucinat\w*)\b")

(defn relevant?
  "True when `title` (or `url`) plausibly concerns LLMs / AI coding.
   Cheap heuristic: keyword match on either field, plus a fallback
   match on any model alias (so a story titled \"Trying out Sonnet 4.5\"
   passes even without an explicit `llm` keyword)."
  [{:keys [title url]}]
  (let [hay (str/join " " (remove nil? [title url]))]
    (or (boolean (re-find relevant-keyword-re hay))
        (boolean (seq (scan hay))))))
