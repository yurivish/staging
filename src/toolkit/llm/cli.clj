(ns toolkit.llm.cli
  "Invoke Claude through the `claude -p` CLI for structured JSON output.
   Uses the user's local Claude Code subscription rather than direct
   Anthropic API credentials, so no API key is needed.

   The wire is:
     claude -p --no-session-persistence \\
            --model <m> --output-format json \\
            --json-schema '<JSON-Schema>' \\
            --append-system-prompt '<system>'
       <user-message via stdin>

   `--output-format json` wraps the response in
   `{:type \"result\" :is_error false :result \"<inner-json-string>\"}`.
   `--json-schema` constrains `result` to match the given schema. We
   parse the wrapper, then JSON-parse the inner `result`.

   The process invocation is behind `*invoke-claude*` so tests can
   redef it without spawning real subprocesses."
  (:require [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.string :as str]))

(def ^:dynamic *invoke-claude*
  "Run claude with argv and stdin; return `{:exit n :out s :err s}`.
   Tests rebind to a fixture."
  (fn invoke-claude [argv stdin]
    (apply shell/sh (concat argv [:in stdin]))))

(defn- ->kw-map
  "Convert snake_case keyword keys to kebab-case at the top level."
  [m]
  (reduce-kv (fn [a k v]
               (assoc a (keyword (str/replace (name k) #"_" "-")) v))
             {} m))

(defn- extract-json-object
  "If `s` is JSON, parse it. Otherwise look for the first `{...}` block
   and parse that. Returns parsed map or nil."
  [s]
  (when (string? s)
    (or (try (json/read-str s :key-fn keyword) (catch Throwable _ nil))
        (let [lo (.indexOf ^String s "{")
              hi (.lastIndexOf ^String s "}")]
          (when (and (>= lo 0) (> hi lo))
            (try (json/read-str (subs s lo (inc hi)) :key-fn keyword)
                 (catch Throwable _ nil)))))))

(defn- parse-result
  "Take `{:exit :out :err}`, return the inner-result map or nil."
  [{:keys [exit out]}]
  (when (zero? exit)
    (let [wrap (extract-json-object out)]
      (when (and wrap (not (:is_error wrap)))
        (extract-json-object (:result wrap))))))

(defn call-json!
  "Run `claude -p` with a system prompt, user message, and JSON schema.
   Returns the parsed JSON response as a map, or `nil` on transport /
   auth / parse failure.

   Opts:
     :system     system prompt (default \"\")
     :user       user message (default \"\"), passed via stdin
     :schema     JSON Schema map for the response (required)
     :model      model name (default \"claude-haiku-4-5\")
     :keys       :kebab (default — `is_mind_change` → `:is-mind-change`)
                 or :snake (`:is_mind_change` left as-is)
     :extra-argv extra CLI flags as a vector (advanced)"
  [{:keys [system user schema model keys extra-argv]
    :or   {system "" user "" model "claude-haiku-4-5" keys :kebab}}]
  (let [argv (vec
               (concat ["claude" "-p"
                        "--no-session-persistence"
                        "--model" model
                        "--output-format" "json"
                        "--json-schema" (json/write-str schema)
                        "--append-system-prompt" system]
                       extra-argv))
        result (*invoke-claude* argv (or user ""))
        parsed (parse-result result)]
    (when (map? parsed)
      (case keys
        :kebab (->kw-map parsed)
        :snake parsed))))
