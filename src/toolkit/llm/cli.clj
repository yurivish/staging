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
   `{:type \"result\" :is_error false :result \"<text>\" ...}`.
   When `--json-schema` is also given, the validated structured output
   lands in the wrapper's `structured_output` field as a parsed object;
   `result` is just the model's free-form prose (often \"Done!\").
   We pull `structured_output` first; we also fall back to scanning
   `result` for an embedded JSON object so a stray prose-only response
   still works.

   The process invocation is behind `*invoke-claude*` so tests can
   redef it without spawning real subprocesses."
  (:require [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [toolkit.llm.cache :as cache]))

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
  "Take `{:exit :out :err}`, return the inner-result map or nil.
   Prefers the wrapper's `structured_output` field (set when
   `--json-schema` is honored) over scanning `result` prose."
  [{:keys [exit out]}]
  (when (zero? exit)
    (let [wrap (extract-json-object out)]
      (when (and wrap (not (:is_error wrap)))
        (or (let [so (:structured_output wrap)]
              (cond (map? so)    so
                    (string? so) (extract-json-object so)))
            (extract-json-object (:result wrap)))))))

(defn- call-json-uncached!
  "The bare subprocess call: build argv, invoke `claude -p`, parse the
   wrapper, normalize keys. Returns the parsed map or nil on
   failure. No caching."
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

(defn- cache-key-string
  "Stable key derivation for cache lookups. Inputs joined with
   `\\n--\\n` separators (the `podcast.llm/cache-key` convention).
   `keys` (`:kebab` vs `:snake`) is included because identical raw
   responses produce different return shapes; mixing them would serve
   a kebab map to a `:keys :snake` caller."
  [{:keys [cache-tag system user schema model keys extra-argv]
    :or   {system "" user "" model "claude-haiku-4-5" keys :kebab}}]
  (str "claude-cli"      "\n--\n"
       (or cache-tag "default") "\n--\n"
       model             "\n--\n"
       (name keys)       "\n--\n"
       system            "\n--\n"
       user              "\n--\n"
       (pr-str schema)   "\n--\n"
       (pr-str (or extra-argv []))))

(defn call-json!
  "Run `claude -p` with a system prompt, user message, and JSON schema.
   Returns the parsed JSON response as a map, or `nil` on transport /
   auth / parse failure.

   Caching is on by default (LMDB at `cache/llm/lmdb` via
   `toolkit.llm.cache`). Identical opts are served from cache without
   spawning a subprocess. Pass `:bypass? true` to skip the cache for
   one call (the result is also not written to the cache, so a
   subsequent default call still misses).

   Opts:
     :system     system prompt (default \"\")
     :user       user message (default \"\"), passed via stdin
     :schema     JSON Schema map for the response (required)
     :model      model name (default \"claude-haiku-4-5\")
     :keys       :kebab (default — `is_mind_change` → `:is-mind-change`)
                 or :snake (`:is_mind_change` left as-is)
     :bypass?    bool — default false; set true to skip the cache
                 entirely for this one call (no read, no write).
     :cache-tag  string — namespace within the shared cache (default
                 \"default\"). Different tags hash to different keys
                 so identical (system, user, schema, model) tuples
                 from different pipelines don't share entries unless
                 you want them to.
     :extra-argv extra CLI flags as a vector (advanced)"
  [{:keys [bypass?] :as opts}]
  (if bypass?
    (call-json-uncached! opts)
    (:value (cache/through! (cache-key-string opts)
                            (fn [] (call-json-uncached! opts))))))
