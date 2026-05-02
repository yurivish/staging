(ns toolkit.datapotamus.steps.claude-code
  "Datapotamus step that runs Claude Code (the Agent SDK CLI, `claude -p`)
   as a subprocess. One input prompt in, one result map out — no matter how
   many tool calls Claude makes internally.

   ## Single-emission invariant — why this is exactly 1-in / 1-out

   Claude Code does variable amounts of work per invocation: many tool calls
   on a hard task, few on an easy one. The reason this step still emits
   *exactly one* `:out` message per `:in` is in how we consume the CLI's
   output. With the default `--output-format json`, the CLI buffers the
   entire agent loop and prints one JSON document on stdout when the run
   terminates; we slurp it and emit one message.

   With `:stream? true` (opt-in) we switch to `--output-format stream-json`
   and read one JSON event per line as the agent works — system init,
   each assistant turn, each tool_use / tool_result, then a final `result`
   event whose shape matches the buffered output. We project each
   intermediate event into a `trace/emit` status event on the scoped
   pubsub (`:phase :assistant-text`, `:tool-use`, `:tool-result`, etc.)
   and return the `result` event's data exactly as if we'd been in
   buffered mode. The data port still emits exactly one final message
   per input — streaming changes only the *side-band* trace channel,
   not the data contract.

   The invariant is on the `:out` *data port*. Side-band `:status` events
   (`trace/emit`) are independent of it: this step publishes one at run
   start, one at run finish, and (in stream mode) one per agent turn so
   observers see a live heartbeat across what would otherwise be a long
   silent gap.

   On failure the step emits zero `:out` messages and a `:failure` event on
   the standard trace channel (handled by `flow.clj`'s catch in
   `run-data-or-signal`).

   ## Use

       (claude-code/step :review
         {:append-system-prompt \"You are a security-focused reviewer.\"
          :allowed-tools        [\"Read\" \"Bash(git diff *)\"]
          :max-turns            10})

   The step accepts either a string prompt or a `{:prompt \"...\" ...overrides}`
   map on `:in`; the map merges over the static config. Output on `:out` is
   the parsed result map: `{:result :session-id :total-cost-usd :usage
   :stop-reason :structured-output?}`.

   The opts map mirrors `claude` CLI flags 1:1 (kebab-cased keywords); see
   https://code.claude.com/docs/en/cli-reference for the full list. The
   only default we set is `:permission-mode :dontAsk` (read-only). Opt in
   to writes with `:allowed-tools` / `:permission-mode :acceptEdits`, or
   to full autonomy with `:dangerously-skip-permissions? true` (sandbox
   only).

   `run!` is the same logic as a plain function — useful from the REPL
   without spinning up a flow.

   ## Auth

   By default, the step does NOT pass `--bare`, so the CLI inherits the
   user's `claude auth login` OAuth / subscription auth from the keychain
   and `~/.claude/`. That's what makes it work locally without setup.

   Pass `:bare? true` for scripted/CI runs that want a deterministic
   environment; in bare mode the caller must provide `ANTHROPIC_API_KEY`
   themselves (via `:env` or the JVM environment). There is no
   auto-fallback to `claude.key`.

   ## Drift check before extending

   Claude Code's flag names and defaults change fairly often. Before
   adding a new keyword opt or relying on a behavior you haven't
   verified, run `claude --help` against the installed version and
   cross-check https://code.claude.com/docs/en/cli-reference. If you're
   touching `->args`, do the cross-check first."
  (:refer-clojure :exclude [run!])
  (:require [babashka.process :as bp]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [toolkit.datapotamus.step :as step]
            [toolkit.datapotamus.trace :as trace])
  (:import [java.io BufferedReader Reader]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; CLI arg construction
;; ============================================================================

;; Drift check: Claude Code's flags and defaults change. Before adding to
;; this fn or trusting a behavior, run `claude --help` and cross-check
;; https://code.claude.com/docs/en/cli-reference against the installed
;; version. Last verified: claude 2.1.120 / docs as of 2026-04.
(defn- ->args
  "Build the argv vector. Keys mirror CLI flags 1:1 (kebab-case → flag).
   Boolean keys ending in ? emit only the flag when true; nil/missing keys
   are skipped. Vector values for repeating-arg flags (`--allowedTools`,
   `--add-dir`) become multiple positional args after the flag."
  [{:keys [prompt
           bare? permission-mode dangerously-skip-permissions?
           model
           append-system-prompt append-system-prompt-file
           system-prompt system-prompt-file
           allowed-tools disallowed-tools tools
           max-turns max-budget-usd fallback-model
           add-dir settings setting-sources
           mcp-config strict-mcp-config?
           resume continue? fork-session? session-id no-session-persistence?
           json-schema extra-args
           stream?]}]
  (cond-> ["claude" "--print" "--output-format" (if stream? "stream-json" "json")]
    stream?                        (conj "--verbose")  ; required by CLI for stream-json
    bare?                          (conj "--bare")
    permission-mode                (into ["--permission-mode" (name permission-mode)])
    dangerously-skip-permissions?  (conj "--dangerously-skip-permissions")
    model                          (into ["--model" model])
    append-system-prompt           (into ["--append-system-prompt" append-system-prompt])
    append-system-prompt-file      (into ["--append-system-prompt-file" append-system-prompt-file])
    system-prompt                  (into ["--system-prompt" system-prompt])
    system-prompt-file             (into ["--system-prompt-file" system-prompt-file])
    (seq allowed-tools)            (into (cons "--allowedTools" allowed-tools))
    (seq disallowed-tools)         (into (cons "--disallowedTools" disallowed-tools))
    tools                          (into ["--tools" tools])
    max-turns                      (into ["--max-turns" (str max-turns)])
    max-budget-usd                 (into ["--max-budget-usd" (str max-budget-usd)])
    fallback-model                 (into ["--fallback-model" fallback-model])
    (seq add-dir)                  (into (cons "--add-dir" add-dir))
    settings                       (into ["--settings" settings])
    (seq setting-sources)          (into ["--setting-sources"
                                          (str/join "," (map name setting-sources))])
    mcp-config                     (into ["--mcp-config" mcp-config])
    strict-mcp-config?             (conj "--strict-mcp-config")
    resume                         (into ["--resume" resume])
    continue?                      (conj "--continue")
    fork-session?                  (conj "--fork-session")
    session-id                     (into ["--session-id" session-id])
    no-session-persistence?        (conj "--no-session-persistence")
    json-schema                    (into ["--json-schema" (json/write-str json-schema)])
    (seq extra-args)               (into extra-args)
    prompt                         (conj prompt)))

;; ============================================================================
;; Subprocess plumbing
;; ============================================================================

(defn- json->kebab
  "Parse a JSON string with snake_case keys → kebab-cased Clojure keywords."
  [s]
  (json/read-str s :key-fn (fn [k] (keyword (str/replace k "_" "-")))))

(defn- watch-cancel!
  "Virtual thread: poll the cancel promise; on delivery, .destroy the
   subprocess (then .destroyForcibly after a grace). Exits when the process
   is no longer alive even if cancel never fires, so we don't leak threads."
  [cancel ^Process proc]
  (when cancel
    (Thread/startVirtualThread
     ^Runnable
     (fn []
       (loop []
         (let [v (deref cancel 250 ::pending)]
           (cond
             (not= v ::pending)
             (do (.destroy proc)
                 (Thread/sleep 1500)
                 (when (.isAlive proc) (.destroyForcibly proc)))

             (.isAlive proc) (recur))))))))

(defn- drain-stderr!
  "Virtual thread: slurp stderr into the StringBuilder."
  [err ^StringBuilder sb]
  (Thread/startVirtualThread
   ^Runnable
   (fn [] (.append sb ^String (slurp err)))))

;; ============================================================================
;; Stream-json (`:stream? true`) consumer
;; ============================================================================
;;
;; Verified against claude 2.1.119 (2026-04). Event shapes observed live:
;;   {"type":"system","subtype":"status",...}                 — early heartbeat, skip
;;   {"type":"system","subtype":"init","session_id",...}      — session ready
;;   {"type":"assistant","message":{"content":[{type:"text"|"thinking"|"tool_use",...}]}}
;;   {"type":"user","message":{"content":[{type:"tool_result", tool_use_id, content, is_error?}]}}
;;   {"type":"result", ...}                                   — final summary, same shape as
;;                                                              buffered --output-format json
;;
;; Claude Code implements `--json-schema` as a synthetic `StructuredOutput`
;; tool: the model emits a tool_use with name "StructuredOutput" and the
;; structured args; the harness echoes a tool_result acknowledgement; the
;; final `result` event's `structured_output` field is what the caller sees.

(def ^:private preview-len 500)
(def ^:private text-preview-len 200)

(defn- truncate-str [^String s n]
  (cond
    (nil? s)        nil
    (<= (count s) n) s
    :else           (str (subs s 0 n) "…")))

(defn- coerce-content
  "Tool-result content can be a string or a list of content blocks
   (e.g. for image-bearing results). Flatten to a string for preview."
  [c]
  (cond
    (string? c)     c
    (sequential? c) (str/join "\n" (keep #(or (:text %) (some-> % :content coerce-content)) c))
    :else           (pr-str c)))

(defn- project-block
  "Project one content block from an assistant or user message into trace
   events. Returns a vector of zero or more event-data maps."
  [block turn-index]
  (case (:type block)
    "text"     [{:phase :assistant-text
                 :turn-index turn-index
                 :length (count (:text block))
                 :preview (truncate-str (:text block) text-preview-len)}]
    "thinking" [{:phase :assistant-thinking
                 :turn-index turn-index
                 :length (count (or (:thinking block) (:text block) ""))}]
    "tool_use" [{:phase :tool-use
                 :turn-index turn-index
                 :tool (:name block)
                 :tool-use-id (:id block)
                 :input-preview (truncate-str (json/write-str (:input block)) preview-len)}]
    "tool_result"
    (let [content (coerce-content (:content block))]
      [{:phase :tool-result
        :tool-use-id (:tool_use_id block)
        :is-error (boolean (:is_error block))
        :content-length (count content)
        :content-preview (truncate-str content preview-len)}])
    ;; Unknown block type — just skip silently rather than fail the run.
    []))

(defn- project-event
  "Project one parsed stream event into trace events. Returns a vector of
   zero or more event-data maps. Top-level discrimination on `:type`."
  [event turn-index]
  (case (:type event)
    "system"
    (case (:subtype event)
      "init"   [{:phase :session-init
                 :session-id (:session_id event)
                 :model (:model event)
                 :cwd (:cwd event)
                 :tools-count (count (:tools event))
                 :mcp-servers-count (count (:mcp_servers event))}]
      ;; status, etc. — quiet
      [])

    "assistant"
    (vec (mapcat #(project-block % turn-index)
                 (-> event :message :content)))

    "user"
    (vec (mapcat #(project-block % turn-index)
                 (-> event :message :content)))

    ;; "result" handled at the consumer level (it's the return value, not a
    ;; status event) — :run-finished is emitted by run! itself.
    "result" []
    []))

(defn- parse-stream-line
  "Parse one NDJSON line. Returns the event map (raw, snake_case keywords)
   or {:parse-error true :line line :ex ex} on failure."
  [^String line]
  (try
    (json/read-str line :key-fn keyword)
    (catch Throwable ex
      {:parse-error true :line line :ex ex})))

(defn- consume-stream!
  "Read NDJSON events line-by-line from `rdr`, emit `trace/emit` status
   events through `ctx`, and return the data of the final `result` event
   (kebab-keyed to match buffered-mode output). Throws if the stream
   ends without a `result` event."
  [^BufferedReader rdr ctx]
  (loop [final nil
         turn-index 0]
    (let [line (.readLine rdr)]
      (if (nil? line)
        (or final (throw (ex-info "claude stream ended without :result event" {})))
        (let [parsed (parse-stream-line line)]
          (cond
            (:parse-error parsed)
            (do
              (when ctx
                (trace/emit ctx {:phase :stream-parse-error
                                 :line-preview (truncate-str line text-preview-len)
                                 :exception-message (ex-message (:ex parsed))}))
              (recur final turn-index))

            :else
            (let [next-idx (cond-> turn-index
                             (= "assistant" (:type parsed)) inc)]
              (when ctx
                (doseq [te (project-event parsed turn-index)]
                  (trace/emit ctx te)))
              (recur (if (= "result" (:type parsed))
                       ;; convert snake → kebab to match buffered branch
                       (json->kebab line)
                       final)
                     next-idx))))))))

(defn run!
  "Run `claude -p` with the given opts; return the parsed result map.
   Throws ex-info on non-zero exit. See namespace docstring for the opts
   schema. Recognized non-CLI opts: `:cwd` (working directory), `:env`
   (extra env vars merged into ProcessBuilder), `:cancel` (a promise; when
   delivered, destroys the subprocess), `:ctx` (a handler ctx; when
   present, `:run-started` and `:run-finished` status events are emitted
   on its scoped pubsub)."
  [{:keys [cwd env cancel ctx] :as opts}]
  (let [defaults  {:permission-mode :dontAsk}
        opts'     (merge defaults opts)
        args      (->args opts')]
    (when ctx
      (trace/emit ctx (cond-> {:phase :run-started}
                        (:model opts')         (assoc :model (:model opts'))
                        (:max-turns opts')     (assoc :max-turns (:max-turns opts'))
                        (seq (:allowed-tools opts')) (assoc :allowed-tools (:allowed-tools opts')))))
    (let [proc      (bp/process args (cond-> {:in  ""             ; close stdin; CLI warns "no stdin in 3s" otherwise
                                              :out :stream
                                              :err :stream}
                                       cwd (assoc :dir cwd)
                                       env (assoc :extra-env env)))
          stderr-sb (StringBuilder.)
          _stderr-t (drain-stderr! (:err proc) stderr-sb)
          _cancel-t (watch-cancel! cancel (:proc proc))
          stream?   (boolean (:stream? opts'))
          ;; Stream mode: read line-by-line, emit per-event trace events,
          ;; return the final :result event's data. Buffered: slurp all,
          ;; parse one JSON document. Either way, we wait for proc exit
          ;; before checking the exit code.
          [result raw-stdout]
          (if stream?
            (with-open [rdr (io/reader (:out proc))]
              (let [r (try (consume-stream! rdr ctx)
                           (catch Throwable _ nil))]
                ;; Drain any remaining bytes for diagnostics — usually empty in
                ;; stream mode since we read to EOF, but keeps the error path
                ;; consistent.
                [r ""]))
            (let [stdout (slurp (:out proc))]
              [(try (json->kebab stdout)
                    (catch Throwable _ nil))
               stdout]))
          exit (:exit @proc)]
      (cond
        (not (zero? exit))
        (throw (ex-info "claude exited non-zero"
                        {:exit       exit
                         :stderr     (str stderr-sb)
                         :raw-stdout raw-stdout
                         :args       args}))

        (nil? result)
        (throw (ex-info (if stream?
                          "claude stream ended without :result event"
                          "claude returned non-JSON on stdout")
                        {:exit       exit
                         :stderr     (str stderr-sb)
                         :raw-stdout raw-stdout
                         :args       args}))

        :else
        (do
          (when ctx
            (trace/emit ctx (cond-> {:phase :run-finished}
                              (contains? result :stop-reason)    (assoc :stop-reason    (:stop-reason result))
                              (contains? result :total-cost-usd) (assoc :total-cost-usd (:total-cost-usd result))
                              (contains? result :num-turns)      (assoc :num-turns      (:num-turns result))
                              (contains? result :is-error)       (assoc :is-error       (:is-error result)))))
          result)))))

;; ============================================================================
;; Datapotamus step
;; ============================================================================

(defn- normalize-input
  "Per-call data → opts map merged over static config."
  [config data]
  (cond
    (string? data) (assoc config :prompt data)
    (map? data)    (merge config data)
    :else (throw (ex-info (str "claude-code step: unsupported input "
                               (pr-str (type data)))
                          {:data data}))))

(defn step
  "Datapotamus step that drives `claude -p`. See namespace docstring for
   contract and opts. Single :in / :out; on success emits exactly one
   result map on :out per input; on failure emits zero (and a :failure
   event on the trace channel via flow.clj)."
  [id config]
  (step/step id nil
             (fn [ctx _state data]
               (let [opts   (normalize-input config data)
                     result (run! (assoc opts :cancel (:cancel ctx) :ctx ctx))]
                 {:out [result]}))))
