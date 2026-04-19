(ns toolkit.llm
  "Provider-agnostic client for chat-completion LLM APIs (Anthropic, Google).

   Shape:
     - A `Provider` protocol with two methods: `-query` (one-shot) and
       `-query-stream` (SSE streaming). Public wrappers `query`,
       `query-text`, `query-stream` call through to the protocol.
     - A `Request` is a plain map; providers translate it to their native
       wire format.
     - `query` returns `{:text joined-text :raw parsed-response}`.
     - `query-stream` returns `[ch stop!]`: a core.async channel of string
       chunks plus an idempotent cleanup fn. Errors are put onto the
       channel as `Throwable` instances (check with `instance?`) and the
       channel is closed. The channel is also closed on normal stream end.

   Request map:
     {:model         \"claude-sonnet-4-6\"   ; required
      :max-tokens    1024                    ; required
      :system        \"...\"                 ; optional
      :messages      [<message> ...]         ; required
      :output-schema <clojure-map-or-nil>}   ; optional JSON Schema

   Messages:
     {:role \"user\" | \"assistant\"
      :content [<content-block> ...]}

   Content blocks:
     {:type \"text\" :text \"...\"}
     {:type \"document\" :title \"...\" :context \"...\"
      :source {:type \"base64\"  :media-type \"...\" :data \"...\"}}
     {:type \"document\"
      :source {:type \"text\"    :media-type \"...\" :data \"...\"}}
     {:type \"document\"
      :source {:type \"content\" :content [<content-block> ...]}}

   Errors are thrown (or delivered on the stream channel) as `ex-info` with
   a `:reason` keyword: `:http-status`, `:parse-error`, `:max-tokens`,
   `:cancelled`.

   Providers live in `toolkit.llm.anthropic` and `toolkit.llm.google`; each
   exposes a `make` factory that returns a record implementing this
   protocol."
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.string :as str])
  (:import [java.io BufferedReader InputStream InputStreamReader]
           [java.nio.charset StandardCharsets]))

;; --- protocol ---------------------------------------------------------------

(defprotocol Provider
  (-query        [this req]
    "Run a one-shot request. Returns `{:text ... :raw ...}`.")
  (-query-stream [this req]
    "Start a streaming request. Returns `[ch stop!]`."))

;; --- public API -------------------------------------------------------------

(defn query
  "Run a one-shot request. Returns `{:text joined-string :raw parsed-response}`.
   Throws `ex-info` on HTTP or parse errors."
  [provider req]
  (-query provider req))

(defn query-text
  "Run a one-shot request and return just the joined text."
  [provider req]
  (:text (query provider req)))

(defn query-stream
  "Start a streaming request. Returns `[ch stop!]` — a core.async channel
   of text chunks (Strings) plus an idempotent zero-arg cleanup fn. On
   error a `Throwable` is put on the channel and the channel is closed;
   consumers should check `(instance? Throwable v)` after each take."
  [provider req]
  (-query-stream provider req))

;; --- convenience constructors ----------------------------------------------

(defn text-block
  "A plain `{:type \"text\" :text s}` content block."
  [s]
  {:type "text" :text s})

(defn user-msg
  "Shorthand for a single-text user message."
  [s]
  {:role "user" :content [(text-block s)]})

(defn assistant-msg
  "Shorthand for a single-text assistant message."
  [s]
  {:role "assistant" :content [(text-block s)]})

;; --- shared utilities (used by provider namespaces) ------------------------

(defn fail!
  "Throw an ex-info with `:reason` keyword and extra kv data."
  [reason msg & {:as extra}]
  (throw (ex-info msg (merge {:reason reason} extra))))

(defn write-json
  "Serialize a Clojure value to a JSON string. Delegates to data.json,
   which accepts nested maps, sequences, strings, numbers, booleans, nil."
  ^String [v]
  (json/write-str v))

(defn read-json
  "Parse a JSON string into a Clojure value with keyword keys."
  [^String s]
  (json/read-str s :key-fn keyword))

(defn check-required!
  "Validate that the request map has the fields a provider needs."
  [req]
  (when (str/blank? (:model req))
    (fail! :invalid-request "missing :model" :req req))
  (when-not (pos-int? (:max-tokens req))
    (fail! :invalid-request "missing or non-positive :max-tokens" :req req))
  (when-not (seq (:messages req))
    (fail! :invalid-request "missing or empty :messages" :req req)))

(defn ^:private utf8-reader ^BufferedReader [^InputStream is]
  (BufferedReader. (InputStreamReader. is StandardCharsets/UTF_8)))

(defn- field-value
  "Strip the `field:` prefix, then one optional leading space, per the
   SSE spec. `(field-value \"data: foo\" 4)` → `\"foo\"`;
   `(field-value \"data:foo\" 4)` → `\"foo\"`;
   `(field-value \"data:  foo\" 4)` → `\" foo\"`."
  [^String line prefix-len]
  (let [rest (subs line prefix-len)]
    (if (and (pos? (.length rest)) (= \space (.charAt rest 0)))
      (subs rest 1)
      rest)))

(defn start-sse-stream!
  "Spawn a daemon thread that reads an SSE stream from `is` and dispatches
   parsed events onto a new channel. Returns `[ch stop!]`.

   Framing follows the SSE spec:
     - Lines starting with `:` are comments and are skipped.
     - `data:` fields are accumulated; multiple `data:` lines within a
       single event are concatenated with `\\n` before dispatch.
     - A blank line terminates the current event — the concatenated data
       payload is passed to `(parse payload)`.
     - `event:`, `id:`, and `retry:` are recognized as field lines (not
       dispatched as data) and are otherwise ignored.
     - Any pending event is flushed on stream EOF.

   `parse` returns a seqable of zero or more event maps (or `nil` for
   \"no events this payload\"). Each event has a `:type` keyword:

     {:type :chunk   :value str}        — put the string onto the channel
     {:type :error   :value throwable}  — put the throwable and terminate
     {:type :done}                      — terminate the stream cleanly

   Unknown `:type`s are ignored. This is how future extensions (e.g.
   `:partial` events from an incremental JSON wrapper that buffers
   accumulated text across chunks) can layer on without a core change.

   `stop!` is idempotent: closes the input stream (aborting any blocking
   read) and closes the channel."
  [^InputStream is parse]
  (let [ch       (async/chan 32)
        stopped? (atom false)
        stop!    (fn []
                   (when (compare-and-set! stopped? false true)
                     (try (.close is) (catch Throwable _))
                     (async/close! ch)))
        dispatch (fn [^StringBuilder data]
                   (when (pos? (.length data))
                     (let [payload (.toString data)
                           events  (try (parse payload)
                                        (catch Throwable t
                                          [{:type :error :value t}]))]
                       (doseq [ev events
                               :while (not @stopped?)]
                         (case (:type ev)
                           :chunk (async/>!! ch (:value ev))
                           :error (do (async/>!! ch (:value ev))
                                      (reset! stopped? true))
                           :done  (reset! stopped? true)
                           nil)))))]
    (doto (Thread.
           ^Runnable
           (fn []
             (try
               (with-open [r (utf8-reader is)]
                 (let [data (StringBuilder.)]
                   (loop []
                     (when-not @stopped?
                       (let [line (try (.readLine r)
                                       (catch Throwable _ nil))]
                         (cond
                           ;; EOF: flush any pending event and exit.
                           (nil? line)
                           (dispatch data)

                           ;; Blank line: event boundary.
                           (zero? (.length ^String line))
                           (do (dispatch data)
                               (.setLength data 0)
                               (recur))

                           ;; Comment: skip.
                           (.startsWith ^String line ":")
                           (recur)

                           ;; `data:` field — accumulate.
                           (.startsWith ^String line "data:")
                           (do
                             (when (pos? (.length data))
                               (.append data \newline))
                             (.append data ^String (field-value line 5))
                             (recur))

                           ;; Other fields (event:, id:, retry:) — ignored.
                           :else
                           (recur)))))))
               (catch Throwable t
                 (when-not @stopped?
                   (try (async/>!! ch t) (catch Throwable _))))
               (finally
                 (async/close! ch)))))
      (.setDaemon true)
      (.setName "toolkit.llm-sse-reader")
      (.start))
    [ch stop!]))
