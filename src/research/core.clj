(ns research.core
  "Tiny Datapotamus example: a search term in, a structured list of
   RSS/Atom feeds out. Drives Claude Code (`claude -p`) with WebSearch
   + WebFetch, constrains the answer with a JSON schema.

   Pipeline:

       term ─▶ :build-request ─▶ :research ─▶ :extract ─▶ result

   :build-request   wrap the term in a prompt
   :research        cc/step — runs `claude -p` with WebSearch / WebFetch
   :extract         pluck the schema-conforming payload off the result

   `flow/run-seq` appends its own collector and attributes each
   `:out` value back to the input that produced it, so we don't need
   a hand-rolled sink — the example flow is just the three steps above.

   One-shot from the shell (swap the term):

       clojure -M -e '(require (quote research.core)) (research.core/find-feeds! \"rationalist blogs\")'

   REPL:

       clojure -M:dev
       user=> (require 'research.core)
       user=> (research.core/find-feeds! \"rationalist blogs\")"
  (:require [clojure.pprint :as pp]
            [toolkit.datapotamus.steps.claude-code :as cc]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]))

;; --- :build-request ---------------------------------------------------------

(defn- prompt [term]
  (str "Find 5 high-quality, actively-updated RSS or Atom feeds about: " term ".\n\n"
       "For each feed, return:\n"
       "  - feed-url: the RSS/Atom XML endpoint itself (the URL a feed reader subscribes to).\n"
       "  - home-url: the human-facing page about the feed (a podcast's show page, a blog's homepage).\n"
       "These are usually different URLs — do not put the same value in both.\n\n"
       "Use WebSearch to discover candidates, and WebFetch to verify each feed-url "
       "actually returns XML before including it."))

(def build-request
  (step/step :build-request (fn [term] {:prompt (prompt term)})))

;; --- :research --------------------------------------------------------------

;; The schema lives next to its only consumer. `cc/step` forwards it to
;; `claude --json-schema`, which is what makes the agent's terminal
;; output a parseable map instead of free text.
(def feeds-schema
  {:type     "object"
   :required ["feeds"]
   :properties
   {:feeds {:type  "array"
            :items {:type     "object"
                    :required ["title" "feed-url" "home-url" "description"]
                    :properties
                    {:title       {:type "string"}
                     :feed-url    {:type        "string"
                                   :description "RSS/Atom XML endpoint."}
                     :home-url    {:type        "string"
                                   :description "Human-facing page (e.g. podcast show page or blog homepage)."}
                     :description {:type "string"}}}}}})

;; The heart of the example: one Claude Code subprocess driven by the
;; agent loop, scoped to web research via `:allowed-tools`, terminating
;; on schema-conforming output.
(def research
  (cc/step :research
           {:model         "sonnet"
            :allowed-tools ["WebSearch" "WebFetch"]
            :max-turns     20
            :json-schema   feeds-schema}))

;; --- :extract ---------------------------------------------------------------

;; `cc/step` emits the full Claude result map; `:structured-output` is
;; the schema-conforming payload. `step/step` with a keyword as the
;; "function" is sugar for `(fn [m] (m kw))`.
(def extract
  (step/step :extract :structured-output))

;; --- entry point ------------------------------------------------------------

;; `step/serial` wires the three steps end-to-end. `flow/run-seq` runs
;; the chain against the singleton input vector and returns
;; `:outputs` aligned to it — `[[result-map]]` here, since one input
;; produced one output.
(defn build-flow
  "Static pipeline: build-request → research → extract."
  []
  (step/serial build-request research extract))

(defn find-feeds! [term]
  (let [flow   (build-flow)
        res    (flow/run-seq flow [term])
        result (ffirst (:outputs res))]
    (when (= :completed (:state res))
      (pp/pprint result))
    {:state (:state res) :result result :error (:error res)}))
