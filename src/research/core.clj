(ns research.core
  "Tiny Datapotamus example: a search term in, a structured list of
   RSS/Atom feeds out. Drives Claude Code (`claude -p`) with WebSearch
   + WebFetch, constrains the answer with a JSON schema.

   Pipeline:

       term ─▶ :build-request ─▶ :research ─▶ :extract ─▶ :collect ─▶ atom

   :build-request   wrap the term in a prompt
   :research        cc/step — runs `claude -p` with WebSearch / WebFetch
   :extract         pluck the schema-conforming payload off the result
   :collect         stash each row into an out-atom

   One-shot from the shell (swap the term):

       clojure -M -e '(require (quote research.core)) (research.core/find-feeds! \"rationalist blogs\")'

   REPL:

       clojure -M:dev
       user=> (require 'research.core)
       user=> (research.core/find-feeds! \"rationalist blogs\")"
  (:require [clojure.pprint :as pp]
            [toolkit.datapotamus.claude-code :as cc]
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

;; --- :collect ---------------------------------------------------------------

(defn- collect-into [a]
  (step/step :collect {:ins {:in ""} :outs {}}
             (fn [_ctx _s row] (swap! a conj row) {})))

;; --- the flow ---------------------------------------------------------------

;; `step/serial` wires the four steps end-to-end: each step's `:out`
;; becomes the next step's `:in`.
(defn- build-flow [out]
  (step/serial build-request research extract (collect-into out)))

;; --- entry point ------------------------------------------------------------

(defn find-feeds! [term]
  (let [out (atom [])
        res (flow/run! (build-flow out) {:data term})]
    (when (= :completed (:state res))
      (pp/pprint (first @out)))
    {:state (:state res) :result (first @out) :error (:error res)}))
