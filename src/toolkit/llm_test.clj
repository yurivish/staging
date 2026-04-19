(ns toolkit.llm-test
  "Offline tests for toolkit.llm providers. No network calls — they verify
   request-body translation and SSE-event parsing against fixture data."
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [toolkit.llm :as llm]
            [toolkit.llm.anthropic :as anthropic]
            [toolkit.llm.google :as google])
  (:import [java.io ByteArrayInputStream]
           [java.nio.charset StandardCharsets]))

;; --- private-var helpers ---------------------------------------------------

(def a-build    @#'anthropic/build-body)
(def a-parse    @#'anthropic/parse-sse-event)
(def g-build    @#'google/build-body)
(def g-parse    @#'google/parse-sse-event)

(defn- bytes-stream [^String s]
  (ByteArrayInputStream. (.getBytes s StandardCharsets/UTF_8)))

(defn- drain
  "Eagerly drain a channel to a vector of taken values (including any
   terminal Throwable)."
  [ch]
  (loop [acc []]
    (if-let [v (async/<!! ch)]
      (recur (conj acc v))
      acc)))

;; --- request builders ------------------------------------------------------

(deftest anthropic-build-basic-request
  (let [req  {:model      "claude-sonnet-4-6"
              :max-tokens 1024
              :system     "be brief"
              :messages   [(llm/user-msg "hi")]}
        body (a-build req false)]
    (is (= "claude-sonnet-4-6" (:model body)))
    (is (= 1024 (:max_tokens body))
        "kebab :max-tokens must become snake_case :max_tokens")
    (is (= "be brief" (:system body)))
    (is (not (contains? body :stream))
        "no :stream key when stream? is false")
    (is (= [{:role "user" :content [{:type "text" :text "hi"}]}]
           (:messages body)))))

(deftest anthropic-build-stream-flag
  (let [body (a-build {:model "claude-haiku-4-5" :max-tokens 10
                       :messages [(llm/user-msg "x")]}
                      true)]
    (is (true? (:stream body)))))

(deftest anthropic-build-output-schema
  (let [schema {:type "object"
                :properties {:name {:type "string"}}
                :required ["name"]}
        body   (a-build {:model "claude-sonnet-4-6" :max-tokens 128
                         :messages [(llm/user-msg "extract")]
                         :output-schema schema}
                        false)]
    (is (= {:format {:type "json_schema" :schema schema}}
           (:output_config body))
        "schema must be wrapped in output_config.format")))

(deftest anthropic-build-document-block
  (let [req  {:model "claude-sonnet-4-6" :max-tokens 64
              :messages [{:role "user"
                          :content [{:type "document"
                                     :title "T" :context "C"
                                     :source {:type "base64"
                                              :media-type "application/pdf"
                                              :data "ZGF0YQ=="}}
                                    {:type "text" :text "summarize"}]}]}
        body (a-build req false)
        doc  (-> body :messages first :content first)]
    (is (= "document" (:type doc)))
    (is (= "T" (:title doc)))
    (is (= "C" (:context doc)))
    (is (= {:type "base64" :media_type "application/pdf" :data "ZGF0YQ=="}
           (:source doc))
        ":media-type must become :media_type; data passes through")))

(deftest google-build-basic-request
  (let [body (g-build {:model "gemini-2.5-flash" :max-tokens 256
                       :system "be brief"
                       :messages [(llm/user-msg "hi")]})]
    (is (= {:parts [{:text "be brief"}]} (:systemInstruction body)))
    (is (= 256 (get-in body [:generationConfig :maxOutputTokens])))
    (is (= [{:role "user" :parts [{:text "hi"}]}] (:contents body)))))

(deftest google-role-mapping
  (let [body (g-build {:model "m" :max-tokens 10
                       :messages [(llm/user-msg "hi")
                                  (llm/assistant-msg "hello")]})]
    (is (= ["user" "model"]
           (mapv :role (:contents body)))
        "assistant must be renamed to model")))

(deftest google-build-output-schema
  (let [schema {:type "object" :properties {:x {:type "integer"}}}
        body   (g-build {:model "m" :max-tokens 10
                         :messages [(llm/user-msg "x")]
                         :output-schema schema})
        cfg    (:generationConfig body)]
    (is (= "application/json" (:responseMimeType cfg)))
    (is (= schema (:responseJsonSchema cfg)))))

(deftest google-build-document-inline-data
  (let [body (g-build {:model "m" :max-tokens 10
                       :messages [{:role "user"
                                   :content [{:type "document"
                                              :source {:type "base64"
                                                       :media-type "image/png"
                                                       :data "ZGF0YQ=="}}]}]})
        part (-> body :contents first :parts first)]
    (is (= {:inlineData {:mimeType "image/png" :data "ZGF0YQ=="}}
           part)
        "Gemini inlineData uses camelCase keys")))

(deftest google-build-document-content-flattens-text
  (let [body (g-build {:model "m" :max-tokens 10
                       :messages [{:role "user"
                                   :content [{:type "document"
                                              :source {:type "content"
                                                       :content [{:type "text" :text "a"}
                                                                 {:type "text" :text "b"}]}}]}]})
        parts (-> body :contents first :parts)]
    (is (= [{:text "a"} {:text "b"}] parts))))

;; --- request validation ----------------------------------------------------

(deftest missing-model-throws
  (is (thrown-with-msg?
        Exception #"missing :model"
        (llm/check-required! {:max-tokens 10 :messages [(llm/user-msg "x")]}))))

(deftest missing-max-tokens-throws
  (is (thrown-with-msg?
        Exception #"missing or non-positive :max-tokens"
        (llm/check-required! {:model "m" :messages [(llm/user-msg "x")]}))))

(deftest missing-messages-throws
  (is (thrown-with-msg?
        Exception #"missing or empty :messages"
        (llm/check-required! {:model "m" :max-tokens 10}))))

;; --- factory validation ----------------------------------------------------

(deftest factories-require-api-key
  (is (thrown-with-msg? Exception #"missing :api-key"
                        (anthropic/make {})))
  (is (thrown-with-msg? Exception #"missing :api-key"
                        (google/make {:api-key "   "}))))

;; --- SSE event parsing: Anthropic ------------------------------------------

(deftest anthropic-parses-text-delta
  (is (= [{:type :chunk :value "Hello"}]
         (a-parse "{\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}"))))

(deftest anthropic-ignores-non-text-delta
  (is (nil? (seq (a-parse "{\"type\":\"message_start\"}"))))
  (is (nil? (seq (a-parse "{\"type\":\"ping\"}"))))
  (is (nil? (seq (a-parse "{\"type\":\"content_block_start\"}")))))

(deftest anthropic-max-tokens-becomes-error
  (let [ev (first (a-parse "{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"}}"))]
    (is (= :error (:type ev)))
    (is (instance? clojure.lang.ExceptionInfo (:value ev)))
    (is (= :max-tokens (:reason (ex-data (:value ev)))))))

(deftest anthropic-stream-error-becomes-error
  (let [ev (first (a-parse "{\"type\":\"error\",\"error\":{\"message\":\"overloaded\"}}"))]
    (is (= :error (:type ev)))
    (is (instance? clojure.lang.ExceptionInfo (:value ev)))
    (is (= :stream-error (:reason (ex-data (:value ev)))))))

(deftest anthropic-message-stop-becomes-done
  (is (= [{:type :done}] (a-parse "{\"type\":\"message_stop\"}"))))

(deftest anthropic-bad-json-is-ignored
  (is (nil? (seq (a-parse "not json")))))

;; --- SSE event parsing: Google ---------------------------------------------

(deftest google-parses-text-chunk
  (is (= [{:type :chunk :value "hey"}]
         (g-parse "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hey\"}]}}]}"))))

(deftest google-joins-multiple-parts
  (is (= [{:type :chunk :value "ab"}]
         (g-parse "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"a\"},{\"text\":\"b\"}]}}]}"))))

(deftest google-max-tokens-becomes-error
  (let [ev (first (g-parse "{\"candidates\":[{\"content\":{\"parts\":[]},\"finishReason\":\"MAX_TOKENS\"}]}"))]
    (is (= :error (:type ev)))
    (is (= :max-tokens (:reason (ex-data (:value ev)))))))

(deftest google-finish-stop-becomes-done
  (is (= [{:type :done}]
         (g-parse "{\"candidates\":[{\"content\":{\"parts\":[]},\"finishReason\":\"STOP\"}]}"))))

(deftest google-unknown-finish-becomes-error
  (let [ev (first (g-parse "{\"candidates\":[{\"content\":{\"parts\":[]},\"finishReason\":\"SAFETY\"}]}"))]
    (is (= :error (:type ev)))
    (is (= :stream-error (:reason (ex-data (:value ev)))))))

;; --- end-to-end SSE stream -------------------------------------------------

(defn- anthropic-sse-fixture []
  (str/join "\n"
    ["event: message_start"
     "data: {\"type\":\"message_start\"}"
     ""
     "event: content_block_delta"
     "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello, \"}}"
     ""
     "event: content_block_delta"
     "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"world!\"}}"
     ""
     "event: message_stop"
     "data: {\"type\":\"message_stop\"}"
     ""]))

(deftest anthropic-stream-end-to-end
  (let [in        (bytes-stream (anthropic-sse-fixture))
        [ch stop] (llm/start-sse-stream! in a-parse)
        values    (drain ch)]
    (is (= ["Hello, " "world!"] values))
    (stop)))

(defn- google-sse-fixture []
  (str/join "\n"
    ["data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"one \"}]}}]}"
     ""
     "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"two\"}]},\"finishReason\":\"STOP\"}]}"
     ""]))

(deftest google-stream-end-to-end
  (let [in        (bytes-stream (google-sse-fixture))
        [ch stop] (llm/start-sse-stream! in g-parse)
        values    (drain ch)]
    (is (= ["one " "two"] values))
    (stop)))

;; --- decorator test support ------------------------------------------------

(defn- stub-provider
  "Provider for decorator tests. Invokes `query-fn`/`stream-fn` on each
   call; records call counts and per-call args in `state`."
  [{:keys [query-fn stream-fn]}]
  (let [state (atom {:query-calls [] :stream-calls []})
        p     (reify llm/Provider
                (-query [_ req]
                  (swap! state update :query-calls conj {:req req :t (System/nanoTime)})
                  (query-fn req))
                (-query-stream [_ req]
                  (swap! state update :stream-calls conj {:req req :t (System/nanoTime)})
                  (stream-fn req)))]
    {:provider p :state state}))

;; --- rate-limit ------------------------------------------------------------

(deftest rate-limit-paces-calls
  (let [{:keys [provider state]}
        (stub-provider {:query-fn (fn [_] {:text "ok" :raw nil})})
        limited (llm/with-rate-limit provider
                  {:default {:per-second 10.0 :burst 1}})
        ;; With burst=1, after the first call we wait ~100ms per subsequent.
        t0      (System/nanoTime)]
    (dotimes [_ 3]
      (llm/query limited {:model "m" :max-tokens 10
                          :messages [(llm/user-msg "x")]}))
    (let [elapsed-ms (/ (- (System/nanoTime) t0) 1e6)]
      (is (= 3 (count (:query-calls @state))))
      (is (>= elapsed-ms 180)
          (str "3 calls at per-second=10 burst=1 should take ~200ms+, got "
               (long elapsed-ms) "ms")))))

(deftest rate-limit-is-per-model
  (let [{:keys [provider state]}
        (stub-provider {:query-fn (fn [_] {:text "ok" :raw nil})})
        ;; Fast model has a loose limit, slow model has a tight one. We only
        ;; call the fast model, so total elapsed should stay small even
        ;; though the slow model's limiter is also built.
        limited (llm/with-rate-limit provider
                  {:limits  {"fast" {:per-second 1000.0 :burst 100}
                             "slow" {:per-second 1.0    :burst 1}}
                   :default {:per-second 1.0 :burst 1}})
        t0      (System/nanoTime)]
    (dotimes [_ 5]
      (llm/query limited {:model "fast" :max-tokens 10
                          :messages [(llm/user-msg "x")]}))
    (let [elapsed-ms (/ (- (System/nanoTime) t0) 1e6)]
      (is (= 5 (count (:query-calls @state))))
      (is (< elapsed-ms 500)
          (str "5 fast calls should complete quickly, got "
               (long elapsed-ms) "ms")))))

(deftest stop!-closes-channel-even-with-pending-data
  (let [in        (bytes-stream (anthropic-sse-fixture))
        [ch stop] (llm/start-sse-stream! in a-parse)]
    (stop)
    ;; channel must eventually close; draining must terminate
    (is (some? (drain ch)) "channel drains without deadlock")))

;; --- SSE framing correctness ------------------------------------------------

(deftest sse-skips-comment-lines
  (let [fixture (str/join "\n"
                  [": this is a heartbeat"
                   "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}"
                   ""
                   ": another comment"
                   ""
                   "data: {\"type\":\"message_stop\"}"
                   ""])
        [ch stop] (llm/start-sse-stream! (bytes-stream fixture) a-parse)]
    (is (= ["hi"] (drain ch)))
    (stop)))

(deftest sse-accumulates-multiline-data-fields
  ;; Per the SSE spec, multiple `data:` lines within one event are joined
  ;; with \n before dispatch. We verify via a parser that echoes its input.
  (let [fixture   (str/join "\n"
                    ["data: {\"x\":"
                     "data:  1}"
                     ""])
        captured  (atom nil)
        parse     (fn [payload]
                    (reset! captured payload)
                    [{:type :done}])
        [ch stop] (llm/start-sse-stream! (bytes-stream fixture) parse)]
    (drain ch)
    (stop)
    (is (= "{\"x\":\n 1}" @captured)
        "data: lines must concatenate with \\n; leading space after the colon is stripped once")))

(deftest sse-handles-no-space-after-colon
  (let [fixture   (str/join "\n"
                    ["data:{\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}"
                     ""])
        [ch stop] (llm/start-sse-stream! (bytes-stream fixture) a-parse)]
    (is (= ["ok"] (drain ch)))
    (stop)))

(deftest sse-ignores-event-and-id-fields
  (let [fixture (str/join "\n"
                  ["event: content_block_delta"
                   "id: 42"
                   "retry: 1000"
                   "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"go\"}}"
                   ""])
        [ch stop] (llm/start-sse-stream! (bytes-stream fixture) a-parse)]
    (is (= ["go"] (drain ch)))
    (stop)))

(deftest sse-flushes-pending-event-on-eof
  ;; No trailing blank line — event must still dispatch on EOF.
  (let [fixture   "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"end\"}}"
        [ch stop] (llm/start-sse-stream! (bytes-stream fixture) a-parse)]
    (is (= ["end"] (drain ch)))
    (stop)))

(deftest parser-may-return-multiple-events-per-payload
  ;; A stateful wrapper (e.g. future incremental-JSON parser that buffers
  ;; accumulated text) must be free to emit more than one event for a
  ;; single SSE payload. Simulate that here with a parser that splits the
  ;; payload on `|` and emits a :chunk per segment.
  (let [fixture  (str/join "\n"
                   ["data: a|b|c"
                    ""
                    "data: d|done"
                    ""])
        parse    (fn [payload]
                   (let [parts (str/split payload #"\|")]
                     (mapv (fn [p]
                             (if (= p "done")
                               {:type :done}
                               {:type :chunk :value p}))
                           parts)))
        [ch stop] (llm/start-sse-stream! (bytes-stream fixture) parse)
        values    (drain ch)]
    (is (= ["a" "b" "c" "d"] values)
        "all :chunk events from a single payload must arrive in order")
    (stop)))
