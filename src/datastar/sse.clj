(ns datastar.sse
  "Support for Server-Sent Events over http-kit.

   Fields are ordered to match the Datastar spec (event, id, retry, data):
   https://github.com/starfederation/datastar/blob/develop/sdk/ADR.md"
  (:require
   [clojure.spec.alpha :as s]
   [clojure.string :as string]
   [org.httpkit.server :as hk])
  (:import
   [com.aayushatharva.brotli4j Brotli4jLoader]
   [com.aayushatharva.brotli4j.encoder BrotliOutputStream Encoder$Mode Encoder$Parameters]
   [java.io ByteArrayOutputStream OutputStream]
   [java.util.zip GZIPOutputStream]
   [org.httpkit.server AsyncChannel]))

;; SSE is designed to support incrmemental writing of a compressed message with periodic flushing.
;; We model write-bytes as potentially flushing to support uncompressed writing through
;; the same high-level interface (see send!, which works for compressed and uncompressed sse streams).
;; This is a low-level interface and is not thread-safe.
(defprotocol SSE
  (write-bytes! [this bytes])                  ; write, and potentially flush
  (send-bytes! [this bytes close-after-send])) ; write, flush, and optionally close

;; An SSESink is where an SSE record's outgoing bytes land. Streaming uses a ChannelSink
;; (forwards to http-kit's AsyncChannel); synchronous responses use a BufferSink
;; (accumulates into a ByteArrayOutputStream that the caller drains into a Ring body).
(defprotocol SSESink
  (sink-write! [this bytes close?]))

;; Sink for async SSE responses
(defrecord ChannelSink [^AsyncChannel ch]
  SSESink
  (sink-write! [_ data close?]
    (hk/send! ch data close?)))

;; Sink for sync SSE responses
(defrecord BufferSink [^ByteArrayOutputStream buf]
  SSESink
  (sink-write! [_ data _close?]
    (when data (.write buf ^bytes data))))

(defrecord UncompressedSSE
           [sink]
  SSE
  (write-bytes! [this data]
    (send-bytes! this data false))
  (send-bytes! [_ data close-after-send]
    (sink-write! sink data close-after-send)))

(defrecord CompressedSSE
           [sink
            ^ByteArrayOutputStream out-stream
            ^OutputStream enc-stream]
  SSE
  (write-bytes! [_ data]
    (.write enc-stream ^bytes data))
  (send-bytes! [this data close-after-send]
    (when data (write-bytes! this data))
    (.flush enc-stream)
    (let [result (.toByteArray out-stream)]
      (.reset out-stream)
      (when close-after-send (.close enc-stream))
      (sink-write! sink result close-after-send))))

(defn send!
  ([sse s] (send! sse s false))
  ([sse s close-after-send]
   (s/assert string? s)
   (s/assert boolean? close-after-send)
   (write-bytes! sse (.getBytes ^String s "UTF-8"))
   (send-bytes! sse nil close-after-send)))

(defn close! [sse]
  (send-bytes! sse nil true))

;;
;; GZip
;;

;; we compress and send one message at a time, flushing after each.

(defn gzip-sse [sink & [opts]]
  (let [out-stream (ByteArrayOutputStream.)
        enc-stream (if-let [buffer-size (:buffer-size opts)]
                     (GZIPOutputStream. out-stream buffer-size true)
                     (GZIPOutputStream. out-stream true))]
    (->CompressedSSE sink out-stream enc-stream)))

;;
;; Brotli
;;

(defn brotli-encoder-params [quality window-size]
  (doto (Encoder$Parameters.)
    ;; Compression mode for UTF-8 formatted text input.
    ;; Default is GENERIC in which the compressor knows nothing about the properties of the input.
    (.setMode Encoder$Mode/TEXT)
    ;; log2(LZ window size), in range [10, 24] or -1 for default value
    (.setWindow window-size)
    ;; Compression quality, in range [0, 11] or -1 for default value
    (.setQuality quality)))

;; We use the same default as the Go implementation.
;; Quality ranges from 0-11; larger values yield better but slower compression.
;; Window size ranges from 10-24; window size is `(pow(2, x) - 16)`; larger window size means more
;; lookback and the decoder needs more memory to decode; -1 lets the compressor decide the size.
(defn brotli-sse [sink & {:keys [quality window-size buffer-size] :or {quality 6 window-size -1}}]
  (Brotli4jLoader/ensureAvailability)
  (let [out-stream (ByteArrayOutputStream.)
        encoder-params (brotli-encoder-params quality window-size)
        enc-stream (if buffer-size
                     (BrotliOutputStream. out-stream encoder-params buffer-size)
                     (BrotliOutputStream. out-stream encoder-params))]
    (->CompressedSSE sink out-stream enc-stream)))

;;
;; Stream
;;

(defn parse-accept-field [^String accept-elem-str]
  (-> (string/split accept-elem-str #";" 2)
      first
      string/trim))

(defn parse-accept-encoding
  [accept-encoding-str]
  (let [encoding-elems (string/split accept-encoding-str #",")]
    (mapv parse-accept-field encoding-elems)))

(def ^:private ^String default-content-encoding "identity")

(defn preferred-content-encoding
  "Pick the highest-ranked contact encoding that is also accepted by the client, if any, or 'identity'.
  Ignores client-specified quality factors, which is useful when the server has strong preferences."
  [accept-encoding-str ranked-prefs]
  (let [encodings (parse-accept-encoding (or accept-encoding-str default-content-encoding))
        first-match (some (into #{} encodings) ranked-prefs)]
    (or first-match default-content-encoding)))

;; Shared header and encoding negotiation for streaming (`stream`) and
;; synchronous (`response`) entry points. Returns the response headers and a
;; constructor `->sse` that, given an SSESink, yields an SSE record of the appropriate type.
;;
;; We use a content-encoding based on *our* ranked preferences since datastar strongly benefits
;; from Brotli's larger compression window, so when possible we always prefer Brotli.
(defn- negotiate-response
  [request {:keys [brotli-opts gzip-opts]}]
  (let [accept-encoding-str (get-in request [:headers "accept-encoding"])
        encoding (preferred-content-encoding accept-encoding-str ["br" "gzip" "identity"])
        headers  (cond-> {"content-type"  "text/event-stream; charset=UTF-8"
                          "cache-control" "no-cache"}
                   encoding (assoc "content-encoding" (name encoding)))
        ->sse    (case encoding
                   "br"       (fn [sink] (brotli-sse sink brotli-opts))
                   "gzip"     (fn [sink] (gzip-sse sink gzip-opts))
                   "identity" ->UncompressedSSE
                   ->UncompressedSSE)]
    {:headers headers :->sse ->sse}))

(def ^:private default-on-close (constantly nil))

(defn stream
  "Returns a Ring response that streams SSE over http-kit's AsyncChannel.

   `on-open` receives the SSE object once the channel is ready; `on-close` receives
   [sse status] where status is http-kit's :client-close or :server-close.

   The response status line (200) is written to the wire before `on-open` fires,
   so there is no way to return a non-2xx from a handler that has already called
   `stream`. If a request should be rejected, do it *before* invoking `stream` and
   return a plain Ring response. Errors that emerge mid-stream must be surfaced
   in-band as SSE events (e.g. an `event: error` or a Datastar signal patch)
   before closing."
  [request {:keys [on-open on-close] :or {on-close default-on-close} :as opts}]

  (s/assert (s/nilable fn?) on-open)
  (s/assert (s/nilable fn?) on-close)

  (let [{:keys [headers ->sse]} (negotiate-response request opts)
        ;; holds the SSE object once constructed, then passed into on-close.
        sse-atom    (atom nil)
        ;; http-kit ignores :status / :headers on the handler's returned response map when the body
        ;; is an AsyncChannel, so we push the initial response ourselves via send! with a
        ;; {:status :headers} map before any body bytes go out.
        wrap-on-open (fn [on-open]
                       (fn [ch]
                         (hk/send! ch {:status 200 :headers headers} false)
                         (on-open (reset! sse-atom (->sse (->ChannelSink ch))))))
        chan-opts   (cond-> {}
                      on-open  (assoc :on-open (wrap-on-open on-open))
                      ;; IMPORTANT: Must register an on-close handler (even a no-op) to ensure proper
                      ;; channel closure detection. When a client disconnects, http-kit's RingHandler
                      ;; only calls AsyncChannel.onClose() if hasCloseHandler() returns true (see
                      ;; RingHandler.clientClose()). Without a handler, closedRan remains false and send
                      ;; operations continue returning true, silently discarding data after disconnect.
                      on-close (assoc :on-close (fn [_ch status] (on-close @sse-atom status))))]
    (assoc (hk/as-channel request chan-opts) :status 200 :headers headers)))

(defn response
  "Runs `f` synchronously with an SSE object and returns a Ring response map
   whose body is the accumulated (and possibly compressed) SSE byte stream.

   Uses the same content-type + encoding negotiation as `stream`; opts may
   include :brotli-opts and :gzip-opts."
  [request opts f]
  (let [{:keys [headers ->sse]} (negotiate-response request opts)
        buf (ByteArrayOutputStream.)
        sse (->sse (->BufferSink buf))]
    (f sse)
    (close! sse)
    {:status  200
     :headers headers
     :body    (.toByteArray buf)}))

;; perf notes:
;; - http-kit sets Transfer-Encoding: chunked (see AsyncChannel.firstWrite)
;; - the fastest path may be sending data as a ByteBuffer: https://github.com/http-kit/http-kit/blob/1388f31e0d3bf5e01b52ff95de0f44cfe0308453/src/java/org/httpkit/HttpUtils.java#L138C36-L138C46
;; - there is a third argument to BrotliOutputStream. that specifies the internal brotli buffer size, by default 16kb.
;; references:
;; - Anders Murphy on Brotli: https://andersmurphy.com/2025/04/15/why-you-should-use-brotli-sse.html
;; - Anders's Brotli implementation: https://github.com/andersmurphy/hyperlith/blob/0ec0b567f197be539c0d71ad19d752cd9824cf2e/src/hyperlith/impl/brotli.clj#L39
;; - AsyncChannel: https://github.com/http-kit/http-kit/blob/1388f31e0d3bf5e01b52ff95de0f44cfe0308453/src/java/org/httpkit/server/AsyncChannel.java#L26

;; Helpful test commands for compression:
;; curl --include --no-buffer --include 'http://localhost:8080/events'
;; curl --include --header 'Accept-Encoding: gzip' --no-buffer --include 'http://localhost:8080/events' --compressed
;; curl --include --header 'Accept-Encoding: gzip' --no-buffer --include 'http://localhost:8080/events' --compressed
