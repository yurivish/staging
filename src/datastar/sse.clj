(ns datastar.sse
  "Support for Server-Sent Events, based on the Pedesal SSE implementation.

   Adds support for specifying an event `retry` field, and reorders fields
   to match the Datastar spec (event, id, retry, data):
   https://github.com/starfederation/datastar/blob/develop/sdk/ADR.md"
  (:require
   [clojure.core.async :refer [<! go]]
   [clojure.spec.alpha :as s]
   [clojure.string :as string]
   [org.httpkit.server :as hk])
  (:import
   [com.aayushatharva.brotli4j Brotli4jLoader]
   [com.aayushatharva.brotli4j.encoder BrotliOutputStream Encoder$Mode Encoder$Parameters]
   [java.io ByteArrayOutputStream]
   [java.util.zip GZIPOutputStream]
   [org.httpkit.server AsyncChannel]))

;; SSE is designed to support incrmemental writing of a compressed message with periodic flushing.
;; The semantics of write-bytes as potentially flushing is to support uncompressed writing through
;; the same high-level interface (see send!, which works for compressed and uncompressed sse streams).
;; This is a low-level interface, and not thread-safe.
(defprotocol SSE
  (write-bytes! [this bytes])                  ; write, and potentially flush
  (send-bytes! [this bytes close-after-send])) ; write, flush, and optionally close

;; An SSESink is where an SSE record's outgoing bytes land. Streaming uses a ChannelSink
;; (forwards to http-kit's AsyncChannel); synchronous responses use a BufferSink
;; (accumulates into a ByteArrayOutputStream that the caller drains into a Ring body).
(defprotocol SSESink
  (sink-write! [this bytes close?]))

;; Used for async SSE responses
(defrecord ChannelSink [^AsyncChannel ch]
  SSESink
  (sink-write! [_ data close?]
    (hk/send! ch data close?)))

;; Used for sync SSE responses
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
            enc-stream]
  SSE
  (write-bytes! [_ data]
    (.write enc-stream data))
  (send-bytes! [this data close-after-send]
    (when data (write-bytes! this data))
    (.flush enc-stream)
    (let [result (.toByteArray out-stream)]
      (.reset out-stream)
      (when close-after-send (.close enc-stream))
      (sink-write! sink result close-after-send))))

(defn send!
  ([sse data]
   (send! sse data false))

  ([sse data close-after-send]
   (s/assert (s/coll-of string?) data)
   (s/assert boolean? close-after-send)
   (doseq [s data] (write-bytes! sse (.getBytes s "UTF-8")))
   (send-bytes! sse nil close-after-send)))

(defn close! [sse]
  (send-bytes! sse nil true))

;;
;; GZip
;;

;; we compress and send one message at a time.
;; todo: figure out how to elegantly accept strings or []byte for data in the protocol
;; todo: spec that the data is not nil - for compressed it matters! or we should coerce data to empty string [done]

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

;; TODO: Use the same defaults as Brotli in Go
(defn brotli-sse [sink & {:keys [quality window-size buffer-size] :or {quality 5 window-size 24}}]
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

;; This request key is used by Pedestal to signal when the initial contents of a response
;; including headers set by interceptors have been sent, and our SSE response can begin.
(def ^:private commited-ch-key :io.pedestal.http.request/response-commited-ch)

;; Shared header + encoding negotiation for both the streaming (`stream`) and
;; synchronous (`response`) entry points. Returns the response headers and a
;; constructor `->sse` that, given an SSESink, yields the appropriate SSE record.
;;
;; We use a content-encoding based on *our* ranked preferences since d* takes strong advantage of
;; Brotli's larger compression window, so we prefer it to gzip whenever possible.
;;
;; All header keys are kept lowercase for case-consistency. In Pedestal, this was also necessary
;; to overwrite headers already set by upstream interceptors (which are title-cased); if two keys
;; differ only in case they both get emitted and the SSE stream ends up with both application/json
;; and text/event-stream content-types.
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

(defn- default-on-close [_ch _status])

;; on-open args are [sse] and on-close args are [sse status] where status is passed from http-kit
;; and is either :client-close or :server-close
(defn stream
  [request {:keys [on-open on-close] :or {on-close default-on-close} :as opts}]

  (s/assert (s/nilable fn?) on-open)
  (s/assert (s/nilable fn?) on-close)

  (let [committed-ch (commited-ch-key request)
        {:keys [headers ->sse]} (negotiate-response request opts)
        ;; holds the SSE object once constructed, then passed into on-close.
        sse-atom    (atom nil)
        ;; If the Pedestal committed-ch is present on the request, wait for it before
        ;; firing on-open — Pedestal has already committed the initial response line
        ;; + headers. On bare http-kit the key is absent, and http-kit ignores
        ;; :status / :headers on the handler's returned response map when the body is
        ;; an AsyncChannel, so we must push the initial response ourselves via send!
        ;; with a {:status :headers} map before any body bytes go out.
        wrap-on-open (if committed-ch
                       (fn [on-open] (fn [ch]
                                       (go (<! committed-ch) (on-open (reset! sse-atom (->sse (->ChannelSink ch)))))))
                       (fn [on-open] (fn [ch]
                                       (hk/send! ch {:status 200 :headers headers} false)
                                       (on-open (reset! sse-atom (->sse (->ChannelSink ch)))))))
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
   whose body is the accumulated (possibly compressed) SSE byte stream.

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
