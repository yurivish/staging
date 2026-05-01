(ns toolkit.llm.cache
  "LMDB-backed key→value cache for LLM responses.

   Both toolkit.llm callers (HTTP-driven, raw request body) and
   langchain4j callers (typed builder; no wire body) can share one store.
   Keys are caller-defined byte arrays — typically a SHA-256 of whatever
   inputs uniquely identify the call. The cache itself is opaque:
   bytes → bytes, with `*-json` helpers for the common case.

   Design:
     • Backed by `toolkit.lmdb`. One env, one Dbi. Open at startup, reuse.
     • Two layers: `get-bytes` / `put-bytes!` (raw), and `get-json` /
       `put-json!` / `compute!` (cooked, JSON-roundtrip via
       clojure.data.json).
     • `compute!` is the most common entry: returns `{:value v :cache hit?}`,
       calling `produce` only on miss.

   Why LMDB and not files: cheaper iteration when entries are small and
   numerous (no inode churn), atomic writes, and a single store the rest
   of the toolkit already exercises. Why not keyword-keyed: cache keys
   must survive across processes and across schema changes; opaque bytes
   make that the caller's problem, with `key-bytes-of` providing the
   default path."
  (:refer-clojure :exclude [get])
  (:require [clojure.data.json :as json]
            [toolkit.lmdb :as lmdb])
  (:import [java.nio.charset StandardCharsets]
           [java.security MessageDigest]))

;; --- key derivation ---------------------------------------------------------

(defn sha256-bytes
  "SHA-256 digest of `s` as a 32-byte array."
  ^bytes [^String s]
  (.digest (MessageDigest/getInstance "SHA-256")
           (.getBytes s StandardCharsets/UTF_8)))

(defn key-bytes-of
  "Default key derivation: SHA-256 of `s`. Callers stitch their inputs
   into a stable string (e.g. with `\\n--\\n` separators) and pass it
   through this. Different request shapes natively produce different
   keys without coordination."
  ^bytes [^String s]
  (sha256-bytes s))

;; --- store lifecycle --------------------------------------------------------

(defn open
  "Open (create if absent) an LMDB-backed cache at `path`. Returns a
   handle map `{:env :dbi}`. Close with `close`.

   Opts (forwarded to toolkit.lmdb):
     :map-size  hard ceiling on store bytes (LMDB address space, not
                disk). Default 1 GiB."
  ([path] (open path nil))
  ([path {:keys [map-size] :or {map-size (* 1024 1024 1024)}}]
   (let [env (lmdb/open-env path {:map-size map-size :max-dbs 4})
         dbi (lmdb/open-dbi env "llm-cache")]
     {:env env :dbi dbi})))

(defn close
  "Close the cache. Idempotent-ish: safe to call once at JVM shutdown."
  [{:keys [env]}]
  (when env (lmdb/close-env env)))

;; --- raw bytes API ----------------------------------------------------------

(defn get-bytes
  "Fetch the value bytes for `key-bytes`, or nil if absent."
  ^bytes [{:keys [env dbi]} ^bytes key-bytes]
  (lmdb/get env dbi key-bytes))

(defn put-bytes!
  "Store `value-bytes` under `key-bytes`. Overwrites."
  [{:keys [env dbi]} ^bytes key-bytes ^bytes value-bytes]
  (lmdb/put! env dbi key-bytes value-bytes))

;; --- cooked JSON API --------------------------------------------------------

(defn- bytes->json [^bytes bs]
  (json/read-str (String. bs StandardCharsets/UTF_8) :key-fn keyword))

(defn- json->bytes ^bytes [v]
  (.getBytes ^String (json/write-str v) StandardCharsets/UTF_8))

(defn get-json
  "Fetch and JSON-decode the value at `key-bytes`, or nil if absent or
   undecodable."
  [cache key-bytes]
  (when-let [bs (get-bytes cache key-bytes)]
    (try (bytes->json bs) (catch Throwable _ nil))))

(defn put-json!
  "JSON-encode `value` and store it at `key-bytes`."
  [cache key-bytes value]
  (put-bytes! cache key-bytes (json->bytes value)))

(defn compute!
  "Returns `{:value v :cache :hit|:miss}`. On hit, `v` is the cached JSON
   value. On miss, `(produce)` is called once, its result stored, and
   returned. `produce` must return a JSON-roundtrippable value."
  [cache key-bytes produce]
  (if-let [v (get-json cache key-bytes)]
    {:value v :cache :hit}
    (let [v (produce)]
      (put-json! cache key-bytes v)
      {:value v :cache :miss})))

;; --- shared default store + simple bypass arg --------------------------------
;;
;; All LLM callers (`toolkit.llm.cli`, `podcast.llm`, `podcast.cc`, …)
;; route through the same on-disk LMDB at `cache/llm/lmdb`. One env, one
;; shutdown hook, one accessor. Caching is on by default. Public-facing
;; transports take an explicit `:bypass? true` opt to skip the cache for
;; one call. There is no global toggle: disabling caching everywhere is
;; just `rm -rf cache/llm/lmdb` followed by re-running.

(def ^:private default-store-delay
  (delay
    (let [c (open "cache/llm/lmdb")]
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. ^Runnable (fn [] (close c))))
      c)))

(defn default-store
  "The shared LLM cache. All transports route through this."
  []
  @default-store-delay)

(defn through!
  "Run `(produce)` through the default cache. Returns
   `{:value v :cache :hit|:miss}`. On hit, `v` is the cached JSON
   value and `produce` is not called. Nil results from `produce` are
   NOT cached so transient errors retry on the next run.

   `key-string` is what gets SHA-256'd into the LMDB key. Callers
   stitch their inputs with `\\n--\\n` separators (the
   `podcast.llm/cache-key` convention)."
  [^String key-string produce]
  (let [k (key-bytes-of key-string)
        s (default-store)]
    (if-let [v (get-json s k)]
      {:value v :cache :hit}
      (let [v (produce)]
        (when v
          (put-json! s k v))
        {:value v :cache :miss}))))
