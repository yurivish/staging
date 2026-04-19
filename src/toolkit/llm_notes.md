# Notes for adding more features for next time

Missing features:

1. Response caching: For non-streaming requests only.
   SHA256-keyed cache of request→response (Go uses BoltDB). Adds infrastructure but useful in dev loops. Could use the existing toolkit.lmdb as backing store.
2. Per-model rate limiting
   Token-bucket limiter per model (Go uses x/time/rate). Prevents burning through RPM quota.
3. Request deduplication - https://cs.opensource.google/go/x/sync/+/master:singleflight/singleflight.go
   Coalesce identical concurrent requests (Go uses singleflight). Matters mainly when you have the cache on top of it.

---

Claude's thoughts for how to attempt this better next time:

● All 33 tests pass. The code is back to iteration-1 state plus the SSE framing correctness fix (which was a real bug, not scaffolding — Claude/Gemini work today but the reader now handles comments, multi-line
data:, no-space-after-colon, and EOF flush correctly).

---

Thoughts on simpler abstractions for next time

A few structural observations from the work I just backed out:

The Provider protocol is the wrong seam for these concerns. Caching, rate-limiting, and dedup are about HTTP calls and in-flight state, not about the provider-specific request/response translation. Wrapping
Provider forced each decorator to think about both -query (one-shot) and -query-stream (channel-based), which is where the complexity exploded — stream caching alone needs chunk buffering, tee-while-streaming,
error/stop detection, and race-free finalization.

Stream caching is most of what went wrong. Caching one-shot query is ~15 lines; caching query-stream is 10× that and three race conditions. For the 90% use case (same prompt on a retry/replay), you probably
don't even need stream caching — just cache query, and have replay return a single chunk.

Simpler shape to revisit later: three plain higher-order functions at the call site, not protocol decorators:

;; No protocol gymnastics. Just wrap the call.
(defn query-cached [cache provider req]
(or (cache-get cache req)
(let [r (llm/query provider req)]
(cache-put! cache req r)
r)))

(defn query-limited [limiter provider req]
(acquire! limiter (:model req))
(llm/query provider req))

(defn query-once [inflight provider req]
;; singleflight over a plain fn, not a protocol
(single-flight! inflight (cache-key req) #(llm/query provider req)))

Composition becomes (query-cached cache (fn [] (query-limited limiter provider req))) — explicit at the call site, no reify. Streams can remain uncached for now; add back only when the shape is clear.

The better layer is HTTP, not Provider. The three decorators are really about HTTP requests, and if the two providers shared an HTTP helper (the deferred refactor from iteration 1), caching/limiting/dedup could
hook in there once instead of being reified at the Provider level, and wouldn't need to handle streams as a separate case.

I'd revisit this by (1) extracting shared HTTP helpers first, (2) adding these concerns as HTTP middleware, (3) caching query only initially, (4) deferring stream caching until there's a concrete need.
