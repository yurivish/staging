(ns toolkit.datapotamus.combinators.control
  "Pacing and resilience.

     `rate-limited` — passthrough step paced by a shared token bucket.
     `with-backoff` — wrap a handler-map's :on-data with exponential-
                      backoff retry; emits dead-letters on exhaustion."
  (:require [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

;; ============================================================================
;; Rate limiting — shared token bucket gate
;; ============================================================================

(defn- acquire-blocking!
  "Atomically refill and consume one token from `bucket` (an atom over
   `{:tokens d :last-ms long}`), blocking until a token is available.
   Refill rate `rps` (tokens/sec); cap at `burst`."
  [bucket rps burst]
  (loop []
    (let [now      (System/currentTimeMillis)
          old      @bucket
          elapsed  (max 0.0 (/ (- now (:last-ms old)) 1000.0))
          refill   (* elapsed (double rps))
          tokens'  (min (+ (:tokens old) refill) (double burst))]
      (if (>= tokens' 1.0)
        (if (compare-and-set! bucket old {:tokens (- tokens' 1.0) :last-ms now})
          true
          (recur))
        (let [need     (- 1.0 tokens')
              wait-ms  (max 1 (long (Math/ceil (* (/ need (double rps)) 1000))))]
          (Thread/sleep wait-ms)
          (recur))))))

(defn rate-limited
  "Passthrough step that paces messages through a shared token bucket.
   Insert it before a worker pool to bound the pool's effective RPS
   regardless of K. Each `:on-data` invocation consumes 1 token; the
   step blocks (Thread/sleep) until a token is available.

   Because all dispatched messages flow through the single proc that
   owns this step, the bucket is naturally shared — there is no
   per-worker subdivision. A free worker can no longer 'blow through'
   its own bucket because there are no per-worker buckets.

   Config:
     :rps   — refill rate in tokens per second
     :burst — bucket capacity (max immediate tokens)
     :id    — sid for this step (default :rate-limited)

   Usage:
     (step/serial
       upstream
       (ct/rate-limited {:rps 10 :burst 5})
       (cw/stealing-workers k expensive-step))"
  [{:keys [rps burst id]
    :or   {id :rate-limited}}]
  (assert (pos? rps) "rate-limited: :rps must be positive")
  (assert (pos? burst) "rate-limited: :burst must be positive")
  (let [bucket (atom {:tokens (double burst)
                      :last-ms (System/currentTimeMillis)})]
    (step/step id nil
               (fn [ctx _s _d]
                 (acquire-blocking! bucket rps burst)
                 {:out [(msg/pass ctx)]}))))

;; ============================================================================
;; Backoff / retry — exponential with jitter, dead-letter on exhaustion
;; ============================================================================

(defn with-backoff
  "Wrap an inner handler-map's `:on-data` so that exceptions matching
   `:retry?` trigger exponential-backoff retry. After `:max-retries`
   exhausted (or a non-matching exception), the failure is emitted on
   the inner's `:out` port as a dead-letter map tagged
   `:dead-letter? true` (instead of propagating the exception).

   Single-port design: dead-letters share the inner's `:out` port,
   marked by the `:dead-letter?` flag. Downstream code routes by
   inspecting the flag — keeps the handler-map shape compatible with
   any consumer that expects a single output port (including
   `cw/stealing-workers`). For a clean two-port routing, follow
   `with-backoff` with a `(step/step :route …)` that splits on the
   flag.

   Backoff schedule: attempt N (0-indexed) sleeps for
   `min(base-ms * 2^N, max-ms) * (1 + jitter * rand)`.

   Config:
     :max-retries — number of retries after the first attempt (default 3,
                    so up to 4 attempts total)
     :base-ms     — initial backoff (default 100)
     :max-ms      — cap on a single sleep (default 30000)
     :jitter      — multiplier for additive randomness in [0, jitter * cap]
                    (default 0.5; pass 0 for deterministic timing)
     :retry?      — predicate on the thrown Throwable. Default: retry
                    everything. To retry on a flag in `ex-data`, pass
                    e.g. `(comp :transient ex-data)`.

   Dead-letter payload shape (on `:out`):
     {:dead-letter? true :error string :ex-data map-or-nil
      :data input-data :attempts n}"
  [{:keys [max-retries base-ms max-ms jitter retry? id]
    :or   {max-retries 3 base-ms 100 max-ms 30000 jitter 0.5
           retry? (constantly true) id :backoff}}
   inner]
  (assert (>= max-retries 0) "with-backoff: :max-retries must be non-negative")
  (assert (pos? base-ms) "with-backoff: :base-ms must be positive")
  (assert (step/handler-map? inner)
          "with-backoff: inner must be a handler-map")
  (let [user-on-data (:on-data inner)
        wrapped
        (assoc inner :on-data
               (fn [ctx s d]
                 (loop [attempt 0]
                   (let [outcome (try {:ok (user-on-data ctx s d)}
                                      (catch Throwable t {:err t}))]
                     (cond
                       (contains? outcome :ok)
                       (:ok outcome)

                       (and (retry? (:err outcome))
                            (< attempt max-retries))
                       (let [base (* (double base-ms) (Math/pow 2 attempt))
                             cap  (min base (double max-ms))
                             jit  (if (zero? jitter) 0.0 (* cap jitter (rand)))
                             wait (max 1 (long (+ cap jit)))]
                         (Thread/sleep wait)
                         (recur (inc attempt)))

                       :else
                       (let [err (:err outcome)]
                         {:out
                          [(msg/child ctx
                                      {:dead-letter? true
                                       :error        (.getMessage err)
                                       :ex-data      (ex-data err)
                                       :data         d
                                       :attempts     (inc attempt)})]}))))))]
    {:procs {id wrapped} :conns [] :in id :out id}))
