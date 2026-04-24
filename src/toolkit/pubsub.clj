(ns toolkit.pubsub
  "Subject-based pub/sub over `toolkit.sublist`. Subjects and patterns
   are vectors of tokens — strings for literals, `:*` (single-token) and
   `:>` (tail) for wildcards. Publishers send to literal vectors.
   Queue groups are supported — one subscriber per group receives each
   message.

   Handlers are 3-ary: `(handler subject msg match-result)`. The
   `match-result` is the full sublist match (every `:plain` and every
   `:groups` member), shared across all handlers fired by a single
   publish; it's there for tracing/debug subscribers and can be ignored
   by ordinary handlers via `(fn [subj msg _] ...)`.

   Handlers run synchronously on the publisher's thread. If a handler
   throws, the stack is logged to *err* and remaining handlers still run.

   Each `sub` call returns a zero-arg unsub fn. Unsub is idempotent.
   Subscribing the same handler twice yields two independent subscriptions
   — each delivers, each has its own unsub."
  (:require [clojure.core.async :as async]
            [toolkit.sublist :as sl]))

(defn make
  "Returns a fresh pubsub."
  []
  {:subs (sl/make)})

(defn sub
  "Subscribes `handler` (a fn of [subject msg match-result]) to
   `subject-pattern` (a vector of tokens, optionally containing `:*` /
   `:>` wildcards). Returns a zero-arg unsub fn. Opts:
   `{:queue name :id any}` — `:queue` joins a queue group (one-per-group
   delivery); `:id` is an optional caller-facing label, orthogonal to
   identity (each sub call is independent regardless of `:id`)."
  ([ps subject-pattern handler] (sub ps subject-pattern handler nil))
  ([ps subject-pattern handler {:keys [queue id]}]
   (let [stored {::token (Object.) :id id :handler handler :queue queue}
         opts   (when queue {:queue queue})]
     (sl/insert! (:subs ps) subject-pattern stored opts)
     (fn unsub! []
       (sl/remove! (:subs ps) subject-pattern stored opts)
       nil))))

(defn- pick-one
  "Collapses a sublist match result into the final delivery set: every
   `:plain` subscriber plus one random member from each non-empty queue
   group. Lives here (not in sublist) because queue-group delivery is a
   pubsub concern — the sublist is pure routing."
  [{:keys [plain groups]}]
  (reduce (fn [acc [_ members]]
            (if (seq members)
              (conj acc (rand-nth members))
              acc))
          plain
          groups))

(defn- invoke [handler subject msg match-result]
  (try
    (handler subject msg match-result)
    (catch Throwable t
      (let [pw (java.io.PrintWriter. *err*)]
        (.println pw (str "[pubsub] handler threw on " (pr-str subject) " — " (.getMessage t)))
        (.printStackTrace t pw)
        (.flush pw)))))

(defn pub
  "Publishes `msg` to every subscriber matching `subject` (a literal
   vector). Runs all handlers synchronously on the caller's thread.
   Queue-group subs deliver to exactly one member per group (random
   choice).

   Each handler is invoked as `(handler subject msg match-result)` where
   `match-result` is the full sublist match — every `:plain` and every
   `:groups` member — shared across all handlers fired by this publish.
   Use it for tracing / debug subscribers that want to see the full
   fan-out; ignore it otherwise."
  [ps subject msg]
  (let [result (sl/match (:subs ps) subject)]
    (doseq [{:keys [handler]} (pick-one result)]
      (invoke handler subject msg result))))

(defn sub-chan
  "Subscribes `subject-pattern` and returns `[ch stop!]`. `[subject msg]`
   pairs flow onto `ch` (the match-result is dropped — channel consumers
   rarely want it). `stop!` unsubscribes and closes `ch` — call it when
   done. Messages are put! onto the channel asynchronously, so size
   `buf-size` for your throughput."
  ([ps subject-pattern buf-size] (sub-chan ps subject-pattern buf-size nil))
  ([ps subject-pattern buf-size opts]
   (let [ch    (async/chan buf-size)
         unsub (sub ps subject-pattern
                    (fn [subject msg _] (async/put! ch [subject msg]))
                    opts)]
     [ch (fn stop! [] (unsub) (async/close! ch))])))

(def valid-subject? sl/valid-subject?)
(def valid-pattern? sl/valid-pattern?)
