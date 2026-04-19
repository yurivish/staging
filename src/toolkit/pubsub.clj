(ns toolkit.pubsub
  "Subject-based pub/sub over `toolkit.sublist`. Subscribers register
   against subject patterns with `*` (single-token) and `>` (tail)
   wildcards; publishers send to literal subjects. Queue groups are
   supported — one subscriber per group receives each message.

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
  "Subscribes `handler` (a fn of [subject msg]) to `subject-pattern`.
   Returns a zero-arg unsub fn. Opts: `{:queue name :id any}` — `:queue`
   joins a queue group (one-per-group delivery); `:id` is an optional
   caller-facing label, orthogonal to identity (each sub call is
   independent regardless of `:id`)."
  ([ps subject-pattern handler] (sub ps subject-pattern handler nil))
  ([ps subject-pattern handler {:keys [queue id]}]
   (let [stored {::token (Object.) :id id :handler handler :queue queue}
         opts   (when queue {:queue queue})]
     (sl/insert! (:subs ps) subject-pattern stored opts)
     (fn unsub! []
       (sl/remove! (:subs ps) subject-pattern stored opts)
       nil))))

(defn- invoke [handler subject msg]
  (try
    (handler subject msg)
    (catch Throwable t
      (let [pw (java.io.PrintWriter. *err*)]
        (.println pw (str "[pubsub] handler threw on " (pr-str subject) " — " (.getMessage t)))
        (.printStackTrace t pw)
        (.flush pw)))))

(defn pub
  "Publishes `msg` to every subscriber matching `subject`. Runs all
   handlers synchronously on the caller's thread. Queue-group subs
   deliver to exactly one member per group (random choice)."
  [ps subject msg]
  (doseq [{:keys [handler]} (sl/pick-one (sl/match (:subs ps) subject))]
    (invoke handler subject msg)))

(defn sub-chan
  "Subscribes `subject-pattern` and returns `[ch stop!]`. `[subject msg]`
   pairs flow onto `ch`. `stop!` unsubscribes and closes `ch` — call it when
   done. Messages are put! onto the channel asynchronously, so size
   `buf-size` for your throughput."
  ([ps subject-pattern buf-size] (sub-chan ps subject-pattern buf-size nil))
  ([ps subject-pattern buf-size opts]
   (let [ch    (async/chan buf-size)
         unsub (sub ps subject-pattern
                    (fn [subject msg] (async/put! ch [subject msg]))
                    opts)]
     [ch (fn stop! [] (unsub) (async/close! ch))])))

(def valid-subject? sl/valid-subject?)
(def valid-pattern? sl/valid-pattern?)
