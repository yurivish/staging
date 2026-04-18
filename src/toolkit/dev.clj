(ns toolkit.dev
  "REPL harness helpers for component-based apps.

   The app owns its `sys` atom and its `start!`/`stop!` fns; this ns just
   provides the refresh-and-restart dance they all end up writing. See
   the README for the canonical user.clj shape."
  (:require [clojure.tools.namespace.repl :as repl]))

(defn- log-err [msg t]
  (binding [*out* *err*]
    (println msg)
    (when t (.printStackTrace ^Throwable t *err*))))

(defn refresh
  "Calls tools.namespace.repl/refresh with a thread-local *ns* binding.
   Required off REPL threads — refresh internally does (set! *ns* …),
   which needs *ns* to be thread-locally bound. Returns whatever refresh
   returns (:ok, a Throwable, or some other status keyword)."
  []
  ;; Any existing ns works as the anchor — refresh updates *ns* itself
  ;; as it loads each file. We use toolkit.dev (ourselves) because it's
  ;; guaranteed to exist whenever this fn runs.
  (binding [*ns* (the-ns 'toolkit.dev)] (repl/refresh)))

(defn reload!
  "Orchestrates stop → refresh → before-start → start, serialized under
   `lock`. On refresh failure, logs to *err* and leaves the system
   stopped so the next edit can retry.

   Opts:
     :start         — no-arg fn that starts the system
     :stop          — no-arg fn that stops the system
     :lock          — monitor object that serializes concurrent reloads
     :before-start  — (optional) no-arg fn run after successful refresh
                      and before start (e.g. toolkit.hotreload/arm!)"
  [{:keys [start stop lock before-start]}]
  (locking lock
    (stop)
    (let [r (refresh)]
      (cond
        (= :ok r)               (do (when before-start (before-start))
                                    (start))
        (instance? Throwable r) (log-err (str "[reload] refresh failed: "
                                              (.getMessage ^Throwable r))
                                         r)
        :else                   (log-err (str "[reload] refresh: " r) nil)))))
