(ns toolkit.hotreload
  "Hot-reload-by-reconnect for Datastar apps.

   Inject `snippet` into dev pages and register `handler` at `path`. When
   the browser first opens the SSE it receives `window.location.reload()`;
   subsequent connections park silently until `arm!` is called again. Call
   `arm!` from your reload orchestration (e.g. pass it as `:before-start`
   to `toolkit.dev/reload!`) so the next reconnect fires a reload.

   Pattern lifted from the Go toolkit's req.HotReloadHandler — but where
   Go relies on sync.Once resetting when the process exits, we arm
   explicitly so the same code works under tools.namespace/refresh."
   ;; Opt out of tools.namespace reload. dev/user.clj references `arm!`
   ;; and its bytecode (which is never regenerated — user.clj must
   ;; survive refresh) caches a Var object that would be orphaned if
   ;; this ns reloaded. Stateless toolkit fns tolerate orphaning, but
   ;; `arm!` closes over the `armed` atom whose identity matters — a
   ;; stale Var would flip a now-unused atom while the live handler
   ;; reads the fresh one, and reloads would silently stop firing.
   ;; See the "refresh orphans Vars" gotcha in toolkit/README.md.
  {:clojure.tools.namespace.repl/load false
   :clojure.tools.namespace.repl/unload false}
  (:require [datastar.core :as d]))

(def path "/hot-reload")

(def snippet
  [:div#hotreload
   {:data-init (str "@get('" path
                    "', {retryMaxCount: 1000, retryInterval: 20, retryMaxWaitMs: 200})")
    ;; Dim the page while the SSE is reconnecting (server restarting). The
    ;; server's reload script fires on reconnect success, which clears the
    ;; filter by virtue of reloading the document. No need to clear on
    ;; `started` — that would flicker during retry attempts.
    :data-on:datastar-fetch (str "['error','retrying'].includes(evt.detail.type)"
                                 " && (document.body.style.filter='sepia(1) hue-rotate(-30deg) saturate(1.5)')")}])

(def ^:private armed (atom true))
(defonce ^:private clients (atom #{}))

(defn arm!
  "Re-arm so the next SSE connect fires a browser reload. Use this when
   something outside the server's control (a code reload that restarts the
   process) will cause clients to reconnect anyway."
  []
  (reset! armed true))

(defn reload-now!
  "Push a reload to every currently connected hot-reload client over its
   existing SSE. Use this when the server stays up (e.g. a static-asset
   change) and you want the browser to reload immediately."
  []
  (doseq [sse @clients]
    (try (d/execute-script sse "window.location.reload()")
         (catch Throwable _))))

(defn handler
  "Long-lived SSE handler. Tracks open streams so `reload-now!` can push a
   reload down them. First connect after `arm!` also sends a reload
   script; subsequent connects park until the client closes."
  [req]
  (d/sse-stream
   req
   {:on-open  (fn [sse]
                (swap! clients conj sse)
                (when (compare-and-set! armed true false)
                  (d/execute-script sse "window.location.reload()")))
    :on-close (fn [sse _] (swap! clients disj sse))}))
