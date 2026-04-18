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

(defn arm!
  "Re-arm so the next SSE connect fires a browser reload."
  []
  (reset! armed true))

(defn handler
  "Long-lived SSE handler. First connect after `arm!` sends a reload
   script; subsequent connects park until the client closes."
  [req]
  (d/sse-stream
   req
   {:on-open  (fn [sse]
                (when (compare-and-set! armed true false)
                  (d/execute-script sse "window.location.reload()")))
    :on-close (fn [_ _])}))
