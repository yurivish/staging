# Star

next: simplify the server file; figure out how to write a little webpage concisely, factoring out any tools or introducing libraries as needed. potentially including honeysql and jdbc sqlite or whatever i used last time - go look.

todo: write down the ideas here.
write it myself, for the summarization is the point.

goal is to have a 'home' for concise development with a live, malleable workflow.

getting there!

need to articulate the practices, too - set up dev/user.clj, use hot reload, how those integrate w the push-refresh on static files, j dev for dev, etc.

- Use Component to set up stateful systems. We have particular support for a live development flow for web services that auto-reloads all changed namespaces, then restarts the main system using tools.namespace, whenever edits are made to code or static files.
- I'm curating a `toolkit` library of useful packages, including my own datastar bindings for http-kit, a simple file-watcher to support auto-reload,
- demo has the webapp.

## Bugs

- http-kit does not reliably close SSE connections when the client closes them

  eg. can test with

  ```clojure
  (defn loading-handler  [req]
  (d/sse-stream
   req
   {:on-open
    (fn [sse]
      (io-thread
       (doseq [n (range 11) :while (d/sse-open? sse)]
         (println n)
         (d/patch-elements sse [:div#loading (str n)])
         (Thread/sleep 100))
       (d/sse-close! sse)))
    :on-close (fn [_sse _status] (println "CLOSE"))}))
  ```
