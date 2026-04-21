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

# Goals

- [SCS](https://github.com/alexedwards/scs)-equivalent sessions in terms of robustness and security.
-

# AI next steps

Current provider coverage via langchain4j 1.13.0 (see `deps.edn`):

- **Anthropic** — `langchain4j-anthropic` → `AnthropicChatModel`
- **OpenAI** — `langchain4j-open-ai` → `OpenAiChatModel`
- **Gemini (API-key)** — `langchain4j-google-ai-gemini` → `GoogleAiGeminiChatModel`
  (for GCP/Vertex auth swap in `langchain4j-vertex-ai-gemini` →
  `VertexAiGeminiChatModel` instead)

## Local Ollama

Ollama exposes an OpenAI-compatible endpoint at
`http://localhost:11434/v1`, so no extra dependency is needed — reuse
`OpenAiChatModel` with the base URL overridden:

```clojure
(-> (OpenAiChatModel/builder)
    (.baseUrl "http://localhost:11434/v1")
    (.apiKey "ollama")          ; any non-blank string; Ollama ignores it
    (.modelName "llama3.2")     ; whatever is `ollama pull`ed locally
    (.build))
```

If you need Ollama-native features the OpenAI shim doesn't expose
(model management, some streaming options), add
`dev.langchain4j/langchain4j-ollama` and use `OllamaChatModel` — note
that module is still on the `-beta` track.

## Logging

`org.slf4j/slf4j-nop` is pinned in main deps to silence the
"No SLF4J providers" warning that langchain4j triggers. Swap it for
`ch.qos.logback/logback-classic` (full-featured) or
`org.slf4j/slf4j-simple` (minimal stderr) if you want real log output.
