# LLM Demo

```clj
(def api-key (string/trim (slurp "claude.key")))

(def model
  (-> (AnthropicStreamingChatModel/builder)
      (.apiKey api-key)
      (.modelName "claude-sonnet-4-5")
      (.build)))

(def done (promise))

(.chat model
       "In one sentence: what is Clojure?"
       (reify StreamingChatResponseHandler
         (onPartialResponse [_this chunk]
           (print chunk)
           (flush))
         (onCompleteResponse [_this _response]
           (println)               ;; trailing newline
           (deliver done :ok))
         (onError [_ err]
           (deliver done err))))

@done
```
