(ns demo.server
  "Same shape as the in-browser-dispatch repro, but patches now arrive via a
   real SSE response from the server (triggered by a button's @get).

   Run:  clj -M:run
   Open: http://localhost:8080 and click 'patch' repeatedly, watching the
   console for 'init' logs."
  (:require
   [clojure.core.async :refer [<! go-loop timeout]]
   [datastar.core :as d]
   [hiccup2.core :as h]
   [org.httpkit.server :as hk]))

(def datastar-cdn
  "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.0/bundles/datastar.js")

(def css "
*, *::before, *::after { box-sizing: border-box; }
* { margin: 0; }
:root {
  --base-color: oklch(92.2% 0 0);
  --base-color-content: oklch(25.3267% 0.015896 252.417568);
}
@media (prefers-color-scheme: dark) {
  :root {
    --base-color: oklch(25.3267% 0.015896 252.417568);
    --base-color-content: oklch(92.2% 0 0);
  }
}
body {
  display: grid;
  place-content: center;
  background-color: var(--base-color);
  font-family: system-ui;
  height: 100vh;
  padding: 4rem;
  text-align: center;
}
div { color: var(--base-color-content); font-size: 2rem; }
button { font: inherit; padding: .5rem 1rem; }
")

(def messages
  ["Stop overcomplicating it."
   "Backend controls state."
   "Props down, Events up."
   "Flamegraphs don't care about your feelings."
   "Practice yourself, for heaven's sake, in little things; and thence proceed to greater"
   "Freedom is the only worthy goal in life. It is won by disregarding things that lie beyond our control."
   "Be the change you want to see."
   "https://data-star.dev/ 🚀"])

(defn random-light-color []
  (format "hsl(%d, 70%%, 85%%)" (rand-int 360)))

(defn page-contents [_msg]
  [:div#page {:style     (str "background: " (random-light-color) "; min-height: 100vh")}
   [:h1 "datastar + http-kit demo"]
   [:p "Tick count updated via SSE:"]
   [:div#tick {:data-init "console.log('init'); @get('/events')"} "waiting…"]
   [:button {:data-on:click "@get('/patch')"} "patch"]])

(defn page []
  (str "<!doctype html>"
       (h/html
        [:html
         [:head
          [:script {:type "module" :defer true :src datastar-cdn}]]
         [:body (page-contents "Hello there!")]])))

(defn patch [req]
  (d/sse-response req {}
                  (fn [sse]
                    (d/patch-elements sse (page-contents (rand-nth messages))))))

(defn events [req]
  (d/sse-stream
   req
   {:on-open
    (fn [sse]
      (go-loop [n 0]
        (d/patch-elements sse [:div#tick {:data-init "console.log('init'); @get('/events')"} (str "tick " n)])
        (<! (timeout 1000))
        (recur (inc n))))}))

(defn handler [req]
  (case (:uri req)
    "/"       {:status 200 :headers {"content-type" "text/html; charset=utf-8"} :body (page)}
    "/events" (events req)
    "/patch"  (patch req)
    {:status 404 :body "not found"}))

(defonce server (atom nil))

(defn start! []
  (when @server (@server))
  (reset! server (hk/run-server #'handler {:port 8080}))
  (println "http://localhost:8080"))

(defn stop! [] (when-let [s @server] (s) (reset! server nil)))

(defn -main [& _] (start!) @(promise))
