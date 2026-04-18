(ns demo.server
  "Tiny http-kit demo for the datastar package.

   Run from a REPL:
     (require 'demo.server)
     (demo.server/start!)
     (demo.server/stop!)

   Or from the CLI:
     clj -M -m demo.server"
  (:require
   [clojure.core.async :refer [<! go-loop timeout]]
   [datastar.core :as d]
   [hiccup2.core :as h]
   [org.httpkit.server :as hk]))

(def datastar-cdn
  "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.0/bundles/datastar.js")

(defn page []
  (str
   "<!doctype html>"
   (h/html
    [:html
     [:head
      [:title "datastar demo"]
      [:script {:type "module" :src datastar-cdn}]]
     [:body
      [:h1 "datastar + http-kit demo"]
      [:p "Tick count updated via SSE:"]
      [:div#tick {:data-init "@get('/events')"} "waiting…"]]])))

(defn index [_req]
  {:status  200
   :headers {"content-type" "text/html; charset=utf-8"}
   :body    (page)})

(defn events [req]
  (d/sse-stream
   req
   {:on-open
    (fn [sse]
      (go-loop [n 0]
        (d/patch-elements sse [:div#tick (str "tick " n)])
        (<! (timeout 1000))
        (recur (inc n))))}))

(defn handler [req]
  (case (:uri req)
    "/"       (index req)
    "/events" (events req)
    {:status 404 :body "not found"}))

(defonce server (atom nil))

(defn start!
  ([] (start! 8080))
  ([port]
   (when @server (@server))
   (reset! server (hk/run-server #'handler {:port port}))
   (println "listening on http://localhost:" port)))

(defn stop! []
  (when-let [s @server] (s) (reset! server nil)))

(defn -main [& _]
  (start!)
  @(promise))
