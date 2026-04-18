(ns demo.server
  "Run:  clj -M:run
   Open: http://localhost:8080

   Shows that a server-push SSE stream (ticking #count every second) coexists
   with user-triggered actions (button randomizes the page background) without
   either interfering with the other."
  (:require
   [clojure.core.async :refer [alt! chan close! go-loop timeout]]
   [datastar.core :as d]
   [hiccup2.core :as h]
   [org.httpkit.server :as hk]))

(def datastar-cdn
  "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.0/bundles/datastar.js")

(def css "
body {
  display: flex; flex-direction: column;
  align-items: center; justify-content: center;
  height: 100vh; margin: 0;
  font-family: system-ui; gap: 1.5rem;
}
#count  { font-size: 4rem; }
button  { font-size: 2rem; padding: .5rem 1.5rem; }
")

(def counter  (atom 0))
(def bg-color (atom "hsl(210, 70%, 85%)"))

(defn random-light-color []
  (format "hsl(%d, 70%%, 85%%)" (rand-int 360)))

(defn body-contents []
  [:body {:style (str "background: " @bg-color)}
   [:div#count (str @counter)]
   [:button {:data-on:click "@get('/randomize')"} "randomize bg"]])

(defn page []
  (str "<!doctype html>"
       (h/html
        [:html
         [:head
          [:style (h/raw css)]
          [:script {:type "module" :defer true :src datastar-cdn}]]
         ;; data-init lives only on the initial render so it fires exactly once;
         ;; patches from /randomize and /events deliberately don't carry it.
         (-> (body-contents)
             (update 1 assoc :data-init "console.log('init'); @get('/events')"))])))

(defn events [req]
  (let [closed (chan)]
    (d/sse-stream
     req
     {:on-open  (fn [sse]
                  (println "open")
                  (go-loop []
                    (swap! counter inc)
                    (d/patch-elements sse [:div#count (str @counter)])
                    (alt! closed         :done
                          (timeout 1000) (recur))))
      :on-close (fn [_sse status] (println "close" status) (close! closed))})))

(defn randomize [req]
  (reset! bg-color (random-light-color))
  (d/sse-response req {} (fn [sse] (d/patch-elements sse (body-contents)))))

(defn handler [req]
  (case (:uri req)
    "/"          {:status 200 :headers {"content-type" "text/html; charset=utf-8"} :body (page)}
    "/events"    (events req)
    "/randomize" (randomize req)
    {:status 404 :body "not found"}))

(defonce server (atom nil))

(defn start! []
  (when @server (@server))
  (reset! server (hk/run-server #'handler {:port 8080}))
  (println "http://localhost:8080"))

(defn stop! [] (when-let [s @server] (s) (reset! server nil)))

(defn -main [& _] (start!) @(promise))
