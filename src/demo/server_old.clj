(ns demo.server-old
  "Run:  clj -M:run     (prod, no watcher)
         clj -M:dev     (dev REPL)
   Open: http://localhost:8080

   Shows that a server-push SSE stream (ticking #count every second) coexists
   with user-triggered actions (button randomizes the page background) without
   either interfering with the other."
  (:require
   [clojure.core.async :refer [alt! chan close! go-loop timeout]]
   [com.stuartsierra.component :as component]
   [datastar.core :as d]
   [hiccup2.core :as h]
   [org.httpkit.server :as hk]
   [toolkit.hotreload :as hotreload]))

(def datastar-cdn
  "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.0/bundles/datastar.js")

(def css "body {
            display: flex; flex-direction: column;
            align-items: center; justify-content: center;
            height: 100vh; margin: 0;
            font-family: system-ui; gap: 1.5rem;
          }
          #count  { font-size: 4rem; }
          button  { font-size: 2rem; padding: .5rem 1.5rem; }")

(defn random-light-color []
  (format "hsl(%d, 70%%, 85%%)" (rand-int 360)))

(defn body-contents [{:keys [counter bg-color]} dev?]
  [:body {:style (str "background: " @bg-color) :data-init "console.log('init'); @get('/events')"}
   [:div#count (str @counter)]
   [:button {:data-on:click "@get('/randomize')"} "randomize bg" (random-uuid)]
   (when dev? hotreload/snippet)])

(defn page [app-state dev?]
  (str "<!doctype html>"
       (h/html
        [:html
         [:head
          [:style (h/raw css)]
          [:script {:type "module" :defer true :src datastar-cdn}]]
         ;; data-init lives only on the initial render so it fires exactly once;
         ;; patches from /randomize and /events deliberately don't carry it.
         (-> (body-contents app-state dev?))])))

(defn events [req {:keys [counter] :as _app-state}]
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

(defn randomize [req {:keys [bg-color] :as app-state} dev?]
  (reset! bg-color (random-light-color))
  (d/sse-response req {} (fn [sse] (d/patch-elements sse (body-contents app-state dev?)))))

(defn make-handler [app-state dev?]
  (fn [req]
    (condp = (:uri req)
      "/"          {:status 200 :headers {"content-type" "text/html; charset=utf-8"} :body (page app-state dev?)}
      "/events"    (events req app-state)
      "/randomize" (randomize req app-state dev?)
      hotreload/path      (if dev? (hotreload/handler req) {:status 404 :body "not found"})
      {:status 404 :body "not found"})))

(defrecord AppState [counter bg-color]
  component/Lifecycle
  (start [this]
    (assoc this
           :counter  (atom 0)
           :bg-color (atom "hsl(210, 70%, 85%)")))
  (stop [this]
    (assoc this :counter nil :bg-color nil)))

(defrecord Server [port app-state dev? stop-fn]
  component/Lifecycle
  (start [this]
    (println (str "http://localhost:" port))
    ;; see https://github.com/http-kit/http-kit/blob/master/wiki/3-Server.md#security
    (assoc this :stop-fn (hk/run-server (make-handler app-state dev?) {:port port :legacy-unsafe-remote-addr? false})))
  (stop [this]
    (when stop-fn (stop-fn :timeout 100))
    (assoc this :stop-fn nil)))

(defn prod-system []
  (component/system-map
   :app-state (map->AppState {})
   :server    (component/using (map->Server {:port 8080}) [:app-state])))

(defn -main [& _]
  (let [system (component/start (prod-system))
        done   (promise)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. #(do (component/stop system) (deliver done :bye))))
    @done))
