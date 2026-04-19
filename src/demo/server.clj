(ns demo.server
  (:require
   [clojure.core.async :refer [alt! chan close! go-loop timeout]]
   [toolkit.datastar.core :as d]
   [com.stuartsierra.component :as component]
   [hiccup2.core :as h]
   [ring.util.http-response :as r]
   [ring.util.response :as resp]
   [org.httpkit.server :as hk]
   [reitit.ring :as ring]
   [toolkit.hotreload :as hotreload]))

(def datastar-cdn "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.0/bundles/datastar.js")

(defn page [{:keys [head body dev?]}]
  (-> (r/ok (str "<!doctype html>"
                 (h/html
                  [:html
                   [:head
                    head
                    [:script {:type "module" :defer true :src datastar-cdn}]]
                   [:body {:data-init "@get('/stream')"} body (when dev? hotreload/snippet)]])))
      (resp/content-type "text/html; charset=utf-8")))

(defn index-page [app]
  (page {:body (h/html
                [:div @(:counter app)]
                [:button {:data-on:click "@get('/inc')"} "click meeep"]
                [:button {:data-on:click "@get('/loading')"} "start loading"]
                [:span (random-uuid)]
                [:div#loading])
         :dev? (:dev? app)}))

(defn inc-handler [app]
  (fn [_]
    (println "incrementing counter: " @(:counter app))
    (swap! (:counter app) inc)
    (index-page app)))

(defn static-handler [dev?]
  (if dev?
    (ring/create-file-handler     {:path "/static" :root "resources/public"})
    (ring/create-resource-handler {:path "/static" :root "public"})))

(defn index-handler [app]
  (fn [_] (index-page app)))

(defn loading-handler [_app]
  (fn [req]
    (let [closed (chan)]
      (d/sse-stream
       req
       {:on-open  (fn [sse]
                    (go-loop [n 0]
                      (d/patch-elements sse [:div#loading (str n)])
                      (if (>= n 10)
                        (d/sse-close! sse)
                        (alt! closed        :done
                              (timeout 500) (recur (inc n))))))
        :on-close (fn [_sse _status] (close! closed))}))))

(defn app-routes [app]
  (ring/ring-handler
   (ring/router
    [["/", (index-handler app)]
     ["/stream" r/ok]
     ["/loading" (loading-handler app)]
     ["/inc" (inc-handler app)]
     (when (:dev? app) [hotreload/path hotreload/handler])])
   (static-handler (:dev? app))))

;; Component representing app state.
(defrecord App [counter bg-color dev?]
  component/Lifecycle
  (start [this]
    (assoc this
           :counter  (atom 0)
           :bg-color (atom "hsl(210, 70%, 85%)")))
  (stop [this]
    (assoc this :counter nil :bg-color nil)))

;; Component representing a web server.
(defrecord Server [port app dev? stop-fn]
  component/Lifecycle
  (start [this]
    (println (str "http://star.test:" port))
    ;; `legacy-unsafe-remote-addr? false` ensures that :remote-addr cannot be spoofed.
    (let [opts {:legacy-unsafe-remote-addr? false :port port}
          routes (app-routes app)]
      (assoc this :stop-fn (hk/run-server routes opts))))

  (stop [this]
    (when stop-fn (stop-fn :timeout 100))
    (assoc this :stop-fn nil)))

(defn prod-system [{:keys [port dev?]}]
  (component/system-map
   :app (map->App {:dev? dev?})
   :server (component/using (map->Server {:port port :dev? dev?}) [:app])))

(defn -main [& _]
  (let [system (component/start (prod-system {:port 8080}))
        done (promise)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(do (component/stop system) (deliver done :bye))))
    @done))
