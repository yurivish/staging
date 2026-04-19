(ns demo.server
  (:require
   ;; [clojure.core.async :refer [alt! chan close! go-loop timeout]]
   [com.stuartsierra.component :as component]
   ;; [datastar.core :as d]
   ;; [hiccup2.core :as h]
   [ring.util.http-response :as r]
   [org.httpkit.server :as hk]
   [reitit.ring :as ring]
   #_[toolkit.hotreload :as hotreload]))

(defn handler [_]
  {:status 200, :body "ok"})

(defn inc-handler [app]
  (fn [_]
    (swap! (:counter app) inc)
    (r/ok (str @(:counter app)))))

(defn app-routes [app _dev?]
  (ring/ring-handler
   (ring/router
    [["/all" handler]
     ["/inc" (inc-handler app)]
     ["/ping" {:name ::ping
               :get handler
               :post handler}]])))

(defrecord App [counter bg-color]
  component/Lifecycle
  (start [this]
    (assoc this
           :counter  (atom 0)
           :bg-color (atom "hsl(210, 70%, 85%)")))
  (stop [this]
    (assoc this :counter nil :bg-color nil)))

(defrecord Server [port app dev? stop-fn]
  component/Lifecycle
  (start [this]
    (println (str "http://star.test:" port))
    ;; `legacy-unsafe-remote-addr? false` ensures :remote-addr cannot be spoofed
    ;; and will contain the true remote ip address.
    (let [opts {:legacy-unsafe-remote-addr? false :port port}
          routes (app-routes app dev?)]
      (assoc this :stop-fn (hk/run-server routes opts))))

  (stop [this]
    (when stop-fn (stop-fn :timeout 100))
    (assoc this :stop-fn nil)))

(defn prod-system [& [{:keys [port dev?]}]]
  (component/system-map
   :app (map->App {})
   :server (component/using (map->Server {:port port :dev? dev?}) [:app])))

(defn -main [& _]
  (let [system (component/start (prod-system))
        done (promise)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(do (component/stop system) (deliver done :bye))))
    @done))
