(ns user
  (:require [clojure.tools.namespace.repl :as repl]
            [com.stuartsierra.component :as component]
            [toolkit.dev :as dev]
            [toolkit.hotreload :as hr]
            [toolkit.watcher :as watcher]
            ;; Eager-load so compile errors surface at REPL start. We don't
            ;; alias — dev-system resolves factories dynamically (see the
            ;; toolkit README for why).
            [demo.server]))

(repl/set-refresh-dirs "src/demo")

;; Two independent lifecycles. The file watcher lives outside the app system
;; so a failed refresh doesn't kill it — otherwise fixing the syntax error
;; wouldn't be observed, because the watcher went down with the system.
(defonce ^:private sys nil)
(defonce ^:private fw nil)
(defonce ^:private lock (Object.))

(declare reload!)

(defn- dev-system []
  (component/system-map
   :app-state ((requiring-resolve 'demo.server/map->AppState) {})
   :server    (component/using
               ((requiring-resolve 'demo.server/map->Server)
                {:port 8080 :dev? true})
               [:app-state])))

(defn- start-sys! [] (alter-var-root #'sys #(or % (component/start (dev-system)))))
(defn- stop-sys!  [] (alter-var-root #'sys (fn [s] (some-> s component/stop) nil)))

(defn start! []
  (alter-var-root #'fw
    (fn [w]
      (or w
          (component/start
           ((requiring-resolve 'toolkit.watcher/map->FileWatcher)
            {:dir "src/demo" :interval-ms 100 :on-change #(reload!)})))))
  (start-sys!))

(defn stop! []
  (stop-sys!)
  (alter-var-root #'fw (fn [w] (some-> w component/stop) nil)))

(defn reload! []
  (dev/reload! {:start start-sys! :stop stop-sys! :lock lock :before-start hr/arm!}))
