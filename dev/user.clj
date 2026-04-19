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

(def refresh-dir "src")
;; Path excluded from the autoreload watcher. dev/user.clj is outside
;; set-refresh-dirs so its compiled bytecode survives refresh, but that
;; also means its Var refs to this ns's deps get orphaned on reload.
;; toolkit.hotreload is the one dep where that's a correctness bug
;; (it holds the `armed` atom whose identity matters), so it opts out
;; of tools.namespace reload in its ns decl — and we mirror that here
;; so saving the file doesn't fire a no-op reload cycle.
(def nonreloadable-file "src/toolkit/hotreload.clj")
(def static-dir  "resources/public")

(defn- reloadable-clj? [^java.io.File f]
  (and (watcher/clj-file? f)
       (not= (.getPath f) nonreloadable-file)))

(repl/set-refresh-dirs refresh-dir)

;; Two independent lifecycles. The file watcher lives outside the app system
;; so a failed refresh doesn't kill it — otherwise fixing the syntax error
;; wouldn't be observed, because the watcher went down with the system.
(defonce ^:private sys nil)
(defonce ^:private fw nil)
(defonce ^:private lock (Object.))

(declare reload!)

(defn- dev-system []
  ((requiring-resolve 'demo.server/prod-system) {:port 8080 :dev? true}))

(defn- start-sys! [] (alter-var-root #'sys #(or % (component/start (dev-system)))))
(defn- stop-sys!  [] (alter-var-root #'sys (fn [s] (some-> s component/stop) nil)))

(defn start! []
  (alter-var-root #'fw
                  (fn [w]
                    (or w
                        (component/start
                         (watcher/map->FileWatcher
                          {:interval-ms 100
                           :watches [{:dir refresh-dir :include? reloadable-clj?       :on-change reload!}
                                     ;; note: edit this if we want to filter out large directories
                                     {:dir static-dir  :include? (constantly true)     :on-change hr/reload-now!}]})))))
  (start-sys!))

(defn stop! []
  (stop-sys!)
  (alter-var-root #'fw (fn [w] (some-> w component/stop) nil)))

(defn reload! []
  (dev/reload! {:start start-sys! :stop stop-sys! :lock lock :before-start hr/arm!}))
