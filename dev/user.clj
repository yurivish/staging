(ns user
  (:require [clojure.tools.namespace.repl :as repl]
            [com.stuartsierra.component :as component]
            [toolkit.dev :as dev]
            [toolkit.hotreload :as hr]
            [toolkit.watcher :as watcher]
            ;; Eager-load so compile errors surface at REPL start. Aliases
            ;; for these are installed below via `install-toolkit-aliases!`
            ;; — not here — because tools.namespace/refresh remove-ns's and
            ;; reloads each ns, leaving ns-decl aliases pointing at the old
            ;; (now-orphaned) Namespace object. Re-installing after every
            ;; refresh keeps `data/foo` etc. resolving to fresh Vars.
            [toolkit.data]
            [toolkit.datastar.core]
            [toolkit.datastar.sse]
            [toolkit.h2]
            [toolkit.llm]
            [toolkit.llm.anthropic]
            [toolkit.lmdb]
            [toolkit.pubsub]
            [toolkit.singleflight]
            [toolkit.sqlite]
            [toolkit.stree]
            [toolkit.sublist]
            [toolkit.web]
            ;; dev-system resolves demo.server factories dynamically (see
            ;; the toolkit README for why) — just eager-load here.
            [demo.server]))

(def ^:private toolkit-aliases
  '{data         toolkit.data
    d            toolkit.datastar.core
    sse          toolkit.datastar.sse
    h2           toolkit.h2
    llm          toolkit.llm
    anthropic    toolkit.llm.anthropic
    lmdb         toolkit.lmdb
    pubsub       toolkit.pubsub
    singleflight toolkit.singleflight
    sqlite       toolkit.sqlite
    stree        toolkit.stree
    sublist      toolkit.sublist
    web          toolkit.web})

(defn- install-toolkit-aliases! []
  (let [user-ns (find-ns 'user)]
    (doseq [[a ns-sym] toolkit-aliases]
      (when-let [n (find-ns ns-sym)]
        ;; .addAlias throws if the alias already points to a different ns,
        ;; which is exactly the post-refresh case. Remove first.
        (.removeAlias user-ns a)
        (.addAlias user-ns a n)))))

(install-toolkit-aliases!)

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
  ((requiring-resolve 'demo.server/system) {:port 8080 :dev? true}))

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
                                     ;; note: edit this if we want to filter out any files or directories
                                     {:dir static-dir  :include? (constantly true)     :on-change hr/reload-now!}]})))))
  (start-sys!))

(defn stop! []
  (stop-sys!)
  (alter-var-root #'fw (fn [w] (some-> w component/stop) nil)))

(defn reload! []
  (dev/reload! {:start start-sys! :stop stop-sys! :lock lock
                :before-start (fn []
                                (install-toolkit-aliases!)
                                (hr/arm!))}))

;; Auto-start on load. `clojure -M:dev` explicitly calls `-e (user/start!)`,
;; but `clojure -M:dev:rebel` (what `just dev` runs) loses that because
;; rebel-readline's `:main-opts` replaces ours when aliases combine. Starting
;; here is idempotent — the `fw`/`sys` defonces guard repeat calls.
(start!)
