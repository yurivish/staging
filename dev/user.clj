(ns user
  (:require [clojure.tools.namespace.repl :as repl]
            [com.stuartsierra.component :as component]
            [toolkit.dev :as dev]
            [toolkit.hotreload :as hr]
            [toolkit.watcher :as watcher]
            ;; Eager-load the toolkit namespaces (without :as) so they're
            ;; available at the REPL as `toolkit.data/foo` etc. Qualified
            ;; lookup goes through find-ns on every call, so it survives
            ;; tools.namespace refresh — no post-refresh alias fixup needed.
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
            ;; the toolkit README) — just eager-load here.
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
  (dev/reload! {:start start-sys! :stop stop-sys! :lock lock :before-start hr/arm!}))

;; Auto-start on load. `clojure -M:dev` explicitly calls `-e (user/start!)`,
;; but `clojure -M:dev:rebel` (what `just dev` runs) loses that because
;; rebel-readline's `:main-opts` replaces ours when aliases combine. Starting
;; here is idempotent — the `fw`/`sys` defonces guard repeat calls.
(start!)

;; ---- datapotamus ---------------------------------------------------------
(require '[clojure.java.io :as dp-io]
         '[next.jdbc :as dp-jdbc]
         '[toolkit.datapotamus.daemon :as dp-daemon]
         '[toolkit.datapotamus.demo :as dp-demo]
         '[toolkit.datapotamus.store :as dp-store]
         '[toolkit.datapotamus.trace :as dp-trace])

(defonce dp (atom nil))

(defn dp-start! []
  (let [db  (.getAbsolutePath (dp-io/file "data/dp.sqlite"))
        in  (.getAbsolutePath (dp-io/file "data/in"))
        _   (dp-io/make-parents (dp-io/file in "x"))
        ds  (dp-jdbc/get-datasource {:dbtype "sqlite" :dbname db})
        _   (dp-store/migrate! ds)
        _   (dp-jdbc/execute! ds
                              ["CREATE TABLE IF NOT EXISTS results (
                 rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                 run_id TEXT, msg_id TEXT, data_id TEXT,
                 written_at INTEGER, data TEXT)"])
        tc  (component/start (dp-trace/make {:datasource ds :batch-size 50 :flush-ms 100}))
        dae (component/start (dp-daemon/make
                              {:watch-dir in :datasource ds
                               :events-ch (:events-ch tc)
                               :pipeline (dp-demo/pipeline {:datasource ds})
                               :stable-gap-ms 500 :idle-complete-ms 300}))]
    (reset! dp {:ds ds :tc tc :dae dae :in in :db db})
    (println "datapotamus watching" in "db" db)))

(defn dp-stop! []
  (when-let [{:keys [dae tc]} @dp]
    (component/stop dae) (component/stop tc) (reset! dp nil)
    (println "datapotamus stopped")))

(defn dp-runs [] (dp-store/list-runs (:ds @dp) 10))

(defn dp-events []
  (when-let [r (first (dp-runs))]
    (dp-store/get-events (:ds @dp) (:run-id r) nil)))
