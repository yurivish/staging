(ns demo.server
  (:require
   [clojure.core.async :refer [io-thread]]
   [clojure.data.json :as json]
   [com.stuartsierra.component :as component]
   [hiccup2.core :as h]
   [ring.middleware.session.memory :as session-mem]
   [ring.util.http-response :as r]
   [org.httpkit.server :as hk]
   [reitit.ring :as ring]
   [toolkit.datastar.core :as d]
   [toolkit.hotreload :as hotreload]
   [toolkit.web :as web]))

;; Conventions
;;
;; - We use the Component library to manage stateful resources.
;;
;; - We use function name suffixes to indicate the "type" of a function.
;;
;; - Functions with the suffix `-handler` are higher-order functions that return Ring handlers.
;;   Their arguments represent state (eg. components or their fields) and each returns a handler
;;   that uses that state. Handlers that do not require state take no arguments.
;;
;; - Functions with the suffix `-wrap` are reitit middleware.
;;
;; - Functions with the suffix `-page` use `web/html` to produce HTML for full pages.
;;
;; - Ring handlers can return HTML using `web/html`, which accepts Hiccup markup
;;   and returns a special body form that is converted by `web/wrap-html` middleware into HTML.
;;
;; - We define a top-level `system` variable containing the system map for our web server.
;;
;; - Live reload is handled through tools.namespace, which is aware of `system`, the toolkit, and
;;   our static files directory.

(def static-path "/static")

(defn page [{:keys [head body dev?]}]
  (web/html
   [:html
    [:head head
     [:script {:type "module" :defer true :src (str static-path "/datastar-pro.js")}]]
    [:body {:data-init    "@get('/stream')"
            :data-signals (json/write-str {:csrf (web/csrf-token)})}
     body (when dev? hotreload/snippet)]]))

(defn index-page [app]
  (page {:body (h/html
                [:div @(:counter app)]
                [:button {:data-on:click "@get('/inc')"} "click meeep"]
                [:button {:data-on:click "@get('/loading')"} "start loading"]
                [:span (random-uuid)]
                [:div#loading])
         :dev? (:dev? app)}))

(defn stream-handler []
  (fn [_] (r/ok "we are streaming.")))

(defn inc-handler [app]
  (fn [_]
    (println "incrementing counter: " @(:counter app))
    (swap! (:counter app) inc)
    (index-page app)))

(defn index-handler [app]
  (fn [_] (index-page app)))

(defn loading-handler  []
  (fn [req]
    (d/sse-stream
     req
     {:on-open
      (fn [sse]
        (io-thread
         (doseq [n (range 11) :while (d/sse-open? sse)]
           (println n)
           (d/patch-elements sse [:div#loading (str n)])
           (Thread/sleep 100))
         (d/sse-close! sse)))
      :on-close (fn [_sse _status] (println "CLOSE"))})))

(defn static-handler [dev?]
  (if dev?
    (ring/create-file-handler     {:path static-path :root "resources/public"})
    (ring/create-resource-handler {:path static-path :root "public"})))

(defn routes [app]
  (let [dev? (:dev? app)]
    (ring/ring-handler
     (ring/router
      [["/", (index-handler app)]
       ["/stream" (stream-handler)]
       ["/loading" (loading-handler)]
       ["/inc" (inc-handler app)]
       (when dev? [hotreload/path hotreload/handler])])
     (static-handler dev?)
     {:middleware (web/default-middleware
                   {:secure?       (not dev?)
                    :cookie-name   "demo-session"
                    :session-store (session-mem/memory-store (:sessions app))})})))

;; App state component
(defrecord App [counter bg-color sessions dev?]
  component/Lifecycle
  (start [this]
    (assoc this
           :counter  (atom 0)
           :bg-color (atom "hsl(210, 70%, 85%)")
           ;; In-memory session store; survives requests, not restarts. Swap for a
           ;; TTL-capable store in prod (ring-ttl-session, or an lmdb-backed one).
           :sessions (atom {})))

  (stop [this]
    (assoc this
           :counter nil
           :bg-color nil
           :sessions nil)))

;; Web server component
(defrecord Server [port app stop-fn]
  component/Lifecycle
  (start [this]
    (println (str "http://star.test:" port))
    ;; `legacy-unsafe-remote-addr? false` ensures that :remote-addr cannot be spoofed.
    (let [opts {:legacy-unsafe-remote-addr? false :port port}]
      (assoc this :stop-fn (hk/run-server (routes app) opts))))

  (stop [this]
    (when stop-fn (stop-fn :timeout 100))
    (assoc this :stop-fn nil)))

(defn system [{:keys [port dev?]}]
  (component/system-map
   :app (map->App {:dev? dev?})
   :server (component/using (map->Server {:port port}) [:app])))

(defn -main [& _]
  (let [system (component/start (system {:port 8080}))
        done (promise)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(do (component/stop system) (deliver done :bye))))
    @done))
