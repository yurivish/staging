(ns demo.server
  (:require
   [clojure.core.async :refer [io-thread]]
   [toolkit.datastar.core :as d]
   [com.stuartsierra.component :as component]
   [hiccup2.core :as h]
   [ring.util.http-response :as r]
   [ring.util.response :as resp]
   [org.httpkit.server :as hk]
   [reitit.ring :as ring]
   [toolkit.hotreload :as hotreload]))

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
;; - Functions with the suffix `-page` use `html` to produce HTML for full pages.
;;
;; - Ring handlers can return HTML using the `(html ...)` function, which accepts Hiccup markup
;;   and returns a special body form that is converted by the `wrap-html` middleware into HTML.
;;
;; - We define a top-level `system` variable containing the system map for our web server.
;;
;; - Live reload is handled through tools.namespace, which is aware of `system`, the toolkit, and
;;   our static files directory.

(defn html
  "Helper function for an HTML Ring response"
  [args]
  (-> (r/ok {:star/type :html :args args})
      (resp/content-type "text/html; charset=utf-8")))

(def static-path "/static")

(defn page [{:keys [head body dev?]}]
  (html
   [:html
    [:head head
     [:script {:type "module" :defer true :src (str static-path "/datastar-pro.js")}]]
    [:body {:data-init "@get('/stream')"} body (when dev? hotreload/snippet)]]))

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

(defn wrap-html
  "Middleware that turns hiccup into HTML, allowinb a richer intermediate structure to be returned from HTML routes,
  which eases testing and programmatic postprocessing."
  [handler]
  (fn [request]
    (let [response (handler request)]
    ; do we need contains?
      (if (and (map? (:body response)) (= (:star/type (:body response)) :html))
        (assoc response :body (str "<!doctype html>" (h/html (:args (:body response)))))
        response))))

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
     {:middleware [wrap-html]})))

;; App state component
(defrecord App [counter bg-color dev?]
  component/Lifecycle
  (start [this]
    (assoc this
           :counter  (atom 0)
           :bg-color (atom "hsl(210, 70%, 85%)")))

  (stop [this]
    (assoc this
           :counter nil
           :bg-color nil)))

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

;; "-handler" suffix for (defn [& state-args] (fn [req] resp-map))
;; "-page" suffix for functions that produce hiccup for an entire page
;; something else for subtemplates
;;
;; html templates: fn -> html
;; response fns: -> ring response map, eg. via r/ok
;; handler fns: same as response fn, but directly per-route?
;; need a naming convention to distinguish handler-makers from handlers

;; solution: convention: all handlers take optional state arguments and return a handler (fn from req to response map).

;; where do sse responses live? hmm. streaming vs not streaming...
