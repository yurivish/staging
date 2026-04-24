(ns demo.server
  (:require
   [toolkit.os-guard]
   [clojure.core.async :refer [io-thread]]
   [clojure.data.json :as json]
   [com.stuartsierra.component :as component]
   [hiccup2.core :as h]
   [hn.core :as hn]
   [ring.middleware.session.memory :as session-mem]
   [ring.util.http-response :as r]
   [org.httpkit.server :as hk]
   [reitit.ring :as ring]
   [toolkit.datapotamus.flow :as flow]
   [toolkit.datapotamus.viz :as viz]
   [toolkit.datastar.core :as d]
   [toolkit.hotreload :as hotreload]
   [toolkit.pubsub :as pubsub]
   [toolkit.sqlite :as sqlite]
   [toolkit.web :as web])
  #_(:import
     [dev.langchain4j.model.anthropic AnthropicStreamingChatModel #_AnthropicChatModel]
     [dev.langchain4j.model.chat.response StreamingChatResponseHandler]))

;; (def model
;;   (-> (AnthropicChatModel/builder)
;;       (.apiKey key)
;;       (.modelName "claude-sonnet-4-5")
;;       (.build)))

;; (println (.chat model "In one sentence: what is Clojure?"))

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

;; ----- Viz (datapotamus flow visualizer) ------------------------------------

(defn viz-page [app]
  (web/html
   [:html
    [:head
     [:meta {:charset "UTF-8"}]
     [:title "datapotamus viz"]
     [:link {:rel "stylesheet" :href "/static/viz.css"}]
     [:script {:type :module :defer true :src (str static-path "/datastar-pro.js")}]]
    [:body {:data-init    "@get('/viz/stream')"
            :data-signals (json/write-str {:csrf (web/csrf-token)})}
     [:h1.viz-h1 "datapotamus"]
     [:div.viz-toolbar
      [:button.viz-btn {:data-on:click "@get('/viz/start')"} "Start flow"]]
     (viz/all-flows @(:viz-store app))
     (when (:dev? app) hotreload/snippet)]]))

(defn viz-handler [app]
  (fn [_req] (viz-page app)))

(defn viz-stream-handler [app]
  (viz/viz-stream-handler (:viz-store app) (:viz-ticker app)))

(defn viz-start-handler [app]
  (fn [_req]
    (let [fid      (str (random-uuid))
          flow-def (hn/build-flow (atom []))]
      (viz/register-flow! (:viz-store app) fid (viz/from-step flow-def) "hn")
      ;; Arm one render tick immediately so the new section appears.
      ((:tick! (:viz-ticker app)))
      (Thread/startVirtualThread
       (fn []
         (try
           (let [h (flow/start! flow-def {:flow-id fid
                                          :pubsub  (:viz-pubsub app)})]
             ;; Hand the live flow graph to viz — it spawns a ping loop
             ;; that adds queue depth to the cards alongside in-flight.
             (viz/attach-handle! (:viz-store app) fid
                                 (:toolkit.datapotamus.flow/graph h)
                                 (:viz-ticker app))
             (flow/inject! h {:data :tick})
             (let [sig (flow/await-quiescent! h)]
               (flow/stop! h)
               (viz/mark-complete! (:viz-store app) fid
                                   (if (= :quiescent sig) :completed :failed))
               ((:tick! (:viz-ticker app)))))
           (catch Throwable t
             (.printStackTrace t)
             (viz/mark-complete! (:viz-store app) fid :failed)
             ((:tick! (:viz-ticker app)))))))
      (r/no-content))))

(defn static-handler [dev?]
  (web/static-handler {:path static-path :dev? dev?}))

(defn routes [app]
  (let [dev? (:dev? app)]
    (ring/ring-handler
     (ring/router
      [["/", (index-handler app)]
       ["/stream" (stream-handler)]
       ["/loading" (loading-handler)]
       ["/inc" (inc-handler app)]
       ["/viz"        (viz-handler app)]
       ["/viz/stream" (viz-stream-handler app)]
       ["/viz/start"  (viz-start-handler app)]
       (when dev? [hotreload/path hotreload/handler])])
     (static-handler dev?)
     {:middleware (web/default-middleware
                   {:secure?       (not dev?)
                    :cookie-name   "demo-session"
                    :session-store (session-mem/memory-store (:sessions app))})})))

;; App state component
(defrecord App [counter bg-color sessions sqlite dev?
                viz-pubsub viz-store viz-ticker viz-unsub]
  component/Lifecycle
  (start [this]
    (let [ps (pubsub/make)
          st (viz/make-store)
          tk (viz/make-ticker 200)]
      (assoc this
             :counter    (atom 0)
             :bg-color   (atom "hsl(210, 70%, 85%)")
             ;; In-memory session store; survives requests, not restarts. Swap for a
             ;; TTL-capable store in prod (ring-ttl-session, or an lmdb-backed one).
             :sessions   (atom {})
             :viz-pubsub ps
             :viz-store  st
             :viz-ticker tk
             :viz-unsub  (viz/attach! ps st tk))))

  (stop [this]
    (when viz-unsub (viz-unsub))
    (assoc this
           :counter nil
           :bg-color nil
           :sessions nil
           :viz-pubsub nil
           :viz-store nil
           :viz-ticker nil
           :viz-unsub nil)))

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

(defn system [{:keys [port dev? db-path]}]
  (component/system-map
   :sqlite (sqlite/map->Sqlite {:path (or db-path "demo.sqlite")})
   :app    (component/using (map->App {:dev? dev?}) [:sqlite])
   :server (component/using (map->Server {:port port}) [:app])))

(defn -main [& _]
  (let [system (component/start (system {:port 8080}))
        done (promise)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(do (component/stop system) (deliver done :bye))))
    @done))
