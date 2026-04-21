(ns demo.server-test
  (:require [clojure.data.json :as json]
            [clojure.string :as string]
            [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [org.httpkit.client :as http-client]
            [org.httpkit.server :as hk]
            [demo.server :as server]))

(defn- with-running-server
  "Start the App component + an http-kit server on a random port, yield the base URL, then clean up."
  [dev? f]
  (let [app (component/start (server/map->App {:dev? dev?}))
        srv (hk/run-server
             (server/routes app)
             {:port 0
              :legacy-return-value?       false
              :legacy-unsafe-remote-addr? false})]
    (try (f (str "http://localhost:" (hk/server-port srv)))
         (finally
           (hk/server-stop! srv)
           (component/stop app)))))

(defn- demo-session-cookie
  "http-kit returns :set-cookie as either a single string or a collection of strings.
  Find the session cookie and return its raw Set-Cookie header value."
  [headers]
  (let [v  (:set-cookie headers)
        xs (cond (string? v) [v] (coll? v) v :else [])]
    (first (filter #(re-find #"^demo-session=" %) xs))))

(deftest session-cookie-has-secure-dev-defaults
  (with-running-server true
    (fn [base]
      (let [{:keys [status headers]} @(http-client/get base {:timeout 2000})
            cookie (demo-session-cookie headers)]
        (is (= 200 status))
        ;; wrap-anti-forgery stores the CSRF token in :session on first request,
        ;; which forces wrap-session to emit a Set-Cookie even though the handler
        ;; never touched :session itself.
        (is cookie "demo-session cookie is set on first request")
        (is (re-find #"(?i)HttpOnly"       cookie) "HttpOnly blocks JS cookie access (XSS)")
        (is (re-find #"(?i)SameSite=Lax"   cookie) "SameSite=Lax blocks cross-site POST CSRF")
        (is (re-find #"(?i)Path=/"         cookie) "Path=/ scopes cookie to full app")
        (is (not (re-find #"(?i);\s*Secure" cookie))
            "dev must NOT set Secure — browsers drop Secure cookies over http://localhost")))))

(deftest session-cookie-is-secure-in-prod
  (with-running-server false
    (fn [base]
      (let [{:keys [headers]} @(http-client/get base {:timeout 2000})
            cookie (demo-session-cookie headers)]
        (is cookie)
        (is (re-find #"(?i);\s*Secure" cookie)
            "prod MUST set Secure — prevents cookie leak over plaintext HTTP")))))

(deftest csrf-blocks-post-without-token
  (with-running-server true
    (fn [base]
      (let [{:keys [status]} @(http-client/post base {:body "" :timeout 2000})]
        ;; wrap-anti-forgery runs before routing, so this 403 proves CSRF is active
        ;; regardless of whether / has a POST handler registered.
        (is (= 403 status) "POST without CSRF token is rejected")))))

(defn- extract-bootstrap-signals
  "Pull the :data-signals JSON blob from the rendered page body."
  [body]
  (let [[_ raw] (re-find #"data-signals=\"([^\"]+)\"" body)]
    (json/read-str (string/replace raw "&quot;" "\"") :key-fn keyword)))

(defn- cookie-header
  "Take the first matching Set-Cookie value and reduce it to 'name=value' for
  the request Cookie header."
  [set-cookies prefix]
  (some->> set-cookies
           (filter #(string/starts-with? % prefix))
           first
           (re-find #"^[^;]+")))

(deftest csrf-accepts-post-with-signal-token
  (with-running-server true
    (fn [base]
      ;; 1. GET to obtain a session + the CSRF token embedded in data-signals.
      (let [{:keys [headers body]} @(http-client/get base {:timeout 2000})
            set-cookies (let [v (:set-cookie headers)]
                          (cond (string? v) [v] (coll? v) v :else []))
            cookie (cookie-header set-cookies "demo-session=")
            {:keys [csrf]} (extract-bootstrap-signals body)]
        (is cookie)
        (is csrf)
        ;; 2. POST back with the same session cookie and the token as a signal.
        ;; Datastar-Request: true is what read-signals gates body parsing on.
        (let [{:keys [status]}
              @(http-client/post (str base "/inc")
                                 {:timeout 2000
                                  :headers {"Cookie"           cookie
                                            "Content-Type"     "application/json"
                                            "Datastar-Request" "true"}
                                  :body    (json/write-str {:csrf csrf})})]
          (is (= 200 status) "POST with correct token in signals is accepted"))))))
