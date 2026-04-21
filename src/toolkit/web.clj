(ns toolkit.web
  "Sensible-default web middleware stack for DataStar-first apps.

  Exposes secure session cookies, DataStar-aware CSRF (token read from signals,
  not an X-CSRF-Token header), a `{:star/type :html :args hiccup}` response
  convention that `wrap-html` renders to a string, and a signals pre-parser
  that caches `d/read-signals` on `:body-params` so CSRF and handlers share one
  parse of the single-use Ring body.

  Most apps want `default-middleware`; assemble from the individual pieces if
  you need non-standard ordering."
  (:require
   [hiccup2.core :as h]
   [ring.middleware.anti-forgery :as anti-forgery]
   [ring.middleware.keyword-params :as kparams]
   [ring.middleware.params :as params]
   [ring.middleware.session :as session]
   [ring.middleware.session.memory :as session-mem]
   [ring.util.http-response :as r]
   [ring.util.response :as resp]
   [toolkit.datastar.core :as d]))

;; ----- Response helpers

(defn html
  "Helper function for an HTML Ring response. Returns a 200 with a body of
  `{:star/type :html :args hiccup}`; `wrap-html` renders it to a string."
  [args]
  (-> (r/ok {:star/type :html :args args})
      (resp/content-type "text/html; charset=utf-8")))

(defn csrf-token
  "Read the current request's CSRF token. Bound by wrap-anti-forgery; nil outside a request."
  []
  anti-forgery/*anti-forgery-token*)

(defn rotate-session
  "Force a new session id on auth-state changes (login, logout, privilege change).
  Ring has no built-in regeneration; assoc-ing a fresh :session rotates the id and
  prevents session fixation. Call on every auth transition."
  [response new-session-data]
  (assoc response :session new-session-data))

;; ----- Middleware

(defn wrap-html
  "Middleware that turns hiccup into HTML, allowing a richer intermediate structure
  to be returned from HTML routes, which eases testing and programmatic postprocessing."
  [handler]
  (fn [request]
    (let [response (handler request)]
      (if (and (map? (:body response)) (= (:star/type (:body response)) :html))
        (assoc response :body (str "<!doctype html>" (h/html (:args (:body response)))))
        response))))

(defn wrap-cache-signals
  "Parse DataStar signals once and cache on :body-params. Downstream readers
  (CSRF, any handler that wants signals) read :body-params directly — the Ring
  body is single-use. Parses every DataStar request regardless of downstream use."
  [handler]
  (fn [req]
    (handler (if-let [signals (d/read-signals req)]
               (assoc req :body-params signals)
               req))))

;; ----- The whole stack

(defn default-middleware
  "Returns the reitit-ready middleware vector, outermost first. Call this and
  splat into your router's :middleware.

  Options:
  - :secure?          browsers won't set Secure cookies over http://localhost, so
                      pass false in dev. Default true.
  - :session-store    a ring SessionStore. Default: a fresh in-memory store.
                      Swap to a TTL-capable store for prod (ring-ttl-session, lmdb, etc.).
  - :cookie-name      session cookie name. Default \"session\".
  - :csrf-signal-key  which key in the parsed signals carries the CSRF token.
                      Default :csrf.

  The other cookie attrs (HttpOnly, SameSite=Lax, Path=/) are non-negotiable —
  apps that genuinely need custom cookie config should skip this and assemble
  from the parts."
  [{:keys [secure? session-store cookie-name csrf-signal-key]
    :or   {secure?         true
           cookie-name     "session"
           csrf-signal-key :csrf}}]
  (let [store (or session-store (session-mem/memory-store))]
    ;; Order is outermost → innermost: session must see every inner request;
    ;; anti-forgery needs :session and :form-params already attached.
    [#(session/wrap-session
       %
       {:store        store
        :cookie-name  cookie-name
        ;; HttpOnly: blocks JS access (XSS token theft).
        ;; SameSite=Lax: blocks cross-site POST CSRF while allowing top-level nav.
        ;; Secure: prod-only — browsers won't set Secure cookies over http://localhost.
        :cookie-attrs {:http-only true
                       :same-site :lax
                       :secure    secure?
                       :path      "/"}})
     kparams/wrap-keyword-params
     params/wrap-params
     wrap-cache-signals
     ;; CSRF token is shipped as a DataStar signal, so wrap-anti-forgery reads
     ;; from the parsed signals rather than the default X-CSRF-Token header.
     #(anti-forgery/wrap-anti-forgery
       %
       {:read-token (fn [req] (some-> req :body-params (get csrf-signal-key)))})
     wrap-html]))
