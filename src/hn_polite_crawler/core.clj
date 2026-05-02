(ns hn-polite-crawler.core
  "Polite paginated crawler over the HN Algolia search API.

   Stress-tests two new Datapotamus combinators in concert:
   `ct/rate-limited` (shared-bucket gate) caps total RPS across the
   whole worker pool, and `ct/with-backoff` retries 429s with
   exponential backoff + jitter. Persistent failures are routed to
   dead-letters via the `:dead-letter?` flag on `:out`.

   One-shot:
     clojure -M -e \"(require 'hn-polite-crawler.core) (hn-polite-crawler.core/run-once! [\\\"pg\\\" \\\"dang\\\"] \\\"crawl.json\\\")\""
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators.control :as ct]
            [toolkit.datapotamus.combinators.workers :as cw]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

(def ^:private base "https://hn.algolia.com/api/v1")

(defn- fetch-page!
  "One page of an Algolia author-history search. Throws ex-info with
   `{:transient true}` on 429 or 5xx so `ct/with-backoff` retries it."
  [{:keys [username page]}]
  (let [url  (str base "/search_by_date?tags=comment,author_" username
                  "&hitsPerPage=100&page=" page)
        resp @(http/get url {:timeout 15000})
        st   (:status resp)]
    (cond
      (= st 429)        (throw (ex-info "rate-limited" {:transient true :status 429 :user username}))
      (and st (>= st 500))  (throw (ex-info "server-err" {:transient true :status st :user username}))
      (and st (>= st 400))  (throw (ex-info "client-err" {:status st :user username}))
      :else (json/read-str (:body resp) :key-fn keyword))))

(defn- crawl-user!
  "Paginate one user's full history. Returns
   `{:user u :history [...] :pages n}`. `fetch-page!` raises on
   transient errors; `ct/with-backoff` upstream is what makes the
   wrapped retry work."
  [username]
  (loop [page 0 hits []]
    (let [pg        (fetch-page! {:username username :page page})
          new-hits  (or (:hits pg) [])
          last?     (or (empty? new-hits)
                        (>= (inc page) (or (:nbPages pg) 0)))
          all-hits  (into hits new-hits)]
      (if last?
        {:user username :history all-hits :pages (inc page)}
        (recur (inc page) all-hits)))))

;; --- Worker handler-map -----------------------------------------------------

(def ^:private fetch-handler
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out ""}}
    :on-data
    (fn [ctx _ {:keys [user]}]
      {:out [(msg/child ctx (crawl-user! user))]})}))

;; --- Pipeline ---------------------------------------------------------------

(defn build-flow
  "Build the polite-crawler flow over a fixed list of usernames.

   Config:
     :k           — pool size (default 8)
     :rps         — total requests-per-second across the pool (default 5)
     :burst       — token-bucket burst capacity (default 5)
     :max-retries — per-user retry budget on transient errors (default 4)
     :base-ms     — initial backoff delay in ms (default 200)"
  ([usernames] (build-flow usernames {}))
  ([usernames {:keys [k rps burst max-retries base-ms]
               :or   {k 8 rps 5 burst 5 max-retries 4 base-ms 200}}]
   (step/serial
    :polite-crawler
    (step/step :emit-users nil
               (fn [ctx _s _tick]
                 {:out (msg/children ctx (mapv #(hash-map :user %) usernames))}))
    (ct/rate-limited {:rps rps :burst burst})
    (cw/stealing-workers
     :pool k
     (ct/with-backoff {:max-retries max-retries :base-ms base-ms
                      :retry?      (comp :transient ex-data)}
                     fetch-handler)))))

;; --- run-once! --------------------------------------------------------------

(defn run-once!
  ([usernames] (run-once! usernames "./crawl.json" {}))
  ([usernames out-path] (run-once! usernames out-path {}))
  ([usernames out-path opts]
   (let [res  (flow/run-seq (build-flow usernames opts) [:tick])
         outs (vec (mapcat identity (:outputs res)))]
     (when (= :completed (:state res))
       (spit out-path (with-out-str (json/pprint outs))))
     {:state         (:state res)
      :n-success     (count (remove :dead-letter? outs))
      :n-dead-letter (count (filter :dead-letter? outs))
      :error         (:error res)})))
