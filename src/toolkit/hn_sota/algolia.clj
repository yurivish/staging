(ns toolkit.hn-sota.algolia
  "Algolia HN search wrapper. Single fn `top-stories-24h` → seq of
   story stubs from the past 24 hours.

   Uses Algolia's default ordering for `tags=story` with no query
   string, which empirically tracks point-count for non-query searches
   — close enough to a 24h front-page list for a demo without depending
   on an extra `tags=front_page` round-trip per story."
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]))

(def ^:private base "https://hn.algolia.com/api/v1")

(defn- ts-24h-ago []
  (- (long (/ (System/currentTimeMillis) 1000)) (* 24 60 60)))

(defn top-stories-24h
  "Fetch up to `n` of the top stories created in the last 24 hours.
   Returns a vector of `{:id :title :url :points :author :created-at}`.
   Stories without a `:url` (Show HN, Ask HN, dead) are kept — the
   filter stage decides relevance separately.

   `n` is capped to 1000 by Algolia's `hitsPerPage` ceiling."
  [n]
  (let [url    (str base "/search?tags=story"
                    "&numericFilters=created_at_i%3E" (ts-24h-ago)
                    "&hitsPerPage=" (min n 1000))
        body   (-> @(http/get url) :body)
        parsed (json/read-str body :key-fn keyword)]
    (mapv (fn [h]
            {:id         (Long/parseLong (:objectID h))
             :title      (:title h)
             :url        (:url h)
             :points     (:points h)
             :author     (:author h)
             :created-at (:created_at_i h)})
          (:hits parsed))))
