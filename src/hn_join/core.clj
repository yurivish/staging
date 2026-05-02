(ns hn-join.core
  "Story↔root-comment join. Fetches top HN stories and their comment
   trees, then emits one joined row per story whose data combines
   the story metadata with the root-level comment texts.

   Stress-tests `c/join-by-key` — N-input batch join over a single
   tree-fetch source, splitting into two semantic streams (stories,
   comments) and re-joining by `:story-id`.

   One-shot:
     clojure -M -e \"(require 'hn-join.core) (hn-join.core/run-once! \\\"join.json\\\")\""
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]
            [toolkit.hn.tree-fetch :as tree-fetch]))

(def ^:private base "https://hacker-news.firebaseio.com/v0")

(defn- get-json [url]
  (-> @(http/get url) :body (json/read-str :key-fn keyword)))

(defn- mk-fetch-top-ids [n]
  (step/step :fetch-top-ids
             (fn [_tick]
               (vec (take n (get-json (str base "/topstories.json")))))))

;; --- Split: tree → story-row + per-root-comment-row ----------------------

(def ^:private split-step
  "Receives a fetched tree on :in. Emits the story header on :stories
   and one row per root comment on :comments. Both rows carry the
   :story-id for the join."
  (let [hm (step/handler-map
            {:ports {:ins {:in ""} :outs {:stories "" :comments ""}}
             :on-data
             (fn [ctx _ tree]
               (let [sid       (:id tree)
                     story-row {:story-id sid
                                :title    (:title tree)
                                :url      (:url tree)}
                     comments  (mapv (fn [k]
                                       {:story-id sid
                                        :text     (:text k)})
                                     (filterv #(= "comment" (:type %))
                                              (:kid-trees tree)))]
                 {:stories  [(msg/child ctx story-row)]
                  :comments (msg/children ctx comments)}))})]
    {:procs {:split hm} :conns [] :in :split :out :split}))

;; --- Join step -----------------------------------------------------------

(def ^:private join-step
  (c/join-by-key
   {:ports    [:stories :comments]
    :key-fns  {:stories :story-id :comments :story-id}
    :on-match (fn [k items]
                (let [story (first (:stories items))]
                  {:story-id      k
                   :title         (:title story)
                   :url           (:url story)
                   :root-comments (mapv :text (:comments items))}))}))

;; --- Pipeline assembly ---------------------------------------------------

(defn build-flow
  ([] (build-flow {}))
  ([{:keys [n-stories tree-workers]
     :or   {n-stories 30 tree-workers 8}}]
   (step/serial :hn-join
                (mk-fetch-top-ids n-stories)
                (tree-fetch/step {:k tree-workers :get-json get-json})
                ;; Hand-wire the split → join: split has two outs, the
                ;; join expects two named ins; the wiring connects them
                ;; explicitly inside a beside.
                {:procs (merge (:procs split-step)
                               (:procs join-step))
                 :conns [[[(:out split-step) :stories] [(:in join-step) :stories]]
                         [[(:out split-step) :comments] [(:in join-step) :comments]]]
                 :in    (:in split-step)
                 :out   (:out join-step)})))

(defn run-once!
  ([] (run-once! "./join.json" {}))
  ([out-path] (run-once! out-path {}))
  ([out-path opts]
   (let [res  (flow/run-seq (build-flow opts) [:tick])
         outs (distinct (mapcat identity (:outputs res)))]
     (when (= :completed (:state res))
       (spit out-path (with-out-str (json/pprint outs))))
     {:state (:state res) :n (count outs) :error (:error res)})))
