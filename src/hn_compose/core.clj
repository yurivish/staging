(ns hn-compose.core
  "Composition pipeline — the integration test for Datapotamus's new
   primitives. Takes a stream of mixed story/comment events, splits
   by kind, runs comments through a tumbling window keyed by
   (author, window-start), then joins per-author counts against
   stories landing in the same window. Stories are pre-gated by a
   shared rate-limit (proves the gate doesn't break composition with
   downstream joins).

   The whole pipeline exists to prove the primitives compose without
   losing tokens, deadlocking, or breaking quiescence — and to give
   the upcoming visualization tool a meaty multi-stage trace to
   render."
  (:require [clojure.data.json :as json]
            [toolkit.datapotamus.combinators.aggregate :as ca]
            [toolkit.datapotamus.combinators.control :as ct]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

;; --- Bare handler-maps used directly as procs ----------------------------

(def ^:private split-handler
  (step/handler-map
   {:ports {:ins {:in ""}
            :outs {:stories "" :comments ""}}
    :on-data
    (fn [ctx _ ev]
      (case (:kind ev)
        :story    {:stories  [(msg/child ctx ev)]}
        :comment  {:comments [(msg/child ctx ev)]}
        {}))}))

(defn- explode-by-author-handler []
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out ""}}
    :on-data
    (fn [ctx _ {:keys [window-start items]}]
      (let [by-by (group-by :by items)]
        {:out (msg/children
               ctx
               (mapv (fn [[author comments]]
                       {:author author
                        :window-start window-start
                        :n (count comments)})
                     by-by))}))}))

(defn- stories-bucket-handler [size-ms]
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out ""}}
    :on-data
    (fn [ctx _ story]
      (let [t  (long (:time story))
            ws (* (long size-ms) (long (quot t (long size-ms))))]
        {:out [(msg/child ctx (assoc story :window-start ws))]}))}))

;; --- Pipeline assembly ---------------------------------------------------

(defn build-flow
  "Returns the composition pipeline. All proc ids are explicit so the
   trace is easy to navigate in a visualizer.

   Config:
     :size-ms — tumbling window size used by the comments side
     :rps     — global rate-limit RPS for the stories side (default 1000)
     :burst   — bucket capacity (default 1000)"
  ([] (build-flow {}))
  ([{:keys [size-ms rps burst]
     :or   {size-ms 60000 rps 1000 burst 1000}}]
   (let [;; Stories sub-pipeline (flat — inner procs at outer level).
         rl       (ct/rate-limited {:rps rps :burst burst :id :stories-rate-limit})
         bkt      {:procs {:stories-bucket (stories-bucket-handler size-ms)}
                   :conns [] :in :stories-bucket :out :stories-bucket}
         stories  (step/serial rl bkt)
         ;; Comments sub-pipeline.
         win      (ca/tumbling-window {:size-ms size-ms :time-fn :time
                                      :id :comments-window
                                      :on-window (fn [start end items]
                                                   {:window-start start
                                                    :window-end   end
                                                    :items        items})})
         exp      {:procs {:explode-by-author (explode-by-author-handler)}
                   :conns [] :in :explode-by-author :out :explode-by-author}
         comments (step/serial win exp)
         ;; Join.
         join     (ca/join-by-key
                   {:id      :join
                    :ports   [:stories :comment-counts]
                    :key-fns {:stories       (juxt :by :window-start)
                              :comment-counts (juxt :author :window-start)}
                    :on-match
                    (fn [[author ws] items]
                      (let [story (first (:stories items))
                            cc    (first (:comment-counts items))]
                        {:author            author
                         :window-start      ws
                         :title             (:title story)
                         :story-id          (:id story)
                         :n-recent-comments (or (:n cc) 0)}))})]
     {:procs (merge {:split split-handler}
                    (:procs stories)
                    (:procs comments)
                    (:procs join))
      :conns (concat (:conns stories)
                     (:conns comments)
                     [[[:split :stories]      [(:in stories) :in]]
                      [[:split :comments]     [(:in comments) :in]]
                      [[(:out stories) :out]  [(:in join) :stories]]
                      [[(:out comments) :out] [(:in join) :comment-counts]]])
      :in    :split
      :out   (:out join)})))

(defn run-once!
  ([events] (run-once! events "./compose.json" {}))
  ([events out-path] (run-once! events out-path {}))
  ([events out-path opts]
   (let [res  (flow/run-seq (build-flow opts) events)
         outs (distinct (mapcat identity (:outputs res)))]
     (when (= :completed (:state res))
       (spit out-path (with-out-str (json/pprint outs))))
     {:state (:state res) :n (count outs) :error (:error res)})))
