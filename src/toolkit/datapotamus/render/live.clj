(ns toolkit.datapotamus.render.live
  "Live status view: periodic terminal redraw of a running pipeline's
   tree with stats columns. The data layer is `render.stats` (snapshot
   the watcher); the rendering layer is `render/render-with-stats`. This
   ns is just the I/O wrapper — start a thread, take a snapshot every
   `interval-ms`, clear screen, print, repeat.

   Usage:
     (let [ps      (pubsub/make)
           watcher (stats/make)
           _       (stats/attach! ps watcher)
           h       (flow/start! my-pipeline {:pubsub ps})
           stop!   (live/watch! h my-pipeline watcher)]
       ;; ... do work, watch the terminal update ...
       (stop!)
       (flow/stop! h))"
  (:require [toolkit.datapotamus.render :as render]
            [toolkit.datapotamus.render.stats :as stats]))

(def ^:private clear-screen "\033[H\033[2J")

(defn- redraw [stepmap stats-map]
  (print clear-screen)
  (run! println (render/render-with-stats stepmap stats-map))
  (flush))

(defn watch!
  "Start a daemon thread that redraws the rendered tree every
   `interval-ms` (default 200ms). Returns a zero-arg fn that stops
   the thread.

   The watcher must already be attached to the same pubsub the flow
   was started with."
  ([flow-handle stepmap watcher]
   (watch! flow-handle stepmap watcher nil))
  ([flow-handle stepmap watcher {:keys [interval-ms]
                                 :or   {interval-ms 200}}]
   (let [stop?  (volatile! false)
         thread (Thread.
                 ^Runnable
                 (fn []
                   (while (not @stop?)
                     (try
                       (redraw stepmap
                               (stats/stats-map-from-flow
                                flow-handle stepmap watcher))
                       (Thread/sleep (long interval-ms))
                       (catch InterruptedException _
                         (vreset! stop? true))
                       (catch Throwable t
                         (println "live/watch! redraw error:" (ex-message t)))))))]
     (.setDaemon thread true)
     (.start thread)
     (fn stop! []
       (vreset! stop? true)
       (.interrupt thread)))))
