(ns toolkit.datapotamus.trace
  (:require [clojure.core.async :as a]
            [com.stuartsierra.component :as component]
            [toolkit.datapotamus.store :as store]))

(set! *warn-on-reflection* true)

(defrecord TraceCollector [datasource batch-size flush-ms events-ch stop-ch done-ch]
  component/Lifecycle
  (start [this]
    (let [events-ch (or events-ch (a/chan 256))
          stop-ch   (a/chan)
          done-ch   (a/chan)]
      (a/thread
        (try
          (loop [batch [] timer (a/timeout flush-ms)]
            (let [[v ch] (a/alts!! [events-ch timer stop-ch])]
              (cond
                (= ch timer)
                (do (store/insert-events! datasource batch)
                    (recur [] (a/timeout flush-ms)))

                (or (= ch stop-ch) (nil? v))
                (store/insert-events! datasource
                  (loop [acc batch]
                    (if-let [e (a/poll! events-ch)]
                      (recur (conj acc e))
                      acc)))

                :else
                (let [batch' (conj batch v)]
                  (if (>= (count batch') batch-size)
                    (do (store/insert-events! datasource batch')
                        (recur [] (a/timeout flush-ms)))
                    (recur batch' timer))))))
          (finally (a/close! done-ch))))
      (assoc this :events-ch events-ch :stop-ch stop-ch :done-ch done-ch)))
  (stop [this]
    (when stop-ch (a/close! stop-ch))
    (when done-ch (a/<!! done-ch))
    (assoc this :events-ch nil :stop-ch nil :done-ch nil)))

(defn make
  [{:keys [datasource batch-size flush-ms]
    :or {batch-size 100 flush-ms 50}}]
  (map->TraceCollector {:datasource datasource
                         :batch-size batch-size :flush-ms flush-ms}))
