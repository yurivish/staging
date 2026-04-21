(ns toolkit.sqlite
  (:require [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]))

(defrecord Sqlite [path datasource]
  component/Lifecycle
  (start [this]
    (assoc this :datasource (jdbc/get-datasource {:dbtype "sqlite" :dbname path})))
  (stop [this]
    (assoc this :datasource nil)))
