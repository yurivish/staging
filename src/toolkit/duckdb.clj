;; DuckDB component, mirroring `toolkit.sqlite` for shape and lifecycle.
;;
;; `path` is a filesystem path to a `.duckdb` file. An empty string opens
;; an in-memory database (`jdbc:duckdb:`); any other value is a file path.
;;
;; Callers use `next.jdbc` against `:datasource` directly — no query
;; helpers are provided here, same as for SQLite. `-main` is a top-level
;; demo: `clojure -M:duckdb` creates the database, inserts a few rows,
;; runs two queries, and prints the results.
(ns toolkit.duckdb
  (:require [toolkit.os-guard]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]))

(defrecord Duckdb [path datasource]
  component/Lifecycle
  (start [this]
    (assoc this :datasource
           (jdbc/get-datasource {:jdbcUrl (str "jdbc:duckdb:" path)})))
  (stop [this]
    (assoc this :datasource nil)))

(def ^:private people
  [[1 "Ada"      37]
   [2 "Grace"    45]
   [3 "Linus"    22]
   [4 "Margaret" 31]])

(defn -main [& args]
  (let [path (or (first args) "target/duckdb-demo.duckdb")]
    (when-let [parent (.getParentFile (io/file path))]
      (.mkdirs parent))
    (let [db (component/start (->Duckdb path nil))
          ds (:datasource db)]
      (try
        (jdbc/execute! ds
          ["CREATE TABLE IF NOT EXISTS people
              (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"])
        (jdbc/execute! ds ["DELETE FROM people"])
        (jdbc/with-transaction [tx ds]
          (doseq [[id name age] people]
            (jdbc/execute! tx
              ["INSERT INTO people (id, name, age) VALUES (?, ?, ?)" id name age])))
        ;; DuckDB's JDBC driver returns empty getTableName(), so next.jdbc
        ;; produces unqualified keywords (:id, :name) rather than :people/id.
        (println "rows:")
        (doseq [{:keys [id name age]}
                (jdbc/execute! ds ["SELECT id, name, age FROM people ORDER BY id"])]
          (println " " id name age))
        (let [[{:keys [avg_age n]}]
              (jdbc/execute! ds ["SELECT AVG(age) AS avg_age, COUNT(*) AS n FROM people"])]
          (println "aggregate: n =" n " avg_age =" avg_age))
        (println "db file:" (.getAbsolutePath (io/file path)))
        (finally
          (component/stop db))))))
