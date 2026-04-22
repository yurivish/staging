(ns toolkit.sqlite-test
  (:require [clojure.test :refer [deftest is testing]]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [toolkit.sqlite :as sqlite])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-db-file ^File []
  (-> (Files/createTempFile "sqlite-test-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn- temp-dir ^File []
  (-> (Files/createTempDirectory "sqlite-migrations-" (into-array FileAttribute []))
      .toFile))

(defn- write-sql! [^File dir filename content]
  (spit (File. dir ^String filename) content))

(defn- delete-tree! [^File f]
  (when (.isDirectory f)
    (doseq [c (.listFiles f)] (delete-tree! c)))
  (.delete f))

(defn- with-sqlite [f]
  (let [db-file (temp-db-file)
        dir     (temp-dir)
        ds      (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath db-file)})]
    (try
      (f ds dir)
      (finally
        (.delete db-file)
        (delete-tree! dir)))))

(deftest create-and-query
  (let [file (temp-db-file)
        db   (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath file)})]
    (try
      (jdbc/execute! db (sql/format {:create-table :t
                                     :with-columns [[:id :integer [:primary-key]]
                                                    [:name :text]]}))
      (jdbc/execute! db (sql/format {:insert-into :t
                                     :values      [{:id 1 :name "alice"}
                                                   {:id 2 :name "bob"}]}))
      (is (= ["alice" "bob"]
             (map :t/name
                  (jdbc/execute! db (sql/format {:select   [:name]
                                                 :from     [:t]
                                                 :order-by [[:id :asc]]})))))
      (finally
        (.delete file)))))

(deftest migrate-applies-in-order
  (with-sqlite
    (fn [ds dir]
      (write-sql! dir "002-add-col.sql" "ALTER TABLE items ADD COLUMN note TEXT")
      (write-sql! dir "001-init.sql"    "CREATE TABLE items (id INTEGER PRIMARY KEY)")
      (is (= ["001-init.sql" "002-add-col.sql"] (sqlite/migrate! ds dir)))
      (is (= [[1 "001-init.sql"] [2 "002-add-col.sql"]]
             (map (juxt :migrations/migration_number :migrations/migration_name)
                  (jdbc/execute! ds
                    ["SELECT migration_number, migration_name
                      FROM migrations ORDER BY migration_number"]))))
      (jdbc/execute! ds ["INSERT INTO items (id, note) VALUES (1, 'ok')"]))))

(deftest migrate-is-idempotent
  (with-sqlite
    (fn [ds dir]
      (write-sql! dir "001-init.sql" "CREATE TABLE items (id INTEGER PRIMARY KEY)")
      (write-sql! dir "002-add.sql"  "ALTER TABLE items ADD COLUMN note TEXT")
      (sqlite/migrate! ds dir)
      (is (= [] (sqlite/migrate! ds dir)))
      (is (= 2 (-> (jdbc/execute! ds ["SELECT COUNT(*) AS c FROM migrations"])
                   first :c))))))

(deftest migrate-runs-only-new-files
  (with-sqlite
    (fn [ds dir]
      (write-sql! dir "001-init.sql" "CREATE TABLE items (id INTEGER PRIMARY KEY)")
      (is (= ["001-init.sql"] (sqlite/migrate! ds dir)))
      (write-sql! dir "002-add.sql" "ALTER TABLE items ADD COLUMN note TEXT")
      (is (= ["002-add.sql"] (sqlite/migrate! ds dir))))))

(deftest migrate-handles-multi-statement-file
  (with-sqlite
    (fn [ds dir]
      (write-sql! dir "001-multi.sql"
        "CREATE TABLE a (id INTEGER);
         CREATE TABLE b (id INTEGER);
         INSERT INTO a (id) VALUES (1);
         INSERT INTO a (id) VALUES (2);")
      (sqlite/migrate! ds dir)
      (is (= 2 (-> (jdbc/execute! ds ["SELECT COUNT(*) AS c FROM a"]) first :c)))
      (is (= 0 (-> (jdbc/execute! ds ["SELECT COUNT(*) AS c FROM b"]) first :c))))))

(deftest migrate-rolls-back-on-failure
  (with-sqlite
    (fn [ds dir]
      (write-sql! dir "001-bad.sql"
        "CREATE TABLE items (id INTEGER);
         INSERT INTO nonexistent (x) VALUES (1);")
      (is (thrown? Exception (sqlite/migrate! ds dir)))
      (is (empty? (jdbc/execute! ds
                    ["SELECT name FROM sqlite_master
                      WHERE type='table' AND name='items'"])))
      (is (= 0 (-> (jdbc/execute! ds ["SELECT COUNT(*) AS c FROM migrations"])
                   first :c))))))

(deftest migrate-rejects-duplicate-numbers
  (with-sqlite
    (fn [ds dir]
      (write-sql! dir "001-a.sql" "SELECT 1")
      (write-sql! dir "001-b.sql" "SELECT 2")
      (let [ex (try (sqlite/migrate! ds dir) nil
                    (catch clojure.lang.ExceptionInfo e e))]
        (is (some? ex))
        (is (= 1 (:number (ex-data ex))))
        (is (= #{"001-a.sql" "001-b.sql"} (set (:files (ex-data ex)))))))))

(deftest migrate-ignores-non-matching-files
  (with-sqlite
    (fn [ds dir]
      (write-sql! dir "001-init.sql" "CREATE TABLE items (id INTEGER)")
      (write-sql! dir "README.md"    "docs")
      (write-sql! dir "notes.sql"    "garbage")
      (write-sql! dir "0001-nope.sql" "garbage")
      (is (= ["001-init.sql"] (sqlite/migrate! ds dir))))))

(deftest migrate-empty-dir-returns-empty
  (with-sqlite
    (fn [ds dir]
      (is (= [] (sqlite/migrate! ds dir)))
      (testing "migrations table is still created"
        (is (seq (jdbc/execute! ds
                   ["SELECT name FROM sqlite_master
                     WHERE type='table' AND name='migrations'"])))))))
