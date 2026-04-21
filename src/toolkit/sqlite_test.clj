(ns toolkit.sqlite-test
  (:require [clojure.test :refer [deftest is]]
            [honey.sql :as sql]
            [next.jdbc :as jdbc])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-db-file ^File []
  (-> (Files/createTempFile "sqlite-test-" ".sqlite" (into-array FileAttribute []))
      .toFile))

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
