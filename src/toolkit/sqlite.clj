;; SQLite component + numbered-file migrations.
;;
;; `migrate!` applies forward-only schema migrations from a directory of
;; `NNN-description.sql` files, where NNN is a three-digit version. Files
;; are sorted numerically and run in order, each inside its own
;; transaction. A `migrations` table (auto-created on first run) records
;; which numbers have been applied, so `migrate!` is idempotent and safe
;; to call on every startup — it only runs what's new since last time.
;;
;; The pattern assumes: migrations are checked into the repo alongside
;; the code that depends on them; numbers are allocated monotonically
;; (conflicts between branches are resolved at merge time, not runtime);
;; and rollbacks are handled by writing a new forward migration, not by
;; running a reverse one. This matches the model used by tools like
;; Rails, Flyway, and golang-migrate for the "plain SQL in version
;; control" case.
(ns toolkit.sqlite
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]))

(defrecord Sqlite [path datasource]
  component/Lifecycle
  (start [this]
    (assoc this :datasource (jdbc/get-datasource {:dbtype "sqlite" :dbname path})))
  (stop [this]
    (assoc this :datasource nil)))

(def ^:private migration-pattern #"^(\d{3})-.*\.sql$")

(defn- list-migrations [dir]
  (let [seen (volatile! {})]
    (->> (.listFiles (io/file dir))
         (keep (fn [^java.io.File f]
                 (when-let [[_ n] (re-matches migration-pattern (.getName f))]
                   (let [num  (Long/parseLong n)
                         nm   (.getName f)]
                     (when-let [prev (@seen num)]
                       (throw (ex-info "duplicate migration number"
                                       {:number num :files [prev nm]})))
                     (vswap! seen assoc num nm)
                     {:number num :name nm :file f}))))
         (sort-by :number)
         vec)))

(defn- split-statements [sql]
  (->> (str/split sql #";\s*(?:\r?\n|$)")
       (map str/trim)
       (remove str/blank?)))

(defn- ensure-migrations-table! [ds]
  (jdbc/execute! ds
    ["CREATE TABLE IF NOT EXISTS migrations (
        migration_number INTEGER PRIMARY KEY,
        migration_name   TEXT NOT NULL,
        applied_at       INTEGER NOT NULL DEFAULT (unixepoch()))"]))

(defn- applied-numbers [ds]
  (into #{}
        (map :migrations/migration_number)
        (jdbc/execute! ds ["SELECT migration_number FROM migrations"])))

(defn migrate!
  "Applies pending NNN-*.sql migrations from `dir` to `ds`. Each runs in
   its own transaction. Returns the names of migrations applied."
  [ds dir]
  (ensure-migrations-table! ds)
  (let [done    (applied-numbers ds)
        pending (remove #(done (:number %)) (list-migrations dir))]
    (mapv
      (fn [{:keys [number name ^java.io.File file]}]
        (jdbc/with-transaction [tx ds]
          (doseq [stmt (split-statements (slurp file))]
            (jdbc/execute! tx [stmt]))
          (jdbc/execute! tx
            ["INSERT INTO migrations (migration_number, migration_name) VALUES (?, ?)"
             number name]))
        name)
      pending)))
