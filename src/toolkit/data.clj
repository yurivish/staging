(ns toolkit.data
  "Load named files out of a flat directory-of-subdirectories layout:

     base/
       shard-a/data.parquet
       shard-a/meta.json
       shard-b/data.parquet
       shard-b/meta.json

   `list-files` walks the layout; `load-parquet` and `load-json` fan it out in
   parallel and return [subdir-name payload] pairs."
  (:require
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [tech.v3.libs.parquet :as parquet])
  (:import
   [java.io File]))

(defn list-files
  "Paths to `file-name` under each immediate subdirectory of `base-path`,
   skipping subdirectories where the file is absent."
  [base-path file-name]
  (->> (.listFiles (io/file base-path))
       (filter #(.isDirectory ^File %))
       (map    #(io/file % file-name))
       (filter #(.exists ^File %))
       (map    #(.getPath ^File %))))

(defn- parent-dir ^String [path]
  (.getName (.getParentFile (io/file path))))

(defn load-parquet
  "For every subdir of `base-path` containing `file-name`, load it as a parquet
   dataset. Returns a seq of [subdir-name dataset] pairs, read in parallel.
   `opts` is forwarded to `tech.v3.libs.parquet/parquet->ds` — e.g.
   `{:column-allowlist [\"a\" \"b\"]}`."
  ([base-path file-name] (load-parquet base-path file-name nil))
  ([base-path file-name opts]
   (pmap (fn [path]
           (try
             [(parent-dir path) (parquet/parquet->ds path opts)]
             (catch Exception e
               (throw (ex-info (str "failed to load parquet: " path)
                               {:path path} e)))))
         (list-files base-path file-name))))

(defn load-parquet-columns [base-path file-name columns]
  (load-parquet base-path file-name {:column-allowlist columns}))

(defn load-json
  "For every subdir of `base-path` containing `file-name`, parse it as JSON.
   Returns a seq of [subdir-name value] pairs, read in parallel. `opts` is a
   map of options forwarded as kwargs to `clojure.data.json/read` — e.g.
   `{:key-fn keyword}`."
  ([base-path file-name] (load-json base-path file-name nil))
  ([base-path file-name opts]
   (let [kwargs (mapcat identity opts)]
     (pmap (fn [path]
             [(parent-dir path)
              (with-open [r (io/reader path)]
                (apply json/read r kwargs))])
           (list-files base-path file-name)))))
