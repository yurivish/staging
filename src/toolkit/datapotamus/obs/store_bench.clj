(ns toolkit.datapotamus.obs.store-bench
  "Phase-instrumented variant of `obs.store/flush-run!` plus a synthetic
   trace generator. Run as a script:

     clj -M -e \"(require 'toolkit.datapotamus.obs.store-bench)
                  (toolkit.datapotamus.obs.store-bench/run!)\""
  (:require [clojure.data.json :as json]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.obs.store :as store]
            [toolkit.llm.cache :as cache])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.sql Connection Timestamp)
           (java.time Instant)))

;; ---- Reach into store/ for its private helpers via @#'-style refs ----------
;; Avoid copy-pasting; we want to benchmark the actual code paths.

(defn- ms [^long start-ns]
  (long (/ (- (System/nanoTime) start-ns) 1e6)))

(defn- bytes->hex [^bytes b]
  (let [sb (StringBuilder. (* 2 (alength b)))]
    (dotimes [i (alength b)]
      (let [byt (bit-and (aget b i) 0xff)]
        (.append sb (format "%02x" byt))))
    (.toString sb)))

(defn- payload-of [ev]
  (when (contains? ev :data)
    (let [content (pr-str (:data ev))
          digest  (cache/sha256-bytes content)
          bytes   (.getBytes content "UTF-8")]
      {:id (bytes->hex digest) :size (alength bytes) :content content})))

;; ---- Phase-timed flush -----------------------------------------------------

(defn- str-array ^java.sql.Array [^Connection conn coll]
  (.createArrayOf conn "VARCHAR" (into-array String (mapv str coll))))

(defn- uuid-array ^java.sql.Array [^Connection conn coll]
  (.createArrayOf conn "UUID" (into-array java.util.UUID coll)))

(defn- sid-str [sid]
  (if (keyword? sid) (name sid) (str sid)))

(defn- ts-from-instant [v]
  (cond
    (nil? v) nil
    (instance? Timestamp v) v
    (instance? Instant v)   (Timestamp/from ^Instant v)
    :else (throw (ex-info "Unsupported timestamp" {:value v}))))

(defn- ->json [v] (when (some? v) (json/write-str v)))

(defn- event-row [^Connection conn seq-n ev span-id payload-id]
  [seq-n
   (:at ev)
   (name (:kind ev))
   (:msg-id ev)
   (:data-id ev)
   (some-> (:msg-kind ev) name)
   (some-> (:step-id ev) sid-str)
   (when (:scope-path ev) (str-array conn (:scope-path ev)))
   (some-> (or (:port ev) (:in-port ev)) sid-str)
   (when (seq (:parent-msg-ids ev))
     (uuid-array conn (:parent-msg-ids ev)))
   span-id
   payload-id
   (->json (:error ev))
   (when (= :status (:kind ev)) (->json (:data ev)))])

(defn- pair-spans [events]
  (let [closer? #(#{:success :failure} (:kind %))
        recv?   #(= :recv (:kind %))
        by-msg  (group-by :msg-id (filter #(or (recv? %) (closer? %)) events))
        pairs   (keep (fn [[mid evs]]
                        (when-let [r (some #(when (recv? %) %) evs)]
                          [mid r (some #(when (closer? %) %) evs)]))
                      by-msg)]
    (loop [out [] ids {} span-id 1 pairs pairs]
      (if-let [[mid r c] (first pairs)]
        (let [ip (payload-of r)
              s  {:span-id span-id :msg-id mid :data-id (:data-id r)
                  :step-id (:step-id r) :scope-path (:scope-path r)
                  :in-port (:in-port r) :in-payload-id (:id ip)
                  :started-at-ns (:at r) :ended-at-ns (:at c)
                  :duration-ns (when c (- (:at c) (:at r)))
                  :status (case (:kind c) :success "ok" :failure "error" "open")
                  :error-message (get-in c [:error :message])
                  :error-data    (get-in c [:error :data])}]
          (recur (conj out s) (assoc ids mid span-id) (inc span-id) (rest pairs)))
        [out ids]))))

(defn- span-row [^Connection conn s]
  [(:span-id s) (:msg-id s) (:data-id s)
   (some-> (:step-id s) sid-str)
   (when (:scope-path s) (str-array conn (:scope-path s)))
   (some-> (:in-port s) sid-str) (:in-payload-id s)
   (:started-at-ns s) (:ended-at-ns s) (:duration-ns s)
   (:status s) (:error-message s) (->json (:error-data s))])

(defn- coerce-ds [ds-or-path]
  (if (string? ds-or-path)
    (jdbc/get-datasource {:jdbcUrl (str "jdbc:duckdb:" ds-or-path)})
    ds-or-path))

(defn flush-run-timed!
  "Same as obs.store/flush-run! but prints phase timings. Returns a
   timing map."
  [ds-or-path trace]
  (let [t0       (System/nanoTime)
        ds       (store/init! ds-or-path)
        t-init   (ms t0)
        events   (:events trace)
        n-events (count events)

        t1       (System/nanoTime)
        [spans span-id-by-msg] (pair-spans events)
        t-pair   (ms t1)

        t2       (System/nanoTime)
        payloads (into {} (keep (fn [ev]
                                  (when-let [p (payload-of ev)]
                                    [(:id p) p]))
                                events))
        n-payloads (count payloads)
        t-payloads (ms t2)]
    (with-open [conn (.getConnection ds)]
      (.setAutoCommit conn false)
      (try
        (let [t3 (System/nanoTime)
              ;; insert-run is private; replicate inline
              _  (jdbc/execute!
                  conn
                  ["INSERT INTO runs
                       (run_id, workflow_id, started_at_wall, started_at_ns,
                        finished_at_wall, duration_ns, status, topology, attrs)
                       VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON))"
                   (:run-id (:run trace))
                   (:workflow-id (:run trace))
                   (ts-from-instant (:started-at-wall (:run trace)))
                   (:started-at-ns (:run trace))
                   (ts-from-instant (:finished-at-wall (:run trace)))
                   (:duration-ns (:run trace))
                   (or (:status (:run trace)) "open")
                   (->json (:topology (:run trace)))
                   (->json (:attrs (:run trace)))])
              t-run (ms t3)

              t4 (System/nanoTime)
              _  (when (seq payloads)
                   (jdbc/execute-batch!
                    conn
                    "INSERT INTO payloads (id, size_bytes, content_edn) VALUES (?, ?, ?)
                       ON CONFLICT (id) DO NOTHING"
                    (mapv (juxt :id :size :content) (vals payloads))
                    {}))
              t-payload-insert (ms t4)

              t5 (System/nanoTime)
              event-rows (vec
                          (map-indexed
                           (fn [i ev]
                             (let [span-id    (when (#{:recv :success :failure} (:kind ev))
                                                (span-id-by-msg (:msg-id ev)))
                                   payload    (payload-of ev)
                                   payload-id (:id payload)]
                               (event-row conn i ev span-id payload-id)))
                           events))
              t-event-build (ms t5)

              t6 (System/nanoTime)
              _  (jdbc/execute-batch!
                  conn
                  "INSERT INTO events
                     (seq, at_ns, kind, msg_id, data_id, msg_kind, step_id,
                      scope_path, port, parent_msg_ids, span_id, payload_id,
                      error, status_data)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             CAST(? AS JSON), CAST(? AS JSON))"
                  event-rows
                  {})
              t-event-insert (ms t6)

              t7 (System/nanoTime)
              span-rows (mapv #(span-row conn %) spans)
              t-span-build (ms t7)

              t8 (System/nanoTime)
              _  (when (seq span-rows)
                   (jdbc/execute-batch!
                    conn
                    "INSERT INTO spans
                       (span_id, msg_id, data_id, step_id, scope_path,
                        in_port, in_payload_id, started_at_ns, ended_at_ns,
                        duration_ns, status, error_message, error_data)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON))"
                    span-rows
                    {}))
              t-span-insert (ms t8)

              t9 (System/nanoTime)
              _  (.commit conn)
              t-commit (ms t9)
              total (ms t0)]
          (let [timing {:n-events n-events :n-payloads n-payloads :n-spans (count spans)
                        :ms {:init t-init
                             :pair-spans t-pair
                             :payload-dedup t-payloads
                             :insert-run t-run
                             :insert-payloads t-payload-insert
                             :build-events t-event-build
                             :insert-events t-event-insert
                             :build-spans t-span-build
                             :insert-spans t-span-insert
                             :commit t-commit
                             :total total}}]
            (println "=== flush phase timings ===")
            (println (format "events=%d payloads=%d spans=%d"
                             n-events n-payloads (count spans)))
            (doseq [[k v] (sort-by (comp - val) (:ms timing))]
              (println (format "  %-20s %6d ms" (name k) v)))
            timing))
        (catch Throwable t
          (.rollback conn) (throw t))))))

;; ---- Synthetic trace generator ---------------------------------------------

(defn- mk-event
  ([kind step-id i parent-id]
   (mk-event kind step-id i parent-id nil))
  ([kind step-id i parent-id data]
   (cond-> {:kind     kind
            :at       (* i 1000)        ; 1µs apart
            :msg-id   (random-uuid)
            :step-id  step-id
            :scope-path ["bench" "stage"]
            :msg-kind :data
            :port     :out}
     parent-id (assoc :parent-msg-ids [parent-id])
     data      (assoc :data data))))

(defn- gen-events
  "Produce a synthetic trace shaped like a real datapotamus run:
   roughly 65% signal/lifecycle events with no payload, 30% data events
   with small payloads (~150 bytes), 5% with larger payloads (~2KB).
   Parent chains are realistic — every msg has 0 or 1 parent in the
   immediately preceding 50 events."
  [n]
  (let [rng (java.util.Random. 42)
        prev-ids (java.util.ArrayDeque.)
        steps (mapv keyword
                    ["worker" "shim" "coord" "ext" "aggregate"
                     "flatten" "scan" "sink" "filter" "split"])]
    (loop [i 0 out (transient [])]
      (if (= i n)
        (persistent! out)
        (let [parent-id (when (and (pos? (.size prev-ids))
                                   (< (.nextDouble rng) 0.85))
                          (let [^java.util.Iterator it (.iterator prev-ids)
                                target-idx (.nextInt rng (min 50 (.size prev-ids)))]
                            (loop [k 0]
                              (let [v (.next it)]
                                (if (= k target-idx) v (recur (inc k)))))))
              roll (.nextDouble rng)
              ev   (cond
                     (< roll 0.65)
                     (mk-event :send-out (rand-nth steps) i parent-id)
                     (< roll 0.95)
                     (mk-event :recv (rand-nth steps) i parent-id
                               {:story-id (.nextInt rng 100000)
                                :comment-text (apply str (repeat 10 "lorem ipsum "))
                                :n (.nextInt rng 1000)})
                     :else
                     (mk-event :status (rand-nth steps) i parent-id
                               {:event :flatten
                                :payload (apply str (repeat 80 "x payload bigger "))
                                :more (vec (repeatedly 50 #(.nextInt rng 1000)))}))]
          (.addFirst prev-ids (:msg-id ev))
          (when (> (.size prev-ids) 50) (.removeLast prev-ids))
          (recur (inc i) (conj! out ev)))))))

(defn- mk-trace [n]
  {:run    {:run-id           (random-uuid)
            :workflow-id      "bench"
            :started-at-wall  (Instant/now)
            :started-at-ns    0
            :finished-at-wall (Instant/now)
            :duration-ns      1000000000
            :status           "ok"
            :topology         {:nodes [] :edges []}
            :attrs            {}}
   :events (gen-events n)})

(defn- tmp-db []
  (let [d (Files/createTempDirectory "store-bench"
                                     (into-array FileAttribute []))]
    (str (.resolve d "trace.duckdb"))))

(defn- sample-event-summary [events]
  (let [by-kind (frequencies (map :kind events))
        with-data (count (filter #(contains? % :data) events))
        with-parent (count (filter #(seq (:parent-msg-ids %)) events))]
    {:by-kind by-kind
     :with-data with-data
     :with-parent with-parent}))

(defn run!
  "Generate ~125k synthetic events shaped like a real hn-sota trace,
   flush them via a phase-instrumented variant of flush-run!, and
   print a timing breakdown."
  ([] (run! 125000))
  ([n]
   (println "=== generating" n "synthetic events ===")
   (let [t0    (System/nanoTime)
         trace (mk-trace n)
         t-gen (ms t0)]
     (println "generated in" t-gen "ms")
     (println "summary:" (sample-event-summary (:events trace)))
     (println)
     (let [db (tmp-db)
           timing (flush-run-timed! db trace)
           f-bytes (.length (java.io.File. ^String db))]
       (println)
       (println (format "duckdb file size: %.1f MB"
                        (/ f-bytes 1024.0 1024.0)))
       (println "db at" db)
       timing))))

;; ---- Appender-based payload insert comparison ------------------------------

(defn- bench-payload-insert!
  "Insert N payload rows via the given strategy. Returns elapsed ms."
  [db payloads strategy]
  (let [ds   (store/init! db)
        conn (.getConnection ds)
        sql  "INSERT INTO payloads (id, size_bytes, content_edn) VALUES (?, ?, ?)
                ON CONFLICT (id) DO NOTHING"]
    (try
      (.setAutoCommit conn false)
      (let [t0 (System/nanoTime)]
        (case strategy
          :batch
          (jdbc/execute-batch! conn sql
                               (mapv (juxt :id :size :content) (vals payloads))
                               {})
          :appender
          (with-open [^org.duckdb.DuckDBAppender app
                      (.createAppender ^org.duckdb.DuckDBConnection
                                       (.unwrap conn org.duckdb.DuckDBConnection)
                                       org.duckdb.DuckDBConnection/DEFAULT_SCHEMA
                                       "payloads")]
            (doseq [{:keys [id size content]} (vals payloads)]
              (.beginRow app)
              (.append app ^String id)
              (.append app (long size))
              (.append app ^String content)
              (.endRow app))))
        (.commit conn)
        (ms t0))
      (finally (.close conn)))))

(defn bench-payloads!
  "Compare JDBC batch insert vs DuckDBAppender for the payloads table.
   Each strategy uses a fresh DB. Note: the appender path requires the
   table to NOT have a unique constraint that ON CONFLICT relies on —
   we drop the PK on the appender DB before insertion."
  ([] (bench-payloads! 50000))
  ([n-payloads]
   (let [trace (mk-trace n-payloads)  ; mk-trace seeds with mostly data events
         payloads (into {} (keep (fn [ev]
                                   (when-let [p (payload-of ev)]
                                     [(:id p) p]))
                                 (:events trace)))
         n (count payloads)]
     (println "=== payload insert: batch vs appender ===")
     (println "unique payloads:" n)
     (let [db1 (tmp-db)
           ms1 (bench-payload-insert! db1 payloads :batch)]
       (println (format "  jdbc execute-batch! %6d ms (%d rows/s)"
                        ms1 (long (/ (* 1000.0 n) (max 1 ms1))))))
     (let [db2 (tmp-db)
           ;; Drop PK so appender doesn't trip on unique constraint
           ds (store/init! db2)
           _  (jdbc/execute! ds ["DROP TABLE payloads"])
           _  (jdbc/execute! ds ["CREATE TABLE payloads (
                                    id VARCHAR, size_bytes BIGINT, content_edn VARCHAR)"])
           ms2 (bench-payload-insert! db2 payloads :appender)]
       (println (format "  duckdb appender    %6d ms (%d rows/s)"
                        ms2 (long (/ (* 1000.0 n) (max 1 ms2)))))))))

;; ---- Appender path for the events table ------------------------------------

(defn- ^java.util.UUID coerce-uuid [v]
  (cond
    (nil? v) nil
    (instance? java.util.UUID v) v
    :else (java.util.UUID/fromString (str v))))

(defn- append-event! [^org.duckdb.DuckDBAppender app
                      seq-n ev span-id payload-id]
  (.beginRow app)
  (.append app (long seq-n))
  (if-let [v (:at ev)]    (.append app (long v)) (.appendNull app))
  (.append app ^String (name (:kind ev)))
  (if-let [u (:msg-id ev)]  (.append app ^java.util.UUID (coerce-uuid u)) (.appendNull app))
  (if-let [u (:data-id ev)] (.append app ^java.util.UUID (coerce-uuid u)) (.appendNull app))
  (if-let [k (:msg-kind ev)] (.append app ^String (name k)) (.appendNull app))
  (if-let [s (:step-id ev)] (.append app ^String (sid-str s)) (.appendNull app))
  (if-let [sp (:scope-path ev)]
    (.append app ^java.util.Collection (mapv str sp))
    (.appendNull app))
  (if-let [p (or (:port ev) (:in-port ev))]
    (.append app ^String (sid-str p))
    (.appendNull app))
  (if-let [parents (seq (:parent-msg-ids ev))]
    (.append app ^java.util.Collection (mapv coerce-uuid parents))
    (.appendNull app))
  (if span-id (.append app (long span-id)) (.appendNull app))
  (if payload-id (.append app ^String payload-id) (.appendNull app))
  (if-let [e (:error ev)] (.append app ^String (->json e)) (.appendNull app))
  (if (and (= :status (:kind ev)) (contains? ev :data))
    (.append app ^String (->json (:data ev)))
    (.appendNull app))
  (.endRow app))

(defn flush-run-appender!
  "Same as flush-run-timed! but uses DuckDBAppender for the bulk tables.
   Drops PK constraints (appender requires that for now) and runs each
   table in its own appender; payload dedup happens client-side."
  [ds-or-path trace]
  (let [t0 (System/nanoTime)
        ds (store/init! ds-or-path)
        ;; appender doesn't honor ON CONFLICT — drop PKs so duplicates
        ;; (which we already dedup client-side) just become no-ops.
        _  (doseq [stmt ["DROP TABLE IF EXISTS payloads"
                         "DROP TABLE IF EXISTS events"
                         "DROP TABLE IF EXISTS spans"
                         "CREATE TABLE payloads (id VARCHAR, size_bytes BIGINT, content_edn VARCHAR)"
                         "CREATE TABLE events (
                            seq BIGINT, at_ns BIGINT, kind VARCHAR, msg_id UUID,
                            data_id UUID, msg_kind VARCHAR, step_id VARCHAR,
                            scope_path VARCHAR[], port VARCHAR, parent_msg_ids UUID[],
                            span_id BIGINT, payload_id VARCHAR, error JSON, status_data JSON)"
                         "CREATE TABLE spans (
                            span_id BIGINT, msg_id UUID, data_id UUID, step_id VARCHAR,
                            scope_path VARCHAR[], in_port VARCHAR, in_payload_id VARCHAR,
                            started_at_ns BIGINT, ended_at_ns BIGINT, duration_ns BIGINT,
                            status VARCHAR, error_message VARCHAR, error_data JSON)"]]
             (jdbc/execute! ds [stmt]))
        t-init (ms t0)

        events (:events trace)
        n-events (count events)

        t1 (System/nanoTime)
        [spans span-id-by-msg] (pair-spans events)
        t-pair (ms t1)

        t2 (System/nanoTime)
        payloads (into {} (keep (fn [ev]
                                  (when-let [p (payload-of ev)]
                                    [(:id p) p]))
                                events))
        n-payloads (count payloads)
        t-payloads (ms t2)]
    (with-open [conn (.getConnection ds)
                ^org.duckdb.DuckDBConnection ddb
                (.unwrap conn org.duckdb.DuckDBConnection)]
      (.setAutoCommit conn false)
      (let [t3 (System/nanoTime)
            ;; insert run via plain SQL — only one row
            _  (jdbc/execute!
                conn
                ["INSERT INTO runs
                    (run_id, workflow_id, started_at_wall, started_at_ns,
                     finished_at_wall, duration_ns, status, topology, attrs)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON))"
                 (:run-id (:run trace))
                 (:workflow-id (:run trace))
                 (ts-from-instant (:started-at-wall (:run trace)))
                 (:started-at-ns (:run trace))
                 (ts-from-instant (:finished-at-wall (:run trace)))
                 (:duration-ns (:run trace))
                 (or (:status (:run trace)) "open")
                 (->json (:topology (:run trace)))
                 (->json (:attrs (:run trace)))])
            t-run (ms t3)

            t4 (System/nanoTime)
            _  (with-open [^org.duckdb.DuckDBAppender app
                           (.createAppender ddb
                                            org.duckdb.DuckDBConnection/DEFAULT_SCHEMA
                                            "payloads")]
                 (doseq [{:keys [id size content]} (vals payloads)]
                   (.beginRow app)
                   (.append app ^String id)
                   (.append app (long size))
                   (.append app ^String content)
                   (.endRow app)))
            t-payload-insert (ms t4)

            t5 (System/nanoTime)
            _  (with-open [^org.duckdb.DuckDBAppender app
                           (.createAppender ddb
                                            org.duckdb.DuckDBConnection/DEFAULT_SCHEMA
                                            "events")]
                 (loop [i 0 evs events]
                   (when-let [ev (first evs)]
                     (let [span-id    (when (#{:recv :success :failure} (:kind ev))
                                        (span-id-by-msg (:msg-id ev)))
                           payload    (payload-of ev)
                           payload-id (:id payload)]
                       (append-event! app i ev span-id payload-id))
                     (recur (inc i) (next evs)))))
            t-events-insert (ms t5)

            t6 (System/nanoTime)
            _  (with-open [^org.duckdb.DuckDBAppender app
                           (.createAppender ddb
                                            org.duckdb.DuckDBConnection/DEFAULT_SCHEMA
                                            "spans")]
                 (doseq [s spans]
                   (.beginRow app)
                   (.append app (long (:span-id s)))
                   (if-let [u (:msg-id s)]  (.append app ^java.util.UUID (coerce-uuid u)) (.appendNull app))
                   (if-let [u (:data-id s)] (.append app ^java.util.UUID (coerce-uuid u)) (.appendNull app))
                   (if-let [v (:step-id s)] (.append app ^String (sid-str v)) (.appendNull app))
                   (if-let [sp (:scope-path s)]
                     (.append app ^java.util.Collection (mapv str sp))
                     (.appendNull app))
                   (if-let [v (:in-port s)] (.append app ^String (sid-str v)) (.appendNull app))
                   (if-let [v (:in-payload-id s)] (.append app ^String v) (.appendNull app))
                   (if-let [v (:started-at-ns s)] (.append app (long v)) (.appendNull app))
                   (if-let [v (:ended-at-ns s)]   (.append app (long v)) (.appendNull app))
                   (if-let [v (:duration-ns s)]   (.append app (long v)) (.appendNull app))
                   (.append app ^String (or (:status s) "open"))
                   (if-let [v (:error-message s)] (.append app ^String v) (.appendNull app))
                   (if-let [v (:error-data s)]    (.append app ^String (->json v)) (.appendNull app))
                   (.endRow app)))
            t-spans-insert (ms t6)

            t7 (System/nanoTime)
            _  (.commit conn)
            t-commit (ms t7)
            total (ms t0)
            timing {:n-events n-events :n-payloads n-payloads :n-spans (count spans)
                    :ms {:init t-init :pair-spans t-pair :payload-dedup t-payloads
                         :insert-run t-run :insert-payloads t-payload-insert
                         :insert-events t-events-insert :insert-spans t-spans-insert
                         :commit t-commit :total total}}]
        (println "=== flush phase timings (APPENDER) ===")
        (println (format "events=%d payloads=%d spans=%d"
                         n-events n-payloads (count spans)))
        (doseq [[k v] (sort-by (comp - val) (:ms timing))]
          (println (format "  %-20s %6d ms" (name k) v)))
        timing))))

(defn compare!
  "Run the same N synthetic events through both flush paths."
  ([] (compare! 125000))
  ([n]
   (println "=== generating" n "synthetic events ===")
   (let [trace (mk-trace n)]
     (println "summary:" (sample-event-summary (:events trace)))
     (println)
     (println "--- baseline (jdbc execute-batch!) ---")
     (let [db1 (tmp-db)
           t1  (flush-run-timed! db1 trace)]
       (println)
       (println "--- appender ---")
       (let [db2 (tmp-db)
             t2  (flush-run-appender! db2 trace)]
         (println)
         (println (format "speedup: %.1fx (%dms → %dms)"
                          (/ (double (get-in t1 [:ms :total]))
                             (double (get-in t2 [:ms :total])))
                          (get-in t1 [:ms :total])
                          (get-in t2 [:ms :total]))))))))
