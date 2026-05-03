(ns toolkit.hn-sota.deadlock-probe
  "Run the hn-sota pipeline with a timeout on `await-quiescent!`. If the
   flow doesn't quiesce in time, snapshot the recorder atom in-place
   (non-destructive — the flow keeps running while we read), flush the
   partial trace into a DuckDB file, and run a battery of diagnostic
   SQL queries against it.

   The trace tells us exactly which events did and did not fire — that
   makes deadlocks visible as counter imbalances: msgs with `:recv` but
   no terminating `:success`/`:failure`, msgs sent on a port that never
   surfaces as a downstream `:recv`, etc."
  (:require [next.jdbc :as jdbc]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.obs.store :as store]
            [toolkit.datapotamus.recorder :as recorder]
            [toolkit.datapotamus.step :as step]
            [toolkit.hn-sota.core :as core]
            [toolkit.pubsub :as pubsub])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(defn- tmp-db []
  (let [d (Files/createTempDirectory "hn-deadlock"
                                     (into-array FileAttribute []))]
    (str (.resolve d "deadlock.duckdb"))))

(defn- snapshot-trace
  "Non-destructive snapshot of the recorder's state at this instant.
   The recorder thread keeps writing — we just deref the atom."
  [rec run-meta]
  (let [snap @(:trace rec)]
    (-> snap
        (assoc :run (-> run-meta
                        (assoc :finished-at-wall (java.time.Instant/now))
                        (assoc :duration-ns 0)
                        (assoc :status "deadlock-snapshot"))))))

(def ^:private diagnostic-queries
  "SQL queries that surface common stuck-flow patterns from the events
   table. Each query returns rows in {:label, :sql, :rows} shape."
  [{:label "events by step × kind"
    :sql "SELECT step_id, kind, COUNT(*) AS n
            FROM events
           WHERE step_id IS NOT NULL
           GROUP BY step_id, kind
           ORDER BY step_id, kind"}

   {:label "msgs received but never closed (recv without success/failure)"
    :sql "SELECT recv.step_id, COUNT(*) AS n_pending,
                 MIN(recv.msg_id) AS sample_msg_id
            FROM events recv
           WHERE recv.kind = 'recv'
             AND NOT EXISTS (
               SELECT 1 FROM events done
                WHERE done.msg_id = recv.msg_id
                  AND done.kind IN ('success', 'failure'))
           GROUP BY recv.step_id
           ORDER BY n_pending DESC"}

   {:label "msgs sent that never landed as a downstream recv"
    :sql "SELECT s.step_id AS sender, s.port,
                 COUNT(*) AS n_orphan,
                 MIN(s.msg_id) AS sample_msg_id
            FROM events s
           WHERE s.kind = 'send-out'
             AND NOT EXISTS (
               SELECT 1 FROM events r
                WHERE r.msg_id = s.msg_id
                  AND r.kind = 'recv')
           GROUP BY s.step_id, s.port
           ORDER BY n_orphan DESC"}

   {:label "open spans (have started_at_ns but no ended_at_ns)"
    :sql "SELECT step_id, COUNT(*) AS n_open
            FROM spans
           WHERE ended_at_ns IS NULL
           GROUP BY step_id
           ORDER BY n_open DESC"}

   {:label "per-msg-kind totals (data vs signal vs input-done)"
    :sql "SELECT msg_kind, kind, COUNT(*) AS n
            FROM events
           WHERE msg_kind IS NOT NULL
           GROUP BY msg_kind, kind
           ORDER BY msg_kind, kind"}

   {:label "scope-prefix activity (which subgraph is busy)"
    :sql "SELECT scope_path[1] AS root_scope,
                 COUNT(*) AS n_events,
                 COUNT(DISTINCT msg_id) AS n_msgs
            FROM events
           WHERE scope_path IS NOT NULL
           GROUP BY scope_path[1]
           ORDER BY n_events DESC"}])

(defn- run-diagnostics! [db]
  (let [ds (jdbc/get-datasource {:jdbcUrl (str "jdbc:duckdb:" db)})]
    (doseq [{:keys [label sql]} diagnostic-queries]
      (println)
      (println "===" label "===")
      (let [rows (jdbc/execute! ds [sql])]
        (if (empty? rows)
          (println "  (no rows)")
          (doseq [r rows]
            (println " " (pr-str r))))))))

(defn run-with-deadlock-probe!
  "Run the pipeline with the given opts. Wait up to `await-ms` for
   quiescence; if hit, snapshot the recorder, flush to DuckDB, run
   diagnostic queries, and return the DB path."
  ([] (run-with-deadlock-probe! {}))
  ([{:keys [n-stories tree-workers await-ms]
     :or   {n-stories 10 tree-workers 8 await-ms 60000}
     :as   opts}]
   (let [db        (tmp-db)
         flow      (step/serial :probe (core/build-flow opts) (step/sink))
         ps        (pubsub/make)
         run-meta  (store/make-run-meta flow {:workflow-id "hn-sota-probe"})
         rec       (recorder/start-recorder! ps run-meta)
         handle    (flow/start! flow {:pubsub ps})]
     (println (format "starting flow (n-stories=%d, await-ms=%d) → %s"
                      n-stories await-ms db))
     (flow/inject! handle {:data :tick})
     (flow/inject! handle {})  ; close
     (let [signal (flow/await-quiescent! handle await-ms)]
       (println :await-signal signal)
       (cond
         (= :quiescent signal)
         (println "→ quiesced normally, no deadlock detected")

         (= :timeout signal)
         (do
           (println (format "→ DEADLOCK after %dms; snapshotting recorder…"
                            await-ms))
           (let [snap (snapshot-trace rec run-meta)]
             (println (format "  snapshot has %d events" (count (:events snap))))
             (store/flush-run! db snap)
             (println (format "  flushed to %s" db))
             (run-diagnostics! db)))

         :else
         (println "→ flow failed:" signal))
       ;; Cleanup — but don't block on it; the worker pool may still be parked.
       (future
         (try
           ((:stop rec))
           (flow/stop! handle)
           (catch Throwable t
             (println "cleanup error (ignored):" (ex-message t)))))
       db))))
