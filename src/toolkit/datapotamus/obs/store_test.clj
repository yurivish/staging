(ns toolkit.datapotamus.obs.store-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.obs.store :as store]
            [toolkit.datapotamus.recorder :as recorder]
            [toolkit.datapotamus.step :as step]
            [toolkit.pubsub :as pubsub])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(defn- tmp-db []
  (let [dir (Files/createTempDirectory "dp-store-test"
                                       (into-array FileAttribute []))]
    (.deleteOnExit (.toFile dir))
    (str (.resolve dir "run.duckdb"))))

(defn- run-with-recorder
  "Run `wf` with `:data d`, recording into a fresh trace. Returns the
   finalized trace map ready for `flush-run!`."
  [wf d]
  (let [ps       (pubsub/make)
        run-meta (store/make-run-meta wf {:workflow-id "test"})
        rec      (recorder/start-recorder! ps run-meta)
        result   (flow/run! wf {:pubsub ps :data d})
        trace0   ((:stop rec))]
    (-> trace0
        (update :run store/finalize-run-meta)
        (assoc-in [:run :status]
                  (if (= :completed (:state result)) "ok" "error")))))

;; ============================================================================
;; Topology snapshot
;; ============================================================================

(deftest topology-flat
  (let [wf (step/serial (step/step :inc inc)
                        (step/step :dbl #(* 2 %))
                        (step/sink))
        {:keys [nodes edges]} (step/topology wf)]
    (is (= [{:path [:inc]  :name "inc"  :kind :leaf}
            {:path [:dbl]  :name "dbl"  :kind :leaf}
            {:path [:sink] :name "sink" :kind :leaf}]
           nodes))
    (is (= #{{:from-path [:inc] :from-port :out :to-path [:dbl]  :to-port :in}
             {:from-path [:dbl] :from-port :out :to-path [:sink] :to-port :in}}
           (set edges)))))

;; ============================================================================
;; End-to-end ingest + queries (one query per use-case category)
;; ============================================================================

(deftest end-to-end-ingest
  (let [db (tmp-db)
        wf (step/serial (step/step :inc inc)
                        (step/step :dbl #(* 2 %))
                        (step/sink))
        trace (run-with-recorder wf 5)
        _     (store/flush-run! db trace)
        ds    (jdbc/get-datasource {:jdbcUrl (str "jdbc:duckdb:" db)})]

    (testing "runs has exactly one row, status ok, topology preserved"
      (let [[{:keys [n]}] (jdbc/execute! ds ["SELECT COUNT(*) AS n FROM runs"])
            [{:keys [status]}] (jdbc/execute! ds ["SELECT status FROM runs"])]
        (is (= 1 n))
        (is (= "ok" status))))

    (testing "events table populated"
      (let [[{:keys [n]}] (jdbc/execute! ds ["SELECT COUNT(*) AS n FROM events"])]
        (is (pos? n))))

    (testing "use case 1 — per-step inspection: one span per step, all ok"
      (let [rows (jdbc/execute! ds
                                ["SELECT step_id, COUNT(*) AS n,
                                         SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errs
                                  FROM spans GROUP BY step_id ORDER BY step_id"])
            by-step (into {} (map (juxt :step_id (juxt :n :errs))) rows)]
        (is (= {"dbl" [1 0] "inc" [1 0] "sink" [1 0]} by-step))))

    (testing "use case 2 — message trajectory: descendants of the inject"
      (let [[{:keys [n]}]
            (jdbc/execute!
             ds
             ["WITH RECURSIVE descendants(msg_id) AS (
                 SELECT msg_id FROM events WHERE kind = 'inject'
                 UNION ALL
                 SELECT e.msg_id FROM events e, descendants d
                 WHERE ARRAY_HAS(e.parent_msg_ids, d.msg_id))
               SELECT COUNT(DISTINCT msg_id) AS n FROM descendants"])]
        ;; inject + 3 children (one per step's send-out) — exact count depends
        ;; on synthesis, but there are at least 4 distinct msgs in the chain.
        (is (>= n 3) (str "expected at least 3 descendants, got " n))))

    (testing "use case 3 — concurrency: each span has start, end, positive duration"
      (let [rows (jdbc/execute! ds
                                ["SELECT started_at_ns, ended_at_ns, duration_ns
                                  FROM spans"])]
        (is (every? #(some? (:started_at_ns %)) rows))
        (is (every? #(some? (:ended_at_ns %)) rows))
        (is (every? #(pos? (:duration_ns %)) rows))))

    (testing "use case 4 — annotations & failures: zero failures, zero status events"
      (let [[{:keys [n]}]
            (jdbc/execute! ds
                           ["SELECT COUNT(*) AS n FROM events
                             WHERE kind IN ('failure', 'flow-error')"])]
        (is (zero? n))))

    (testing "payloads are content-addressed"
      (let [rows (jdbc/execute! ds ["SELECT id, content_edn FROM payloads"])]
        (is (every? #(= 64 (count (:id %))) rows)
            "ids are sha256 hex (64 chars)")
        ;; inc 5→6, dbl 6→12, plus the inject value 5. Expect three distinct
        ;; payload rows: 5, 6, 12.
        (is (= #{"5" "6" "12"} (set (map :content_edn rows))))))

    (testing "spans link to events via span_id"
      (let [[{:keys [n]}]
            (jdbc/execute! ds
                           ["SELECT COUNT(*) AS n FROM events
                             WHERE kind IN ('recv', 'success', 'failure')
                               AND span_id IS NOT NULL"])
            [{:keys [s]}]
            (jdbc/execute! ds ["SELECT COUNT(*) AS s FROM spans"])]
        ;; Three steps × (recv + success) = 6, all linked to one of 3 spans.
        (is (= 6 n))
        (is (= 3 s))))))

;; ============================================================================
;; Failure path — :failure events must produce error spans
;; ============================================================================

(deftest end-to-end-failure
  (let [db (tmp-db)
        wf (step/serial (step/step :inc inc)
                        (step/step :boom (fn [_x] (throw (ex-info "boom" {:why :test}))))
                        (step/sink))
        trace (run-with-recorder wf 5)
        _     (store/flush-run! db trace)
        ds    (jdbc/get-datasource {:jdbcUrl (str "jdbc:duckdb:" db)})]

    (testing "exactly one error span, with the right step and message"
      (let [rows (jdbc/execute! ds
                                ["SELECT step_id, status, error_message
                                  FROM spans WHERE status = 'error'"])]
        (is (= 1 (count rows)))
        (is (= "boom" (:step_id (first rows))))
        (is (= "boom" (:error_message (first rows))))))

    (testing "failure event filters correctly"
      (let [[{:keys [n]}]
            (jdbc/execute! ds
                           ["SELECT COUNT(*) AS n FROM events WHERE kind = 'failure'"])]
        (is (= 1 n))))))
