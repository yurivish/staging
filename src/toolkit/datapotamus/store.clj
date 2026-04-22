(ns toolkit.datapotamus.store
  (:require [clojure.data.json :as json]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]))

(set! *warn-on-reflection* true)

(def ^:private schema
  ["CREATE TABLE IF NOT EXISTS runs (
      run_id        TEXT PRIMARY KEY,
      pipeline_id   TEXT NOT NULL,
      input_path    TEXT NOT NULL,
      input_slug    TEXT NOT NULL,
      input_mtime   INTEGER,
      input_size    INTEGER,
      state         TEXT NOT NULL,
      cancel_reason TEXT,
      started_at    INTEGER NOT NULL,
      finished_at   INTEGER,
      error         TEXT)"
   "CREATE INDEX IF NOT EXISTS runs_by_slug_started ON runs(input_slug, started_at DESC)"
   "CREATE INDEX IF NOT EXISTS runs_by_state        ON runs(state)"
   "CREATE TABLE IF NOT EXISTS trace_events (
      seq            INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id       TEXT NOT NULL UNIQUE,
      run_id         TEXT NOT NULL REFERENCES runs(run_id),
      at             INTEGER NOT NULL,
      kind           TEXT NOT NULL,
      step_id        TEXT,
      to_step_id     TEXT,
      msg_id         TEXT,
      data_id        TEXT,
      parent_msg_ids TEXT,
      payload_ref    TEXT,
      provenance     TEXT,
      error          TEXT)"
   "CREATE INDEX IF NOT EXISTS trace_by_run_seq ON trace_events(run_id, seq)"
   "CREATE INDEX IF NOT EXISTS trace_by_msg_id  ON trace_events(msg_id)"])

(defn migrate! [ds]
  (doseq [stmt schema] (jdbc/execute! ds [stmt])))

(defn- js [v] (some-> v json/write-str))
(defn- jp [s] (when s (json/read-str s :key-fn keyword)))

(defn insert-run! [ds r]
  (jdbc/execute! ds
                 (sql/format {:insert-into :runs
                              :values [{:run_id      (str (:run-id r))
                                        :pipeline_id (name (:pipeline-id r))
                                        :input_path  (:input-path r)
                                        :input_slug  (:input-slug r)
                                        :input_mtime (:input-mtime r)
                                        :input_size  (:input-size r)
                                        :state       (name (:state r))
                                        :started_at  (:started-at r)}]})))

(defn update-run! [ds run-id patch]
  (let [m (cond-> {}
            (:state patch)         (assoc :state (name (:state patch)))
            (:cancel-reason patch) (assoc :cancel_reason (name (:cancel-reason patch)))
            (:finished-at patch)   (assoc :finished_at (:finished-at patch))
            (:error patch)         (assoc :error (:error patch)))]
    (when (seq m)
      (jdbc/execute! ds (sql/format {:update :runs :set m
                                     :where [:= :run_id (str run-id)]})))))

(defn- row->run [r]
  (when r
    {:run-id        (parse-uuid (:runs/run_id r))
     :pipeline-id   (keyword (:runs/pipeline_id r))
     :input-path    (:runs/input_path r)
     :input-slug    (:runs/input_slug r)
     :input-mtime   (:runs/input_mtime r)
     :input-size    (:runs/input_size r)
     :state         (keyword (:runs/state r))
     :cancel-reason (some-> (:runs/cancel_reason r) keyword)
     :started-at    (:runs/started_at r)
     :finished-at   (:runs/finished_at r)
     :error         (:runs/error r)}))

(defn get-run [ds run-id]
  (row->run (first (jdbc/execute! ds
                                  (sql/format {:select [:*] :from :runs
                                               :where [:= :run_id (str run-id)]})))))

(defn list-runs [ds limit]
  (mapv row->run (jdbc/execute! ds (sql/format {:select [:*] :from :runs
                                                :order-by [[:started_at :desc]]
                                                :limit limit}))))

(defn- ev->row [e]
  {:event_id       (str (:event-id e))
   :run_id         (str (:run-id e))
   :at             (:at e)
   :kind           (name (:kind e))
   :step_id        (some-> (:step-id e) name)
   :to_step_id     (some-> (:to-step-id e) name)
   :msg_id         (some-> (:msg-id e) str)
   :data_id        (some-> (:data-id e) str)
   :parent_msg_ids (js (mapv str (:parent-msg-ids e [])))
   :payload_ref    (js (:payload-ref e))
   :provenance     (js (:provenance e))
   :error          (js (:error e))})

(defn- row->ev [r]
  {:seq            (:trace_events/seq r)
   :event-id       (parse-uuid (:trace_events/event_id r))
   :run-id         (parse-uuid (:trace_events/run_id r))
   :at             (:trace_events/at r)
   :kind           (keyword (:trace_events/kind r))
   :step-id        (some-> (:trace_events/step_id r) keyword)
   :to-step-id     (some-> (:trace_events/to_step_id r) keyword)
   :msg-id         (some-> (:trace_events/msg_id r) parse-uuid)
   :data-id        (some-> (:trace_events/data_id r) parse-uuid)
   :parent-msg-ids (mapv parse-uuid (jp (:trace_events/parent_msg_ids r)))
   :payload-ref    (jp (:trace_events/payload_ref r))
   :provenance     (jp (:trace_events/provenance r))
   :error          (jp (:trace_events/error r))})

(defn insert-events! [ds events]
  (when (seq events)
    (jdbc/execute! ds (sql/format {:insert-into :trace_events
                                   :values (mapv ev->row events)}))))

(defn get-events
  "Events for a run, ordered by seq. Pass `after-seq` to tail (exclusive)."
  [ds run-id after-seq]
  (mapv row->ev
        (jdbc/execute! ds
                       (sql/format
                        (cond-> {:select [:*] :from :trace_events
                                 :where [:= :run_id (str run-id)]
                                 :order-by [[:seq :asc]]}
                          after-seq (update :where #(vector :and % [:> :seq after-seq])))))))
