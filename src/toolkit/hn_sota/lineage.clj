(ns toolkit.hn-sota.lineage
  "Walk the persisted DuckDB trace from a final ranking row back to
   the comments that produced it. The demo punchline.

   The aggregator emits a stream of cumulative summary msgs (one per
   incoming mention). Each summary is built via `msg/merge` over all
   mention msgs seen so far for its model, so its `parent_msg_ids`
   transitively reach every contributing mention. We seed the walk at
   the latest aggregator emission whose payload mentions our target
   model, then recurse backward through `parent_msg_ids` and surface
   every `:send-out` event at `scan-mentions` whose msg_id appears in
   the walk — each such event's payload is a single (model_id, comment)
   pair with the originating story metadata stamped on (see
   `toolkit.hn-sota.core/scan-mentions`)."
  (:require [clojure.edn :as edn]
            [next.jdbc :as jdbc]))

(defn- ds-of [db-or-ds]
  (if (string? db-or-ds)
    (jdbc/get-datasource {:jdbcUrl (str "jdbc:duckdb:" db-or-ds)})
    db-or-ds))

(def ^:private msgs-cte
  ;; Per-msg projection (one row per msg_id, with the union of its
  ;; parent_msg_ids across lifecycle events). Recursing over events
  ;; directly explodes — each msg gets recv/success/send-out/split rows
  ;; with the same parent_msg_ids, and following all of them in a UNION
  ;; ALL recursion blows up exponentially.
  "msgs AS (
     SELECT msg_id,
            ARRAY_DISTINCT(FLATTEN(LIST(parent_msg_ids))) AS parents
       FROM events
      WHERE parent_msg_ids IS NOT NULL
      GROUP BY msg_id)")

(def ^:private walk-sql
  (str
   "WITH RECURSIVE " msgs-cte ",
      seed AS (
        SELECT m.msg_id, m.parents
          FROM events e
          JOIN payloads p ON e.payload_id = p.id
          JOIN msgs m     ON m.msg_id = e.msg_id
         WHERE e.step_id = 'aggregate'
           AND e.kind    = 'send-out'
           AND p.content_edn LIKE ?
         ORDER BY e.seq DESC
         LIMIT 1),
      walk(msg_id, parents) AS (
        SELECT msg_id, parents FROM seed
        UNION
        SELECT m.msg_id, m.parents
          FROM walk w, msgs m
         WHERE list_contains(w.parents, m.msg_id))
    SELECT DISTINCT p.content_edn
      FROM walk w
      JOIN events e   ON e.msg_id = w.msg_id
      JOIN payloads p ON e.payload_id = p.id
     WHERE e.step_id = 'scan-mentions'
       AND e.kind    = 'send-out'"))

(defn contributors
  "Return every (model_id, comment) pair from the trace whose lineage
   contributed to the final ranking row for `model-id`. One result per
   distinct mention-payload — duplicates collapsed by content hash.

   Each entry: {:model_id :sentiment :raw :comment-text :comment-id
                :comment-by :story-id :story-title :story-url :hn-link}.
   Sorted by sentiment desc then by story-id."
  [db-or-ds model-id]
  (let [ds      (ds-of db-or-ds)
        ;; LIKE pattern matches the EDN serialization of a map with
        ;; :model_id "<id>" — payloads are pr-str'd by obs.store.
        pattern (str "%:model_id \"" model-id "\"%")
        rows    (jdbc/execute! ds [walk-sql pattern])]
    (->> rows
         (keep (fn [{:keys [content_edn]}]
                 (try
                   (let [m (edn/read-string content_edn)]
                     (when (= model-id (:model_id m))
                       {:model_id     (:model_id m)
                        :sentiment    (:sentiment m)
                        :raw          (:raw m)
                        :comment-text (:comment-text m)
                        :comment-id   (:comment-id m)
                        :comment-by   (:comment-by m)
                        :story-id     (:story-id m)
                        :story-title  (:story-title m)
                        :story-url    (:story-url m)
                        :hn-link      (str "https://news.ycombinator.com/item?id="
                                           (:comment-id m))}))
                   (catch Throwable _ nil))))
         (sort-by (juxt (comp - :sentiment) :story-id))
         vec)))

(defn ranking
  "Convenience: pull the final ranking from the trace store directly
   (instead of the JSON spit). Returns a vec sorted by mentions desc."
  [db-or-ds]
  (let [ds   (ds-of db-or-ds)
        rows (jdbc/execute!
              ds
              ["SELECT p.content_edn
                  FROM events e
                  JOIN payloads p ON e.payload_id = p.id
                 WHERE e.step_id = 'aggregate' AND e.kind = 'send-out'
                 ORDER BY e.seq"])]
    ;; Last emission per model_id wins; that's the final cumulative row.
    (->> rows
         (keep #(try (edn/read-string (:content_edn %)) (catch Throwable _ nil)))
         (reduce (fn [acc r] (assoc acc (:model_id r) r)) {})
         vals
         (sort-by :mentions >)
         vec)))
