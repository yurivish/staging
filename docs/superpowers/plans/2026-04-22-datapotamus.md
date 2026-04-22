# `toolkit.datapotamus` v1 Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a headless Clojure library (`toolkit.datapotamus`) that watches an input directory, runs `core.async.flow` pipelines per file, and persists trace events to SQLite. Verify end-to-end with an arithmetic demo (`file → read → sub1 → repeat3 → sqlite-sink`).

**Spec:** `/home/yurivish/.claude/plans/i-recently-added-a-ticklish-snowflake.md`. Cuts applied to this plan vs. that spec: no `ids` ns (use `random-uuid`/`parse-uuid` from `clojure.core`); no migration file (schema as Clojure vector); no `sink_outputs` table (query `trace_events` where `kind='sink-wrote'`); no public-API facade ns; no `send-in-event`, no `:complete-when`/`:hard-timeout-ms`; collector persists events only (runner is authoritative for `runs`).

**Tech Stack:** Clojure 1.12, `core.async`, `core.async.flow`, next.jdbc + honeysql on SQLite, `com.stuartsierra/component`, existing `toolkit.filewatcher` + `toolkit.sqlite`. No new deps.

**Project conventions:**
- Tests co-located: `src/toolkit/datapotamus/foo.clj` + `src/toolkit/datapotamus/foo_test.clj`, ns `toolkit.datapotamus.foo-test`.
- `clojure -X:test :patterns '[".*datapotamus.*-test$"]'` runs the suite.
- `(set! *warn-on-reflection* true)` at top of each non-test ns.
- Commit messages short, lowercase, present-tense (e.g. `datapotamus: store`).
- Components are `defrecord` implementing `component/Lifecycle`.
- **Sparse Clojure**: no defensive branches for impossible cases, minimal docstrings (one line max when non-obvious), no speculative fields.

---

## Task 1: Store (schema + runs CRUD + trace events CRUD)

**Files:**
- Create: `src/toolkit/datapotamus/store.clj`
- Create: `src/toolkit/datapotamus/store_test.clj`

- [ ] **Step 1: Write failing test**

`src/toolkit/datapotamus/store_test.clj`:
```clojure
(ns toolkit.datapotamus.store-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.store :as store])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *ds* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-store-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn with-db [t]
  (let [f (tmp-db)
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try (store/migrate! ds) (binding [*ds* ds] (t))
         (finally (.delete f)))))

(use-fixtures :each with-db)

(deftest migrate-creates-tables
  (let [names (->> (jdbc/execute! *ds* ["SELECT name FROM sqlite_master WHERE type='table'"])
                   (map :sqlite_master/name) set)]
    (is (contains? names "runs"))
    (is (contains? names "trace_events"))))

(deftest migrate-is-idempotent
  (store/migrate! *ds*) (store/migrate! *ds*))

(deftest run-insert-get-update
  (let [run-id (random-uuid)]
    (store/insert-run! *ds* {:run-id run-id :pipeline-id :demo
                              :input-path "p" :input-slug "p"
                              :state :pending :started-at 1})
    (is (= :pending (:state (store/get-run *ds* run-id))))
    (store/update-run! *ds* run-id {:state :running})
    (is (= :running (:state (store/get-run *ds* run-id))))
    (store/update-run! *ds* run-id {:state :completed :finished-at 9})
    (let [r (store/get-run *ds* run-id)]
      (is (= :completed (:state r)))
      (is (= 9 (:finished-at r))))))

(deftest list-runs-desc
  (dotimes [i 3]
    (store/insert-run! *ds* {:run-id (random-uuid) :pipeline-id :d
                              :input-path (str i) :input-slug (str i)
                              :state :completed :started-at i}))
  (is (= [2 1 0] (map :started-at (store/list-runs *ds* 10)))))

(deftest events-batch-insert-and-cursor
  (let [run-id (random-uuid)]
    (store/insert-run! *ds* {:run-id run-id :pipeline-id :d
                              :input-path "p" :input-slug "p"
                              :state :running :started-at 0})
    (store/insert-events! *ds*
      (mapv (fn [i] {:event-id (random-uuid) :run-id run-id :at i
                     :kind :recv :step-id :a :msg-id (random-uuid)})
            (range 5)))
    (let [all (store/get-events *ds* run-id nil)]
      (is (= 5 (count all)))
      (is (= [0 1 2 3 4] (map :at all))))
    (let [cursor (:seq (nth (store/get-events *ds* run-id nil) 2))
          tail   (store/get-events *ds* run-id cursor)]
      (is (= 2 (count tail))))))

(deftest events-store-json-fields
  (let [run-id (random-uuid) m1 (random-uuid) m2 (random-uuid)]
    (store/insert-run! *ds* {:run-id run-id :pipeline-id :d
                              :input-path "p" :input-slug "p"
                              :state :running :started-at 0})
    (store/insert-events! *ds*
      [{:event-id (random-uuid) :run-id run-id :at 1 :kind :send-out
        :step-id :a :to-step-id :b :msg-id m1 :data-id (random-uuid)
        :parent-msg-ids [m2] :provenance {:k 1}
        :payload-ref {:kind :sqlite :location "results:7"}}])
    (let [e (first (store/get-events *ds* run-id nil))]
      (is (= [m2] (:parent-msg-ids e)))
      (is (= {:k 1} (:provenance e)))
      (is (= {:kind "sqlite" :location "results:7"} (:payload-ref e))))))
```

- [ ] **Step 2: Verify failure**

Run: `clojure -X:test :patterns '[".*datapotamus.store-test$"]'`
Expected: ns fails to load.

- [ ] **Step 3: Implement store**

`src/toolkit/datapotamus/store.clj`:
```clojure
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
```

- [ ] **Step 4: Verify pass**

Run: `clojure -X:test :patterns '[".*datapotamus.store-test$"]'`
Expected: 6 tests, pass.

- [ ] **Step 5: Commit**

```bash
git add src/toolkit/datapotamus/store.clj src/toolkit/datapotamus/store_test.clj
git commit -m "datapotamus: store"
```

---

## Task 2: Proc — trace primitives + `step-proc`

**Files:**
- Create: `src/toolkit/datapotamus/proc.clj`
- Create: `src/toolkit/datapotamus/proc_test.clj`

- [ ] **Step 1: Write failing test**

`src/toolkit/datapotamus/proc_test.clj`:
```clojure
(ns toolkit.datapotamus.proc-test
  (:require [clojure.core.async.flow :as flow]
            [clojure.test :refer [deftest is]]
            [toolkit.datapotamus.proc :as proc]))

(defn- msg [data]
  {:msg-id (random-uuid) :data-id (random-uuid)
   :run-id (random-uuid) :data data})

(deftest derive-carries-run-fresh-ids-new-data
  (let [p (msg 10) c (proc/derive p 9)]
    (is (= (:run-id p) (:run-id c)))
    (is (not= (:msg-id p) (:msg-id c)))
    (is (not= (:data-id p) (:data-id c)))
    (is (= 9 (:data c)))))

(deftest send-out-event-shape
  (let [p (msg 10) c (proc/derive p 9)
        e (proc/send-out-event :a c :parent-msg-ids [(:msg-id p)]
                                :to-step-id :b :provenance {:idx 0})]
    (is (= :send-out (:kind e)))
    (is (= :a (:step-id e)))
    (is (= :b (:to-step-id e)))
    (is (= [(:msg-id p)] (:parent-msg-ids e)))
    (is (= {:idx 0} (:provenance e)))))

(deftest recv-success-failure-merge-sink-shapes
  (let [m (msg nil)]
    (is (= :recv     (:kind (proc/recv-event :s m))))
    (is (= :success  (:kind (proc/success-event :s m))))
    (let [f (proc/failure-event :s m (ex-info "boom" {}))]
      (is (= :failure (:kind f)))
      (is (= "boom"   (get-in f [:error :message]))))
    (let [e (proc/merge-event :s m [(random-uuid) (random-uuid)])]
      (is (= :merge (:kind e)))
      (is (= 2 (count (:parent-msg-ids e)))))
    (is (= :sink-wrote
           (:kind (proc/sink-wrote-event :s m {:kind :sqlite :location "t:1"}))))))

(deftest step-proc-emits-recv-sendout-success
  (let [sfn (proc/step-proc :dec (fn [m] (update m :data dec)))
        _ (sfn) state (sfn {}) state (sfn state :resume)
        seed (msg 10)
        [_ out] (sfn state :in seed)
        evs (::flow/report out)
        child (first (:out out))]
    (is (= 9 (:data child)))
    (is (= (:run-id seed) (:run-id child)))
    (is (= [:recv :send-out :success] (mapv :kind evs)))
    (is (= [(:msg-id seed)] (:parent-msg-ids (second evs))))))

(deftest step-proc-rethrows-with-failure-event-attached
  (let [sfn (proc/step-proc :boom (fn [_] (throw (ex-info "no" {:a 1}))))]
    (sfn)
    (let [s (sfn {})]
      (try (sfn s :in (msg nil))
           (is false "should have thrown")
           (catch clojure.lang.ExceptionInfo ex
             (is (some? (::proc/failure-event (ex-data ex)))))))))
```

- [ ] **Step 2: Verify failure**

Run: `clojure -X:test :patterns '[".*datapotamus.proc-test$"]'`
Expected: ns fails to load.

- [ ] **Step 3: Implement proc**

`src/toolkit/datapotamus/proc.clj`:
```clojure
(ns toolkit.datapotamus.proc
  (:require [clojure.core.async.flow :as flow]))

(set! *warn-on-reflection* true)

(defn- now [] (System/currentTimeMillis))

(defn derive [parent new-data]
  {:msg-id (random-uuid) :data-id (random-uuid)
   :run-id (:run-id parent) :data new-data})

(defn- base [kind m]
  {:event-id (random-uuid) :run-id (:run-id m) :at (now) :kind kind
   :msg-id (:msg-id m) :data-id (:data-id m)})

(defn recv-event    [step-id m] (assoc (base :recv    m) :step-id step-id))
(defn success-event [step-id m] (assoc (base :success m) :step-id step-id))

(defn failure-event [step-id m ^Throwable ex]
  (assoc (base :failure m) :step-id step-id
         :error {:message (ex-message ex) :data (ex-data ex)
                 :stack   (with-out-str (.printStackTrace ex))}))

(defn send-out-event [step-id child & {:keys [parent-msg-ids to-step-id provenance]}]
  (cond-> (assoc (base :send-out child) :step-id step-id
                 :parent-msg-ids (vec parent-msg-ids))
    to-step-id (assoc :to-step-id to-step-id)
    provenance (assoc :provenance provenance)))

(defn merge-event [step-id child parent-msg-ids]
  (assoc (base :merge child) :step-id step-id
         :parent-msg-ids (vec parent-msg-ids)))

(defn sink-wrote-event [step-id m payload-ref]
  (assoc (base :sink-wrote m) :step-id step-id :payload-ref payload-ref))

(defn step-proc
  "Wrap a pure (msg -> msg') fn into a one-in-one-out flow step-fn that
   auto-emits :recv / :send-out / :success (or :failure + rethrow)."
  [step-id f]
  (fn
    ([] {:params {} :ins {:in ""} :outs {:out ""}})
    ([_] {})
    ([s _] s)
    ([s _ m]
     (try
       (let [c (derive m (:data (f m)))
             evs [(recv-event step-id m)
                  (send-out-event step-id c :parent-msg-ids [(:msg-id m)])
                  (success-event step-id c)]]
         [s {:out [c] ::flow/report evs}])
       (catch Throwable ex
         (throw (ex-info (ex-message ex)
                          (assoc (ex-data ex)
                                 ::failure-event (failure-event step-id m ex))
                          ex)))))))
```

- [ ] **Step 4: Verify pass**

Run: `clojure -X:test :patterns '[".*datapotamus.proc-test$"]'`
Expected: 5 tests, pass.

- [ ] **Step 5: Commit**

```bash
git add src/toolkit/datapotamus/proc.clj src/toolkit/datapotamus/proc_test.clj
git commit -m "datapotamus: proc"
```

---

## Task 3: Registry

**Files:**
- Create: `src/toolkit/datapotamus/registry.clj`
- Create: `src/toolkit/datapotamus/registry_test.clj`

- [ ] **Step 1: Write failing test**

`src/toolkit/datapotamus/registry_test.clj`:
```clojure
(ns toolkit.datapotamus.registry-test
  (:require [clojure.test :refer [deftest is]]
            [toolkit.datapotamus.registry :as reg]))

(deftest install-returns-prior-and-replaces
  (let [r (reg/make)
        a {:run-id (random-uuid)} b {:run-id (random-uuid)}]
    (is (nil? (reg/install! r "s" a)))
    (is (= a (reg/install! r "s" b)))
    (is (= b (reg/current r "s")))))

(deftest remove-only-if-matches
  (let [r (reg/make) rid (random-uuid)]
    (reg/install! r "s" {:run-id rid})
    (reg/remove-if-matches! r "s" (random-uuid))
    (is (= rid (:run-id (reg/current r "s"))))
    (reg/remove-if-matches! r "s" rid)
    (is (nil? (reg/current r "s")))))
```

- [ ] **Step 2: Verify failure**

Run: `clojure -X:test :patterns '[".*datapotamus.registry-test$"]'`
Expected: ns fails to load.

- [ ] **Step 3: Implement**

`src/toolkit/datapotamus/registry.clj`:
```clojure
(ns toolkit.datapotamus.registry)

(set! *warn-on-reflection* true)

(defn make [] (atom {}))

(defn install!
  "Install entry under slug. Returns prior entry (or nil). Caller stops any prior."
  [reg slug entry]
  (get (first (swap-vals! reg assoc slug entry)) slug))

(defn current [reg slug] (get @reg slug))

(defn remove-if-matches!
  "Dissoc slug iff its :run-id still matches; avoids clobbering a successor."
  [reg slug run-id]
  (swap! reg (fn [m] (if (= run-id (get-in m [slug :run-id])) (dissoc m slug) m))))
```

- [ ] **Step 4: Verify pass**

Run: `clojure -X:test :patterns '[".*datapotamus.registry-test$"]'`
Expected: 2 tests, pass.

- [ ] **Step 5: Commit**

```bash
git add src/toolkit/datapotamus/registry.clj src/toolkit/datapotamus/registry_test.clj
git commit -m "datapotamus: registry"
```

---

## Task 4: TraceCollector

**Files:**
- Create: `src/toolkit/datapotamus/trace.clj`
- Create: `src/toolkit/datapotamus/trace_test.clj`

- [ ] **Step 1: Write failing test**

`src/toolkit/datapotamus/trace_test.clj`:
```clojure
(ns toolkit.datapotamus.trace-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is use-fixtures]]
            [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.store :as store]
            [toolkit.datapotamus.trace :as trace])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *ds* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-trace-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn with-db [t]
  (let [f (tmp-db)
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try (store/migrate! ds) (binding [*ds* ds] (t))
         (finally (.delete f)))))

(use-fixtures :each with-db)

(defn- seed [ds run-id]
  (store/insert-run! ds {:run-id run-id :pipeline-id :d
                          :input-path "p" :input-slug "p"
                          :state :running :started-at 0}))

(deftest collector-batches-events-to-sqlite
  (let [run-id (random-uuid) _ (seed *ds* run-id)
        tc (component/start (trace/make {:datasource *ds* :batch-size 10 :flush-ms 20}))
        ch (:events-ch tc)]
    (try
      (dotimes [i 3]
        (a/>!! ch {:event-id (random-uuid) :run-id run-id :at i :kind :recv}))
      (Thread/sleep 100)
      (is (= 3 (count (store/get-events *ds* run-id nil))))
      (finally (component/stop tc)))))
```

- [ ] **Step 2: Verify failure**

Run: `clojure -X:test :patterns '[".*datapotamus.trace-test$"]'`
Expected: ns fails to load.

- [ ] **Step 3: Implement**

`src/toolkit/datapotamus/trace.clj`:
```clojure
(ns toolkit.datapotamus.trace
  (:require [clojure.core.async :as a]
            [com.stuartsierra.component :as component]
            [toolkit.datapotamus.store :as store]))

(set! *warn-on-reflection* true)

(defrecord TraceCollector [datasource batch-size flush-ms events-ch stop-ch done-ch]
  component/Lifecycle
  (start [this]
    (let [events-ch (or events-ch (a/chan 256))
          stop-ch   (a/chan)
          done-ch   (a/chan)]
      (a/thread
        (try
          (loop [batch [] timer (a/timeout flush-ms)]
            (let [[v ch] (a/alts!! [events-ch timer stop-ch])]
              (cond
                (or (= ch stop-ch) (nil? v))
                (store/insert-events! datasource batch)

                (= ch timer)
                (do (store/insert-events! datasource batch)
                    (recur [] (a/timeout flush-ms)))

                :else
                (let [batch' (conj batch v)]
                  (if (>= (count batch') batch-size)
                    (do (store/insert-events! datasource batch')
                        (recur [] (a/timeout flush-ms)))
                    (recur batch' timer))))))
          (finally (a/close! done-ch))))
      (assoc this :events-ch events-ch :stop-ch stop-ch :done-ch done-ch)))
  (stop [this]
    (when stop-ch (a/close! stop-ch))
    (when done-ch (a/<!! done-ch))
    (assoc this :events-ch nil :stop-ch nil :done-ch nil)))

(defn make
  [{:keys [datasource batch-size flush-ms]
    :or {batch-size 100 flush-ms 50}}]
  (map->TraceCollector {:datasource datasource
                         :batch-size batch-size :flush-ms flush-ms}))
```

- [ ] **Step 4: Verify pass**

Run: `clojure -X:test :patterns '[".*datapotamus.trace-test$"]'`
Expected: 1 test, pass.

- [ ] **Step 5: Commit**

```bash
git add src/toolkit/datapotamus/trace.clj src/toolkit/datapotamus/trace_test.clj
git commit -m "datapotamus: trace collector"
```

---

## Task 5: sqlite-sink

**Files:**
- Create: `src/toolkit/datapotamus/sinks.clj`
- Create: `src/toolkit/datapotamus/sinks_test.clj`

- [ ] **Step 1: Write failing test**

`src/toolkit/datapotamus/sinks_test.clj`:
```clojure
(ns toolkit.datapotamus.sinks-test
  (:require [clojure.core.async.flow :as flow]
            [clojure.test :refer [deftest is use-fixtures]]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.sinks :as sinks])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *ds* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-sinks-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn with-db [t]
  (let [f (tmp-db)
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try
      (jdbc/execute! ds ["CREATE TABLE results (
                           rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                           run_id TEXT, msg_id TEXT, data_id TEXT,
                           written_at INTEGER, data TEXT)"])
      (binding [*ds* ds] (t))
      (finally (.delete f)))))

(use-fixtures :each with-db)

(deftest sqlite-sink-writes-and-emits-sink-wrote
  (let [sfn (sinks/sqlite-sink :sink {:table :results :datasource *ds*})
        m {:msg-id (random-uuid) :data-id (random-uuid)
           :run-id (random-uuid) :data 9}
        _ (sfn) s (sfn {})
        [_ out] (sfn s :in m)
        ev (first (::flow/report out))]
    (is (empty? (:out out)))
    (is (= :sink-wrote (:kind ev)))
    (is (= :sqlite (get-in ev [:payload-ref :kind])))
    (is (re-matches #"results:\d+" (get-in ev [:payload-ref :location])))
    (is (= 1 (count (jdbc/execute! *ds*
                      ["SELECT * FROM results WHERE run_id=?"
                       (str (:run-id m))]))))))
```

- [ ] **Step 2: Verify failure**

Run: `clojure -X:test :patterns '[".*datapotamus.sinks-test$"]'`
Expected: ns fails to load.

- [ ] **Step 3: Implement**

`src/toolkit/datapotamus/sinks.clj`:
```clojure
(ns toolkit.datapotamus.sinks
  (:require [clojure.core.async.flow :as flow]
            [clojure.data.json :as json]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.proc :as proc]))

(set! *warn-on-reflection* true)

(defn sqlite-sink
  "Step-fn: write each msg as a row into `:table` of `:datasource`.
   Table is expected to have run_id/msg_id/data_id/written_at/data TEXT cols."
  [step-id {:keys [table datasource]}]
  (fn
    ([] {:params {} :ins {:in ""} :outs {}})
    ([_] {})
    ([s _] s)
    ([s _ m]
     (let [now (System/currentTimeMillis)
           res (jdbc/execute-one! datasource
                 (sql/format {:insert-into table
                              :values [{:run_id (str (:run-id m))
                                        :msg_id (str (:msg-id m))
                                        :data_id (str (:data-id m))
                                        :written_at now
                                        :data (json/write-str (:data m))}]})
                 {:return-keys true})
           rowid (or (:last_insert_rowid res)
                     (get res (keyword (name table) "rowid"))
                     (:next.jdbc/update-count res))
           ref {:kind :sqlite :location (str (name table) ":" rowid)}]
       [s {::flow/report [(proc/sink-wrote-event step-id m ref)]}]))))
```

- [ ] **Step 4: Verify pass**

Run: `clojure -X:test :patterns '[".*datapotamus.sinks-test$"]'`
Expected: 1 test, pass. If the rowid key differs in this next.jdbc version, inspect `res` and pick the right key.

- [ ] **Step 5: Commit**

```bash
git add src/toolkit/datapotamus/sinks.clj src/toolkit/datapotamus/sinks_test.clj
git commit -m "datapotamus: sqlite sink"
```

---

## Task 6: Runner — per-run flow orchestration

**Files:**
- Create: `src/toolkit/datapotamus/runner.clj`
- Create: `src/toolkit/datapotamus/runner_test.clj`

This is the first place we integrate with `core.async.flow`. If the test fails due to API surprises, the implementer should inspect `(ns-publics 'clojure.core.async.flow)` and adjust the wrapper before writing more code.

- [ ] **Step 1: Write failing test**

`src/toolkit/datapotamus/runner_test.clj`:
```clojure
(ns toolkit.datapotamus.runner-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.proc :as proc]
            [toolkit.datapotamus.runner :as runner]
            [toolkit.datapotamus.sinks :as sinks]
            [toolkit.datapotamus.store :as store]
            [toolkit.datapotamus.trace :as trace])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *ds* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-runner-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn with-db [t]
  (let [f (tmp-db)
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try
      (store/migrate! ds)
      (jdbc/execute! ds ["CREATE TABLE results (
                           rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                           run_id TEXT, msg_id TEXT, data_id TEXT,
                           written_at INTEGER, data TEXT)"])
      (binding [*ds* ds] (t))
      (finally (.delete f)))))

(use-fixtures :each with-db)

(defn- run-with [pipeline seed]
  (let [tc (component/start (trace/make {:datasource *ds* :batch-size 10 :flush-ms 20}))
        rid (random-uuid)
        _ (store/insert-run! *ds* {:run-id rid :pipeline-id (:pipeline-id pipeline)
                                    :input-path "x" :input-slug "x"
                                    :state :pending :started-at 0})
        r (runner/run-pipeline!
            {:datasource *ds* :events-ch (:events-ch tc) :run-id rid
             :pipeline pipeline :seed {:data seed} :idle-complete-ms 200})]
    (component/stop tc)
    r))

(deftest linear-pipeline-runs-to-completion
  (let [p {:pipeline-id :arith :entry :dec
           :procs {:dec  (proc/step-proc :dec (fn [m] (update m :data dec)))
                   :sink (sinks/sqlite-sink :sink
                          {:table :results :datasource *ds*})}
           :conns [[[:dec :out] [:sink :in]]]}
        r (run-with p 10)]
    (is (= :completed (:state r)))
    (is (= 1 (count (jdbc/execute! *ds* ["SELECT * FROM results"]))))))

(deftest pipeline-failure-records-failed-state
  (let [p {:pipeline-id :boom :entry :boom
           :procs {:boom (proc/step-proc :boom (fn [_] (throw (ex-info "no" {}))))
                   :sink (sinks/sqlite-sink :sink
                          {:table :results :datasource *ds*})}
           :conns [[[:boom :out] [:sink :in]]]}
        r (run-with p 1)]
    (is (= :failed (:state r)))))
```

- [ ] **Step 2: Verify failure**

Run: `clojure -X:test :patterns '[".*datapotamus.runner-test$"]'`
Expected: ns fails to load.

- [ ] **Step 3: Implement**

`src/toolkit/datapotamus/runner.clj`:
```clojure
(ns toolkit.datapotamus.runner
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow]
            [toolkit.datapotamus.store :as store]))

(set! *warn-on-reflection* true)

(defn- emit! [ch run-id kind & {:as extra}]
  (a/>!! ch (merge {:event-id (random-uuid) :run-id run-id
                    :at (System/currentTimeMillis) :kind kind} extra)))

(defn- forward! [ch run-id evs]
  (doseq [e evs] (a/>!! ch (assoc e :run-id run-id))))

(defn- delta [evs]
  (reduce (fn [n e] (case (:kind e) :send-out (inc n) :recv (dec n) n)) 0 evs))

(defn run-pipeline!
  "Runs `pipeline` to completion, failure, or idle-quiescence.
   Required: :datasource :events-ch :run-id :pipeline :seed.
   Optional: :idle-complete-ms (default 500).
   Returns the final run map."
  [{:keys [datasource events-ch run-id pipeline seed idle-complete-ms]
    :or {idle-complete-ms 500}}]
  (let [{:keys [entry procs conns]} pipeline
        g (flow/create-flow
            {:procs (into {} (map (fn [[k sfn]] [k {:proc (flow/process sfn)}]) procs))
             :conns conns})
        {:keys [report-chan error-chan]} (flow/start g)
        seed-msg {:msg-id (random-uuid) :data-id (random-uuid)
                  :run-id run-id :data (:data seed)}]
    (try
      (store/update-run! datasource run-id {:state :running})
      (emit! events-ch run-id :run-started)
      @(flow/inject g [entry :in] [seed-msg])
      (loop [in-flight 1]
        (let [[v ch] (a/alts!! [report-chan error-chan
                                (a/timeout idle-complete-ms)])]
          (cond
            (and (= ch error-chan) (some? v))
            (let [ex (:clojure.core.async.flow/ex v)
                  fail (or (some-> ex ex-data :toolkit.datapotamus.proc/failure-event)
                           {:event-id (random-uuid) :run-id run-id
                            :at (System/currentTimeMillis) :kind :failure
                            :error {:message (some-> ex ex-message)}})]
              (a/>!! events-ch (assoc fail :run-id run-id))
              (flow/stop g)
              (store/update-run! datasource run-id
                                  {:state :failed
                                   :finished-at (System/currentTimeMillis)
                                   :error (get-in fail [:error :message])})
              (store/get-run datasource run-id))

            (and (= ch report-chan) (some? v))
            (let [evs (if (sequential? v) v [v])]
              (forward! events-ch run-id evs)
              (recur (+ in-flight (delta evs))))

            (zero? in-flight)
            (do (flow/stop g)
                (emit! events-ch run-id :run-finished)
                (store/update-run! datasource run-id
                                    {:state :completed
                                     :finished-at (System/currentTimeMillis)})
                (store/get-run datasource run-id))

            :else (recur in-flight))))
      (catch Throwable ex
        (try (flow/stop g) (catch Throwable _))
        (store/update-run! datasource run-id
                            {:state :failed :finished-at (System/currentTimeMillis)
                             :error (ex-message ex)})
        (store/get-run datasource run-id)))))
```

- [ ] **Step 4: Verify pass**

Run: `clojure -X:test :patterns '[".*datapotamus.runner-test$"]'`
Expected: 2 tests, pass.
If the flow's `report-chan` delivers wrapped values or the `error-chan` payload key differs, inspect what comes through and adjust the two branches in the `alt!!` loop.

- [ ] **Step 5: Commit**

```bash
git add src/toolkit/datapotamus/runner.clj src/toolkit/datapotamus/runner_test.clj
git commit -m "datapotamus: runner"
```

---

## Task 7: PipelineDaemon

**Files:**
- Create: `src/toolkit/datapotamus/daemon.clj`
- Create: `src/toolkit/datapotamus/daemon_test.clj`

- [ ] **Step 1: Write failing test**

`src/toolkit/datapotamus/daemon_test.clj`:
```clojure
(ns toolkit.datapotamus.daemon-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.daemon :as daemon]
            [toolkit.datapotamus.proc :as proc]
            [toolkit.datapotamus.sinks :as sinks]
            [toolkit.datapotamus.store :as store]
            [toolkit.datapotamus.trace :as trace])
  (:import [java.io File]
           [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute FileTime]))

(def ^:dynamic *ds* nil)
(def ^:dynamic ^Path *dir* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-daemon-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn fix [t]
  (let [f (tmp-db)
        d (Files/createTempDirectory "dp-daemon-" (into-array FileAttribute []))
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try
      (store/migrate! ds)
      (jdbc/execute! ds ["CREATE TABLE results (
                           rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                           run_id TEXT, msg_id TEXT, data_id TEXT,
                           written_at INTEGER, data TEXT)"])
      (binding [*ds* ds *dir* d] (t))
      (finally
        (.delete f)
        (doseq [p (reverse (iterator-seq
                              (.iterator (Files/walk d (into-array java.nio.file.FileVisitOption [])))))]
          (try (Files/delete p) (catch Throwable _)))))))

(use-fixtures :each fix)

(defn- write-aged! [^Path p ^String s]
  (Files/write p (.getBytes s "UTF-8") (into-array java.nio.file.OpenOption []))
  (Files/setLastModifiedTime
    p (FileTime/fromMillis (- (System/currentTimeMillis) 10000))))

(defn- read-int [m]
  (assoc m :data (Integer/parseInt (str/trim (slurp (:path (:data m)))))))

(deftest daemon-processes-file-drop-end-to-end
  (let [tc (component/start (trace/make {:datasource *ds* :batch-size 10 :flush-ms 20}))
        pipeline {:pipeline-id :demo :entry :read
                  :procs {:read (proc/step-proc :read read-int)
                          :dec  (proc/step-proc :dec (fn [m] (update m :data dec)))
                          :sink (sinks/sqlite-sink :sink
                                 {:table :results :datasource *ds*})}
                  :conns [[[:read :out] [:dec :in]]
                          [[:dec :out]  [:sink :in]]]}
        dae (component/start
              (daemon/make {:watch-dir (str *dir*) :datasource *ds*
                            :events-ch (:events-ch tc) :pipeline pipeline
                            :stable-gap-ms 50 :idle-complete-ms 200}))]
    (try
      (write-aged! (.resolve *dir* "n.txt") "10\n")
      (Thread/sleep 1500)
      (let [runs (store/list-runs *ds* 10)]
        (is (= 1 (count runs)))
        (is (= :completed (:state (first runs))))
        (is (= 1 (count (jdbc/execute! *ds* ["SELECT * FROM results"])))))
      (finally (component/stop dae) (component/stop tc)))))
```

- [ ] **Step 2: Verify failure**

Run: `clojure -X:test :patterns '[".*datapotamus.daemon-test$"]'`
Expected: ns fails to load.

- [ ] **Step 3: Implement**

`src/toolkit/datapotamus/daemon.clj`:
```clojure
(ns toolkit.datapotamus.daemon
  (:require [clojure.core.async :as a]
            [com.stuartsierra.component :as component]
            [toolkit.datapotamus.registry :as reg]
            [toolkit.datapotamus.runner :as runner]
            [toolkit.datapotamus.store :as store]
            [toolkit.filewatcher :as fw])
  (:import [java.nio.file Files LinkOption Path Paths]
           [java.nio.file.attribute BasicFileAttributes]))

(set! *warn-on-reflection* true)

(defn- ^Path ->path [^String s] (Paths/get s (into-array String [])))

(defn- stat [^String abs]
  (when-let [attrs (try (Files/readAttributes (->path abs) BasicFileAttributes
                                                (into-array LinkOption []))
                        (catch Throwable _ nil))]
    {:mtime (.toMillis (.lastModifiedTime ^BasicFileAttributes attrs))
     :size  (.size ^BasicFileAttributes attrs)}))

(defn- handle! [{:keys [watch-dir datasource events-ch pipeline
                         idle-complete-ms registry]}
                 {:keys [path dir?]}]
  (when-not dir?
    (let [slug (str (.relativize (->path watch-dir) (->path path)))
          s    (stat path)
          rid  (random-uuid)
          prior (reg/install! registry slug {:run-id rid})]
      (when prior
        (store/update-run! datasource (:run-id prior)
                            {:state :cancelled
                             :finished-at (System/currentTimeMillis)}))
      (store/insert-run! datasource
        {:run-id rid :pipeline-id (:pipeline-id pipeline)
         :input-path path :input-slug slug
         :input-mtime (:mtime s) :input-size (:size s)
         :state :pending :started-at (System/currentTimeMillis)})
      (a/thread
        (try
          (runner/run-pipeline!
            {:datasource datasource :events-ch events-ch :run-id rid
             :pipeline pipeline
             :seed {:data {:path path :slug slug}}
             :idle-complete-ms idle-complete-ms})
          (finally (reg/remove-if-matches! registry slug rid)))))))

(defrecord PipelineDaemon [watch-dir datasource events-ch pipeline
                           stable-gap-ms idle-complete-ms
                           fw registry stop-ch done-ch]
  component/Lifecycle
  (start [this]
    (let [fw' (-> (fw/make {:interval-ms 50 :safety-gap-ms stable-gap-ms
                             :changes-buffer 64})
                  (fw/watch-dir-recursive watch-dir)
                  fw/start)
          r (reg/make) stop (a/chan) done (a/chan)
          ctx {:watch-dir watch-dir :datasource datasource
               :events-ch events-ch :pipeline pipeline
               :idle-complete-ms idle-complete-ms :registry r}]
      (a/thread
        (try
          (loop []
            (let [ch (fw/changes fw')
                  [v c] (a/alts!! [ch stop])]
              (when (and (= c ch) (some? v))
                (try (handle! ctx v)
                     (catch Throwable ex
                       (binding [*out* *err*]
                         (println "datapotamus daemon error:" (ex-message ex)))))
                (recur))))
          (finally (a/close! done))))
      (assoc this :fw fw' :registry r :stop-ch stop :done-ch done)))
  (stop [this]
    (when stop-ch (a/close! stop-ch))
    (when done-ch (a/<!! done-ch))
    (when fw (fw/stop fw))
    (assoc this :fw nil :registry nil :stop-ch nil :done-ch nil)))

(defn make
  [{:keys [stable-gap-ms idle-complete-ms] :or
    {stable-gap-ms 3000 idle-complete-ms 500} :as opts}]
  (map->PipelineDaemon (assoc opts
                               :stable-gap-ms stable-gap-ms
                               :idle-complete-ms idle-complete-ms)))
```

- [ ] **Step 4: Verify pass**

Run: `clojure -X:test :patterns '[".*datapotamus.daemon-test$"]'`
Expected: 1 test, pass. Timing-sensitive; bump the sleep if flaky.

- [ ] **Step 5: Commit**

```bash
git add src/toolkit/datapotamus/daemon.clj src/toolkit/datapotamus/daemon_test.clj
git commit -m "datapotamus: daemon"
```

---

## Task 8: Arithmetic demo pipeline + integration test

**Files:**
- Create: `src/toolkit/datapotamus/demo.clj`
- Create: `src/toolkit/datapotamus/demo_test.clj`

Demo: `file → read → sub1 → repeat3 → sink`. Seed carries `{:path … :slug …}` from the daemon. `:sub1` is a 1:1 `step-proc`. `:repeat3` is hand-written (Level-2 primitives) because it produces 3 outputs per input.

- [ ] **Step 1: Write failing integration test**

`src/toolkit/datapotamus/demo_test.clj`:
```clojure
(ns toolkit.datapotamus.demo-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is use-fixtures]]
            [com.stuartsierra.component :as component]
            [next.jdbc :as jdbc]
            [toolkit.datapotamus.demo :as demo]
            [toolkit.datapotamus.runner :as runner]
            [toolkit.datapotamus.store :as store]
            [toolkit.datapotamus.trace :as trace])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *ds* nil)

(defn- tmp-db ^File []
  (-> (Files/createTempFile "dp-demo-" ".sqlite" (into-array FileAttribute []))
      .toFile))

(defn with-db [t]
  (let [f (tmp-db)
        ds (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getAbsolutePath f)})]
    (try
      (store/migrate! ds)
      (jdbc/execute! ds ["CREATE TABLE results (
                           rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                           run_id TEXT, msg_id TEXT, data_id TEXT,
                           written_at INTEGER, data TEXT)"])
      (binding [*ds* ds] (t))
      (finally (.delete f)))))

(use-fixtures :each with-db)

(deftest demo-produces-three-rows-of-decremented-value
  (let [tc (component/start (trace/make {:datasource *ds* :batch-size 10 :flush-ms 20}))
        tmp (doto (File/createTempFile "dp-demo-input-" ".txt")
              (#(spit % "10\n")))
        pipeline (demo/pipeline {:datasource *ds*})
        rid (random-uuid)
        _ (store/insert-run! *ds* {:run-id rid :pipeline-id :arith-demo
                                    :input-path (.getAbsolutePath tmp)
                                    :input-slug (.getName tmp)
                                    :state :pending :started-at 0})
        r (runner/run-pipeline!
            {:datasource *ds* :events-ch (:events-ch tc) :run-id rid
             :pipeline pipeline
             :seed {:data {:path (.getAbsolutePath tmp) :slug (.getName tmp)}}
             :idle-complete-ms 200})]
    (try
      (is (= :completed (:state r)))
      (let [rows (jdbc/execute! *ds* ["SELECT data FROM results"])
            vs   (mapv #(json/read-str (:results/data %)) rows)]
        (is (= 3 (count vs)))
        (is (every? #{9} vs)))
      (finally (.delete tmp) (component/stop tc)))))
```

- [ ] **Step 2: Verify failure**

Run: `clojure -X:test :patterns '[".*datapotamus.demo-test$"]'`
Expected: ns fails to load.

- [ ] **Step 3: Implement**

`src/toolkit/datapotamus/demo.clj`:
```clojure
(ns toolkit.datapotamus.demo
  "Arithmetic demo: file → read → sub1 → repeat3 → sqlite-sink."
  (:require [clojure.core.async.flow :as flow]
            [clojure.string :as str]
            [toolkit.datapotamus.proc :as proc]
            [toolkit.datapotamus.sinks :as sinks]))

(set! *warn-on-reflection* true)

(defn- read-file [m]
  (assoc m :data (Integer/parseInt (str/trim (slurp (:path (:data m)))))))

(defn- repeat3
  ([] {:params {} :ins {:in ""} :outs {:out ""}})
  ([_] {})
  ([s _] s)
  ([s _ m]
   (let [kids (mapv (fn [k]
                      (-> (proc/derive m (:data m))
                          (vary-meta assoc :k k)))
                    (range 3))
         evs (-> [(proc/recv-event :repeat3 m)]
                 (into (map-indexed
                         (fn [k c]
                           (proc/send-out-event :repeat3 c
                             :parent-msg-ids [(:msg-id m)]
                             :provenance {:idx k}))
                         kids))
                 (conj (proc/success-event :repeat3 m)))]
     [s {:out kids ::flow/report evs}])))

(defn pipeline [{:keys [datasource]}]
  {:pipeline-id :arith-demo
   :entry :read
   :procs {:read    (proc/step-proc :read read-file)
           :sub1    (proc/step-proc :sub1 (fn [m] (update m :data dec)))
           :repeat3 repeat3
           :sink    (sinks/sqlite-sink :sink
                     {:table :results :datasource datasource})}
   :conns [[[:read :out]    [:sub1 :in]]
           [[:sub1 :out]    [:repeat3 :in]]
           [[:repeat3 :out] [:sink :in]]]})
```

- [ ] **Step 4: Verify pass**

Run: `clojure -X:test :patterns '[".*datapotamus.demo-test$"]'`
Expected: 1 test, pass.

- [ ] **Step 5: Commit**

```bash
git add src/toolkit/datapotamus/demo.clj src/toolkit/datapotamus/demo_test.clj
git commit -m "datapotamus: demo pipeline"
```

---

## Task 9: `dev/user.clj` wiring

**Files:**
- Modify: `dev/user.clj`

- [ ] **Step 1: Inspect existing `dev/user.clj`**

Read `dev/user.clj` to understand its existing structure (hot-reload + `start!`). Do not modify existing top-level forms.

- [ ] **Step 2: Append datapotamus block**

Append to `dev/user.clj`:
```clojure
;; ---- datapotamus ---------------------------------------------------------
(require '[clojure.java.io :as dp-io]
         '[com.stuartsierra.component :as dp-c]
         '[next.jdbc :as dp-jdbc]
         '[toolkit.datapotamus.daemon :as dp-daemon]
         '[toolkit.datapotamus.demo :as dp-demo]
         '[toolkit.datapotamus.store :as dp-store]
         '[toolkit.datapotamus.trace :as dp-trace])

(defonce dp (atom nil))

(defn dp-start! []
  (let [db  (.getAbsolutePath (dp-io/file "data/dp.sqlite"))
        in  (.getAbsolutePath (dp-io/file "data/in"))
        _   (dp-io/make-parents (dp-io/file in "x"))
        ds  (dp-jdbc/get-datasource {:dbtype "sqlite" :dbname db})
        _   (dp-store/migrate! ds)
        _   (dp-jdbc/execute! ds
              ["CREATE TABLE IF NOT EXISTS results (
                 rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                 run_id TEXT, msg_id TEXT, data_id TEXT,
                 written_at INTEGER, data TEXT)"])
        tc  (dp-c/start (dp-trace/make {:datasource ds :batch-size 50 :flush-ms 100}))
        dae (dp-c/start (dp-daemon/make
                          {:watch-dir in :datasource ds
                           :events-ch (:events-ch tc)
                           :pipeline (dp-demo/pipeline {:datasource ds})
                           :stable-gap-ms 500 :idle-complete-ms 300}))]
    (reset! dp {:ds ds :tc tc :dae dae :in in :db db})
    (println "datapotamus watching" in "db" db)))

(defn dp-stop! []
  (when-let [{:keys [dae tc]} @dp]
    (dp-c/stop dae) (dp-c/stop tc) (reset! dp nil)
    (println "datapotamus stopped")))

(defn dp-runs [] (dp-store/list-runs (:ds @dp) 10))

(defn dp-events []
  (when-let [r (first (dp-runs))]
    (dp-store/get-events (:ds @dp) (:run-id r) nil)))
```

- [ ] **Step 3: Sanity-check load**

Run `clojure -M:dev -e "(load-file \"dev/user.clj\") :ok"`
Expected: `:ok`.

- [ ] **Step 4: Commit**

```bash
git add dev/user.clj
git commit -m "datapotamus: dev wiring"
```

---

## Task 10: Full-suite check + manual verification

- [ ] **Step 1: Run full test suite**

```bash
clojure -X:test
```
Expected: all datapotamus tests pass alongside existing toolkit tests.

- [ ] **Step 2: Manual REPL drive**

```bash
rm -f data/dp.sqlite
just dev
```
At REPL:
```clojure
(dp-start!)
```

In a shell:
```bash
echo 10 > data/in/n.txt
```

After ~2s, at REPL:
```clojure
(map (juxt :state :input-slug) (dp-runs))
;; => ([:completed "n.txt"])

(->> (dp-events) (map :kind) frequencies)
;; should include :run-started 1, :recv (≥3), :send-out (≥5: 1+1+3),
;; :success (≥3), :sink-wrote 3, :run-finished 1

(with-open [c (next.jdbc/get-connection (:ds @dp))]
  (next.jdbc/execute! c ["SELECT data FROM results"]))
;; => 3 rows, each data "9"
```

- [ ] **Step 3: Supersede check**

```bash
echo 10 > data/in/n.txt
echo 20 > data/in/n.txt
```
At REPL (after ~2s):
```clojure
(map (juxt :state :input-mtime) (dp-runs))
;; => newest first: [:completed ...] then [:cancelled ...]
```

- [ ] **Step 4: Stop**

```clojure
(dp-stop!)
```

- [ ] **Step 5: No commit needed unless verification surfaced fixes.**
