(ns podcast.multi-task-test
  "End-to-end multi-task `extract!` tests using the in-process mock at
   `podcast.mock-server`. Covers structural correctness, equality
   between single-task and multi-task runs, cache-hit behaviour on
   reruns, and wall-clock speedup with simulated latency."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [podcast.core :as pc]
            [podcast.llm :as llm]
            [podcast.mock-server :as mock]))

(defn- write-fixture! [path]
  (let [doc {:description "Test transcript for multi-task pipeline."
             :paragraphs (vec
                          (for [i (range 8)]
                            {:id (str "p" i)
                             :timestamp (str i ":00")
                             :text (str "Paragraph " i " says something about Person-X.")}))}]
    (io/make-parents path)
    (spit path (with-out-str (json/pprint doc)))))

(defn- read-json [path]
  (json/read-str (slurp path) :key-fn keyword))

(def ^:private base-cfg
  {:mention-model {:model "stub"}
   :resolve-model {:model "stub"}
   :record-model  {:model "stub"}})

(def ^:private chunking {:n 2 :k 1})

(defn- normalize-result
  "Strip strictly-volatile fields (timings, token bookkeeping) and
   normalise the only truly-nondeterministic content field — registry
   `:mention_indices`, which is positional indices into the flattened
   `all-mentions` vec whose construction order depends on chunk arrival
   order through the stealing-workers pool. The semantic content (which
   mentions belong to which entity) is preserved by `:aliases`, which
   we sort to absorb the same ordering effect."
  [r]
  (let [sort-aliases #(if (sequential? %) (vec (sort %)) %)
        norm-reg-entry #(-> % (dissoc :mention_indices) (update :aliases sort-aliases))
        norm-ent-entry (fn [e]
                         (-> e
                             (update :aliases sort-aliases)
                             (update :occurrences
                                     (fn [os] (vec (sort-by (juxt :paragraph_id :chunk-id) os))))))]
    (-> r
        (dissoc :timings :tokens :stage-tokens)
        (update :registry (fn [reg]
                            (into (sorted-map)
                                  (for [[k e] reg] [k (norm-reg-entry e)]))))
        (update :entities (fn [ents]
                            (into (sorted-map)
                                  (for [[k e] ents] [k (norm-ent-entry e)])))))))

(deftest extract-runs-both-tasks
  (let [tmp     (java.io.File/createTempFile "podcast-multi-" "")
        fixture (str tmp ".json")
        out-dir (str tmp "-out")]
    (write-fixture! fixture)
    (with-redefs [llm/cached-chat! (mock/responder)
                  llm/detect-slots (constantly 2)]
      (let [results (pc/extract! (assoc base-cfg :tasks [:sentiment :conspiracy])
                                 fixture :run-dir out-dir :chunking chunking)]

        (testing "results map is keyed by task"
          (is (= #{:sentiment :conspiracy} (set (keys results)))))

        (testing "per-task output files exist with expected shape"
          (doseq [task [:sentiment :conspiracy]]
            (let [path (str out-dir "/" (name task) ".json")
                  r   (read-json path)]
              (is (.exists (io/file path)) (str path " should exist"))
              (is (= (name task) (:task r)))
              (is (= 8 (:n-paragraphs r)))
              (is (pos? (:n-chunks r)))
              (is (map? (:registry r)))
              (is (map? (:entities r)))
              (is (pos? (:n-entities r))
                  "tree-resolve should produce at least one entity"))))

        (testing "run.json accumulates one entry per task"
          (let [meta (read-json (str out-dir "/run.json"))
                runs (:runs meta)
                tasks (set (map :task runs))]
            (is (= 2 (count runs)))
            (is (= #{"sentiment" "conspiracy"} tasks))))

        (testing "per-task n-entities matches in-memory and on-disk"
          (doseq [task [:sentiment :conspiracy]]
            (is (= (:n-entities (results task))
                   (:n-entities (read-json (str out-dir "/" (name task) ".json")))))))))))

(deftest single-task-via-task-key-still-works
  (testing ":task (singular) is auto-promoted to :tasks [t]"
    (let [tmp     (java.io.File/createTempFile "podcast-single-" "")
          fixture (str tmp ".json")
          out-dir (str tmp "-out")]
      (write-fixture! fixture)
      (with-redefs [llm/cached-chat! (mock/responder)
                    llm/detect-slots (constantly 2)]
        (let [results (pc/extract! (assoc base-cfg :task :sentiment)
                                   fixture :run-dir out-dir :chunking chunking)]
          (is (= [:sentiment] (vec (keys results))))
          (is (.exists (io/file (str out-dir "/sentiment.json")))))))))

(deftest multi-task-content-equals-sequential-baselines
  (testing "running each task alone produces the same content as running them combined"
    (let [tmp     (java.io.File/createTempFile "podcast-eq-" "")
          fixture (str tmp ".json")
          dir-s   (str tmp "-S")
          dir-c   (str tmp "-C")
          dir-sc  (str tmp "-SC")]
      (write-fixture! fixture)
      (with-redefs [llm/cached-chat! (mock/responder)
                    llm/detect-slots (constantly 4)]
        (let [run-alone     (pc/extract! (assoc base-cfg :tasks [:sentiment])
                                         fixture :run-dir dir-s :chunking chunking)
              run-cons      (pc/extract! (assoc base-cfg :tasks [:conspiracy])
                                         fixture :run-dir dir-c :chunking chunking)
              run-combined  (pc/extract! (assoc base-cfg :tasks [:sentiment :conspiracy])
                                         fixture :run-dir dir-sc :chunking chunking)]
          (testing "sentiment content matches whether run alone or combined"
            (is (= (normalize-result (run-alone :sentiment))
                   (normalize-result (run-combined :sentiment)))))
          (testing "conspiracy content matches whether run alone or combined"
            (is (= (normalize-result (run-cons :conspiracy))
                   (normalize-result (run-combined :conspiracy))))))))))

(deftest cache-hits-on-rerun
  (testing "shared cache atom: second call to the same config returns zero tokens"
    (let [tmp     (java.io.File/createTempFile "podcast-rerun-" "")
          fixture (str tmp ".json")
          shared  (atom {})]
      (write-fixture! fixture)
      (with-redefs [llm/cached-chat! (mock/responder {:cache shared})
                    llm/detect-slots (constantly 4)]
        (let [config (assoc base-cfg :tasks [:sentiment :conspiracy])
              first-run  (pc/extract! config fixture
                                      :run-dir (str tmp "-r1") :chunking chunking)
              second-run (pc/extract! config fixture
                                      :run-dir (str tmp "-r2") :chunking chunking)]
          (is (pos? (apply + (map :tokens (vals first-run))))
              "first run should consume tokens via the mock responder")
          (is (zero? (apply + (map :tokens (vals second-run))))
              "second run should be all cache hits (zero tokens)"))))))

(deftest multi-task-wall-clock-faster-than-sequential
  (testing "with simulated per-call latency, multi-task < sentiment alone + conspiracy alone"
    (let [tmp     (java.io.File/createTempFile "podcast-wc-" "")
          fixture (str tmp ".json")]
      (write-fixture! fixture)
      (with-redefs [llm/detect-slots (constantly 8)]
        (let [time-it (fn [tasks suffix]
                        ;; Fresh cache per timed run so the mock actually does work.
                        (with-redefs [llm/cached-chat! (mock/responder {:latency-ms 25})]
                          (let [t0 (System/nanoTime)]
                            (pc/extract! (assoc base-cfg :tasks tasks)
                                         fixture
                                         :run-dir (str tmp suffix)
                                         :chunking chunking
                                         :workers 8)
                            (- (System/nanoTime) t0))))
              t-sent  (time-it [:sentiment]                "-s")
              t-cons  (time-it [:conspiracy]               "-c")
              t-multi (time-it [:sentiment :conspiracy]    "-m")
              ms      (fn [ns] (long (/ ns 1e6)))]
          (is (< t-multi (+ t-sent t-cons))
              (format "multi-task should be faster than sequential singles. multi=%dms, seq=%dms"
                      (ms t-multi) (ms (+ t-sent t-cons)))))))))
