(ns podcast.multi-task-test
  "End-to-end multi-task `extract!` test with mocked LLM calls.

   Asserts that running `:tasks [:sentiment :conspiracy]` in one
   invocation produces independent per-task output files in a shared
   run-dir, with per-task structure preserved and aggregated metadata
   in run.json."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [podcast.core :as pc]
            [podcast.llm :as llm]))

(defn- mock-chat
  "Stub for podcast.llm/cached-chat!. Dispatches on stage and parses
   the rendered prompt to keep outputs aligned with chunk content so
   validators accept them."
  [stage _model-cfg system content-parts _schema]
  (let [text (->> content-parts (keep :text) (str/join "\n"))]
    (case stage
      :mentions
      (let [fid (or (second (re-find #"FOCUS[^\[]*\[([^\]\s]+)" text)) "p0")]
        {:value {:mentions [{:paragraph_id fid
                             :mention_text "Person-X"
                             :surface_form "X"}]}
         :tokens 100
         :cache  :miss})

      :tree-leaf
      (let [fid (or (second (re-find #"=== FOCUS[^\n]*\n\[([^\]\s]+)" text)) "p0")]
        {:value {:entities [{:entity_id "e_001"
                             :canonical (str "Person-" fid)
                             :summary (str "intro at " fid)
                             :mention_indices [1]}]}
         :tokens 50
         :cache  :miss})

      :tree-merge
      ;; No merges — entities carry through.
      {:value {:merges []} :tokens 10 :cache :miss}

      :records
      ;; Return zero records (all would need to ground into specific
      ;; registry ids and matching quotes — out of scope for a structural
      ;; orchestration test).
      {:value {:records []} :tokens 20 :cache :miss})))

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

(deftest extract-runs-both-tasks
  (let [tmp     (java.io.File/createTempFile "podcast-multi-" "")
        fixture (str tmp ".json")
        out-dir (str tmp "-out")]
    (write-fixture! fixture)
    (with-redefs [llm/cached-chat! mock-chat
                  llm/detect-slots (constantly 2)]
      (let [config  {:tasks         [:sentiment :conspiracy]
                     :mention-model {:model "stub"}
                     :resolve-model {:model "stub"}
                     :record-model  {:model "stub"}}
            results (pc/extract! config fixture :run-dir out-dir :chunking {:n 2 :k 1})]

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
      (with-redefs [llm/cached-chat! mock-chat
                    llm/detect-slots (constantly 2)]
        (let [config  {:task          :sentiment
                       :mention-model {:model "stub"}
                       :resolve-model {:model "stub"}
                       :record-model  {:model "stub"}}
              results (pc/extract! config fixture :run-dir out-dir :chunking {:n 2 :k 1})]
          (is (= [:sentiment] (vec (keys results))))
          (is (.exists (io/file (str out-dir "/sentiment.json")))))))))
