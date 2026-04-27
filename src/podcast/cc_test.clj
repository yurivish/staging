(ns podcast.cc-test
  "Tests for podcast.cc. Stubs toolkit.datapotamus.claude-code/run! the way
   tree_resolve_test stubs llm/cached-chat! — pure, no subprocess, no LMDB
   miss penalty (cache short-circuits on the second call but Phase 1 only
   exercises misses)."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [podcast.cc :as cc]
            [toolkit.datapotamus.claude-code :as cc-step]
            [toolkit.llm.cache :as cache]))

;; --- fixtures ----------------------------------------------------------------

(defn- fixture-paragraphs []
  [{:id "t0-0"   :seconds 0   :timestamp "00:00:00" :text "Joe is on the show."}
   {:id "t10-1"  :seconds 10  :timestamp "00:00:10" :text "Andy talks about diving."}
   {:id "t30-2"  :seconds 30  :timestamp "00:00:30" :text "He really dislikes celery."}
   {:id "t60-3"  :seconds 60  :timestamp "00:01:00" :text "Joe loves coffee in the morning."}])

(defn- fixture-doc []
  {:description "A test transcript."
   :paragraphs  (fixture-paragraphs)})

(defn- write-fixture-input!
  "Spit a fixture JSON file to a temp path and return its path."
  []
  (let [f (java.io.File/createTempFile "cc-test-" ".json")]
    (.deleteOnExit f)
    (spit f (json/write-str (fixture-doc)))
    (.getAbsolutePath f)))

;; Stubbed cc-step/run! results. Keys match what cc-step/run! would
;; produce: kebab-cased after snake_case → kebab keyword conversion.
(defn- mock-scout-result []
  {:result          ""
   :is-error        false
   :stop-reason     "end_turn"
   :session-id      "s-scout"
   :total-cost-usd  0.0
   :num-turns       3
   :usage           {:input-tokens 100 :output-tokens 200}
   :structured-output
   {:registry
    {:e_001 {:canonical "Joe Rogan"
             :aliases   ["Joe" "the host"]
             :summary   "Podcast host."}
     :e_002 {:canonical "Andy Stumpf"
             :aliases   ["Andy" "the SEAL"]
             :summary   "Retired Navy SEAL guest."}
     :e_003 {:canonical "celery"
             :aliases   ["celery"]
             :summary   "A vegetable."}}}})

(defn- mock-extractor-result []
  ;; cc-step/run! does kebab-casing — so :paragraph_id → :paragraph-id, etc.
  {:result         ""
   :is-error       false
   :stop-reason    "end_turn"
   :session-id     "s-extr"
   :total-cost-usd 0.0
   :num-turns      8
   :usage          {:input-tokens 500 :output-tokens 400}
   :structured-output
   {:records
    [{:paragraph-id "t30-2" :entity-id "e_003"
      :quote "really dislikes celery"
      :polarity "negative" :emotion "disgust"
      :rationale "Speaker expresses dislike."}
     {:paragraph-id "t60-3" :entity-id "e_001"
      :quote "loves coffee"
      :polarity "positive" :emotion "affectionate"
      :rationale "Joe likes coffee."}
     ;; Invalid: paragraph_id not in transcript
     {:paragraph-id "t999-0" :entity-id "e_001"
      :quote "hi" :polarity "positive" :emotion "happy" :rationale "..."}
     ;; Invalid: entity_id not in registry
     {:paragraph-id "t0-0" :entity-id "e_999"
      :quote "Joe is on" :polarity "neutral" :emotion "calm" :rationale "..."}
     ;; Invalid: quote doesn't appear in paragraph
     {:paragraph-id "t0-0" :entity-id "e_001"
      :quote "completely fabricated quote that is not in the paragraph"
      :polarity "neutral" :emotion "calm" :rationale "..."}]}})

(defn- with-cache-disabled
  "Run f with podcast-cc cache opened in a fresh temp dir so prior runs
   don't pollute. cc.clj's @cache-store delay is captured at load time, so
   we redef it for the duration of f."
  [f]
  (let [tmp  (str (java.io.File/createTempFile "cc-cache-" ""))
        _    (io/delete-file tmp true)
        c    (cache/open tmp)
        orig @#'cc/cache-store]
    (try
      (with-redefs [cc/cache-store (delay c)]
        (f))
      (finally (cache/close c)))))

;; --- prepare-agent-cwd! ------------------------------------------------------

(deftest prepare-agent-cwd!-writes-three-files
  (let [tmp (java.io.File/createTempFile "cc-cwd-" "")
        _   (io/delete-file tmp true)
        _   (.mkdirs tmp)
        paras (fixture-paragraphs)
        [cwd transcript-hash]
        (cc/prepare-agent-cwd! tmp (fixture-doc) paras)]
    (testing "cwd contains the three expected files"
      (is (.exists (io/file cwd "paragraphs.txt")))
      (is (.exists (io/file cwd "transcript.json")))
      (is (.exists (io/file cwd "episode.txt"))))
    (testing "paragraphs.txt is one paragraph per line, [<id> @ <ts>] prefixed"
      (let [lines (str/split-lines (slurp (io/file cwd "paragraphs.txt")))]
        (is (= 4 (count lines)))
        (is (str/starts-with? (first lines) "[t0-0 @ 00:00:00]"))))
    (testing "transcript-hash is a hex sha256 (64 chars)"
      (is (= 64 (count transcript-hash)))
      (is (re-matches #"[0-9a-f]{64}" transcript-hash)))
    (testing "transcript.json is valid JSON with sliced paragraphs"
      (let [tj (json/read-str (slurp (io/file cwd "transcript.json"))
                              :key-fn keyword)]
        (is (= 4 (count (:paragraphs tj))))
        (is (= "A test transcript." (:description tj)))))
    (testing "episode.txt is the description verbatim"
      (is (= "A test transcript." (slurp (io/file cwd "episode.txt")))))))

;; --- validate-records --------------------------------------------------------

(deftest validate-records-splits-valid-vs-rejected
  (let [paras            (fixture-paragraphs)
        valid-pids       (set (map :id paras))
        registry-ids     #{"e_001" "e_002" "e_003"}
        paragraphs-by-id (into {} (map (juxt :id :text) paras))
        records [{:paragraph_id "t30-2" :entity_id "e_003"
                  :quote "dislikes celery" :polarity "negative"
                  :emotion "disgust" :rationale "..."}
                 {:paragraph_id "t999-0" :entity_id "e_001"
                  :quote "hi" :polarity "positive" :emotion "happy" :rationale "..."}
                 {:paragraph_id "t0-0" :entity_id "e_999"
                  :quote "Joe is on" :polarity "neutral" :emotion "calm" :rationale "..."}
                 {:paragraph_id "t0-0" :entity_id "e_001"
                  :quote "totally fabricated content not in the source paragraph"
                  :polarity "neutral" :emotion "calm" :rationale "..."}]
        {:keys [valid rejected]}
        (cc/validate-records records valid-pids registry-ids paragraphs-by-id)]
    (is (= 1 (count valid)))
    (is (= 3 (count rejected)))
    (is (= "t30-2" (:paragraph_id (first valid))))))

;; --- end-to-end with cc-step/run! stubbed -----------------------------------

(deftest extract!-end-to-end-with-stubbed-runs
  (with-cache-disabled
    (fn []
      (let [in-path  (write-fixture-input!)
            run-dir  (doto (java.io.File/createTempFile "cc-run-" "")
                      (#(io/delete-file % true))
                      .mkdirs)
            calls    (atom [])
            stub-run (fn [opts]
                       (swap! calls conj (select-keys opts [:append-system-prompt :model]))
                       ;; First call (scout) returns registry; second (extractor) returns records.
                       (if (zero? (mod (count @calls) 2))
                         (mock-extractor-result)
                         (mock-scout-result)))]
        (with-redefs [cc-step/run! stub-run]
          (let [results (cc/extract! cc/local-cfg in-path :run-dir run-dir)
                {:keys [registry records rejected]} (:sentiment results)]
            (testing "scout + extractor each invoked once"
              (is (= 2 (count @calls))))
            (testing "registry has 3 entities, string-keyed"
              (is (= 3 (count registry)))
              (is (every? string? (keys registry))))
            (testing "2 valid records, 3 rejected"
              (is (= 2 (count records)))
              (is (= 3 (count rejected))))
            (testing "records use snake_case keys after snake-keys conversion"
              (is (every? :paragraph_id records))
              (is (every? :entity_id records))))
          (testing "out-cc/N/sentiment.json is written with expected shape"
            (let [f (io/file run-dir "sentiment.json")]
              (is (.exists f))
              (let [out (json/read-str (slurp f) :key-fn keyword)]
                (is (= "sentiment" (:task out)))
                (is (= 4 (:n-paragraphs out)))
                (is (= 3 (:n-entities out)))
                (is (= 2 (:n-records out)))
                (is (= 3 (:n-rejected out)))
                (is (= "local" (:flavor out))))))
          (testing "run.json carries scout/extractor metadata for each task"
            (let [f (io/file run-dir "run.json")]
              (is (.exists f))
              (let [meta (json/read-str (slurp f) :key-fn keyword)
                    run  (first (:runs meta))]
                (is (= "local" (:flavor meta)))
                (is (= "sentiment" (:task run)))
                (is (= "end_turn" (-> run :scout :stop-reason)))
                (is (= "end_turn" (-> run :extractor :stop-reason)))))))))))
