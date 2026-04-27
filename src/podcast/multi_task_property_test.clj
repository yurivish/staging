(ns podcast.multi-task-property-test
  "FRNG-driven property test for the multi-task pipeline. Varies
   chunking, worker count, paragraph count, task ordering, and injects
   small scheduler-perturbing jitter to sample many concurrent
   interleavings. Asserts the core invariant: each task's content is
   independent of whether it ran alone or in a combined :tasks vec.

   Lives in one JVM to amortise startup; iterations are O(milliseconds)
   so several hundred attempts complete in seconds."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [podcast.core :as pc]
            [podcast.llm :as llm]
            [podcast.mock-server :as mock]
            [toolkit.frng :as frng])
  (:import [java.util Random]))

(def ^:private base-cfg
  {:mention-model {:model "stub"}
   :resolve-model {:model "stub"}
   :record-model  {:model "stub"}})

(defn- write-fixture! [path n-paragraphs]
  (let [doc {:description "fuzz transcript"
             :paragraphs (vec
                          (for [i (range n-paragraphs)]
                            {:id (str "p" i)
                             :timestamp (str i ":00")
                             :text (str "Paragraph " i " body.")}))}]
    (io/make-parents path)
    (spit path (with-out-str (json/pprint doc)))))

(defn- normalize-result
  "See podcast.multi-task-test/normalize-result. Strips strictly volatile
   fields and the registry's `:mention_indices` (positional indices into
   the flat all-mentions vec, whose construction order depends on chunk
   arrival through the parallel worker pool)."
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

(defn- mismatch-summary
  "Compact summary of where two normalised results differ — keeps the
   replay seed's output readable in CI logs."
  [task alone combined]
  {:task task
   :alone-n-entities    (:n-entities alone)
   :combined-n-entities (:n-entities combined)
   :alone-n-records     (:n-records alone)
   :combined-n-records  (:n-records combined)
   :alone-canonicals    (set (map :canonical (vals (:registry alone))))
   :combined-canonicals (set (map :canonical (vals (:registry combined))))
   :alone-keys          (set (keys (:registry alone)))
   :combined-keys       (set (keys (:registry combined)))})

(def ^:private attempt-timeout-ms 5000)

(defn- run-with-timeout
  "Run `thunk` on a future; return its value or throw `:hang` if it
   doesn't finish within `attempt-timeout-ms`. Hangs are real failures
   here — the multi-task pipeline must be deadlock-free regardless of
   chunking + worker shape."
  [tag thunk]
  (let [fut (future (thunk))
        v (deref fut attempt-timeout-ms ::timeout)]
    (if (= ::timeout v)
      (do (future-cancel fut)
          (throw (ex-info (str tag " hung past " attempt-timeout-ms "ms")
                          {:hang tag})))
      v)))

(defn- run-attempt
  "One attempt of the property: pick parameters via the FRNG cursor,
   run the three flavours of extract! (sentiment-alone, conspiracy-alone,
   combined), assert per-task equality and that none of the three runs
   hang."
  [f]
  (let [n-paragraphs (frng/range-inclusive f 4 12)
        n-chunk      (frng/range-inclusive f 1 4)
        k-context    (frng/range-inclusive f 0 2)
        workers      (frng/range-inclusive f 1 4)
        ;; Three scheduler perturbations: yield-only (cheap), tiny sleep,
        ;; or none. Each nudges the scheduler into different interleavings.
        jitter-mode  (frng/weighted f {:yield 1 :sleep-1ms 1 :none 1})
        tasks-order  (frng/weighted f {[:sentiment :conspiracy] 1
                                       [:conspiracy :sentiment] 1})
        jitter-fn    (case jitter-mode
                       :yield     #(Thread/yield)
                       :sleep-1ms #(Thread/sleep 1)
                       :none      nil)
        tmp     (java.io.File/createTempFile "podcast-prop-" "")
        fixture (str tmp ".json")
        chunking {:n n-chunk :k k-context}
        params {:n-paragraphs n-paragraphs
                :chunking chunking
                :workers workers
                :jitter jitter-mode
                :tasks-order tasks-order}]
    (write-fixture! fixture n-paragraphs)
    (try
      (with-redefs [llm/cached-chat! (mock/responder {:jitter-fn jitter-fn})
                    llm/detect-slots (constantly workers)]
        (let [run! (fn [tasks suffix]
                     (run-with-timeout
                      (str "extract " tasks)
                      (fn []
                        (pc/extract! (assoc base-cfg :tasks tasks)
                                     fixture
                                     :run-dir (str tmp suffix)
                                     :chunking chunking
                                     :workers workers))))
              run-s  (run! [:sentiment]   "-S")
              run-c  (run! [:conspiracy]  "-C")
              run-sc (run! tasks-order    "-SC")]
          (when-not (= (normalize-result (run-s :sentiment))
                       (normalize-result (run-sc :sentiment)))
            (throw (ex-info "sentiment content differs alone vs combined"
                            {:params params
                             :diff (mismatch-summary :sentiment
                                                     (normalize-result (run-s :sentiment))
                                                     (normalize-result (run-sc :sentiment)))})))
          (when-not (= (normalize-result (run-c :conspiracy))
                       (normalize-result (run-sc :conspiracy)))
            (throw (ex-info "conspiracy content differs alone vs combined"
                            {:params params
                             :diff (mismatch-summary :conspiracy
                                                     (normalize-result (run-c :conspiracy))
                                                     (normalize-result (run-sc :conspiracy)))})))))
      (catch clojure.lang.ExceptionInfo e
        (if (:hang (ex-data e))
          (throw (ex-info (.getMessage e) (assoc (ex-data e) :params params)))
          (throw e)))
      (finally
        (.delete (io/file fixture))))))

(deftest ^:slow multi-task-equality-property
  (let [result (frng/search run-attempt {:attempts 200 :size 256})]
    (when (not= :pass result)
      (let [{:keys [seed size ex]} (:fail result)]
        (println "FAIL — replay with"
                 (pr-str {:seed seed :size size}))
        (println "params/diff:" (pr-str (ex-data ex)))))
    (is (= :pass result)
        "multi-task pipeline must produce per-task content independent of task fanout")))
