(ns podcast.judge
  "Quality-assessment pass: for each extracted record, fetch the source
   paragraph plus a small backward window, send the source text + the
   record's claim to a model, and ask whether the source actually
   supports the claim.

   Output verdicts:
     :supported     — the source clearly justifies the claim.
     :partial       — the source partially supports it (right entity
                      but wrong polarity, or right gist but the quote
                      doesn't actually appear).
     :unsupported   — the source doesn't justify the claim.
     :contradicted  — the source says the opposite of the claim.

   This runs as a posthoc step over a finished extract!. It uses the
   same `cached-chat!` machinery so judgments are cached just like
   extractions — re-running judgment after fixing a record only
   re-fires that one judgment.

   Usage:
     (judge-output! podcast.core/sentiment-config
                    \"out/2/sentiment-slice.json\"
                    \"stumpf.json\"
                    {:window 2 :sample 10 :workers 4})

   `:sample` (optional) limits to a random subset of records — useful
   when judging the full transcript would otherwise take hours on a
   slow local model."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [podcast.llm :as llm]
            [toolkit.llm.cache :as cache]))

;; ============================================================================
;; 1. Schema for the judge's structured output.
;; ============================================================================

(def ^:private judge-schema
  {:type "object"
   :properties
   {:verdict   {:type "string"
                :enum ["supported" "partial" "unsupported" "contradicted"]
                :description "Does the source paragraph support the claim?"}
    :reason    {:type "string"
                :description "One sentence explaining the verdict, citing source phrasing."}
    :confidence {:type "string"
                 :enum ["high" "medium" "low"]
                 :description "How confident you are in the verdict given the source."}}
   :required ["verdict" "reason" "confidence"]})

;; ============================================================================
;; 2. Prompt construction.
;; ============================================================================

(def ^:private judge-system
  "You are auditing automated extractions from podcast transcripts. For each item you are given:
- a SOURCE region — a paragraph from the transcript plus a small backward window for context;
- a TARGET paragraph_id — the specific paragraph the extraction is grounded in;
- a CLAIM — the structured fields the extractor produced (entity / theory, polarity / stance, quote, rationale or summary).

Decide whether the source SUPPORTS the claim:
- supported    — the source paragraph clearly justifies all the claim's load-bearing fields. Polarity matches the affect actually expressed, the quote appears in the source (allowing minor paraphrase), the entity/theory is what the speaker is talking about.
- partial      — the entity is right and the quote is grounded, but the polarity / stance is off, or the rationale stretches the source.
- unsupported  — the source paragraph doesn't actually contain or justify the claim. The quote may not be there, or the speaker isn't expressing what the claim says.
- contradicted — the source says the opposite of the claim (e.g. claim is positive but speaker is negative; claim says theory is asserted but speaker rejects it).

Be ruthless about polarity / stance accuracy. Be lenient about the quote being a substring vs a light paraphrase — if the source has the same content, that's fine. The goal is to flag claims that *misrepresent* the source.

Respond with a JSON object conforming to the supplied schema.")

(defn- render-source-window
  "Return the formatted SOURCE text: target paragraph plus `window`
   paragraphs of backward context, with paragraph_ids tagged."
  [paragraphs-by-idx target-idx window]
  (let [start (max 0 (- target-idx window))
        end   (inc target-idx)]
    (str/join "\n\n"
              (for [i (range start end)
                    :let [p (paragraphs-by-idx i)]
                    :when p]
                (str (when (= i target-idx) "→ ")
                     "[" (:id p) " @ " (:timestamp p) "] "
                     (:text p))))))

(defn- render-claim [record id-key]
  (str/join "\n"
            (filter some?
                    [(format "- entity / theory id: %s" (id-key record))
                     (format "- quote: %s" (pr-str (:quote record)))
                     (when-let [p (:polarity record)]  (format "- polarity: %s" p))
                     (when-let [s (:stance record)]    (format "- stance: %s" s))
                     (when-let [e (:emotion record)]   (format "- emotion: %s" e))
                     (when-let [r (:rationale record)] (format "- rationale: %s" r))
                     (when-let [s (:summary record)]   (format "- summary: %s" s))])))

;; ============================================================================
;; 3. Single-record judge call.
;; ============================================================================

(defn- judge-record!
  "Judge one record. Returns
     {:verdict :reason :confidence :tokens :cache}
   keyed for easy aggregation."
  [{:keys [model-cfg id-key registry paragraphs idx-by-id window]} record]
  (let [target-idx (idx-by-id (:paragraph_id record))
        source-text (render-source-window paragraphs target-idx window)
        canonical (get-in registry [(id-key record) :canonical])
        user-text (str "SOURCE:\n\n" source-text
                       "\n\n=====\n\nTARGET paragraph_id: " (:paragraph_id record)
                       "\n\nCANONICAL entity / theory: " (pr-str canonical)
                       "\n\nCLAIM:\n" (render-claim record id-key))
        content [{:type :text :text user-text}]
        {:keys [value tokens cache]}
        (llm/cached-chat! :judge model-cfg judge-system content judge-schema)]
    (merge {:tokens tokens :cache cache}
           (select-keys value [:verdict :reason :confidence]))))

;; ============================================================================
;; 4. Public: judge an entire output file.
;; ============================================================================

(defn- bounded-pmap
  "Same shape as podcast.core's helper. Inlined to avoid a circular
   dep — judge is a posthoc tool, not part of the core pipeline."
  [n f coll]
  (let [coll (vec coll)
        sem  (java.util.concurrent.Semaphore. (int n))
        futs (mapv (fn [x]
                     (.acquire sem)
                     (future
                       (try (f x) (finally (.release sem)))))
                   coll)]
    (mapv deref futs)))

(defn- log [s] (locking *out* (println s) (flush)))

(defn judge-output!
  "Audit every record (or a `:sample` subset) in `output-path` against
   the source transcript at `transcript-path`. Writes a `judgments.json`
   companion next to the output and returns the in-memory aggregate.

   Options:
     :window  — number of backward-context paragraphs to include in the
                source window. Default 2.
     :sample  — when set, judge only N randomly-chosen records.
     :workers — concurrency. Default: auto-detect via `llm/detect-slots`.
     :model   — override the model config used for judgment. Default:
                whichever record-model the supplied config uses."
  [config output-path transcript-path & {:keys [window sample workers model]
                                          :or   {window 2}}]
  (let [d            (json/read-str (slurp output-path) :key-fn keyword)
        transcript   (json/read-str (slurp transcript-path) :key-fn keyword)
        paragraphs   (vec (:paragraphs transcript))
        idx-by-id    (into {} (map-indexed (fn [i p] [(:id p) i])) paragraphs)
        registry     (:registry d)
        id-key       (case (:task config) :sentiment :entity_id :conspiracy :theory_id)
        all-records  (vec (for [[_ e] (:entities d)
                                o (:occurrences e)]
                            o))
        records      (if sample
                       (vec (take sample (shuffle all-records)))
                       all-records)
        workers      (or workers (llm/detect-slots) 1)
        model-cfg    (or model (:record-model config))
        ctx          {:model-cfg model-cfg
                      :id-key    id-key
                      :registry  registry
                      :paragraphs paragraphs
                      :idx-by-id idx-by-id
                      :window    window}
        _ (log (format "── Judging %d/%d records  (window=%d, workers=%d) ──"
                       (count records) (count all-records) window workers))
        t0 (System/currentTimeMillis)
        judgments
        (bounded-pmap workers
                      (fn [r]
                        (let [j (judge-record! ctx r)]
                          (log (format "  [judge] pid=%-12s verdict=%-12s conf=%s  tokens=%-5d cache=%s"
                                       (:paragraph_id r) (:verdict j)
                                       (or (:confidence j) "-")
                                       (:tokens j) (name (:cache j))))
                          (assoc j :record r)))
                      records)
        elapsed   (- (System/currentTimeMillis) t0)
        verdicts  (frequencies (map :verdict judgments))
        tokens    (apply + (map :tokens judgments))
        out-file  (io/file (.getParentFile (io/file output-path)) "judgments.json")
        result    {:source-path output-path
                   :n-records (count all-records)
                   :n-judged  (count records)
                   :verdicts  verdicts
                   :tokens    tokens
                   :elapsed-ms elapsed
                   :window    window
                   :judgments (vec judgments)}]
    (spit (.getPath out-file) (with-out-str (json/pprint result)))
    (log (format "\nDone. %d judgments in %.1fs (%d tokens). Verdicts: %s.  → %s"
                 (count judgments) (/ elapsed 1000.0) tokens
                 (pr-str verdicts)
                 (.getPath out-file)))
    result))
