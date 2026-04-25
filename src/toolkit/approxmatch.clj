(ns toolkit.approxmatch
  "Approximate substring matching via Sellers' algorithm (1980), a
   modification of the Wagner-Fischer edit-distance algorithm adapted
   for substring search.

   Use case: a long text T and a short, possibly-mangled pattern P.
   `find-all` returns every region of T that approximately matches P
   with edit distance ≤ max-dist, with start/end indices into the
   NFC-normalized original text.

   Both text and pattern are normalized before matching:
     - Unicode NFC (composing equivalent sequences),
     - case-folded to lowercase,
     - whitespace runs collapsed to a single space.
   The max-dist budget therefore applies to the *normalized* forms;
   differences eliminated by normalization (case, extra whitespace,
   NFC vs NFD) are free.

   Algorithm sketch (see the Go port comments for the long version):
     - Phase 1: rolling 2-column DP scan of the normalized text against
       the normalized pattern. dp[i][j] = min edit distance between
       pattern[0:i] and any substring of text ending at position j.
       Base case dp[0][j] = 0 (empty pattern matches anywhere for free).
       Endpoints with dp[m][j] ≤ max-dist are clustered (within m
       positions of each other); we keep the lowest-distance endpoint
       per cluster.
     - Phase 2: for each endpoint, extract a window and run a full
       O(n·m) DP on the window to recover the start position via
       traceback.

   Complexity:
     - O(n·m) time, O(m) space for the main scan.
     - O(m²) per match for verification.

   Limitations:
     - Costs are uniform (every edit is 1). No transposition / phonetic
       weighting.
     - Indexing is in Java chars (UTF-16 code units), not codepoints.
       For BMP-only inputs (ASCII, Latin, most CJK) this is faithful.
       Non-BMP (emoji, ancient scripts) split into surrogate pairs.

   Public API:
     `find-all` — return all matches as `[{:start :end :distance :text}]`
                  ordered by appearance in the text.
     `best`     — convenience: lowest-distance match, or nil."
  (:require [clojure.string :as str]))

;; ============================================================================
;; Normalization
;; ============================================================================

(defn- normalize
  "Returns `{:normed s :pos-map idx-vec :original nfc}` where
   - :normed   the lowercased + ws-collapsed normalized string used for
               distance calculation;
   - :pos-map  pos-map[i] = char index into :original of the i-th
               character of :normed (the first non-space char of a
               collapsed whitespace run);
   - :original the NFC-normalized input — same case + whitespace as the
               caller passed in (modulo NFC composition)."
  [^String s]
  (let [nfc (java.text.Normalizer/normalize s java.text.Normalizer$Form/NFC)
        len (.length nfc)]
    (loop [i 0
           normed (StringBuilder.)
           pos-map (transient [])
           in-space? false]
      (if (>= i len)
        {:normed   (.toString normed)
         :pos-map  (int-array (persistent! pos-map))
         :original nfc}
        (let [c (.charAt nfc i)]
          (cond
            (Character/isWhitespace c)
            (if in-space?
              (recur (inc i) normed pos-map true)
              (recur (inc i)
                     (.append normed \space)
                     (conj! pos-map i)
                     true))
            :else
            (recur (inc i)
                   (.append normed (Character/toLowerCase c))
                   (conj! pos-map i)
                   false)))))))

(defn- map-orig-end
  "Map a normalized end index (exclusive) back to a char index in the
   NFC-original. Past-the-end maps to original length, which naturally
   includes any trailing collapsed whitespace run in the matched span."
  [norm-end ^ints pos-map ^String original]
  (if (>= norm-end (alength pos-map))
    (.length original)
    (aget pos-map norm-end)))

;; ============================================================================
;; Phase 1 — rolling DP scan, finds endpoint clusters.
;; ============================================================================

(defn- scan-endpoints
  "Returns `[[end-index distance] ...]` for the lowest-distance endpoint
   of each cluster (clusters of endpoints within `m` positions are
   collapsed). All distances are ≤ max-dist."
  [^String t ^String p ^long max-dist]
  (let [n (.length t) m (.length p)]
    (if (zero? m)
      []
      (let [^ints prev (int-array (inc m))
            ^ints curr (int-array (inc m))
            *clusters (transient [])
            ;; cluster-state: [last-end best-dist best-j]
            *cluster (volatile! [(- (- m) 1) (inc max-dist) 0])
            flush! (fn []
                     (let [[_ bd bj] @*cluster]
                       (when (<= bd max-dist)
                         (conj! *clusters [bj bd]))))]
        (dotimes [i (inc m)] (aset prev i i))
        (loop [j 1]
          (when (<= j n)
            (aset curr 0 0)
            (let [tc (.charAt t (dec j))]
              (loop [i 1]
                (when (<= i m)
                  (let [pc   (.charAt p (dec i))
                        cost (if (= pc tc) 0 1)]
                    (aset curr i (min (+ (aget prev (dec i)) cost)
                                      (+ (aget prev i) 1)
                                      (+ (aget curr (dec i)) 1))))
                  (recur (inc i)))))
            (let [d (aget curr m)]
              (when (<= d max-dist)
                (let [[last-end bd _] @*cluster]
                  (cond
                    (> (- j last-end) m)
                    (do (flush!)
                        (vreset! *cluster [j d j]))
                    (< d bd)
                    (vreset! *cluster [j d j])
                    :else
                    (vswap! *cluster (fn [[_ bd bj]] [j bd bj]))))))
            (System/arraycopy curr 0 prev 0 (inc m))
            (recur (inc j))))
        (flush!)
        (persistent! *clusters)))))

;; ============================================================================
;; Phase 2 — full DP on a window, traceback to start position.
;; ============================================================================

(defn- window-trace
  "Run full-matrix DP on `window` against `p`, find the best endpoint,
   and traceback to the start. Returns `[start-in-window distance]`."
  [^String window ^String p]
  (let [m (.length p) n (.length window)]
    (if (or (zero? m) (zero? n))
      [0 m]
      (let [dp (make-array Integer/TYPE (inc m) (inc n))]
        (dotimes [i (inc m)] (aset-int dp i 0 i))
        (loop [i 1]
          (when (<= i m)
            (let [pc (.charAt p (dec i))]
              (loop [j 1]
                (when (<= j n)
                  (let [tc (.charAt window (dec j))
                        cost (if (= pc tc) 0 1)
                        diag (+ (aget ^"[[I" dp (dec i) (dec j)) cost)
                        up   (inc (aget ^"[[I" dp (dec i) j))
                        left (inc (aget ^"[[I" dp i (dec j)))]
                    (aset-int dp i j (min diag up left)))
                  (recur (inc j)))))
            (recur (inc i))))
        (let [[best-j best-d]
              (loop [j 1 bj 0 bd (inc m)]
                (if (> j n)
                  [bj bd]
                  (let [v (aget ^"[[I" dp m j)]
                    (if (< v bd)
                      (recur (inc j) j v)
                      (recur (inc j) bj bd)))))
              ;; traceback from (m, best-j) to row 0; prefer diag > up > left
              start
              (loop [i m j best-j]
                (if (or (zero? i) (zero? j))
                  j
                  (let [pc (.charAt p (dec i))
                        tc (.charAt window (dec j))
                        cost (if (= pc tc) 0 1)
                        v (aget ^"[[I" dp i j)]
                    (cond
                      (= v (+ (aget ^"[[I" dp (dec i) (dec j)) cost))
                      (recur (dec i) (dec j))
                      (= v (inc (aget ^"[[I" dp (dec i) j)))
                      (recur (dec i) j)
                      :else
                      (recur i (dec j))))))]
          [start best-d])))))

;; ============================================================================
;; Public API
;; ============================================================================

(defn find-all
  "Return all matches of `pattern` in `text` with edit distance ≤
   `max-dist`. Each match is a map `{:start :end :distance :text}` whose
   `:start` and `:end` are char indices into the NFC-normalized `text`.

   The match span includes any collapsed-whitespace runs that bracket
   the matched content (whitespace differences are free under
   normalization, so reporting the literal range is the principled
   thing). Always returns a vector — empty for blank patterns, nil
   text, or no matches under budget."
  [^String text ^String pattern ^long max-dist]
  (if (or (str/blank? pattern) (nil? text))
    []
    (let [{tn :normed t-pos :pos-map t-orig :original} (normalize text)
          {pn :normed} (normalize pattern)
          clusters (scan-endpoints tn pn max-dist)]
      (vec
       (for [[end d-cluster] clusters
             :let [m (.length ^String pn)
                   ;; window can be at most m + dist runes (all edits are insertions)
                   win-start (max 0 (- end m d-cluster 1))
                   window    (.substring ^String tn win-start end)
                   [s-in-win best-d] (window-trace window pn)
                   norm-start (+ s-in-win win-start)
                   norm-end   end
                   o-start    (aget ^ints t-pos norm-start)
                   o-end      (map-orig-end norm-end t-pos t-orig)]]
         {:start    o-start
          :end      o-end
          :distance best-d
          :text     (.substring ^String t-orig o-start o-end)})))))

(defn best
  "Convenience: lowest-distance match of `pattern` in `text`, or nil."
  [text pattern max-dist]
  (when-let [ms (seq (find-all text pattern max-dist))]
    (apply min-key :distance ms)))

(defn matches?
  "True iff `pattern` appears in `text` with edit distance ≤ `max-dist`."
  [text pattern max-dist]
  (boolean (seq (find-all text pattern max-dist))))
