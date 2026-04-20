(ns toolkit.lmdb-test
  (:require [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [toolkit.lmdb :as lmdb])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn-  temp-dir ^File []
  (-> (Files/createTempDirectory "lmdb-test-" (into-array FileAttribute []))
      .toFile))

(defn- delete-tree [^File f]
  (when (.isDirectory f)
    (doseq [c (.listFiles f)] (delete-tree c)))
  (.delete f))

(defmacro ^:private with-env
  "Create a fresh temp-dir env, bind to env-sym for body, close+delete after."
  [[env-sym opts] & body]
  `(let [dir#   (temp-dir)
         ~env-sym (lmdb/open-env (.getAbsolutePath dir#) ~opts)]
     (try ~@body
          (finally
            (lmdb/close-env ~env-sym)
            (delete-tree dir#)))))

(def ^:private ss {:key-codec lmdb/string-codec :val-codec lmdb/string-codec})

(deftest string-codec-round-trip
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/put! env d "hello" "world")
      (is (= "world" (lmdb/get env d "hello")))
      (is (nil? (lmdb/get env d "absent"))))))

(deftest raw-codec-round-trip
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" nil)]
      (lmdb/put! env d (byte-array [1 2 3]) (byte-array [10 20 30]))
      (is (= [10 20 30] (vec (lmdb/get env d (byte-array [1 2 3]))))))))

(deftest long-be-codec-round-trip
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" {:key-codec lmdb/long-be-codec
                                    :val-codec lmdb/long-be-codec})]
      (lmdb/put! env d 42 12345)
      (is (= 12345 (lmdb/get env d 42)))
      (is (nil? (lmdb/get env d 99))))))

(deftest exists-and-delete
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/put! env d "a" "1")
      (is (true?  (lmdb/exists? env d "a")))
      (is (false? (lmdb/exists? env d "nope")))
      (is (true?  (lmdb/delete! env d "a")))
      (is (false? (lmdb/delete! env d "a")) "second delete on absent key is false")
      (is (nil?   (lmdb/get env d "a"))))))

(deftest with-txn-write-commits-on-success
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (lmdb/put! t d "a" "1")
        (lmdb/put! t d "b" "2"))
      (is (= "1" (lmdb/get env d "a")))
      (is (= "2" (lmdb/get env d "b"))))))

(deftest with-txn-write-aborts-on-throw
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (try
        (lmdb/with-txn [t env :write]
          (lmdb/put! t d "a" "1")
          (throw (ex-info "boom" {})))
        (catch clojure.lang.ExceptionInfo _))
      (is (nil? (lmdb/get env d "a")) "txn aborted on throw — nothing committed"))))

(deftest multi-dbi-atomic-write
  (with-env [env {:max-dbs 4}]
    (let [users    (lmdb/open-dbi env "users"    ss)
          by-email (lmdb/open-dbi env "by-email" ss)]
      (lmdb/with-txn [t env :write]
        (lmdb/put! t users    "alice"      "{:age 30}")
        (lmdb/put! t by-email "a@x.com"    "alice"))
      (is (= "{:age 30}" (lmdb/get env users    "alice")))
      (is (= "alice"     (lmdb/get env by-email "a@x.com"))))))

(deftest reduce-range-all-ordering
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["c" "a" "b" "d"]]
          (lmdb/put! t d k (str "v-" k))))
      (is (= [["a" "v-a"] ["b" "v-b"] ["c" "v-c"] ["d" "v-d"]]
             (mapv vec (lmdb/scan env d (lmdb/all)))))
      (is (= [["d" "v-d"] ["c" "v-c"] ["b" "v-b"] ["a" "v-a"]]
             (mapv vec (lmdb/scan env d (lmdb/all-backward))))))))

(deftest reduce-range-closed-inclusivity
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["a" "b" "c" "d" "e"]]
          (lmdb/put! t d k k)))
      (is (= ["b" "c" "d"]
             (map first (lmdb/scan env d (lmdb/closed "b" "d")))))
      (is (= ["b" "c"]
             (map first (lmdb/scan env d (lmdb/closed-open "b" "d")))))
      (is (= ["c" "d"]
             (map first (lmdb/scan env d (lmdb/open-closed "b" "d")))))
      (is (= ["c"]
             (map first (lmdb/scan env d (lmdb/open "b" "d"))))))))

(deftest reduce-range-prefix
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["user:alice" "user:bob" "user:zed" "widget:1" "zzz"]]
          (lmdb/put! t d k "v")))
      (is (= ["user:alice" "user:bob" "user:zed"]
             (map first (lmdb/scan env d (lmdb/prefix "user:")))))
      (is (= 3 (lmdb/count-range env d (lmdb/prefix "user:")))))))

(deftest reduce-range-early-termination
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["a" "b" "c" "d" "e"]] (lmdb/put! t d k k)))
      (is (= ["a" "b" "c"]
             (lmdb/reduce-range env d (lmdb/all)
                                (fn [acc k _]
                                  (if (= k "d")
                                    (reduced acc)
                                    (conj acc k)))
                                []))))))

(deftest count-range-basic
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["a" "b" "c"]] (lmdb/put! t d k "v")))
      (is (= 3 (lmdb/count-range env d (lmdb/all)))))))

(deftest component-persists-across-restart
  (let [dir   (temp-dir)
        path  (.getAbsolutePath dir)
        specs {:path      path
               :env-opts  {:max-dbs 4}
               :dbi-specs [{:name "users"
                            :key-codec lmdb/string-codec
                            :val-codec lmdb/string-codec}]}]
    (try
      (let [s (component/start (lmdb/map->Store specs))]
        (lmdb/put! (:env s) (lmdb/dbi s "users") "alice" "30")
        (component/stop s))
      (let [s (component/start (lmdb/map->Store specs))]
        (is (= "30" (lmdb/get (:env s) (lmdb/dbi s "users") "alice"))
            "data written in one lifecycle is readable after restart")
        (component/stop s))
      (finally
        (delete-tree dir)))))

;; --- wrapper-owned correctness (not just "forwards to Java") ----------------

(deftest long-be-codec-numeric-ordering
  ;; The docstring promises byte-wise comparison over the big-endian encoding
  ;; matches numeric order for non-negative longs. A lex-over-decimal-string
  ;; bug (e.g. "10" < "2") would show up here. Assert on the ordered vector,
  ;; not a set — multiplicity and order are part of the spec.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" {:key-codec lmdb/long-be-codec
                                    :val-codec lmdb/long-be-codec})]
      (doseq [k [10 2 1 100 20 3 1000 0 500]]
        (lmdb/put! env d (long k) (long k)))
      (is (= [0 1 2 3 10 20 100 500 1000]
             (mapv first (lmdb/scan env d (lmdb/all))))))))

(deftest prefix-terminal-ff-byte
  ;; Prefix ending in 0xFF: byte-successor must bump an earlier byte.
  ;; Assert the scan returns only the keys that begin with [0xAA 0xFF].
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" nil)
          put (fn [t k] (lmdb/put! t d (byte-array k) (byte-array [0])))]
      (lmdb/with-txn [t env :write]
        (put t [0xAA 0xFE 0x00])           ; just below the prefix
        (put t [0xAA 0xFF 0x00])           ; in-prefix
        (put t [0xAA 0xFF 0xFF])           ; in-prefix
        (put t [0xAB 0x00 0x00]))          ; just past the prefix
      (is (= [[0xAA 0xFF 0x00] [0xAA 0xFF 0xFF]]
             (->> (lmdb/scan env d (lmdb/prefix (byte-array [0xAA 0xFF])))
                  (map first)
                  (map #(mapv (fn [b] (bit-and b 0xff)) %))))))))

(deftest prefix-all-ff
  ;; byte-successor returns nil on all-0xFF; range->lmdb must fall through
  ;; to `atLeast`, which should still include the prefixed keys.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" nil)
          put (fn [t k] (lmdb/put! t d (byte-array k) (byte-array [0])))]
      (lmdb/with-txn [t env :write]
        (put t [0xFE 0xFF])                ; before prefix
        (put t [0xFF 0xFF 0x00])           ; in-prefix
        (put t [0xFF 0xFF 0xFF]))          ; in-prefix
      (is (= 2 (lmdb/count-range env d (lmdb/prefix (byte-array [0xFF 0xFF]))))))))

(deftest byte-copy-out-safety
  ;; The wrapper's docstring promises values returned from range ops are
  ;; safe to use after the txn closes — `buf->bytes` must have copied, not
  ;; just duplicated, the backing buffer. Escape the pairs out of the txn
  ;; and assert them after close.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["a" "b" "c"]] (lmdb/put! t d k (str "v-" k))))
      (let [escaped (lmdb/with-txn [t env :read]
                      (lmdb/scan t d (lmdb/all)))]
        (is (= [["a" "v-a"] ["b" "v-b"] ["c" "v-c"]] (mapv vec escaped))
            "pairs remain readable after the txn closed")))))

(deftest put-overwrite-replaces
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/put! env d "k" "v1")
      (lmdb/put! env d "k" "v2")
      (is (= "v2" (lmdb/get env d "k"))
          "second put replaces; no NOOVERWRITE flag is set by default"))))

(deftest explicit-txn-forms
  ;; Run get/exists?/delete!/put! all inside one write txn so we know the
  ;; Txn-as-ctx branch of with-ctx-txn works.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (lmdb/put! t d "a" "1")
        (is (= "1" (lmdb/get t d "a")))
        (is (true?  (lmdb/exists? t d "a")))
        (is (false? (lmdb/exists? t d "missing")))
        (is (true?  (lmdb/delete! t d "a")))
        (is (false? (lmdb/delete! t d "a")))
        (is (nil?   (lmdb/get t d "a")))))))

(deftest unknown-flag-throws
  (with-env [env nil]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unknown env flag"
                          (lmdb/open-env (.getAbsolutePath (temp-dir)) {:flags #{:bogus}})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unknown dbi flag"
                          (lmdb/open-dbi env "d" {:flags #{:bogus}})))))

(deftest component-double-stop-is-safe
  (let [dir   (temp-dir)
        specs {:path      (.getAbsolutePath dir)
               :dbi-specs [{:name "d"}]}]
    (try
      (let [s1 (component/start (lmdb/map->Store specs))
            s2 (component/stop s1)]
        (is (nil? (:env s2)))
        (is (= {} (:dbis s2)))
        (component/stop s2))                 ; double-stop must not throw
      (finally
        (delete-tree dir)))))

;; --- env-or-txn dispatch ----------------------------------------------------

(deftest env-and-txn-ctx-both-work
  ;; Every op must accept both an Env (one-shot) and a Txn (batched).
  ;; They can't be used interleaved on the same thread because LMDB only
  ;; allows one read txn per thread — so test each branch separately.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      ;; Env-ctx path
      (lmdb/put! env d "k" "v")
      (is (= "v"   (lmdb/get env d "k")))
      (is (true?   (lmdb/exists? env d "k")))
      (is (true?   (lmdb/delete! env d "k")))
      (is (false?  (lmdb/exists? env d "k")))
      ;; Txn-ctx path (reuses one open txn)
      (lmdb/with-txn [t env :write]
        (lmdb/put! t d "q" "r")
        (is (= "r" (lmdb/get t d "q")))
        (is (true? (lmdb/exists? t d "q"))))
      (is (= "r" (lmdb/get env d "q")) "write committed on with-txn exit"))))

(deftest write-op-on-read-txn-rejected
  ;; Using a read txn as ctx for a write op should fail fast with a
  ;; clear message, not bubble up an opaque lmdbjava stack.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/put! env d "k" "v")
      (lmdb/with-txn [t env :read]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"write op requires a write txn"
                              (lmdb/put! t d "x" "y")))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"write op requires a write txn"
                              (lmdb/delete! t d "k")))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"write op requires a write txn"
                              (lmdb/clear-dbi! t d)))))))

;; --- get not-found arity ----------------------------------------------------

(deftest get-not-found-sentinel
  (with-env [env nil]
    ;; A decode that can produce nil — proves nil-hit is distinguished from miss.
    (let [nillable {:encode (fn [^String s] (.getBytes s "UTF-8"))
                    :decode (fn [^bytes b]
                              (let [s (String. b "UTF-8")]
                                (when-not (= s "") s)))}
          d        (lmdb/open-dbi env "d" {:key-codec lmdb/string-codec
                                           :val-codec nillable})]
      (lmdb/put! env d "empty" "")
      (lmdb/put! env d "real"  "hello")
      (is (= "hello"   (lmdb/get env d "real" ::miss)))
      (is (= nil       (lmdb/get env d "empty" ::miss))
          "key present, decoded value is nil — return the decoded nil, not the sentinel")
      (is (= ::miss    (lmdb/get env d "absent" ::miss))
          "key absent — return the sentinel")
      (is (= nil       (lmdb/get env d "absent"))
          "2-arg form falls back to nil"))))

;; --- put-new! (conditional insert; replaces :no-overwrite? opt) -------------

(deftest put-new-returns-true-when-absent
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (is (true? (lmdb/put-new! env d "k" "v")))
      (is (= "v" (lmdb/get env d "k"))))))

(deftest put-new-returns-false-and-preserves-when-present
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/put! env d "k" "v1")
      (is (false? (lmdb/put-new! env d "k" "v2")))
      (is (= "v1" (lmdb/get env d "k"))))))

(deftest put-new-works-inside-with-txn
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (is (true?  (lmdb/put-new! t d "a" "1")))
        (is (false? (lmdb/put-new! t d "a" "2"))))
      (is (= "1" (lmdb/get env d "a"))))))

;; --- bulk-append! (MDB_APPEND fast path) ------------------------------------

(deftest bulk-append-sorted-vector-round-trip
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/bulk-append! env d [["a" "1"] ["b" "2"] ["c" "3"]])
      (is (= [["a" "1"] ["b" "2"] ["c" "3"]]
             (mapv vec (lmdb/scan env d (lmdb/all))))))))

(deftest bulk-append-unsorted-input-sorted-internally
  ;; Default path: sort by encoded-byte order so callers hand in any order.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/bulk-append! env d [["c" "3"] ["a" "1"] ["b" "2"]])
      (is (= ["a" "b" "c"] (mapv first (lmdb/scan env d (lmdb/all))))))))

(deftest bulk-append-accepts-any-reducible
  ;; A Clojure map reduces to MapEntry pairs — should Just Work.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/bulk-append! env d {"z" "26" "a" "1" "m" "13"})
      (is (= ["a" "m" "z"] (mapv first (lmdb/scan env d (lmdb/all))))))))

(deftest bulk-append-empty-input-noop
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/bulk-append! env d [])
      (is (zero? (lmdb/count-range env d (lmdb/all)))))))

(deftest bulk-append-single-pair
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/bulk-append! env d [["only" "one"]])
      (is (= "one" (lmdb/get env d "only"))))))

(deftest bulk-append-presorted-happy-path
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/bulk-append! env d [["a" "1"] ["b" "2"] ["c" "3"]] {:presorted? true})
      (is (= 3 (lmdb/count-range env d (lmdb/all)))))))

(deftest bulk-append-presorted-liar-throws-and-aborts
  ;; :presorted? opt-out with actually-unsorted input: LMDB rejects the
  ;; out-of-order put, our with-txn scope aborts the whole batch.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (is (thrown? org.lmdbjava.Dbi$KeyExistsException
                   (lmdb/bulk-append! env d
                                      [["a" "1"] ["c" "3"] ["b" "2"]]
                                      {:presorted? true})))
      (is (zero? (lmdb/count-range env d (lmdb/all)))
          "txn aborted on throw — no partial writes committed"))))

(deftest bulk-append-into-populated-dbi-beyond-max
  ;; Pre-existing keys all strictly less than the batch — allowed.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/put! env d "a" "1")
      (lmdb/bulk-append! env d [["b" "2"] ["c" "3"]])
      (is (= 3 (lmdb/count-range env d (lmdb/all)))))))

(deftest bulk-append-into-populated-dbi-overlapping-aborts
  ;; Pre-existing key falls inside the batch range — MDB_APPEND refuses.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/put! env d "m" "mid")
      (is (thrown? org.lmdbjava.Dbi$KeyExistsException
                   (lmdb/bulk-append! env d [["a" "1"] ["b" "2"]])))
      (is (= {"m" "mid"} (into {} (lmdb/range-reducible env d (lmdb/all))))
          "only the pre-existing key remains; batch fully aborted"))))

(deftest bulk-append-numeric-codec
  ;; long-be-codec: encoded-byte order matches numeric order for
  ;; non-negative longs. Pass values out of numeric order; expect
  ;; numeric-ordered output after the internal sort.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" {:key-codec lmdb/long-be-codec
                                    :val-codec lmdb/long-be-codec})]
      (lmdb/bulk-append! env d [[100 100] [1 1] [1000 1000] [0 0] [5 5]])
      (is (= [0 1 5 100 1000] (mapv first (lmdb/scan env d (lmdb/all))))))))

(deftest bulk-append-accepts-range-reducible-input
  ;; Copy data between dbis by piping a range-reducible into bulk-append!.
  (with-env [env {:max-dbs 4}]
    (let [src (lmdb/open-dbi env "src" ss)
          dst (lmdb/open-dbi env "dst" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["a" "b" "c" "d" "e"]] (lmdb/put! t src k (str "v-" k))))
      (lmdb/bulk-append! env dst (lmdb/range-reducible env src (lmdb/all)))
      (is (= 5 (lmdb/count-range env dst (lmdb/all))))
      (is (= "v-c" (lmdb/get env dst "c"))))))

(deftest bulk-append-inside-with-txn-uses-callers-txn
  ;; The safety rewrite: bulk-append! accepts a Txn ctx, so it can run
  ;; inside an existing write txn without nesting / deadlock. A caller's
  ;; normal put and the bulk batch commit together.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (lmdb/put!         t d "a" "first")
        (lmdb/bulk-append! t d [["b" "2"] ["c" "3"]]))
      (is (= ["a" "b" "c"] (mapv first (lmdb/scan env d (lmdb/all))))
          "caller's put and bulk-append committed together"))))

(deftest bulk-append-rolls-back-with-callers-txn
  ;; When the caller's write txn aborts, the appended keys roll back too.
  ;; This is the reason to pass the caller's Txn rather than open our own.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (try
        (lmdb/with-txn [t env :write]
          (lmdb/bulk-append! t d [["a" "1"] ["b" "2"]])
          (throw (ex-info "boom" {})))
        (catch clojure.lang.ExceptionInfo _))
      (is (zero? (lmdb/count-range env d (lmdb/all)))
          "caller's txn aborted on throw — batch rolled back with it"))))

(deftest bulk-append-on-read-txn-rejected
  ;; Txn-ctx dispatch checks mode: a read txn fails fast, no opaque
  ;; lmdbjava stack.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :read]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"write op requires a write txn"
                              (lmdb/bulk-append! t d [["a" "1"]])))))))

;; --- range-reducible --------------------------------------------------------

(deftest range-reducible-composes-with-transducers
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["a" "b" "c" "d" "e"]] (lmdb/put! t d k (str "v-" k))))
      (let [r (lmdb/range-reducible env d (lmdb/all))]
        (is (= ["a" "b" "c"]
               (into [] (comp (take 3) (map first)) r))
            "take + map first transducer stack")
        (is (= ["v-a" "v-b" "v-c" "v-d" "v-e"]
               (transduce (map second) conj [] r))
            "transduce with map second")
        (is (= {"a" "v-a" "b" "v-b" "c" "v-c" "d" "v-d" "e" "v-e"}
               (into {} r))
            "MapEntry yield: (into {} ...) builds a map directly")))))

(deftest range-reducible-re-reduce-yields-fresh-snapshot
  ;; Each reduce call opens its own txn; two sequential reductions each see
  ;; the latest committed state.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)
          r (lmdb/range-reducible env d (lmdb/all))]
      (lmdb/put! env d "a" "1")
      (is (= 1 (reduce (fn [n _] (inc n)) 0 r)))
      (lmdb/put! env d "b" "2")
      (is (= 2 (reduce (fn [n _] (inc n)) 0 r))
          "second reduce sees the newly-committed key"))))

(deftest range-reducible-short-circuit-closes-txn
  ;; After many short-circuited reductions, num-readers in env-info must
  ;; not accumulate — if the txn leaked, the reader slot would stay busy.
  (with-env [env {:max-readers 8}]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k (range 200)] (lmdb/put! t d (format "k%04d" k) "v")))
      (let [r (lmdb/range-reducible env d (lmdb/all))]
        (dotimes [_ 20]                       ; way more than :max-readers
          (is (= 1 (count (into [] (take 1) r)))))))))

(deftest range-reducible-escape-after-close
  ;; Pairs yielded must survive their txn closing, same as scan.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["a" "b" "c"]] (lmdb/put! t d k (str "v-" k))))
      (let [pairs (into [] (lmdb/range-reducible env d (lmdb/all)))]
        (is (= [["a" "v-a"] ["b" "v-b"] ["c" "v-c"]]
               (mapv vec pairs)))))))

(deftest range-reducible-with-supplied-write-txn
  ;; Iteration inside a write txn should work (cursors run under write txns)
  ;; and must not close the caller's txn on exit.
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["a" "b"]] (lmdb/put! t d k "v"))
        (is (= 2 (count (into [] (lmdb/range-reducible t d (lmdb/all))))))
        ;; The txn is still live — we can keep using it.
        (lmdb/put! t d "c" "v"))
      (is (= 3 (lmdb/count-range env d (lmdb/all))) "final commit included all 3"))))

;; --- env-stats / env-info / dbi-stats ---------------------------------------

(deftest stats-round-trip
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [i (range 50)] (lmdb/put! t d (format "k%02d" i) "v")))
      (is (= 50 (:entries (lmdb/dbi-stats env d))))
      (let [info (lmdb/env-info env)]
        (is (pos? (:map-size info)))
        (is (>= (:max-readers info) 0)))
      (let [es (lmdb/env-stats env)]
        (is (pos? (:page-size es)))))))

(deftest env-info-reports-readers-inside-txn
  (with-env [env nil]
    (let [_ (lmdb/open-dbi env "d" ss)]
      (is (zero? (:num-readers (lmdb/env-info env))))
      (lmdb/with-txn [_ env :read]
        (is (= 1 (:num-readers (lmdb/env-info env))))))))

;; --- sync! / copy-env! / clear-dbi! / drop-dbi! / reader-check --------------

(deftest sync-does-not-throw
  (with-env [env {:flags #{:no-sync}}]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/put! env d "k" "v")
      (lmdb/sync! env)
      (lmdb/sync! env true)
      (is (= "v" (lmdb/get env d "k"))))))

(deftest copy-env-round-trip
  (let [src-dir  (temp-dir)
        dest-dir (temp-dir)
        _        (delete-tree dest-dir)                ; copy wants dest to not exist
        _        (.mkdirs dest-dir)]
    (try
      (let [src (lmdb/open-env (.getAbsolutePath src-dir) nil)
            d   (lmdb/open-dbi src "d" ss)]
        (try
          (lmdb/with-txn [t src :write]
            (doseq [k ["a" "b" "c"]] (lmdb/put! t d k (str "v-" k))))
          (lmdb/copy-env! src (.getAbsolutePath dest-dir))
          (finally (lmdb/close-env src))))
      (let [dest (lmdb/open-env (.getAbsolutePath dest-dir) nil)
            d    (lmdb/open-dbi dest "d" (assoc ss :create? false))]
        (try
          (is (= [["a" "v-a"] ["b" "v-b"] ["c" "v-c"]]
                 (mapv vec (lmdb/scan dest d (lmdb/all))))
              "destination env has the same kv pairs")
          (finally (lmdb/close-env dest))))
      (finally
        (delete-tree src-dir)
        (delete-tree dest-dir)))))

(deftest copy-env-compact
  (let [src-dir  (temp-dir)
        dest-dir (temp-dir)
        _        (delete-tree dest-dir)
        _        (.mkdirs dest-dir)]
    (try
      (let [src (lmdb/open-env (.getAbsolutePath src-dir) nil)
            d   (lmdb/open-dbi src "d" ss)]
        (try
          (lmdb/put! src d "k" "v")
          (lmdb/copy-env! src (.getAbsolutePath dest-dir) {:compact? true})
          (finally (lmdb/close-env src))))
      (let [dest (lmdb/open-env (.getAbsolutePath dest-dir) nil)
            d    (lmdb/open-dbi dest "d" (assoc ss :create? false))]
        (try
          (is (= "v" (lmdb/get dest d "k")))
          (finally (lmdb/close-env dest))))
      (finally
        (delete-tree src-dir)
        (delete-tree dest-dir)))))

(deftest clear-dbi-empties-but-keeps-dbi
  (with-env [env nil]
    (let [d (lmdb/open-dbi env "d" ss)]
      (lmdb/with-txn [t env :write]
        (doseq [k ["a" "b" "c"]] (lmdb/put! t d k "v")))
      (is (= 3 (lmdb/count-range env d (lmdb/all))))
      (lmdb/clear-dbi! env d)
      (is (= 0 (lmdb/count-range env d (lmdb/all))))
      ;; Dbi handle is still usable for subsequent writes.
      (lmdb/put! env d "new" "v")
      (is (= "v" (lmdb/get env d "new"))))))

(deftest reader-check-returns-int
  (with-env [env nil]
    (is (integer? (lmdb/reader-check env)))))
