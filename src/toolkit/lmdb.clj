(ns toolkit.lmdb
  "Small wrapper over `org.lmdbjava/lmdbjava`. Opens an Env (create or open)
   at a directory, opens one or more named Dbis inside it, and transacts.

   Shape:
     - `open-env` / `close-env` — env lifecycle. Multiple envs at once OK.
     - `open-dbi`               — long-lived Dbi handles. Dbis don't need
                                  closing; env close takes them all down.
     - `with-txn [t env mode]`  — unified read/write txn. A single write
                                  txn can update multiple Dbis atomically.
     - `put!` / `get` / `exists?` / `delete!` — all take a *context* as
       the first arg: either an `Env` (opens a one-shot txn) or a `Txn`
       (reuses the one you already have). Same call shape either way.
     - `put-new!`                  — insert-if-absent; returns bool.
     - `bulk-append!`              — MDB_APPEND fast path for sorted
                                     batches; takes any reducible.
     - Range scans via `range-reducible` (composes with `reduce`/
       `transduce`/`into`), `reduce-range` (fast path with `(fn [acc k v])`
       reducer), `scan` (eager vector), `count-range`. All take a context
       and a range spec (`(all)`, `(closed a b)`, `(prefix k)`, ...).
     - `Store` — Stuart Sierra component wrapping an env + a map of opened
       Dbis keyed by name.

   Codecs:
     - A codec is `{:encode (fn [v] byte-array) :decode (fn [byte-array] v)}`.
     - Defaults to `raw-codec` (identity on `byte[]`). Bring nippy/fressian
       yourself if you want arbitrary Clojure values.

   LMDB gotchas worth knowing:
     - `map-size` is a hard ceiling; pick generously (it's address space,
       not disk). Hitting it throws MapFullException.
     - Keys default to 511 bytes max; values are unbounded.
     - Never let a cursor-derived value escape its txn — the backing
       memory is invalid after close. This wrapper copies bytes out on
       every read path, so user-facing values are safe.
     - Read txns pin the MVCC snapshot; don't hold them across long work."
  (:refer-clojure :exclude [get])
  (:require [clojure.java.io :as io]
            [com.stuartsierra.component :as component])
  (:import [org.lmdbjava Env EnvFlags DbiFlags Dbi Txn KeyRange PutFlags
            CopyFlags Stat EnvInfo
            CursorIterable CursorIterable$KeyVal]
           [clojure.lang MapEntry]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(def ^:private ^"[Lorg.lmdbjava.PutFlags;" no-put-flags (into-array PutFlags []))
(def ^:private ^"[Lorg.lmdbjava.PutFlags;" no-overwrite-flags
  (into-array PutFlags [(PutFlags/valueOf "MDB_NOOVERWRITE")]))
(def ^:private ^"[Lorg.lmdbjava.PutFlags;" append-flags
  (into-array PutFlags [(PutFlags/valueOf "MDB_APPEND")]))

(defn- compare-bytes-unsigned ^long [^bytes a ^bytes b]
  (java.util.Arrays/compareUnsigned a b))

;; --- buffer helpers ---------------------------------------------------------

(defn- ->direct-buf ^ByteBuffer [^bytes bs]
  (let [n (alength bs)
        b (ByteBuffer/allocateDirect n)]
    (.put b bs)
    (.flip b)
    b))

(defn- buf->bytes ^"[B" [^ByteBuffer b]
  ;; duplicate so we don't disturb LMDB's position/limit on the shared buffer
  (let [d  (.duplicate b)
        n  (.remaining d)
        bs (byte-array n)]
    (.get d bs)
    bs))

;; --- codecs -----------------------------------------------------------------
;;
;; A codec encodes a value into a byte[] and decodes it back. The encoded
;; bytes are what LMDB stores and — critically — what it *compares*: all
;; range iteration, prefix scans, and `bulk-append!`'s internal sort use
;; byte-wise comparison over the encoded form. If you write a custom codec,
;; its encoded byte order must match the semantic order you want keys to
;; sort in, or range ops will silently return results in the wrong order.
;; See `long-be-codec` for a worked example of this tradeoff.

(def raw-codec
  "byte[] in, byte[] out. The default; LMDB's native shape."
  {:encode identity
   :decode identity})

(def string-codec
  "UTF-8 string ↔ byte[]. Safe for ASCII-prefixable keys; note that
   non-ASCII UTF-8 sorts correctly under LMDB's default byte-wise
   comparator as long as both sides encode UTF-8."
  {:encode (fn [^String s] (.getBytes s StandardCharsets/UTF_8))
   :decode (fn [^bytes b]  (String. b StandardCharsets/UTF_8))})

(def long-be-codec
  "Fixed 8-byte big-endian long. Big-endian so LMDB's byte-wise
   comparator orders non-negative longs numerically. Negative values
   sort *after* non-negative ones under byte comparison — if you need
   signed ordering, write a codec that XORs the sign bit."
  {:encode (fn [^long n]
             (let [a (byte-array 8)]
               (doto (ByteBuffer/wrap a) (.putLong n))
               a))
   :decode (fn [^bytes b]
             (.getLong (ByteBuffer/wrap b)))})

;; --- flags ------------------------------------------------------------------

;; Enum constants are looked up via `valueOf` rather than `EnvFlags/MDB_*`
;; because under Clojure 1.12 the latter is parsed as a qualified method
;; reference, which collides with static-field access on enum types.

(def ^:private env-flag
  {:read-only    (EnvFlags/valueOf "MDB_RDONLY_ENV")
   :no-sync      (EnvFlags/valueOf "MDB_NOSYNC")
   :no-meta-sync (EnvFlags/valueOf "MDB_NOMETASYNC")
   :write-map    (EnvFlags/valueOf "MDB_WRITEMAP")
   :map-async    (EnvFlags/valueOf "MDB_MAPASYNC")
   :no-lock      (EnvFlags/valueOf "MDB_NOLOCK")
   :no-subdir    (EnvFlags/valueOf "MDB_NOSUBDIR")
   :no-readahead (EnvFlags/valueOf "MDB_NORDAHEAD")
   :no-meminit   (EnvFlags/valueOf "MDB_NOMEMINIT")})

(defn- env-flag-array [flags]
  (into-array EnvFlags
              (map (fn [f]
                     (or (env-flag f)
                         (throw (ex-info "unknown env flag"
                                         {:flag f :known (set (keys env-flag))}))))
                   flags)))

(def ^:private dbi-flag
  {:create      (DbiFlags/valueOf "MDB_CREATE")
   :reverse-key (DbiFlags/valueOf "MDB_REVERSEKEY")})

(defn- dbi-flag-array [flags]
  (into-array DbiFlags
              (map (fn [f]
                     (or (dbi-flag f)
                         (throw (ex-info "unknown dbi flag"
                                         {:flag f :known (set (keys dbi-flag))}))))
                   flags)))

;; --- env --------------------------------------------------------------------

(defn open-env
  "Create or open an LMDB environment at `path`. Creates the directory
   if missing.

   Opts:
     :map-size     bytes, hard ceiling on total DB size. Default 1 GiB.
                   This is address space, not disk — pick generously.
     :max-dbs      number of named Dbis this env can hold. Default 8.
                   Cheap; bump freely.
     :max-readers  concurrent read-txn cap. Default 126 (LMDB default).
     :flags        set of env flag keywords (see `env-flag` for names).

   Returns an Env. Close via `close-env` or a component wrapping it."
  (^Env [path] (open-env path nil))
  (^Env [path {:keys [map-size max-dbs max-readers flags]
               :or   {map-size (* 1024 1024 1024)
                      max-dbs  8}}]
   (let [dir (doto (io/file path) .mkdirs)
         b   (-> (Env/create)
                 (.setMapSize (long map-size))
                 (.setMaxDbs  (int  max-dbs)))
         b   (cond-> b max-readers (.setMaxReaders (int max-readers)))]
     (.open b dir (env-flag-array (or flags #{}))))))

(defn close-env [^Env env]
  (.close env))

(defn- stat->map [^Stat s]
  {:entries        (.-entries       s)
   :depth          (.-depth         s)
   :page-size      (.-pageSize      s)
   :branch-pages   (.-branchPages   s)
   :leaf-pages     (.-leafPages     s)
   :overflow-pages (.-overflowPages s)})

(defn env-stats
  "Return env-wide page/entry statistics as a map. Cheap; safe to call
   anytime. See lmdbjava `Env.stat()`."
  [^Env env]
  (stat->map (.stat env)))

(defn env-info
  "Return env state (map-size, reader counts, last txn id, ...) as a
   map. Useful for observability. See lmdbjava `Env.info()`."
  [^Env env]
  (let [^EnvInfo i (.info env)]
    {:map-size            (.-mapSize           i)
     :map-address         (.-mapAddress        i)
     :num-readers         (.-numReaders        i)
     :max-readers         (.-maxReaders        i)
     :last-page-number    (.-lastPageNumber    i)
     :last-transaction-id (.-lastTransactionId i)}))

(defn sync!
  "Flush the env to disk. Only meaningful if the env was opened with
   `:no-sync` or `:no-meta-sync`; with default flags LMDB already syncs
   on every commit. Pass `force?` truthy to force a full fdatasync even
   when LMDB thinks it isn't needed. See `Env.sync(boolean)`."
  ([^Env env] (sync! env false))
  ([^Env env force?]
   (.sync env (boolean force?))
   nil))

(defn copy-env!
  "Hot-copy the env to `dest` (an existing empty directory). Runs while
   writers are active — blocks them only briefly. Pass `:compact? true`
   to rewrite the copy without free space (smaller but slower).
   See `Env.copy(File, CopyFlags...)`."
  ([^Env env dest] (copy-env! env dest nil))
  ([^Env env dest {:keys [compact?]}]
   (let [f     (io/file dest)
         flags (into-array CopyFlags
                           (if compact?
                             [(CopyFlags/valueOf "MDB_CP_COMPACT")]
                             []))]
     (.copy env f flags)
     nil)))

(defn reader-check
  "Release stale reader slots left behind by crashed processes. Returns
   the number of slots freed. Worth calling at startup and occasionally
   if `env-info`'s `:num-readers` drifts upward. See `Env.readerCheck()`."
  [^Env env]
  (.readerCheck env))

;; --- dbi --------------------------------------------------------------------

(defrecord DbiHandle [^Env env ^Dbi dbi key-codec val-codec dbi-name])

(defn open-dbi
  "Open (create if absent) a named Dbi inside `env`. Dbis are meant to be
   long-lived — open once at startup and reuse the handle forever. There
   is no `close-dbi`; env close takes all Dbis down.

   Opts:
     :key-codec   default `raw-codec`
     :val-codec   default `raw-codec`
     :create?     default true (adds MDB_CREATE). Set false to require
                  the Dbi already exist.
     :flags       set of extra dbi-flag keywords (e.g. #{:reverse-key})."
  ([^Env env ^String name] (open-dbi env name nil))
  ([^Env env ^String name {:keys [key-codec val-codec create? flags]
                           :or   {key-codec raw-codec
                                  val-codec raw-codec
                                  create?   true}}]
   (let [flag-set (cond-> (or flags #{})
                    create? (conj :create))
         dbi      (.openDbi env name (dbi-flag-array flag-set))]
     (->DbiHandle env dbi key-codec val-codec name))))

;; --- transactions -----------------------------------------------------------

(defmacro with-txn
  "Open an LMDB transaction on `env` and bind it to `txn-sym` for the
   duration of `body`. `mode` is `:read` or `:write`.

   Semantics: on normal exit of a `:write` txn the txn commits; on throw
   it aborts. `:read` txns just close. Either way the native handle is
   released in a `finally`.

   For multi-op atomic work, pass the bound txn as the context arg to
   `put!`/`get`/... Those helpers accept either an `Env` (opens a fresh
   one-shot txn) or a `Txn` (reuses the one you hand them), so calling
   shape is identical either way.

   Two LMDB invariants that shape how you use this:
     - One txn per thread. A thread that's already inside `with-txn`
       can't open another — nesting on the same thread fails fast.
       This is why every op takes a context: callers thread the one
       txn they own rather than opening a second.
     - Writers serialize env-wide. `with-txn [_ env :write]` on a
       second thread blocks until the first commits. Read txns don't
       block writers or each other, but they do pin the MVCC snapshot
       (old pages can't be reclaimed until the read txn closes), so
       long-held read txns make the DB file grow."
  [[txn-sym env mode] & body]
  `(let [^Env env# ~env
         mode#     ~mode
         ^Txn txn# (case mode#
                     :read  (.txnRead  env#)
                     :write (.txnWrite env#))]
     (try
       (let [~txn-sym  txn#
             result#   (do ~@body)]
         (when (identical? :write mode#) (.commit txn#))
         result#)
       (finally
         (.close txn#)))))

(defmacro ^:private with-ctx-txn
  "Dispatch on env-or-txn: if a `Txn`, use it (and check mode matches);
   otherwise (expected: an `Env`), open a fresh one-shot txn of `mode`
   and close it on exit. `body` runs with `txn-sym` bound to the live txn."
  [[txn-sym ctx mode] & body]
  `(let [ctx#  ~ctx
         mode# ~mode]
     (if (instance? org.lmdbjava.Txn ctx#)
       (let [^org.lmdbjava.Txn t# ctx#]
         (when (and (identical? :write mode#) (.isReadOnly t#))
           (throw (ex-info "write op requires a write txn; got a read txn"
                           {:mode mode#})))
         (let [~txn-sym t#] ~@body))
       (with-txn [t# ctx# mode#]
         (let [~txn-sym t#] ~@body)))))

;; --- dbi admin (need a live txn, so live below with-ctx-txn) ---------------

(defn dbi-stats
  "Per-Dbi page/entry stats (map). `ctx` is an `Env` or a `Txn`. See
   `Dbi.stat(Txn)`."
  [ctx dbi]
  (with-ctx-txn [t ctx :read]
    (stat->map (.stat ^Dbi (:dbi dbi) t))))

(defn clear-dbi!
  "Remove every entry from `dbi`, keeping the Dbi itself open. `ctx` is
   an `Env` or a write `Txn`. See `Dbi.drop(Txn, false)`."
  [ctx dbi]
  (with-ctx-txn [t ctx :write]
    (.drop ^Dbi (:dbi dbi) t false)
    nil))

(defn drop-dbi!
  "Remove every entry from `dbi` *and* delete the Dbi. The DbiHandle is
   unusable after this. `ctx` is an `Env` or a write `Txn`. See
   `Dbi.drop(Txn, true)`."
  [ctx dbi]
  (with-ctx-txn [t ctx :write]
    (.drop ^Dbi (:dbi dbi) t true)
    nil))

;; --- write ops --------------------------------------------------------------
;;
;; Three distinct functions rather than one with a flags map: `put!`
;; (overwrite), `put-new!` (insert-if-absent), `bulk-append!` (MDB_APPEND
;; fast path for sorted batches). The split is deliberate — combinations
;; that don't make sense (append + no-overwrite; single-call append) can't
;; be expressed, and each call site picks one function instead of piloting
;; a state machine of flag combinations. If you're tempted to merge these
;; behind a single `(put! … {:mode …})`, read the history in this file's
;; git log before doing so — the flags map is exactly what this design
;; replaced.

(defn put!
  "Associate `k` → `v` in `dbi`, overwriting any existing value. `ctx`
   is an `Env` (opens a one-shot write txn) or a `Txn` (reuses yours).
   Returns nil. For conditional insert see `put-new!`; for bulk loading
   see `bulk-append!`."
  [ctx dbi k v]
  (with-ctx-txn [t ctx :write]
    (let [kb (->direct-buf ((-> dbi :key-codec :encode) k))
          vb (->direct-buf ((-> dbi :val-codec :encode) v))]
      (.put ^Dbi (:dbi dbi) t kb vb no-put-flags)
      nil)))

(defn put-new!
  "Insert `k` → `v` into `dbi` only if the key is absent. Returns true
   if the write landed, false if `k` was already present (in which
   case the stored value is unchanged). `ctx` is an `Env` or a write
   `Txn`. Wraps LMDB's MDB_NOOVERWRITE."
  [ctx dbi k v]
  (with-ctx-txn [t ctx :write]
    (let [kb (->direct-buf ((-> dbi :key-codec :encode) k))
          vb (->direct-buf ((-> dbi :val-codec :encode) v))]
      (.put ^Dbi (:dbi dbi) t kb vb no-overwrite-flags))))

(defn bulk-append!
  "Bulk-insert `kv-pairs` into `dbi` using LMDB's MDB_APPEND fast path.
   `ctx` is an `Env` (opens a one-shot write txn) or a `Txn` (reuses
   yours — handy when you want the batch to commit atomically
   alongside other writes). Accepts anything reducible over `[k v]`
   entries — vectors, maps (yield MapEntry pairs), another
   `range-reducible`, etc.

   For MDB_APPEND to work, every inserted key must be strictly greater
   (byte-wise) than every key already in `dbi`. By default we encode
   each key once and sort the batch by unsigned-byte order (matching
   LMDB's comparator) before inserting — so callers can hand in pairs
   in any order. Pass `{:presorted? true}` to skip the sort.

   Misordered input (either pre-existing keys overlapping the batch,
   or a presorted input that isn't actually sorted) throws
   `Dbi$KeyExistsException` and aborts the surrounding txn — no
   partial writes commit. Returns nil."
  ([ctx dbi kv-pairs] (bulk-append! ctx dbi kv-pairs nil))
  ([ctx dbi kv-pairs {:keys [presorted?]}]
   (let [k-enc (-> dbi :key-codec :encode)
         v-enc (-> dbi :val-codec :encode)
         encoded (into [] (map (fn [[k v]] [(k-enc k) (v-enc v)])) kv-pairs)
         ordered (if presorted? encoded (sort-by first compare-bytes-unsigned encoded))]
     (when (seq ordered)
       (with-ctx-txn [t ctx :write]
         (doseq [[kb vb] ordered]
           (.put ^Dbi (:dbi dbi) t
                 (->direct-buf kb) (->direct-buf vb) append-flags))))
     nil)))

(defn get
  "Fetch `k` from `dbi`, decoded via the dbi's val-codec. Returns
   `not-found` (nil by default) if the key is absent. `ctx` is an `Env`
   (opens a one-shot read txn) or a `Txn`."
  ([ctx dbi k] (get ctx dbi k nil))
  ([ctx dbi k not-found]
   (with-ctx-txn [t ctx :read]
     (let [kb (->direct-buf ((-> dbi :key-codec :encode) k))
           bb (.get ^Dbi (:dbi dbi) t kb)]
       (if bb
         ((-> dbi :val-codec :decode) (buf->bytes bb))
         not-found)))))

(defn exists?
  "True if `k` is present in `dbi`. `ctx` is an `Env` or a `Txn`."
  [ctx dbi k]
  (with-ctx-txn [t ctx :read]
    (let [kb (->direct-buf ((-> dbi :key-codec :encode) k))]
      (some? (.get ^Dbi (:dbi dbi) t kb)))))

(defn delete!
  "Delete `k` from `dbi`. Returns true if the key existed. `ctx` is an
   `Env` or a write `Txn`."
  [ctx dbi k]
  (with-ctx-txn [t ctx :write]
    (let [kb (->direct-buf ((-> dbi :key-codec :encode) k))]
      (.delete ^Dbi (:dbi dbi) t kb))))

;; --- range specs ------------------------------------------------------------

(defn all          [] [:all])
(defn all-backward [] [:all-backward])
(defn at-least     [k] [:at-least k])
(defn at-most      [k] [:at-most k])
(defn greater-than [k] [:greater-than k])
(defn less-than    [k] [:less-than k])
(defn closed       [a b] [:closed a b])
(defn closed-open  [a b] [:closed-open a b])
(defn open-closed  [a b] [:open-closed a b])
(defn open         [a b] [:open a b])
(defn prefix       [k] [:prefix k])

(defn- byte-successor
  "Lex successor of `lo` by bumping the last non-0xFF byte. Returns nil
   if `lo` is all-0xFF (no successor exists)."
  ^"[B" [^bytes lo]
  (loop [i (dec (alength lo))]
    (cond
      (neg? i) nil
      (= (bit-and (aget lo i) 0xff) 0xff) (recur (dec i))
      :else
      (let [hi (byte-array (inc i))]
        (System/arraycopy lo 0 hi 0 (inc i))
        (aset-byte hi i (unchecked-byte (inc (bit-and (aget lo i) 0xff))))
        hi))))

(defn- range->lmdb ^KeyRange [key-encode spec]
  (let [bb (fn [v] (->direct-buf (key-encode v)))]
    (case (nth spec 0)
      :all          (KeyRange/all)
      :all-backward (KeyRange/allBackward)
      :at-least     (KeyRange/atLeast     (bb (nth spec 1)))
      :at-most      (KeyRange/atMost      (bb (nth spec 1)))
      :greater-than (KeyRange/greaterThan (bb (nth spec 1)))
      :less-than    (KeyRange/lessThan    (bb (nth spec 1)))
      :closed       (KeyRange/closed      (bb (nth spec 1)) (bb (nth spec 2)))
      :closed-open  (KeyRange/closedOpen  (bb (nth spec 1)) (bb (nth spec 2)))
      :open-closed  (KeyRange/openClosed  (bb (nth spec 1)) (bb (nth spec 2)))
      :open         (KeyRange/open        (bb (nth spec 1)) (bb (nth spec 2)))
      :prefix       (let [lo (key-encode (nth spec 1))
                          hi (byte-successor lo)]
                      (if hi
                        (KeyRange/closedOpen (->direct-buf lo) (->direct-buf hi))
                        (KeyRange/atLeast    (->direct-buf lo)))))))

;; --- range iteration --------------------------------------------------------

(defn range-reducible
  "Return an `IReduceInit` over `[k v]` pairs in `range-spec`. Pairs are
   `clojure.lang.MapEntry` instances, so `(into [] ...)`, `(into {} ...)`,
   `(map first ...)`, and destructuring all work.

   `ctx` is an `Env` (opens a fresh read txn for each `reduce`) or a
   `Txn` (reused as-is; not closed). Each pair's bytes are copied out,
   so pairs remain valid after the reducible's txn closes.

   Reduce-only — don't call `seq`/`count`/`first` on it. Use
   `reduce`/`transduce`/`into`, or call `scan` for an eager vector.

   Examples:
     (into {}                        (range-reducible env dbi (all)))
     (into [] (map first)            (range-reducible env dbi (prefix \"u:\")))
     (transduce (take 100) conj []   (range-reducible env dbi (all)))"
  [ctx dbi range-spec]
  (let [kd (-> dbi :key-codec :decode)
        vd (-> dbi :val-codec :decode)]
    (reify clojure.lang.IReduceInit
      (reduce [_ rf init]
        (let [step (fn [^Txn t]
                     (let [kr (range->lmdb (-> dbi :key-codec :encode) range-spec)]
                       (with-open [^CursorIterable it (.iterate ^Dbi (:dbi dbi) t kr)]
                         (reduce (fn [acc ^CursorIterable$KeyVal kv]
                                   (rf acc
                                       (MapEntry/create
                                        (kd (buf->bytes (.key kv)))
                                        (vd (buf->bytes (.val kv))))))
                                 init it))))]
          (if (instance? Txn ctx)
            (step ctx)
            (with-txn [t ctx :read] (step t))))))))

;; `reduce-range` isn't redundant with `range-reducible` + a 2-arg
;; reducer: it passes `k` and `v` as separate args, skipping the per-row
;; `MapEntry/create` allocation the reducible performs. Keep both — use
;; `range-reducible` for idiomatic composition, `reduce-range` when the
;; allocation cost shows up on large scans.

(defn reduce-range
  "Reduce `(f acc k v)` over every `[k v]` pair in `range-spec`. The
   3-arg reducer skips the per-row `MapEntry` allocation that
   `range-reducible` uses — prefer this on ≥1M-row scans, prefer
   `range-reducible` elsewhere. Supports `reduced` for early termination.

   `ctx` is an `Env` or a `Txn`."
  [ctx dbi range-spec f init]
  (let [kd (-> dbi :key-codec :decode)
        vd (-> dbi :val-codec :decode)
        kr (range->lmdb (-> dbi :key-codec :encode) range-spec)]
    (with-ctx-txn [t ctx :read]
      (with-open [^CursorIterable it (.iterate ^Dbi (:dbi dbi) t kr)]
        (reduce (fn [acc ^CursorIterable$KeyVal kv]
                  (f acc
                     (kd (buf->bytes (.key kv)))
                     (vd (buf->bytes (.val kv)))))
                init
                it)))))

(defn scan
  "Eager vector of `[k v]` pairs (as `MapEntry`s) over `range-spec`.
   Handy for small ranges and debugging; for large ranges prefer
   `range-reducible` or `reduce-range`.

   Eager on purpose — a lazy seq realized after the txn closed would
   read freed memory. If you want streaming with early exit, compose a
   transducer onto `range-reducible` (it closes its txn when the
   transducer short-circuits)."
  [ctx dbi range-spec]
  (into [] (range-reducible ctx dbi range-spec)))

(defn count-range
  "Number of entries in `range-spec`."
  [ctx dbi range-spec]
  (reduce (fn [n _] (inc n)) 0 (range-reducible ctx dbi range-spec)))

;; --- component --------------------------------------------------------------

(defrecord Store [path env-opts dbi-specs env dbis]
  component/Lifecycle
  (start [this]
    (let [e  (open-env path env-opts)
          ds (into {}
                   (map (fn [{:keys [name] :as spec}]
                          [name (open-dbi e name (dissoc spec :name))]))
                   dbi-specs)]
      (assoc this :env e :dbis ds)))
  (stop [this]
    (some-> env close-env)
    (assoc this :env nil :dbis {})))

(defn dbi
  "Look up a Dbi handle by name from a started `Store`."
  [store name]
  (get-in store [:dbis name]))

;; --- future work ------------------------------------------------------------
;;
;; Intentionally out of scope (add when a concrete need shows up):
;;
;; - MDB_DUPSORT (multiple values per key, sorted): add `:dup-sort` to
;;   the dbi-flag table, plus iterate-dup / put-dup! / delete-dup!.
;;   Needed for inverted-index-style storage, but puts ~2kb size constraints
;;   on keys and values since keys and MDB_DUPSORT data items must fit on a
;;   node in a regular page.
;;
;; - MDB_INTEGERKEY (native-endian int key ordering, faster comparison):
;;   add a flag and a matching codec (careful: byte layout must match
;;   the host endianness, so this codec isn't portable across machines).
;;
;; - Raw cursor escape hatch (with-cursor): expose CursorIterable plus
;;   positional ops (seek-to-key, prev/next step, peek) for patterns not
;;   covered by KeyRange.
;;
;; - Auto-grow on MapFullException: catch, `.setMapSize` larger, retry.
;;   Requires every open txn to be drained first, so it's not purely
;;   transparent — probably belongs in a caller-owned retry loop.
;;
;; - nippy / fressian codec helpers: trivial once you have the dep,
;;   e.g. `{:encode nippy/freeze :decode nippy/thaw}`. Kept out of core
;;   so the toolkit doesn't pull a transitive serialization dep.
