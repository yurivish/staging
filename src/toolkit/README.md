# toolkit

A small collection of Clojure dev-loop helpers, independent of any particular
app. Assumes the app uses Stuart Sierra's `component` for lifecycle.

## Namespaces

### `toolkit.watcher`

A polling `FileWatcher` component. Configured with `:interval-ms` and
`:watches` — a seq of `{:dir :include? :on-change}` maps. A single scan
thread polls every watch; each watch's `on-change` fires on a detached
thread when files under its `:dir` matching its `:include?` predicate
change. `watcher/clj-file?` is exported for the common case; use
`(constantly true)` to match everything. Stopping the component shuts
down the scan thread.

### `toolkit.hotreload`

Datastar-based browser reload: the page opens an SSE to `/hot-reload` which
retries across server reboots. Two paths fire a reload, for different
situations:

- **Server-restart path (`arm!`)** — for flows where the server process is
  coming down (code reload). Set the flag; the *next* SSE reconnect (which
  the restart forces) sends `window.location.reload()`.
- **Push path (`reload-now!`)** — for flows where the server stays up (a
  static-asset change). Broadcasts the reload script over every currently
  open SSE, no reconnect needed.

Public:
- `path` — route the SSE attaches to (`"/hot-reload"` by convention).
- `snippet` — hiccup `<div data-init="@get(...)">` to inject in dev pages.
- `handler` — the SSE handler. Register at `path`.
- `arm!` — arm for a reload on the next reconnect. Use when the server is
  about to restart.
- `reload-now!` — push a reload to every currently connected client. Use
  when the server stays running.

### `toolkit.dev`

- `refresh` — `tools.namespace.repl/refresh` wrapped with the `*ns*` thread-local
  binding it needs off REPL threads (see gotcha 1 below).
- `reload!` — the stop/refresh/before-start/start dance; takes plain fns so it
  doesn't know about your `sys` atom.

### `toolkit.sublist`

NATS-style subject routing trie. Subscribers register against subject
patterns with `*` (single-token) and `>` (tail) wildcards; publishers look up
matches by literal subject. Queue groups are supported — `pick-one` collapses
a match result into a flat delivery set by load-balancing within each group.

```clojure
(let [sl (sublist/make)]
  (sublist/insert! sl "foo.*.baz" :a)
  (sublist/insert! sl "foo.>"     :b)
  (sublist/match sl "foo.bar.baz"))
;; => {:plain #{:a :b} :groups {}}
```

Public:
- `make` — returns a fresh sublist (an atom).
- `insert!` / `remove!` — register and unregister `(subject, value, queue)`.
  Idempotent; `remove!` prunes empty branches on the way out.
- `match` — returns `{:plain #{...} :groups {queue-name #{...}}}`.
- `pick-one` — flattens a match result for delivery.
- `valid-pattern?` / `valid-subject?` — non-throwing predicates.

Subscriptions are value-deduped. If a pubsub layer on top wants Go-style
multi-delivery (one handle per `sub` call even for identical handlers),
wrap the stored value with a unique token — e.g. `{:handler f ::id (gensym)}`.

### `toolkit.pubsub`

Subject-based pub/sub on top of `toolkit.sublist`. Handlers run
synchronously on the publisher's thread; a thrown handler is logged to
`*err*` and does not block the rest.

```clojure
(let [ps (pubsub/make)]
  (pubsub/sub ps "foo.*" (fn [subj msg] (println subj "→" msg)))
  (pubsub/pub ps "foo.bar" :hello))
;; prints: foo.bar → :hello
```

Public:
- `make` — returns a fresh pubsub.
- `sub` — subscribes a handler `(fn [subject msg])` to a subject
  pattern. Opts: `{:queue name :id any}`. Returns a zero-arg unsub fn
  (idempotent). Two `sub` calls with the same handler + subject yield
  two independent subscriptions.
- `pub` — delivers a message to all matching subs; one-per-group for
  queue-group subs.
- `sub-chan` — returns `[ch stop!]`; messages flow onto `ch` via `put!`,
  `stop!` unsubs and closes the channel.
- `valid-pattern?` / `valid-subject?` — re-exported from sublist.

What's intentionally not included: handler source-location capture
(stack-walking is awkward in Clojure; use `:id` for labels, or build
your own macro around `sub` if you want `&form` line numbers) and debug
subs that see the full match result (call `(sublist/match ...)` directly
if you want to inspect routing).

### `toolkit.lmdb`

Thin Clojure wrapper over `org.lmdbjava/lmdbjava`. One or more envs per
process, each holding named Dbis; a single write txn spans every Dbi in
its env so cross-Dbi updates are atomic.

Every single-op helper (`put!`/`get`/`exists?`/`delete!`/`scan`/…) takes
a *context* as the first arg — either an `Env` (opens a one-shot txn)
or a `Txn` (reuses the one you have). Call shape is identical either
way; you just thread the context you own.

```clojure
(require '[toolkit.lmdb :as lmdb])

(def env   (lmdb/open-env "/tmp/mydb" {:max-dbs 4}))
(def users (lmdb/open-dbi env "users"    {:key-codec lmdb/string-codec
                                          :val-codec lmdb/string-codec}))
(def index (lmdb/open-dbi env "by-email" {:key-codec lmdb/string-codec
                                          :val-codec lmdb/string-codec}))

(lmdb/put! env users "alice" "{:age 30}")    ; env ctx → one-shot txn

(lmdb/with-txn [t env :write]                ; cross-Dbi atomic write
  (lmdb/put! t users "bob"     "{:age 42}")
  (lmdb/put! t index "b@x.com" "bob"))

(lmdb/scan env users (lmdb/prefix "a"))
;; => [["alice" "{:age 30}"]]

(into {} (lmdb/range-reducible env users (lmdb/all)))
;; => {"alice" "{:age 30}", "bob" "{:age 42}"}
```

Public:
- `open-env` / `close-env`, `open-dbi` — env + Dbi lifecycle. Dbis
  don't need closing; env close tears them all down.
- `raw-codec` / `string-codec` / `long-be-codec` — built-in codecs.
  A codec is `{:encode :decode}`; bring nippy/fressian/edn yourself.
- `with-txn [t env mode]` — one unified macro, `mode` is `:read` or
  `:write`. Write txns commit on normal exit, abort on throw.
- `put!` / `get` / `exists?` / `delete!` — `(op ctx dbi k ...)` where
  `ctx` is an `Env` or `Txn`. `get` takes a `not-found` arity matching
  `clojure.core/get`.
- `put-new!` — insert-if-absent; returns `true` if the key was new,
  `false` if it already existed.
- `bulk-append!` — MDB_APPEND fast path for batch loads. `(bulk-append!
  ctx dbi kv-pairs)` takes any reducible of `[k v]` pairs; sorts them
  by encoded-byte order internally, inserts under an env-or-txn ctx.
  Pass `{:presorted? true}` to skip the sort. When called with a
  caller's `Txn`, the batch commits or aborts with the surrounding
  txn — atomic bundling with other writes.
- Range builders: `all`, `all-backward`, `at-least`, `at-most`,
  `greater-than`, `less-than`, `closed`, `closed-open`, `open-closed`,
  `open`, `prefix`. Each returns a plain data vector.
- `range-reducible` — an `IReduceInit` yielding `MapEntry` pairs;
  composes with `reduce`/`transduce`/`into`. Reduce-only (not seq).
- `reduce-range` / `scan` / `count-range` — consume a range.
  `reduce-range` uses a 3-arg `(fn [acc k v])` reducer for the hot
  path (skips the per-row MapEntry allocation). All support early
  termination via `reduced`.
- `env-stats` / `env-info` / `dbi-stats` / `reader-check` —
  observability; return plain maps / ints.
- `sync!` — fdatasync the env (only needed with `:no-sync`).
- `copy-env!` — hot-copy the env to another directory (takes
  `{:compact? true}` to rewrite without free space).
- `clear-dbi!` / `drop-dbi!` — empty or delete a Dbi.
- `Store` / `map->Store` / `dbi` — Stuart Sierra component that
  opens an env + a map of named Dbis on `start`, closes them on
  `stop`. Configure with `:path`, `:env-opts`, and `:dbi-specs` (a
  seq of per-Dbi spec maps including `:name`).

Setup: the namespace needs these JVM flags on JDK 16+ (already added
to `:run`, `:dev`, and `:test` in the project's `deps.edn`):

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

Without them, the first ByteBuffer operation throws
`InaccessibleObjectException` — lmdbjava reflects into
`DirectByteBuffer` internals.

Gotchas:
- `map-size` (default 1 GiB) is a **hard ceiling**. Exceeding it
  throws `MapFullException`; pick generously since it's address
  space, not disk.
- Keys default to 511 bytes max; values are unbounded.
- Never return a seq of raw LMDB buffers across a txn boundary —
  backing memory is invalid after close. This wrapper copies bytes
  out on every read path, so the values you get back are safe.
- Read txns pin the MVCC snapshot; don't hold them across long work
  or the DB file will grow.
- LMDB allows only one txn per thread. Inside `with-txn`, thread the
  bound txn as the `ctx` arg to every helper — passing the env instead
  tries to open a second txn on the same thread and fails fast.

v1 intentionally excludes `DUPSORT`, `INTEGERKEY`, a cursor
escape-hatch, and auto-grow on `MapFullException` — see the
`future work` comment block at the bottom of `lmdb.clj` for where
they'd slot in.

## Canonical `dev/user.clj`

Copy this into your project and adjust the component wiring:

```clojure
(ns user
  (:require [clojure.tools.namespace.repl :as repl]
            [com.stuartsierra.component :as component]
            [toolkit.dev :as dev]
            [toolkit.hotreload :as hr]
            [toolkit.watcher :as watcher]
            ;; Eager-load app namespaces so compile errors surface at REPL
            ;; start. We don't alias them — see gotcha 2 below for why we
            ;; resolve their factories dynamically.
            [myapp.server]))

(repl/set-refresh-dirs "src/myapp")   ; refresh the app, not the toolkit

;; Two independent lifecycles. The file watcher lives outside the app system
;; so a failed refresh doesn't kill it — otherwise fixing the syntax error
;; wouldn't be observed, because the watcher went down with the system.
(defonce ^:private sys nil)
(defonce ^:private fw nil)
(defonce ^:private lock (Object.))

(declare reload!)

(defn- dev-system []
  (component/system-map
   :app-state ((requiring-resolve 'myapp.server/map->AppState) {})
   :server    (component/using
               ((requiring-resolve 'myapp.server/map->Server)
                {:port 8080 :dev? true})
               [:app-state])))

(defn- start-sys! [] (alter-var-root #'sys #(or % (component/start (dev-system)))))
(defn- stop-sys!  [] (alter-var-root #'sys (fn [s] (some-> s component/stop) nil)))

(defn start! []
  (alter-var-root #'fw
    (fn [w]
      (or w
          (component/start
           (watcher/map->FileWatcher
            {:interval-ms 100
             :watches [{:dir "src/myapp"         :include? watcher/clj-file?  :on-change reload!}
                       {:dir "resources/public"  :include? (constantly true)  :on-change hr/arm!}]})))))
  (start-sys!))

(defn stop! []
  (stop-sys!)
  (alter-var-root #'fw (fn [w] (some-> w component/stop) nil)))

(defn reload! []
  (dev/reload! {:start start-sys! :stop stop-sys! :lock lock :before-start hr/arm!}))
```

Leave `dev/user.clj` outside `set-refresh-dirs` — the harness itself is not
refreshed, only the app.

### Why the watcher sits outside the app system

If the watcher were a component in `dev-system`, a failed `reload!` would
tear it down along with the rest of the app. You'd fix the syntax error,
save — and nothing would happen, because there's no watcher left to notice.
You'd have to hit the REPL and type `(reload!)` yourself.

Keeping the watcher in its own var means `reload!` only touches `sys`. The
watcher keeps polling through refresh failures, so fixing the code
triggers an automatic retry exactly the same way the initial edit did.

### App-side hotreload wiring

Three touchpoints in the app:

1. Inject the snippet in dev pages:
   ```clojure
   (when dev? hr/snippet)
   ```
2. Register the route:
   ```clojure
   hr/path (if dev? (hr/handler req) {:status 404 :body "not found"})
   ```
3. Let the Server component take a `:dev?` flag; set it in `dev-system`, leave
   it unset in your prod system.

## Two gotchas worth knowing

Both are about `clojure.tools.namespace.repl/refresh`. The toolkit handles the
first one for you; the second one is still the app's responsibility.

### 1. `*ns*` must be thread-locally bound when calling `refresh`

`refresh` internally does `(set! *ns* …)`. `set!` only works on a
thread-locally bound var — it can't modify a root binding. A REPL thread
always has `*ns*` bound as a thread-local; a bare background thread does not.

The watcher runs `on-change` inline on its scheduler thread, which has no
`*ns*` thread-local. So `toolkit.dev/refresh` wraps the call in `binding`:

```clojure
(binding [*ns* (the-ns 'toolkit.dev)] (repl/refresh))
```

The specific namespace doesn't matter — `refresh` will update `*ns*` itself
as it loads each file. What matters is that a thread-local binding *exists*.

If you call `refresh` elsewhere off a REPL thread (nREPL worker threads,
async event handlers, other watchers), wrap it the same way.

### 2. `refresh` orphans Vars — non-refreshed code must resolve dynamically

`refresh` doesn't just re-eval files; it `remove-ns`'s each changed
namespace, then re-loads it. The reloaded namespace is a *new* `Namespace`
object with *new* `Var` objects. Any old `Var`s are orphaned — they still
hold their old values, but they're detached from the live namespace.

This bites code in non-refreshed namespaces that references
refreshed-namespace vars. The canonical example is `dev/user.clj` (which is
not refreshed, since it sits outside `set-refresh-dirs`) holding a
compile-time alias to app code:

```clojure
(ns user
  (:require [myapp.server :as server]))   ; compile-time Var resolution

(defn dev-system []
  (component/system-map
   :server (server/map->Server {:port 8080})))   ; ← compiled to the old Var
```

After `refresh`, `myapp.server` is a new namespace with a new `map->Server`
Var. But `dev-system`'s bytecode still points at the *old, orphaned* Var —
so it calls the old factory, creating instances of the old `Server` record,
whose handler refs are also stale, and so on. The file on disk changed, but
the running system reflects the pre-refresh code.

Fix: resolve dynamically when you know the target has been refreshed.

```clojure
(defn dev-system []
  (component/system-map
   :server ((requiring-resolve 'myapp.server/map->Server) {:port 8080})))
```

`requiring-resolve` goes through `find-ns`, which always returns the
*current* namespace object — so you always get the current Var and its
current value.

This only matters for things you call *after* `refresh` completes. Function
calls that live inside refreshed namespaces (where the bytecode itself is
regenerated) don't need this — they get fresh Var references automatically.
