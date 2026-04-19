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
