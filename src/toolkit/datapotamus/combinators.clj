(ns toolkit.datapotamus.combinators
  "Draft combinators layered over the datapotamus primitives.

   STATUS: sketches, not yet wired into tests or examples. This file is a
   design artifact — a concrete proposal for what the next layer of
   combinators should look like and a forcing function for deciding which
   ones warrant changes to the underlying system. When any of these ship,
   they should move into datapotamus.clj proper (Part 6 — Combinators) and
   drop the draft/commentary prose.

   Design principles
   -----------------

   1. **Every stage is a step.** Combinators either build on `step`/`proc`
      directly or compose existing steps via `serial`/`merge-steps`/
      `connect`. No new top-level concepts.

   2. **Tokens stay conserved.** Either the combinator is structurally 1:1
      (no stamping) or it reuses the zero-sum-group pattern already present
      in `fan-out` (stamp N siblings with values XOR-summing to 0, rely on
      synthesis to preserve invariants). We don't invent new token algebra.

   3. **Minimal surface.** Each combinator is under ~20 lines. Anything
      that forces a change to `wrap-proc`, `process-outputs`, or the ctx
      contract is flagged at the bottom rather than slipped into the
      combinator body.

   4. **Motivating examples.**
      - `example-podcast-sequential-pipeline` (datapotamus_test.clj, Act XV)
        — a monolithic handler today; should decompose into
        `map`/`mapcat`/`into-vec` stages, each separately traceable.
      - `agent-style-multi-port-with-connect` (datapotamus_test.clj, Act III)
        — hand-wired cycle today; should be a `loop-until`.
      - `dynamic-fan-out` (datapotamus_test.clj, Act III) — `merge-steps`
        plus two explicit `connect`s; should be a `tee`-to-named-sinks
        pattern.

   Post-refactor notes
   -------------------

   Two recent changes in datapotamus.clj reshape what's cheap:

   - The ledger is gone (commit 5de4b70). Synthesis walks in-memory
     `::parents` refs at handler return. Combinators that build
     provisionals via `children`/`merge`/`pass` are short — the DAG walk
     distributes tokens uniformly.
   - `handler-factory` now passes the input port via `ctx` (commit
     30b74f8, datapotamus.clj:499). `(:in-port ctx)` is available inside
     any 3-arg handler, which retroactively unblocks `join-by-key` without
     any change to the factory signature.

   What infrastructure this file does NOT require
   ----------------------------------------------

   - No 4-arity handler form. `:in-port` is already in ctx.
   - No named-port `fan-out` variant. `tee` writes its own factory.
   - No new synthesis hooks. `dp/assoc-tokens` / `dp/dissoc-tokens`
     on provisionals is sufficient.

   What this file does still want (see bottom of file)
   ---------------------------------------------------

   Exactly one: a `:done` → user-handler pathway, needed only for the
   lossless variant of `batch`/`window` and a partial-flush variant of
   `fan-in`. The lossy `batch` sketch below ships without it."
  (:refer-clojure :exclude [map filter mapcat])
  (:require [toolkit.datapotamus :as dp]
            [toolkit.datapotamus.token :as tok]))

;; ============================================================================
;; Part 1 — Stream-shape (1:1, tokens pass through untouched)
;; ============================================================================
;;
;; These are the cheapest items in the proposal. Every one of them is
;; expressible today as a `step` call; the value is naming, not power.
;; The podcast example decomposes cleanly once these exist.

(defn map
  "1:1 pure-fn stage. Alias for the 2-arity form of `dp/step`.

   Present purely for readability:
     (dp/serial (map :inc inc) (map :dbl #(* 2 %)) ...)
   reads as a pipeline; the naked `dp/step` form reads as step declarations.
   Tokens pass through untouched — synthesis handles lineage."
  [id f]
  (dp/step id f))

(defn filter
  "Route data to :pass when (pred? d) is truthy, to :fail otherwise.

   Implemented as a two-port `router`. Tokens are preserved (router is
   structurally 1:1 per input). Users wire `(dp/sink)` on :fail to
   discard, or a downstream step to handle rejects. Deliberately not a
   drop-on-false variant — that would require emitting a signal with the
   tokens still attached, which needs a verified ledger path for
   signal-from-handler. Router suffices and stays honest."
  [id pred?]
  (dp/router id [:pass :fail]
    (fn [d] [{:data d :port (if (pred? d) :pass :fail)}])))

(defn scan
  "Streaming fold: emit (f acc data) per input, threading acc across calls.

   State lives in the step's local `s` map under :acc — not an atom,
   since the proc-fn's `s` is already the right place for per-step
   persistent state. Tokens 1:1."
  [id init f]
  (dp/step id nil
    (fn [_ctx s d]
      (let [acc' (f (:acc s init) d)]
        [(assoc s :acc acc') [[:out acc']]]))))

(defn tap
  "Invoke (f data) for side effects; forward data unchanged.

   The ubiquitous 'debug print' / 'emit metric' stage. Returning `d`
   unchanged keeps this 1:1 on tokens. If users want to observe the full
   envelope rather than just `:data`, they can fall back to a manual
   `dp/step` with the 3-arg handler."
  [id f]
  (dp/step id (fn [d] (f d) d)))

;; ============================================================================
;; Part 2 — Cardinality changers (N-way split, tokens split via zero-sum group)
;; ============================================================================
;;
;; Both `mapcat` and `tee` are structurally `fan-out` with small twists:
;; runtime-determined N for mapcat, named ports for tee. They use
;; `dp/assoc-tokens` to stamp the zero-sum group, the same as core
;; `fan-out`.

(defn mapcat
  "Emit (f data) element-wise on :out, bound by a fresh zero-sum token
   group keyed on `group-id`. Pair with `(dp/fan-in group-id)` (or
   `into-vec` below) to close the group and aggregate.

   Structurally identical to `dp/fan-out` (datapotamus.clj:608) except
   the count comes from `(f data)` rather than a compile-time N.

   Empty-collection case (Q1 in combinators.md): this sketch emits
   nothing, which means the input's tokens don't propagate and a
   downstream fan-in will never see the group open. That's fine for
   `mapcat`-as-terminal-expansion but wrong for
   `mapcat`-inside-`for-each`. The recommended fix is to synthesize a
   single merge-msg carrying the parent's tokens and an empty-vector
   data payload. Deferred until the first caller hits empty."
  ([id f] (mapcat id id f))
  ([id group-id f]
   (dp/step id nil
     (fn [ctx s d]
       (let [items  (vec (f d))
             n      (count items)
             gid    [group-id (:msg-id (:msg ctx))]
             values (tok/split-value 0 n)
             kids   (dp/children ctx items)
             kids'  (mapv (fn [k v] (dp/assoc-tokens k {gid v}))
                          kids values)]
         [s (mapv (fn [k] [:out k]) kids')])))))

(defn tee
  "Broadcast input to each of the named output ports. Tokens split N-ways
   like `fan-out`; downstream branches must absorb their share (sinks or
   their own fan-ins). Named ports make routing intent explicit — the
   usual alternative is `merge-steps` + two or three `connect` calls.

   Eliminates the boilerplate in `dynamic-fan-out`
   (datapotamus_test.clj, Act III) and any other test that wants 'send
   this to three branches' rather than 'fan out N anonymous copies'."
  [id ports]
  (let [n (count ports)]
    (dp/step id
      {:outs (zipmap ports (repeat ""))}
      (fn [ctx s d]
        (let [gid    [id (:msg-id (:msg ctx))]
              values (tok/split-value 0 n)
              kids   (dp/children ctx (repeat n d))
              kids'  (mapv (fn [k v] (dp/assoc-tokens k {gid v}))
                           kids values)]
          [s (mapv (fn [p k] [p k]) ports kids')])))))

;; ============================================================================
;; Part 3 — Error routing
;; ============================================================================

(defn try-catch
  "Wrap a (data → data) fn. Success → :out; thrown exception → :err with
   {:error <ex> :data <input>}. Does NOT produce a :failure trace event
   — the whole point of this combinator is to turn exceptions into
   routable graph data instead of out-of-band failures.

   Composes with `retry` (datapotamus.clj:668):
     (try-catch :risky (fn [d] ((retry-fn 3 f) d)))
   yields 'retry 3 times, then route failures downstream.'

   Open questions from combinators.md:
   - Q10 (pure-fn vs step-wrapping): pure-fn is the scoped choice.
     Wrapping an inner step would require suppressing `wrap-proc`'s
     catch or subscribing to the inner step's pubsub, both invasive.
     Ship pure-fn; revisit if a real use case appears.
   - Q12 (error data shape): picked `{:error ex :data input}` for
     consistency with existing event payloads (`:error` is used by
     failure-event in datapotamus.clj:265)."
  [id f]
  (dp/step id {:ins {:in ""} :outs {:out "" :err ""}}
    (fn [_ctx s d]
      (try
        [s [[:out (f d)]]]
        (catch Throwable t
          [s [[:err {:error t :data d}]]])))))

;; ============================================================================
;; Part 4 — Composition (named wiring patterns)
;; ============================================================================

(defn loop-until
  "Apply body-step repeatedly. After each iteration, (pred? data) decides:
   true → emit on :out and exit the loop; false → feed the message back
   into body-step's input. Termination is the caller's responsibility.

   Packages the back-edge pattern that
   `agent-style-multi-port-with-connect` (datapotamus_test.clj, Act III)
   hand-writes today. The emitted step has the same :in/:out shape as
   `body-step`'s boundaries — it drops into `dp/serial` transparently.

   Open questions (combinators.md Q7-Q9):
   - Trace explosion: long loops create long lineage chains. Accepted.
   - Token accumulation across iterations: each pass through the router
     is a 1-way split (N=1), so XOR is preserved, but a fan-out *inside*
     the body needs a matching fan-in *inside* the body — token groups
     don't cross the loop boundary cleanly.
   - Backpressure: cycles in core.async.flow can deadlock if buffers
     fill. `agent-loop` works; stress test before relying on this for
     high-throughput loops."
  ([id body-step pred?]
   (let [body-id (keyword (str (name id) ".body"))
         test-id (keyword (str (name id) ".test"))]
     (-> (dp/merge-steps
          (dp/as-step body-id body-step)
          (dp/router test-id [:done :again]
                     (fn [d] [{:data d :port (if (pred? d) :done :again)}])))
         (dp/connect [body-id :out]   test-id)
         (dp/connect [test-id :again] body-id)
         (dp/input-at  body-id)
         (dp/output-at [test-id :done])))))

;; NOTE: `loop-until` calls `dp/gen-id`, which is private today
;; (datapotamus.clj:471). Either expose it, or require callers to pass
;; an id. Passing an id is preferable — nameless loops inside traces
;; are hard to correlate with source.

(defn for-each
  "Apply inner-step to each element of (coll-fn data). Emits one :out msg
   whose :data is the vector of inner outputs for that input.

   Definition is three lines once `mapcat` exists. Ordering note
   inherits from `fan-in` — results are in arrival order, not
   collection order; encode positions into the payload pre-split if
   you need them."
  ([id inner-step]           (for-each id identity inner-step))
  ([id coll-fn inner-step]
   (dp/serial
    (mapcat      (keyword (str (name id) ".split")) id coll-fn)
    (dp/as-step  (keyword (str (name id) ".body"))  inner-step)
    (dp/fan-in   (keyword (str (name id) ".join"))  id))))

(defn into-vec
  "Close a group opened by `mapcat` or `fan-out`: emit one :out msg whose
   :data is the vector of upstream messages' :data for that group.

   Identical to `(dp/fan-in id group-id)` — ships as an alias for
   memorability. The name matches Clojure convention ('collect
   everything into a vector') and pairs obviously with `mapcat` at the
   pipeline level."
  [id group-id]
  (dp/fan-in id group-id))

;; ============================================================================
;; Part 5 — Joins (multi-input)
;; ============================================================================

(defn join-by-key
  "Buffer inputs from N named ports, keyed by (key-fn data). When every
   port has produced a message for some key k, emit one msg on :out
   whose :data is a vector of the per-port payloads in `ports` order,
   with tokens XOR-combined across all N parents and lineage pointing
   at all of them.

   No timeout: unmatched keys pile up forever. Use an upstream take-N
   / batch gate if bounded memory matters. n8n documents the same
   property (items can be kept unpaired).

   Previously flagged as gated on a 4-arity handler. No longer true:
   `:in-port` is in ctx (datapotamus.clj:499), so a plain 3-arg handler
   reads it directly.

   Cross-group token note: when messages from two upstream `fan-out`s
   meet here, the merged tokens carry both groups. Downstream fan-ins
   on either group should still close correctly (the merge helper
   XOR-combines both parents' tokens). Worth an explicit test."
  [id ports key-fn]
  (let [n (count ports)]
    (dp/step id
      {:ins  (zipmap ports (repeat ""))
       :outs {:out ""}}
      (fn [ctx s d]
        (let [in-port (:in-port ctx)
              m       (:msg ctx)
              k       (key-fn d)
              s'      (assoc-in s [:pending k in-port] m)
              row     (get-in s' [:pending k])]
          (if (= n (count row))
            (let [parents (mapv #(get row %) ports)
                  out-msg (dp/merge ctx parents (mapv :data parents))]
              [(update s' :pending dissoc k) [[:out out-msg]]])
            [s' []]))))))

;; ============================================================================
;; Part 6 — Stateful accumulators (partial-flush concerns)
;; ============================================================================

(defn batch
  "Accumulate every N inputs into a single :out msg whose :data is the
   vector of batched payloads. Tokens XOR-combine across the N parents
   via the `merge` helper — synthesis treats the stored
   previous-invocation messages as 'external parents' and XORs their
   full :tokens into the output.

   LOSSY AT STREAM END. If the stream terminates with a partial batch
   (count < N), the held messages and their tokens are silently
   dropped. This matters when:
     - a downstream fan-in expects to close a group that partially
       traverses the batch
     - users care about observing all the inputs

   For flows that don't fan-in downstream of batch, or that pad input
   length to a multiple of N, this is fine. For everything else, wait
   for the `:on-done` hook described below.

   Implementation detail: we store the *full envelope* in state (not
   just `:data`) so that `dp/merge` can produce a real multi-parent
   provisional at emit time. The state holds at most (dec N) messages."
  [id n]
  (dp/step id nil
    (fn [ctx s _d]
      (let [buf (conj (:buf s []) (:msg ctx))]
        (if (= n (count buf))
          [(assoc s :buf [])
           [[:out (dp/merge ctx buf (mapv :data buf))]]]
          [(assoc s :buf buf) []])))))

;; `window`, a time-bounded variant of `batch`, is deliberately omitted.
;; It needs a timer source that cooperates with core.async.flow's
;; cancel/pause contract; too big a scope for this draft. A reasonable
;; first pass: accept an `(async-timer ms)` channel in the factory and
;; use alts! against the main input in a custom proc-fn — that's
;; outside the `step`/`handler-factory` model, so it's closer to a
;; first-party addition to Part 6 of datapotamus.clj than a combinator
;; built on top.

;; ============================================================================
;; What this file wants from datapotamus.clj
;; ============================================================================
;;
;; Nothing blocks Tier 1 (map/filter/scan/tap/mapcat/tee/try-catch) or
;; Tier 2 (loop-until/for-each/into-vec/join-by-key) from shipping today
;; — they're drop-in additions that only reach into the public API and
;; the well-known `::extra-tokens` / `::drop-tokens` metadata hook.
;;
;; One small exposure would tighten a couple of sketches:
;;
;; (1) Expose `gen-id` (currently private at datapotamus.clj:471), or
;;     require callers to pass ids. `loop-until` and `for-each` both
;;     call `gen-id` in their nullary-id arity to synthesize sub-ids
;;     (`:loop.body`, `:for-each.split`). Either make `gen-id` public
;;     or drop the nullary arities and require an id. Preference:
;;     require an id — named loops are easier to correlate with
;;     `recv.flow.*.step.>` subscriptions.
;;
;; One real infrastructure change, load-bearing for a future tier:
;;
;; (2) A `:done` → user-handler pathway. Today `wrap-proc`
;;     (datapotamus.clj:700) auto-generates done forwards when all
;;     declared input ports close, bypassing the handler entirely.
;;     The lossless variants of `batch` and `window` — and the
;;     'partial-flush' question for `fan-in` tracked in
;;     combinators.md:314 — all want the same primitive: an opt-in
;;     `:on-done` hook in the factory return that wrap-proc calls
;;     before cascading done. Something like:
;;
;;       (fn []
;;         {:params {} :ins {:in ""} :outs {:out ""}
;;          :on-done (fn [ctx s] [s' port-map])})
;;
;;     ~15 lines of change in wrap-proc, no breaking change for steps
;;     that don't opt in. Worth doing once the second consumer appears
;;     (both `batch`-lossless and `fan-in`-partial-flush will want it
;;     simultaneously).
;;
;; Everything else previously thought to be needed — a 4-arity handler
;; variant for port-aware steps, a `signal`-returning helper for
;; filter-drop, a named-port `fan-out` — turns out not to be necessary
;; against the current code.
