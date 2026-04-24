# Datapotamus

Suppose you have a bunch of computations that want to pass messages to each other. Maybe you're building a pipeline. Maybe you're ingesting a stream. Maybe you have no idea what you're building yet, but you know it involves messages going into things and coming out of other things. OK — Datapotamus is for that.

The part you build is a **graph of steps**. A step is a box with input ports, output ports, and a state variable. Messages go in; the box does something; messages come out. The messages carry data. They also carry a small amount of bookkeeping — we'll get to why, eventually.

The part the library does is run the graph: it wires the steps together, backpressures the channels, cascades closure when inputs stop arriving, and tells you which messages caused which other messages (so you can debug). You start a flow with `flow/start!`, push messages in with `flow/inject!`, and eventually say `flow/stop!` when you've had enough.

That's the whole shape. If you squint, it's just a labeled directed graph with state machines at the nodes. You've seen that before. The fun is how little code you end up writing to coordinate them, and that comes from one trick about the bookkeeping. We'll get to it in a few pages. First, let's run something.

## A single-step computation

We'll use one running example through most of this guide: a file of measurements, one float per line, that we want to clean up and write out somewhere. For now the file is an abstraction; we'll work in vectors of values and pretend.

Here is the simplest possible step. It doubles its input:

```clojure
(require '[toolkit.datapotamus.step :as step]
         '[toolkit.datapotamus.flow :as flow])

(def double-step (step/step :double (fn [n] (* 2 n))))
```

One step. One input port called `:in`. One output port called `:out`. It doubles. Run it across some inputs:

```clojure
(flow/run-seq double-step [1.0 2.0 3.0])
;; => {:state :completed :outputs [[2.0] [4.0] [6.0]] ...}
```

The `:outputs` vector is aligned to the inputs: the first input produced `[2.0]`, the second `[4.0]`, the third `[6.0]`. `run-seq` is a convenience — it starts the flow, injects each input as one message, waits for everything to come out the other side, and then stops the flow. There's also `run!` for the single-input case. For long-running pipelines you use neither of these; you call `start!` / `inject!` / `stop!` yourself. We'll get there.

*The trade-off.* `run-seq` blocks until every input has produced everything it's going to produce. That's great for finite collections; it's the wrong shape for a pipeline you feed continuously. When your use case doesn't fit, drop to the manual API (§10).

## A multi-step pipeline

One step doubling numbers is not very interesting. What we actually want is: read a measurement as a string, parse it, convert units, and format the result. That's a chain of three steps:

```clojure
(def pipeline
  (step/serial
    (step/step :parse   #(Double/parseDouble %))
    (step/step :convert #(* % 2.54))            ; inches → cm
    (step/step :format  #(format "%.2f cm" %))))

(flow/run-seq pipeline ["1.0" "2.5" "4.0"])
;; => {:outputs [["2.54 cm"] ["6.35 cm"] ["10.16 cm"]] ...}
```

`step/serial` wires the `:out` of each step to the `:in` of the next. You don't name the connections; they're implicit from the ordering. If you want something non-linear, `step/connect`, `step/merge-steps`, `step/input-at`, and `step/output-at` let you hand-wire. Both `serial` and `merge-steps` take an optional keyword id as their first argument — when provided, the composed result is self-wrapped under that id as a named subflow (its inner procs get a `[:scope id]` segment on every event, and outer code can't collide with the inner ids). Chains are common enough to deserve sugar. Two more primitives come up often enough to name: `step/sink` (a terminal step that consumes its input and emits nothing — useful at the tail of a run-to-completion graph) and `step/passthrough` (forwards its input unchanged, preserving `:data-id` — the step-level companion to `msg/pass`).

Each step here is written with the simplest possible handler shape — a pure function from data to data. The library wraps it for you. No ctx, no ports, no state, no messages. You hand over a function; it gets called with each value; whatever it returns gets sent on.

*The trade-off.* A pure `data → data` handler can't split its output across ports, can't drop a message, can't keep state, and can't tell you which input caused which output. Often you don't need any of that; when you do, you peel back the wrapper (§6).

## Branching and rejoining

Before peeling anything back, one more black box. Suppose each measurement is expensive to process — say, because conversion involves a network call — and we'd like to work on a batch of them in parallel.

```clojure
(require '[toolkit.datapotamus.combinators :as c])

(def parallel-convert (c/workers 4 slow-convert-step))
```

`workers` gives you `k` parallel copies of an inner step, behind a round-robin router, with a join at the end that puts everything back in one stream. From the outside a `workers` block has one input port and one output port — it looks like a single operation. Internally there are four parallel procs, and each proc has its own trace scope (`<id>.w0`, `<id>.w1`, `<id>.w2`, `<id>.w3`), which surfaces utilization and latency per worker directly in the event stream if you want to look. That is the pattern: parallelism is hidden by default and made observable by stepping down into the decomposition.

Three close relatives, also black boxes for now:

- `c/fan-out` — take one message and split it into one sibling per declared output port. `(c/fan-out :dispatch [:solver :skeptic])` emits one copy on `:solver` and one on `:skeptic`, each carrying a slice of a fresh zero-sum group named by the fan-out's id. A 3-arity variant accepts a selector fn so the port subset (and per-port payload) can depend on the input.
- `c/fan-in` — wait for those siblings to come home, then emit one merged message. The merged payload is a `{port data}` map keyed by the input port each sibling arrived on; pass an optional post-fn (e.g. `vals`) to reshape it.
- `c/parallel` — scatter-gather bracket. Takes an id and a `{port → step}` map; the keys become the parallel ports. Internally builds a `fan-out`, wires each port to its inner step, wires each step's output to the matching `fan-in` port, and exposes a single-`:in`/single-`:out` wrapped step. `(c/parallel :roles {:solver solver-step :skeptic skeptic-step})` is the "one input, N heterogeneous specialists, collect results" pattern — no ports or wiring repeated by hand. Optional `:select` and `:post` kwargs pass through to the underlying fan-out/fan-in.

They close the loop: you fan out to a set of downstreams, you do work in parallel, you fan in by referencing the fan-out's id. `parallel` is the common case where that loop is local; `fan-out` / `fan-in` are the construction material when you need to do work between them that `parallel` can't express (iterative rounds, routing, custom coordination).

*The trade-off.* `fan-in` waits until *all* the siblings have arrived. If one is lost — dropped, or swallowed by a bug — the fan-in waits forever. We'll see in the next section why "waits for all siblings" doesn't need a counter, and also exactly how sharp the "waits forever" cliff is.

## The trick: conservation of token mass

Every message in Datapotamus carries a **token-vector** — a map from `group-id` to `u64`. Tokens compose by XOR. And **every step preserves the XOR-sum of every group from its inputs to its outputs**. Not as a convention; as arithmetic that the machinery enforces.

Why do you care? Because XOR-balance is a free timer.

Suppose you fan out one message into, say, seventeen children. They each go do something — a computation, a database write, another pipeline. Eventually each one sends back a message for the fan-in to collect. You'd like to know when *all seventeen have come home*. Normally you'd write a counter. You'd worry about what happens if one is lost. You'd worry about accounting. You'd discover, in production, that you'd forgotten a case.

In Datapotamus: you mint a fresh group-id at the fan-out, give each of the seventeen children a different u64 piece such that the seventeen pieces XOR to zero, and go about your business. Whoever downstream wants to know "are we done?" just watches the XOR-sum for that group. When it hits zero, every piece has been accounted for. You have not counted anything. You have not named the children. You haven't even said "seventeen." The algebra did it.

That's the trick. Everything else in Datapotamus is scaffolding that makes this trick reliable, composable, and testable in pieces.

The canonical framing is **conservation of token mass**. Tokens are XOR-balanced at mint time, and every operation the framework performs on them preserves that balance. Under automatic propagation, you cannot add or remove tokens by accident — only deliberately, through escape hatches we'll get to later.

*Aside: this is Emmy Noether's trick from 1918 — symmetries imply conservation laws — redirected from reformulating Hamiltonian mechanics toward managing async pipelines. XOR commutes with routing; so XOR-sums are conserved; so closure detection is free. She probably had grander applications in mind. This one still works.*

*The trade-off, and it's a real one.* Conservation gives you a free completion signal **only as long as every message arrives exactly once**. If a message is genuinely dropped — a proc crashes, a bug swallows it, the operating system declines to cooperate — the XOR sum for its group never returns to zero, and the downstream `fan-in` waits forever. No timeout fires, because there is no timeout. A duplicated message is worse: its two copies XOR-cancel in the group, so the group appears to close correctly but with fewer payloads than the producer sent. The `:split` and `:merge` events in the trace log let you reconstruct what went missing, but only after the fact, and only manually. You have traded a counter — which could have had a timeout bolted onto it — for arithmetic that genuinely does not know how to panic. Whether that's the right trade is a design decision you make once per pipeline. For work where every message matters, it's wonderful. For best-effort data streams, you'll want to layer a deadline on top.

## Peeling back: the handler

Now let's take apart the step we wrote in §2. The two-argument `(step/step :double f)` wraps your pure function into a richer handler shape. That shape has several layers, and we'll unlock them one at a time — each motivated by something the running example suddenly needs.

### Layer 1: ports and the port-map

Some lines in the input file are malformed: empty strings, typos, numbers with units attached. We want those routed to a separate `:reject` stream so a human can look at them later. A `data → data` function can't do that — it only has one return value.

The three-argument form of `step/step` gives you ports:

```clojure
(def parse
  (step/step :parse
             {:ins  {:in     "raw line"}
              :outs {:out    "parsed number"
                     :reject "malformed line"}}
             (fn [_ctx _state s]
               (try
                 {:out [(Double/parseDouble s)]}
                 (catch NumberFormatException _
                   {:reject [s]})))))
```

Three things changed. We declared the port shape explicitly (`:ins` and `:outs` maps). The handler now takes `(ctx, state, data)` instead of just `data`. And the return value is a **port-map**: `{port [value ...]}`. Each key is an output port; each value is a vector of things to send out that port.

The `ctx` is a small map the interpreter builds fresh for each invocation: `:msg` (the full input envelope, used by the msg constructors in the next layer), `:in-port` (which input port this message arrived on — matters for multi-input steps like joiners), `:step-id` (this step's trace id), `:ins` / `:outs` (the port specs), `:pubsub` (publish your own events here), and `:cancel` (a promise that's delivered when someone calls `flow/stop!` — deref-poll it in long-running workers that need to abort early).

An empty map drops the message. A map with multiple keys sends different payloads to different ports. The values inside can be bare data (which auto-wraps as a child of the input, carrying lineage) or full msg envelopes built with the constructors we'll see next.

*The trade-off.* You now have to declare the port shape and wrap returns in a map, even when you only want one output. A modest notational tax for a feature you may not always want — which is exactly why `step/step` also has the two-arg form that hides it.

### Layer 2: lineage — which input caused this output?

A new requirement: when `"not-a-number"` shows up on `:reject`, we'd like to know *which line of the input file it was*. The framework already has the answer (auto-wrapping records parent → child), but we haven't asked it yet.

For that, a small vocabulary of **msg constructors**:

```clojure
(require '[toolkit.datapotamus.msg :as msg])

(def parse
  (step/step :parse
             {:ins  {:in     "raw line"}
              :outs {:out    "parsed number"
                     :reject "malformed line"}}
             (fn [ctx _state s]
               (try
                 {:out [(msg/child ctx (Double/parseDouble s))]}
                 (catch NumberFormatException _
                   {:reject [(msg/pass ctx)]})))))
```

`msg/child ctx data` means "this output is a new child of the input message, with new data" — a fresh `:data-id` too, marking a new logical datum in the lineage graph. `msg/pass ctx` means "this is the *same* logical datum flowing through" — it preserves the parent's `:data` *and* its `:data-id`, which is what you want for filters, routers, and passthrough steps where the message is being forwarded rather than transformed. (Attribution in `flow/run-seq` follows `:data-id`, so this distinction matters for input→output tracking.) Three more constructors round out the set:

- `msg/children ctx [d1 d2 ...]` — N siblings from one parent.
- `msg/signal ctx` — lineage with no payload.
- `msg/merge ctx [p1 p2] data` — one child descending from several parents.

The critical idea: the handler doesn't build *finished* output messages. It builds a **recipe** — a tree of pending msgs that records which inputs each output descends from. The framework will fill in the tokens (§8) after the handler returns. If you've met free monads, this is that — the handler says what should happen, the framework fills in details the handler isn't in a position to know.

*The trade-off.* You now have two ways to emit an output (bare data vs. msg constructor), and a reader of your handler has to know which shape applies. The common case (bare data auto-wrapping as `msg/child`) deserves the sugar, but it's a small hazard for someone new to the code.

### Layer 3: state

The auditor's next request: skip duplicates. If the value `2.54` has already been emitted, we don't want to emit it again.

State in a handler is whatever you want — a map is traditional; a set works; a number works; `nil` is fine if you're not keeping any. To update it, wrap the return value in a vector `[state' port-map]`:

```clojure
(def dedup
  (step/step :dedup
             {:ins  {:in  "value"}
              :outs {:out "value, first time seen"}}
             (fn [_ctx state x]
               (if (contains? state x)
                 [state {}]                      ; drop: state unchanged, no output
                 [(conj state x) {:out [x]}])))) ; emit: state updated
```

A plain map return means state is unchanged. A vector `[state' port-map]` means state is updated. The initial state, if you don't configure it (§11), is `{}` — but `state` here is a set because we `conj` onto it, and Clojure is happy with that.

Returning `[state {}]` drops the payload. It doesn't drop the *tokens* — the framework auto-synthesizes a signal on every output port for any data-handler that returns an empty port-map, so the input's tokens propagate onward for downstream closure. This is what you want 99% of the time. For the 1% — pair-mergers, batchers, dribblers that stash the input and emit a merge from it later — return `msg/drain` instead of `{}` to suppress the auto-signal (the stashed ref carries the tokens via the eventual merge, and the auto-signal would double-count).

*The trade-off.* State lives inside the proc, not in the messages. That means state is per-proc: if you fan out to four workers, each worker has its own dedup set. If you want a *global* dedup, put one `dedup` step in front of the fan-out (single proc, global state) rather than inside each worker. Knowing which you want is now a thing you have to think about.

### Layer 4: custom signals, done, lifecycle

There is one more layer. It's where we open our output file. We need two more concepts first — the three envelope shapes (§9) and the full lifecycle (§10) — so we'll come back to it in §11.

## Peeling back: the recipe

Layer 2 above said the handler's outputs are a recipe, not a list. Here's what that means mechanically.

Each msg constructor produces a **pending msg**: a plain map that carries an in-memory ref (under the key `::parents`) to the message or messages it came from. `msg/children ctx [a b c]` produces three pending msgs, each pointing at the same parent. `msg/merge ctx [p1 p2] data` produces one pending msg pointing at two parents.

The handler builds this tree and returns it. It doesn't know yet what tokens each pending msg should carry, because that depends on how many siblings the msg has and which leaves they share. Only after the whole return value is in view can the framework compute the token distribution. That's the next section. But first, the invariant we promised.

### The single-invocation invariant

One rule matters more than almost anything else in this library. The docstring on `step/step` calls it out; it's worth repeating here because it falls out of the synthesis arithmetic in §8 and you need to carry it in your head:

> **Every child message ever derived from a given parent must be emitted from a single handler invocation.**

This is conservation of token mass in disguise. The framework takes each input message's tokens and XOR-splits them evenly among the descendants *present in this handler's output*. If you emit three children here, the parent's tokens get split three ways, one slice per child. If you also squirrel a reference to the parent away in state and emit a fourth child from it in a later call, the framework will split those tokens again — one way — and the fourth child will pick up slices that conflict with the first three. Downstream, the XOR sums fail to balance, `fan-in` waits forever, and you have no idea why.

*The trade-off.* The rule is documented but unchecked at runtime. There's no practical way for the library to notice a violation until conservation fails downstream. The sanctioned pattern for deferred derivation: stash the parent ref in state, return `msg/drain` on the stashing calls (so the auto-signal doesn't propagate tokens that the eventual merge will re-carry), and emit a single `msg/merge` listing all the eventual parents in one later invocation. That's exactly how `c/fan-in` is built — it's the canonical example to crib from.

## Peeling back: synthesis

After the handler returns, a pure fold called **synthesis** runs over its output. This is where the token arithmetic actually happens. It lives in `msg.clj` in about forty lines, and it is the keystone of the whole library.

The fold does this:

1. Walk each pending msg backward through its `::parents` until you hit a **leaf** — a real input message. Those leaves are where tokens come from.
2. For each leaf, count how many pending msgs descend from it. Call that K.
3. XOR-split the leaf's tokens into K pieces. (Cheap: generate K−1 random u64s; the Kth is whatever XORs with them to equal the leaf's value.)
4. Merge the right piece into each descendant's token-vector.
5. Apply any stamped `::assoc-tokens` / `::dissoc-tokens` decorations — the escape hatches for designing custom groups (§12).
6. Strip the refs. Tag `:tokens` on. Emit a `:split` or `:merge` trace event per materialized msg.

All pure. No channels, no atoms, no runtime. Property-test it over randomized provenance DAGs; conservation holds by construction because the only arithmetic is K-way splitting and XOR-merging, both of which preserve the group operation.

This is where the design earns its keep. Conservation is not *asserted* somewhere and then *checked* later. It's baked into the one path that produces output. A message that comes out of synthesis cannot violate conservation, because that is not a thing the synthesis code is capable of doing.

It is, however, a thing user code *can* do — see the single-invocation invariant in §7 above.

*The trade-off.* The K-way split is positional. Each output's tokens are computed from its place in the DAG at this instant, not from any identity it carries. That means if you want to correlate outputs to inputs in some *application-level* way (e.g., "which input produced this output"), use the `:parent-msg-ids` chain or the `:data-id` field. Tokens are for closure detection. The lineage graph underneath them is the provenance mechanism.

## Peeling back: three envelope shapes, no tag required

A message envelope is a plain map. Three shapes, distinguished by which keys are present:

| Kind       | has `:data` | has `:tokens` | Purpose |
|---|---|---|---|
| **data**   | yes | yes | The payload you care about. `nil` is valid data. |
| **signal** | no  | yes | Tokens alone, no payload. Use it to coordinate. |
| **done**   | no  | no  | End of stream. |

"Signal" is the interesting one. It's a pulse that carries tokens but no data — send it through the graph when you want the token math to happen without anyone having to process a payload. Most coordination patterns use signals. In fact, the common case is so common that the framework does it for you: a data handler that returns an empty port-map (`{}` or `[state' {}]`) gets a signal auto-synthesized on every declared output port, so the input's tokens propagate for downstream closure. That's what filters, dedups, and skips rely on. Two cases want the opposite — suppress the auto-signal by returning `msg/drain` instead: deferred propagation (pair-mergers, batchers — see §7), and the rare genuine drop where you *really* want the tokens to die here. Conservation is optional on your terms; just say so explicitly.

Your handler's `:on-data` doesn't see signals; they arrive at `:on-signal` instead (default: broadcast unchanged to every output port).

"Done" cascades. When all of a step's input ports have reported done, the step's `:on-all-closed` hook fires (your chance to flush buffers or emit a final tail), and then a `done` is sent out every output port. Closure propagates from the edges inward until the whole graph is quiet.

## Peeling back: the lifecycle

So far we've driven everything with `flow/run-seq`. That's the convenience layer for the finite-input case. Real pipelines — the ones ingesting from Kafka, from a database stream, from any long-lived source — live one tier below, and that's what the library is actually for. The three tiers peel back from `run-seq`'s simplicity toward the shape production use wants.

### One shot

```clojure
(flow/run!    step {:data my-input})
(flow/run-seq step [a b c])
```

`run!` starts the flow, injects one message, waits for quiescence, returns `{:state :counters :error}`. `run-seq` does the same but injects each element of the collection as its own message, and attributes outputs back to the inputs that caused them (via the lineage graph) — adding `:outputs` to the result.

### Manual

```clojure
(def h (flow/start! step {:pubsub ps :flow-id "ingest"}))
(doseq [item (items-from-kafka)]
  (flow/inject! h {:data item}))
...
(flow/stop! h)
```

`start!` instantiates and begins running. `inject!` routes one envelope into a boundary input port. `stop!` tears down and returns the same shape `run!` does.

- `(inject! h {:data v})` — data envelope.
- `(inject! h {:tokens tm})` — signal envelope (no data, tokens only).
- `(inject! h {})` — done marker.

This is the shape for long-running pipelines. You own the outer loop; you decide when to stop.

### Observable

Layer observability on top of the manual tier:

```clojure
(flow/counters         h)   ; {:sent :recv :completed}
(flow/quiescent?       h)   ; counters balanced?
(flow/await-quiescent! h)   ; block until they balance
```

Quiescence is defined arithmetically: `sent = recv + completed`. No polling, no timeouts, just balance. Every injected envelope leaves the system either as a receipt or as a completion event — another conservation law, if you're keeping score.

For a live event firehose, subscribe on the pubsub directly. Pass your own `pubsub/make` via `{:pubsub ps}` to `start!`, register handlers with `pubsub/sub`, and the flow will publish onto it as it runs. Subjects have the shape `[<kind> "scope" <fid> ("scope" <sub>)* ("step" <sid>)?]`. Glob patterns work: `[:* "scope" "ingest" :>]` gets everything in one flow; `["recv" "scope" :* :>]` gets every receive across all of them. The core never self-subscribes — it only publishes — so you own the subscriber lifecycle.

*The trade-off.* Quiescence is a *structural* condition, not a semantic one. If your state machine produces a different number of outputs per input on average than it consumes, quiescence may never trigger. That's a feature for pipelines that emit retries or batched outputs — you don't want `await-quiescent!` to lie to you — but it means "wait until done" is sometimes not a meaningful question to ask.

## Escape hatch: custom lifecycle with handler-map

Back to the running example. We want to open the output file once, write each converted line as it comes through, flush a footer when the input is done, and close the file on shutdown. The handler shape we've been using has nowhere to put "open" and "close." For that, we drop below `step/step` to the underlying **handler-map**:

```clojure
(defn writer-step [path]
  (step/serial :writer
    (step/handler-map
     {:ports         {:ins {:in "line to write"} :outs {}}
      :on-init       (fn []
                       {:writer (io/writer (io/file path))})
      :on-data       (fn [_ctx state line]
                       (.write (:writer state) (str line "\n"))
                       [state {}])
      :on-all-closed (fn [_ctx state]
                       (.write (:writer state) "# end\n")
                       {})
      :on-stop       (fn [_ctx state]
                       (.close (:writer state))
                       nil)})))
```

- `:on-init` runs once before any messages arrive — allocate resources here.
- `:on-data` is what `step/step` has been installing for us under the hood.
- `:on-signal` (default: broadcast on every output port) fires on signal envelopes.
- `:on-all-closed` runs when every input port has reported done — flush tails here.
- `:on-stop` runs exactly once when the flow tears down — release resources here.

`step/step` fills in defaults for every slot except `:on-data`, which is why you can usually ignore them. When you find yourself opening files, seeding buffers, or needing non-trivial startup and shutdown, drop to `handler-map`.

*The trade-off.* You lose `step`'s safety nets. If you override `:on-signal` and forget to propagate the signal, it silently vanishes — the framework doesn't know what "handle the signal" means for your proc. Same for `:on-all-closed` and the done cascade: if you override it, make sure `done` still propagates, or read `run-done` in `flow.clj` to see what the default was doing for you. Power, responsibility, and so on.

## Escape hatch: explicit token control

With the handler-map in hand, we now have everything needed to build new token-coordinated combinators. This is exactly what `c/fan-out` and `c/fan-in` do under the hood, and the technique is fully available to user code.

Token control has three layers, matching the three ways a message can pick up tokens:

1. **Automatic.** Your handler returns msg envelopes; synthesis XOR-splits the parent's tokens among them. Everything we've done so far.
2. **Annotated.** You return msg envelopes decorated with `msg/assoc-tokens` or `msg/dissoc-tokens`. Synthesis distributes the parent's tokens normally, then applies your annotations on top.
3. **Raw.** You compute token maps directly with `tok/split-value`, `tok/split-tokens`, `tok/merge-tokens` when designing a new protocol from scratch.

Here is the entire body of `c/fan-out`, minus the id/ports boilerplate:

```clojure
(fn [ctx _s d]
  (let [gid     [id (:msg-id (:msg ctx))]
        by-port (selector-fn d)                ; {port payload}
        n       (count by-port)
        values  (tok/split-value 0 n)          ; n u64s that XOR to 0
        kids    (for [[[port payload] v] (map vector by-port values)]
                  [port [(-> (msg/child ctx payload)
                             (msg/assoc-tokens {gid v}))]])]
    (into {} kids)))
```

A handful of lines. `split-value 0 n` gives us `n` u64 values that XOR to zero — a zero-sum group. One `msg/child` is built per declared port, each stamped with a different slice via `assoc-tokens`. Synthesis takes care of the rest: the original input's tokens get distributed across the `n` children as usual, and *on top of that*, each child picks up its slice of the new group.

Downstream, `c/fan-in` accumulates messages tagged with that group, XORs their values, and when the XOR returns to zero calls `msg/dissoc-tokens` to strip the group key from the merged output. The merged payload is a `{port data}` map keyed by the port each sibling arrived on (an optional post-fn reshapes it). The group closes and vanishes. No counter. No accounting.

The same technique supports your own protocols. A priority token that a downstream router reads and dispatches on. A dribble pattern where one producer emits N messages over time, each carrying a balancing piece of a zero-sum group, and downstream knows the stream is complete when the XOR closes. The tests under `assoc-tokens-mints-custom-group` and `dribble-closes-conservation` are in-repo exemplars.

*The trade-off, and it's the sharpest one in the library.* Once you leave the auto-propagation path, conservation is *your* responsibility. If your custom group doesn't balance, the bug is yours to find. The library will happily carry unbalanced mass forever, and you'll discover it when something downstream waits forever for a closure that was never going to come. The `:split` and `:merge` events in the trace log are your only diagnostic. The discipline is simple — always mint zero-sum groups, always strip the group key at the place that knows the group is closed — but the library does not enforce it and cannot.

## The seam between the algebra and the runtime

Not everything is algebra. The runtime — how messages hop between procs, how backpressure works, how errors propagate, how quiescence gets detected — is `core.async.flow`'s territory. It lives in `flow.clj`. Nowhere else. If you see a `core.async` import drifting into `msg.clj` or `step.clj`, something has gone wrong and should be pushed back across the line.

Everything else is pure data and pure functions. Messages, steps, synthesis, tokens. Property-test them without a channel. Unit-test a handler by feeding it synthetic envelopes and reading the return value. You don't need a running flow to verify that a handler does what it says.

That seam is the most important design decision in the library. The algebra doesn't know about its runtime; that's why it stays stable. If you wanted to port Datapotamus to a different substrate — actors, CSP, something else — you'd rewrite `flow.clj` and leave the rest alone.

## Where the code lives

```
msg.clj          envelopes, free algebra, pure synthesis
step.clj         steps, handler-maps, composition
flow.clj         the interpreter — the only file that imports core.async.flow
combinators.clj  fan-out, fan-in, workers
token.clj        the u64 XOR algebra
trace.clj        event constructors and scoped pubsub
                   (don't import this; subscribe on the pubsub instead)
```

Typical aliases:

| Namespace | Alias | When |
|---|---|---|
| `toolkit.datapotamus.step`        | `:as step` | Always |
| `toolkit.datapotamus.flow`        | `:as flow` | Always |
| `toolkit.datapotamus.msg`         | `:as msg`  | Whenever building msgs |
| `toolkit.datapotamus.combinators` | `:as c`    | Fan-out, fan-in, workers |
| `toolkit.datapotamus.token`       | `:as tok`  | Designing new combinators |

---

**One thing to take with you.** A flow is a value. The invariant is a conservation law. Coordination patterns are balance-sheets. Write the state machines; let synthesis balance the tokens; let the runtime move the bytes. You govern from the algebra; the channels do as they're told.
