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

*The trade-off.* `run-seq` blocks until every input has produced everything it's going to produce. That's great for finite collections; it's the wrong shape for a pipeline you feed continuously. When your use case doesn't fit, drop to the manual API (§11).

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

`step/serial` wires the `:out` of each step to the `:in` of the next. You don't name the connections; they're implicit from the ordering. If you want something non-linear, `step/connect`, `step/beside`, `step/input-at`, and `step/output-at` let you hand-wire. Both `serial` and `beside` take an optional keyword id as their first argument — when provided, the composed result is self-wrapped under that id as a named subflow (its inner procs get a `[:scope id]` segment on every event, and outer code can't collide with the inner ids). Chains are common enough to deserve sugar. Two more primitives come up often enough to name: `step/sink` (a terminal step that consumes its input and emits nothing — useful at the tail of a run-to-completion graph) and `step/passthrough` (forwards its input unchanged, preserving `:data-id` — the step-level companion to `msg/pass`).

Each step here is written with the simplest possible handler shape — a pure function from data to data. The library wraps it for you. No ctx, no ports, no state, no messages. You hand over a function; it gets called with each value; whatever it returns gets sent on.

*The trade-off.* A pure `data → data` handler can't split its output across ports, can't drop a message, can't keep state, and can't tell you which input caused which output. Often you don't need any of that; when you do, you peel back the wrapper (§7).

## Branching and rejoining

A pipeline that goes left to right is the easy case. Most real work branches. Work spreads across specialists who return different answers to the same question; work spreads across identical workers to soak up load; and underneath both sits a small algebra of fan-out and fan-in that you reach for when neither of those two shapes fits. In that order.

```clojure
(require '[toolkit.datapotamus.combinators :as c])
```

### Parallel: one input, N specialists

The next feature request: each measurement wants three opinions — a cleaner that normalizes units, a geocoder that resolves where it was taken, and an enricher that pulls adjacent sensor data. Each specialist gets the same measurement; each returns its own view; downstream wants all three bundled into one object so it can decide what to do.

```clojure
(def annotate
  (c/parallel :annotate
    {:clean  clean-step
     :geo    geocode-step
     :enrich enrich-step}))
```

Input to `annotate` is one measurement. Output is one message whose `:data` is `{:clean ... :geo ... :enrich ...}` — the keys are the ports you declared. Port-of-origin doubles as role label through the fan-in, for free; downstream can tell the cleaner's answer from the geocoder's just by looking at the key.

*Runtime planning.* When the set of specialists isn't known until you see the input, `:select` picks a subset of pre-declared ports (optionally with per-port payloads); `:post` reshapes the collected outputs before emission.

```clojure
(c/parallel :plan
  {:w0 worker-0 :w1 worker-1 :w2 worker-2}
  :select (fn [{:keys [question budget]}]
            (let [tasks (plan-fn question budget)  ; returns 1–3 subtasks
                  each  (quot budget (count tasks))]
              (into {} (map-indexed
                        (fn [i t] [(keyword (str "w" i))
                                   {:task t :budget each}])
                        tasks))))
  :post vals)
```

`:select` returns a `{port payload}` map; ports not listed stay idle for that input, so the bracket scales its parallelism to the work at hand. `:post vals` drops the port keys when downstream cares about the outputs and not which worker produced each. The design constraint worth naming out loud: `core.async.flow` requires fixed output-port sets at graph-construction time, so "dynamic" here means *picking from a pre-declared pool*, not growing one at runtime. You declare the pool's maximum width once; the selector uses as little of it as it needs.

*The trade-off.* `parallel` is a bracket with one shape: one input in, scatter to declared ports, each port runs its own step, gather, emit one `{port data}` map. Each port's step can itself be a composed pipeline (`step/serial A B`), two `parallel`s in series compose cleanly (the debate shape below does exactly that), and `parallel` nests inside `parallel` (see §6) — so "multi-step per-port work" and "multiple rounds with cross-sibling reads through the merged map" are already in scope. Drop to `fan-out` / `fan-in` directly when the pattern doesn't fit a port-keyed single-input bracket: groups that span several top-level inputs (dribble, pair-merger, batcher), closure protocols that aren't "every sibling emits once" (quorum, signal-driven), or custom token groups designed via `assoc-tokens` / `dissoc-tokens` (§13).

### Workers: K copies of one thing

Orthogonal axis. `parallel` asks "who should look at this?" `round-robin-workers` asks "how many copies of one specialist do I need, to keep up with the load?" Suppose conversion involves a network call and we want a batch of measurements handled in parallel:

```clojure
(def parallel-convert (c/round-robin-workers 4 slow-convert-step))
```

`round-robin-workers` gives you `k` parallel copies of an inner step, behind a round-robin router, with a join at the end that puts everything back in one stream. From the outside a `round-robin-workers` block has one input port and one output port — it looks like a single operation. Internally there are four parallel procs, and the pool lives under a single `[:scope <id>]` segment in the trace with each worker proc carrying a leaf `:step-id` of `:w0`, `:w1`, `:w2`, `:w3` (the port the router chose for it). Filter the event stream by scope to get "everything in the pool," by `:step-id` to get "everything on one worker" — utilization and latency per worker fall out for free. That is the pattern: parallelism is hidden by default and made observable by stepping down into the decomposition.

### Fan-out / fan-in: the algebra underneath

`c/parallel` is built on two lower-level primitives, exposed because some patterns don't fit a port-keyed single-input bracket: groups that span multiple top-level inputs (dribble, pair-merger, batcher), closure protocols that aren't "every sibling emits once" (quorum, signal-driven), and custom token groups designed via `assoc-tokens` / `dissoc-tokens`. They're also the place to look when you want to understand *how* the scatter-gather actually closes.

- `c/fan-out` takes an id and a list of output ports; it emits one child per port, each carrying a different slice of a fresh zero-sum group keyed on `[id parent-msg-id]`. A three-arity form takes a selector that returns either a subset of ports (broadcast payload to those) or a `{port payload}` map (distinct payloads per port), so the scatter can depend on the input.
- `c/fan-in` takes its own id plus the fan-out's id; it accumulates messages whose tokens carry that group, and when the XOR-sum returns to zero it emits one merged message whose `:data` is a `{port data}` map keyed by the arrival port. An optional post-fn (e.g. `vals`) reshapes the map before emission.

The group key is `[fan-out-id parent-msg-id]`, not just the fan-out id. That matters: two different inputs running through the same fan-out open two different groups that close independently. **Nested fan-outs compose the same way** — the inner fan-out's group is keyed on whichever outer sibling spawned it, so three outer siblings each opening their own inner group close their inner groups independently, and only then does the outer group close. Two XOR-sums, no counters, no bookkeeping. You can nest `c/parallel` inside `c/parallel` for the same reason (we'll do exactly that in the debate example in §6).

*The trade-off.* `fan-in` waits until *all* the siblings have arrived. If one is lost — dropped, or swallowed by a bug — the fan-in waits forever. We'll see in the next section why "waits for all siblings" doesn't need a counter, and also exactly how sharp the "waits forever" cliff is.

## The trick: conservation of token mass

Every message in Datapotamus carries a **token-vector** — a map from `group-id` to `u64`. Tokens compose by XOR. And **every step preserves the XOR-sum of every group from its inputs to its outputs**. Not as a convention; as arithmetic that the machinery enforces.

> **Invariant (conservation of token mass).** For every group `g`, every step `s`, and every invocation of `s`: the XOR-sum of `tokens[g]` over all messages entering `s` equals the XOR-sum over all messages leaving it — counting the signals and merges the framework synthesizes from the handler's recipe. Splits are K-way XOR-partitions; merges are K-way XOR-sums; both preserve the group.

Why do you care? Because XOR-balance is a free timer.

Suppose you fan out one message into, say, seventeen children. They each go do something — a computation, a database write, another pipeline. Eventually each one sends back a message for the fan-in to collect. You'd like to know when *all seventeen have come home*. Normally you'd write a counter. You'd worry about what happens if one is lost. You'd worry about accounting. You'd discover, in production, that you'd forgotten a case.

In Datapotamus: you mint a fresh group-id at the fan-out, give each of the seventeen children a different u64 piece such that the seventeen pieces XOR to zero, and go about your business. Whoever downstream wants to know "are we done?" just watches the XOR-sum for that group. When it hits zero, every piece has been accounted for. You have not counted anything. You have not named the children. You haven't even said "seventeen." The algebra did it.

That's the trick. Everything else in Datapotamus is scaffolding that makes this trick reliable, composable, and testable in pieces.

The canonical framing is **conservation of token mass**. Tokens are XOR-balanced at mint time, and every operation the framework performs on them preserves that balance. Under automatic propagation, you cannot add or remove tokens by accident — only deliberately, through escape hatches we'll get to later.

*Aside: this is Emmy Noether's trick from 1918 — symmetries imply conservation laws — redirected from reformulating Hamiltonian mechanics toward managing async pipelines. XOR commutes with routing; so XOR-sums are conserved; so closure detection is free. She probably had grander applications in mind. This one still works.*

*The trade-off, and it's a real one.* Conservation gives you a free completion signal **only as long as every message arrives exactly once**. If a message is genuinely dropped — a proc crashes, a bug swallows it, the operating system declines to cooperate — the XOR sum for its group never returns to zero, and the downstream `fan-in` waits forever. No timeout fires, because there is no timeout. A duplicated message is worse: its two copies XOR-cancel in the group, so the group appears to close correctly but with fewer payloads than the producer sent. The `:split` and `:merge` events in the trace log let you reconstruct what went missing, but only after the fact, and only manually. You have traded a counter — which could have had a timeout bolted onto it — for arithmetic that genuinely does not know how to panic. Whether that's the right trade is a design decision you make once per pipeline. For work where every message matters, it's wonderful. For best-effort data streams, you'll want to layer a deadline on top.

## Agentic flows

"Agentic," as the term is usually meant: a graph whose nodes are specialists — often LLMs, sometimes smaller tools — whose edges carry intermediate products of reasoning, and whose topology encodes the collaboration protocol. The word means roughly what it wants to mean, and the literature has not fully decided; the interesting object is the graph, not the label. This section tours four shapes that come up often, each built from primitives you've already seen.

The library's three structural properties — conservation of token mass, lineage threaded through every message, and scoped trace events — give you, for free, three things every agent framework reaches for late and awkwardly: reliable completion detection without per-shape counters, a provenance graph that makes "which input caused this output" recoverable, and per-scope observability that maps cleanly onto what a human watching the system wants to see.

### Parallel roles

The simplest multi-agent shape: one question, several heterogeneous roles, one answer per role.

```clojure
(def panel
  (c/parallel :roles
    {:solver  solver-step
     :facts   facts-step
     :skeptic skeptic-step
     :second  second-step}))
```

`panel` takes one input and emits one output whose `:data` is `{:solver "…" :facts "…" :skeptic "…" :second "…"}`. Port-of-origin preserves role identity through the fan-in automatically — downstream never has to guess which answer came from the skeptic, because the skeptic's answer is under `:skeptic`. This is the shape behind every "ask N specialists and compare" workflow, and because it's scatter-gather, `c/parallel` is the right primitive.

### Tool-call loops

Not every agent shape is acyclic. The canonical agent-with-tools is an `:agent` step with two inputs — `:user-in` for the external drive, `:tool-result` for the loop — and two outputs — `:tool-call` to ask a tool for something, `:final` to answer the user. Wire the tool's output back to the agent's `:tool-result` with a plain `step/connect`:

```clojure
(def agent-step
  (step/step :agent
    {:ins  {:user-in   "" :tool-result ""}
     :outs {:tool-call "" :final       ""}}
    (fn [_ctx s _d]
      (let [n (inc (:n s 0))]
        (if (< n 3)
          [(assoc s :n n) {:tool-call [:query]}]
          [(assoc s :n n) {:final     [:answer]}])))))

(def loop-wf
  (-> (step/beside
        agent-step
        (step/step :tool (constantly :tool-response))
        (step/sink))
      (step/connect [:agent :tool-call] [:tool  :in])
      (step/connect [:tool  :out]       [:agent :tool-result])
      (step/connect [:agent :final]     [:sink  :in])
      (step/input-at  [:agent :user-in])
      (step/output-at :sink)))
```

The external world injects one message on `[:agent :user-in]`; the agent emits a `:tool-call` which becomes a `:tool-result` which becomes another `:tool-call`, and the cycle continues until the agent emits on `:final`. The loop terminates because the agent eventually *chooses* `:final`; conservation holds because every tool-call produces exactly one tool-result that produces exactly one re-entry into the agent. Nothing in the synthesis code cares whether a message's ancestry happens to pass through the agent step a second, third, or seventeenth time — the algebra is structural, not topological. If your handler makes a principled choice to stop, the flow reaches quiescence when the trace catches up to that choice.

### Dynamic subtask planning

A planner with runtime-variable parallelism. Same mechanism as the `:select` example in §4 (the `c/parallel :plan` pool), reframed as the pattern it actually is: decide per-input how many specialists to dispatch, what each one gets, and leave the rest idle. Pre-declare the pool's maximum width; let the planner subset it per input; add `:post vals` when downstream only cares about the answers and not which worker produced each. The pool shape means you can't grow past the declared ports, but any subset is yours for free; the graph stays knowable at construction time and every proc has a stable identity in the trace, which is exactly what you want when something goes wrong at 3am.

### Debate

Two `c/parallel` brackets stacked in series, one per round.

```clojure
(def debate
  (step/serial
    (c/parallel :r1 {:a (debater :a) :b (debater :b)})
    (c/parallel :r2 {:a (critic  :a) :b (critic  :b)})
    (step/sink)))
```

The cross-talk is implicit. Round 1's fan-in emits one message whose `:data` is `{:a "debater-a's answer" :b "debater-b's answer"}` — that's the whole round-1 result set. Round 2's fan-out broadcasts that entire map to each critic (one copy to `:a`, one copy to `:b`), and each critic reads its own side from the map plus its peer's side by the other key. No dedicated "swap" step. The shape the fan-in emits is the shape the next fan-out consumes; composition does the rest.

Because `c/parallel` wraps its inner procs under a `[:scope :r1]` or `[:scope :r2]` segment, a pubsub subscriber filtering on one scope gets "everything that happened in round 1" and nothing else. Debates are inspectable round-by-round without any parsing of step-id conventions, and extending to three rounds is one more `c/parallel` in the `serial`.

---

*A pointer forward.* The termination story for cyclic agents — "the agent eventually emits on `:final`" — rests on the three envelope shapes (data, signal, input-done) and the closure cascade that drives the flow to quiescence. See §10 for the envelope table and §11 for the lifecycle that uses it.

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

The critical idea: the handler doesn't build *finished* output messages. It builds a **recipe** — a tree of pending msgs that records which inputs each output descends from. The framework will fill in the tokens (§9) after the handler returns. If you've met free monads, this is that — the handler says what should happen, the framework fills in details the handler isn't in a position to know.

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

A plain map return means state is unchanged. A vector `[state' port-map]` means state is updated. The initial state, if you don't configure it (§12), is `{}` — but `state` here is a set because we `conj` onto it, and Clojure is happy with that.

Returning `[state {}]` drops the payload. It doesn't drop the *tokens* — the framework auto-synthesizes a signal on every output port for any data-handler that returns an empty port-map, so the input's tokens propagate onward for downstream closure. This is what you want 99% of the time. For the 1% — pair-mergers, batchers, dribblers that stash the input and emit a merge from it later — return `msg/drain` instead of `{}` to suppress the auto-signal (the stashed ref carries the tokens via the eventual merge, and the auto-signal would double-count).

*The trade-off.* State lives inside the proc, not in the messages. That means state is per-proc: if you fan out to four workers, each worker has its own dedup set. If you want a *global* dedup, put one `dedup` step in front of the fan-out (single proc, global state) rather than inside each worker. Knowing which you want is now a thing you have to think about.

### Layer 4: custom signals, input-done, lifecycle

There is one more layer. It's where we open our output file. We need two more concepts first — the three envelope shapes (§10) and the full lifecycle (§11) — so we'll come back to it in §12.

## Peeling back: the recipe

Layer 2 above said the handler's outputs are a recipe, not a list. Here's what that means mechanically.

Each msg constructor produces a **pending msg**: a plain map that carries an in-memory ref (under the key `::parents`) to the message or messages it came from. `msg/children ctx [a b c]` produces three pending msgs, each pointing at the same parent. `msg/merge ctx [p1 p2] data` produces one pending msg pointing at two parents.

The handler builds this tree and returns it. It doesn't know yet what tokens each pending msg should carry, because that depends on how many siblings the msg has and which leaves they share. Only after the whole return value is in view can the framework compute the token distribution. That's the next section. But first, the invariant we promised.

### The single-invocation invariant

One rule matters more than almost anything else in this library. The docstring on `step/step` calls it out; it's worth repeating here because it falls out of the synthesis arithmetic in §9 and you need to carry it in your head:

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
5. Apply any stamped `::assoc-tokens` / `::dissoc-tokens` decorations — the escape hatches for designing custom groups (§13).
6. Strip the refs. Tag `:tokens` on. Emit a `:split` or `:merge` trace event per materialized msg.

All pure. No channels, no atoms, no runtime. Property-test it over randomized provenance DAGs; conservation holds by construction because the only arithmetic is K-way splitting and XOR-merging, both of which preserve the group operation.

This is where the design earns its keep. Conservation is not *asserted* somewhere and then *checked* later. It's baked into the one path that produces output. A message that comes out of synthesis cannot violate conservation, because that is not a thing the synthesis code is capable of doing.

It is, however, a thing user code *can* do — see the single-invocation invariant in §8 above.

*The trade-off.* The K-way split is positional. Each output's tokens are computed from its place in the DAG at this instant, not from any identity it carries. That means if you want to correlate outputs to inputs in some *application-level* way (e.g., "which input produced this output"), use the `:parent-msg-ids` chain or the `:data-id` field. Tokens are for closure detection. The lineage graph underneath them is the provenance mechanism.

## Peeling back: three envelope shapes, no tag required

A message envelope is a plain map. Three shapes, distinguished by which keys are present:

| Kind             | has `:data` | has `:tokens` | Purpose |
|---|---|---|---|
| **data**         | yes | yes | The payload you care about. `nil` is valid data. |
| **signal**       | no  | yes | Tokens alone, no payload. Use it to coordinate. |
| **input-done**   | no  | no  | "This input port's upstream has been exhausted." |

"Signal" is the interesting one. It's a pulse that carries tokens but no data — send it through the graph when you want the token math to happen without anyone having to process a payload. Most coordination patterns use signals. In fact, the common case is so common that the framework does it for you: a data handler that returns an empty port-map (`{}` or `[state' {}]`) gets a signal auto-synthesized on every declared output port, so the input's tokens propagate for downstream closure. That's what filters, dedups, and skips rely on. Two cases want the opposite — suppress the auto-signal by returning `msg/drain` instead: deferred propagation (pair-mergers, batchers — see §8), and the rare genuine drop where you *really* want the tokens to die here. Conservation is optional on your terms; just say so explicitly.

Your handler's `:on-data` doesn't see signals; they arrive at `:on-signal` instead (default: broadcast unchanged to every output port).

### `:input-done` is a per-port lifecycle signal, not a processing barrier

Read the name literally: `input-done` says "this upstream input has been exhausted." That's all it says. In particular, it does NOT say:

- "stop processing on this port" — handlers continue to receive data and signals on ports that have been input-done'd. This is load-bearing for cyclic flows where a feedback edge re-feeds a port after its external upstream has closed (e.g. a self-loop that emits N messages back into its own `:in` after `:start`).
- "the system is done" — system termination is a separate, emergent property. See "lifecycle" below for quiescence-based termination.
- "the channel is closed" — channels close on `a/close!`; closure is what *triggers* an input-done envelope, but the two concepts are distinct.

What input-done *does* do: cascade the "input exhausted" signal through the graph. When all of a step's declared input ports have received input-done, its `:on-all-input-done` hook fires (the place to flush buffers or emit aggregated output), and an `input-done` envelope is auto-appended to every declared output port. That's how aggregator and end-of-pipeline patterns hook a "drain me" event without caring about the actual termination condition.

Combinators that gate their own close (e.g. recursive worker pools that need to keep their internal channel open until in-flight work drains) can return `msg/drain` from `:on-all-input-done` to suppress the auto-append — symmetric with `msg/drain`'s role in `:on-data`.

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
- `(inject! h {})` — input-done marker.

This is the shape for long-running pipelines. You own the outer loop; you decide when to stop.

### Observable

Layer observability on top of the manual tier:

```clojure
(flow/counters         h)   ; {:sent :recv :completed}
(flow/quiescent?       h)   ; counters balanced?
(flow/await-quiescent! h)   ; block until they balance
```

Quiescence is defined arithmetically: `sent > 0` and `sent = recv = completed`. Every message sent lands as a receive somewhere; every receive eventually closes with either `:success` or `:failure`; when all three counts meet, nothing is in flight and nothing is pending. No polling, no timeouts, just balance — two conservation laws stacked on top of each other, if you're keeping score.

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
      :on-all-input-done (fn [_ctx state]
                       (.write (:writer state) "# end\n")
                       {})
      :on-stop       (fn [_ctx state]
                       (.close (:writer state))
                       nil)})))
```

- `:on-init` runs once before any messages arrive — allocate resources here.
- `:on-data` is what `step/step` has been installing for us under the hood.
- `:on-signal` (default: broadcast on every output port) fires on signal envelopes.
- `:on-all-input-done` runs when every input port has been input-done'd — flush tails here.
- `:on-stop` runs exactly once when the flow tears down — release resources here.

`step/step` fills in defaults for every slot except `:on-data`, which is why you can usually ignore them. When you find yourself opening files, seeding buffers, or needing non-trivial startup and shutdown, drop to `handler-map`.

*The trade-off.* You lose `step`'s safety nets. If you override `:on-signal` and forget to propagate the signal, it silently vanishes — the framework doesn't know what "handle the signal" means for your proc. Same for `:on-all-input-done` and the input-done cascade: if you override it, make sure `input-done` still propagates (or return `msg/drain` if you mean to gate it), or read `run-input-done` in `flow.clj` to see what the default was doing for you. Power, responsibility, and so on.

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
msg.clj                   envelopes, free algebra, pure synthesis
step.clj                  steps, handler-maps, composition, topology walk
flow.clj                  the interpreter — the only file that imports core.async.flow
token.clj                 the u64 XOR algebra
counters.clj              sent / recv / completed; atom-held record snapshot
trace.clj                 event constructors and scoped pubsub
                            (don't import this; subscribe on the pubsub instead)
recorder.clj              after-the-fact pubsub event accumulator

combinators/core.clj      fan-out, fan-in, parallel
combinators/workers.clj   round-robin-workers, stealing-workers
combinators/aggregate.clj batch/cumulative-by-group, join-by-key,
                            tumbling-window, sliding-window
combinators/control.clj   rate-limited, with-backoff

obs/viz.clj               live event-sourced Datastar visualizer
obs/store.clj             DuckDB-backed trace persistence
steps/claude_code.clj     Datapotamus step wrapping the Claude Code CLI
watchers/latency.clj      per-step latency histogram (ConcurrentHashMap)
```

Typical aliases:

| Namespace | Alias | When |
|---|---|---|
| `toolkit.datapotamus.step`                   | `:as step` | Always |
| `toolkit.datapotamus.flow`                   | `:as flow` | Always |
| `toolkit.datapotamus.msg`                    | `:as msg`  | Whenever building msgs |
| `toolkit.datapotamus.combinators.core`       | `:as cc`   | parallel, fan-out / fan-in |
| `toolkit.datapotamus.combinators.workers`    | `:as cw`   | round-robin-workers, stealing-workers |
| `toolkit.datapotamus.combinators.aggregate`  | `:as ca`   | batch/cumulative-by-group, joins, windows |
| `toolkit.datapotamus.combinators.control`    | `:as ct`   | rate-limited, with-backoff |
| `toolkit.datapotamus.token`                  | `:as tok`  | Designing new combinators |

If a file uses combinators from only one of those four submodules, it's
fine to alias that one as `:as c` and read the prefix as "combinator." For
files that mix two or more, the four-letter aliases keep the call sites
self-explanatory.

## Static-topology views

Two artifacts let you inspect a pipeline's structure offline.

### Terminal renderer (`toolkit.datapotamus.render`)

Renders a stepmap as an indented nested-list view. From a REPL:

```clojure
(require '[toolkit.datapotamus.render :as r]
         '[my.pipeline.ns :as p])

(r/print-pipeline (p/build-flow))   ; prints to *out*
(r/render          (p/build-flow))  ; returns a seq of strings
```

`render/render` and `render/print-pipeline` accept a stepmap, a
topology (`step/topology` output), or a shape tree
(`shape/decompose` output).

### Notation legend

Quick reference:

| Visual element | Meaning |
|---|---|
| Indent under a name (no bracket) | Inside that container |
| `⎡⎢⎣` rail | Parallel section (scatter-gather, multiple branches) |
| `⎢` rail (single column, no corners) | Collapsed parallel section — all branches identical, summarized as one `K× …` row |
| Name at the bracket-root column | A new parallel arm starts |
| Name at a deeper column inside a bracket | Continuation of the previous arm's chain |
| `↓` left column | Outer-shape chain flow (between same-indent siblings) |
| `↓` next to a bracket char (`⎢↓ `) | In-branch chain flow within a parallel arm |
| `⤴ <name>` right | Back-edge to a line above (always named) — across-then-up |
| `⤵ <name>` right | Forward off-spine to a non-adjacent line below — across-then-down |
| `K× <name>` | K identical parallel things collapsed |
| `<name> (combinator)` on a container line | Marks containers built by named combinators (`parallel`, `round-robin-workers`, `stealing-workers`) so the reader doesn't have to infer from the inner proc shape |

The `K` in names like `worker K` or `eK` is a placeholder for a per-instance index. It only appears inside `K× …` blocks (where K members were collapsed by block aggregation). If you see it on a non-aggregated line, that's a bug — the underlying step id is the real digit (`worker 0`, `e3`, etc.).

The rest of this section is the detailed reference. Every line uses
three slots:

```
[left ] [indent + body                ] [right]
```

- **Left** (column 0): `↓ ` if this line falls through to its
  successor in the same shape's `:order` (and that pair is a real
  edge); `  ` (two spaces) otherwise. Trace a contiguous `↓` column
  to read the spine.
- **Body**:
  - `<name>` — a leaf step or container.
  - `⎡ <name>` / `⎢ <name>` / `⎣ <name>` — scatter-gather parallel
    bracket rail. `⎡` marks the first line of the parallel section,
    `⎣` the last, `⎢` middle lines. Within the bracket: a name at
    the bracket-root column is a new branch's root; a name indented
    deeper is a continuation of the previous branch's chain.
  - `K× <inner>` — K identical parallel branches collapsed (e.g.
    `c/round-robin-workers` with k=16). The block carries a
    single-column `⎢` rail (no corners) to keep the parallel-bracket
    signal visible — the multi-branch `⎡⎢⎣` corners would only have
    one logical row of content, so they're replaced by the
    extension-only middle char. The same prefix also marks
    block-aggregated equivalence classes inside `:cycle` and `:prime`
    shapes (e.g. `c/stealing-workers` collapses to one block per
    class: `K× shim K`, `K× worker K`, `K× exit K`). When a class has
    size > 1, digit-suffixed identifiers display as `worker K`.

#### Block aggregation in cycles

Cycle/prime shapes go through a 1-WL color refinement pass that
partitions members into classes that are recursively
indistinguishable. Each class renders as one `K× <representative>`
block. Inter-class edges are summarized using one of these patterns
(based on the bipartite edge structure between two classes):

| Pattern | Condition | Display |
|---|---|---|
| **fan-in** | every `Ci`-member → singleton `Cj` | `K× xK ⤵ y` |
| **fan-out** | singleton `Ci` → every `Cj`-member | `x ⤴ K× yK` |
| **bijection** | `\|Ci\| = \|Cj\| = K`, edges form a perfect matching | `K× xK ⤵ yK` (suffix-K convention) |
| **complete** | every `Ci`-member → every `Cj`-member | `K× xK ⤵ K× yK` |

If any inter-class pattern is *partial* (none of the above), the
faithfulness gate bails on aggregation for that shape; per-member
rendering is used. To force per-member rendering globally, pass
`{:aggregate? false}`:

```clojure
(render/print-pipeline (p/build-flow))                      ; aggregated (default)
(render/print-pipeline (p/build-flow) {:aggregate? false})  ; per-member view
```

Disaggregation is lossless — any block aggregation can be expanded.
- **Right** (after the name):
  - `⤴ <name>` — back-edge to a line above (always named).
  - `⤵ <name>` — forward off-spine to a non-adjacent line below.
  - (Adjacent forward = the spine, encoded by left `↓` only.)
  - The arrows visually indicate the path: `⤴` goes across-then-up
    to find the target above; `⤵` goes across-then-down.

Reading rule: a `↓` in the left column says "the next visible line
is a real edge from me at this shape's level"; everything else is
annotated explicitly on the right.

### JSON export

`clojure -X:export-pipelines` (see `/work/src/datapotamus_export.clj`)
walks every pipeline namespace in the repo, calls its `build-flow`,
and writes a single JSON file at `dev/data/pipelines.json` (gitignored).
Each entry has:

- `:name`, `:description` (ns-form docstring)
- `:topology` — flat `{:nodes :edges}` from `step/topology`, with
  port specs spliced onto each leaf
- `:tree` — `viz/from-step` combinator hierarchy
- `:shape` — recursive shape classification from
  `toolkit.datapotamus.shape/decompose`. Each container carries one
  of: `:chain` (with `:order`), `:scatter-gather` (with `:source`,
  `:sink`, `:branches`), `:cycle` or `:prime` (each with `:order`
  and `:internal-edges`), or `:empty`. Cycles inside chains and
  scatter-gathers nest as `{:kind :cycle ...}` records inside
  `:order`/`:branches`, so the same vocabulary covers any directed
  graph at any level.

The JSON is the substrate for a future custom web visualization,
separate from the live `obs/viz` event-sourced viewer.

### Bulk markdown rendering

`dev/render_pipelines.clj` walks the same registry and writes a
single markdown file with each pipeline's rendered tree. Regenerate:

```
clojure -J--add-opens=java.base/java.nio=ALL-UNNAMED \
        -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
        -M -e '(load-file "/work/dev/render_pipelines.clj")'
```

Default output is `/work/dev/data/pipelines.md`.

---

**One thing to take with you.** A flow is a value. The invariant is a conservation law. Coordination patterns are balance-sheets. Write the state machines; let synthesis balance the tokens; let the runtime move the bytes. You govern from the algebra; the channels do as they're told.
