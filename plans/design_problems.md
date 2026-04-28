# Datapotamus event ontology — open design problems

A working list of concerns surfaced while trying to redesign how events are named, shaped, and routed. Captured for later — none of these are blocking, none have a settled answer.

## 1. The envelope axis isn't routable in pubsub

Events have two orthogonal axes — **lifecycle role** (`:recv`, `:success`, `:send-out`, etc.) and **message envelope shape** (`:data`, `:signal`, `:done`) — but only the lifecycle axis is encoded in the pubsub subject. The envelope axis is a field on the event payload, so subscribers who care about it must subscribe to everything via `[:>]` and filter in-handler. This is documented as the intended behavior at `trace.clj:7` ("Subjects use :kind only. Consumers filter on :msg-kind in the handler.").

Evidence of friction:

- `events-of` helper at `datapotamus_test.clj:74-78` exists exactly to filter by both axes; used by ~10 test sites (lines 422, 441, 474, 482, 486, 490, 516, 535, 569, 981, 986, 1005, 1686, 1708, 1727).
- Downstream printers `hn/core.clj:135` and `doublespeak/core.clj:142` read `:msg-kind` for a trace column with `(some-> (:msg-kind ev) name (or ""))` — they tolerate absence by emitting empty.

## 2. Field-name overlap obscures the two axes

- `:kind` (lifecycle) and `:msg-kind` (envelope) sound like sibling fields on the same axis. They aren't.
- `:signal` (envelope value) collides with the verb "signal" used elsewhere in framework wording (auto-signal, control signal, signaling propagation).

## 3. "Envelope" is right for messages, awkward on events

On a message, "envelope" is a perfect word — a container with a defined shape. On an event, calling the field `:envelope` is a small lie: an event is *about* an envelope, not itself an envelope. The mismatch is especially visible for flow events, which have no envelope at all.

## 4. "Metadata" / "meta" is a misnomer

Renaming `:signal` → `:meta` was attractive (resolves the verb collision, short word) but is misleading: `:data` envelopes also carry tokens (lineage metadata). Calling the token-only message "meta" suggests metadata is exclusive to it, which is false. The defining feature of token-only messages isn't "has metadata"; it's **"lacks payload."**

## 5. The "four-quadrant" model is fictitious symmetry

It's tempting to frame the three envelope kinds as a 2×2 cross product:

|              | has tokens | no tokens |
|--------------|------------|-----------|
| **has data** | `:data`    | (impossible)|
| **no data**  | `:signal`  | `:done`   |

But the (has-data, no-tokens) cell is structurally impossible — every data envelope participates in token bookkeeping (`new-msg` initializes `:tokens {}`). So there are **three discrete roles**, not four cells. The "elegance" we reached for is a story we told ourselves; the actual structure is just three named protocol roles.

## 6. Events are heterogeneous: about-message vs about-flow

Some events wrap a message envelope (`:recv`, `:success`, `:failure`, `:send-out`, `:split`, `:merge`, `:inject`). Some don't (`:run-started`, `:flow-error`, `:status`). This heterogeneity is the **root cause of every schema-design difficulty** we hit. Every attempted fix forks at the same crack:

- "Add envelope to subjects" → "what about events without envelopes?"
- "Force every event to have an envelope value" → "but inventing `:control` is dishonest — it isn't a message-shape, it's an event class"
- "Split message and flow events into separate subject namespaces (`[\"msg\" ...]` vs `[\"flow\" ...]`)" → "verbose, no attested subscriber use case, adds a top-level taxonomy that the event-name already implies"

The crack is real: the system publishes ontologically different things on the same channel.

## 7. The complexity lives at the message↔event boundary

Messages are clean — three protocol roles, well-defined structure (msg.clj:4-12). The complexity arises when we publish observations of those messages alongside observations of the flow runtime on the same channel with a single schema. The two have genuinely different shapes. Any naming or routing decision hits this seam.

## 8. Naming the middle protocol role is genuinely hard

The three message roles are: carry payload, carry only lineage, mark end of stream. The first and third have natural names (`:data`, `:done`). The middle one resists naming because it's defined negatively — "a data envelope minus the data" — and naming things by what they lack is awkward.

Candidates considered:

- `:signal` — works as a noun, but collides with framework verb usage
- `:meta` / `:metadata` — implies metadata is exclusive to this kind, which is false
- `:propagate` / `:tick` / `:passthrough` / `:keepalive` — descriptive but verb-y, or borrowed from other protocols
- `:no-data` / `:dataless` / `:empty-data` — names the absence directly; honest but graceless

No clear winner.

## Field renames discussed (provisional)

These survived the discussion as the least-bad pure-rename moves, independent of any subject change:

- `:kind` → `:event` — frees `kind` from overload; reads naturally as "the event's name."
- `:msg-kind` → `:role` — names the field by what it tags (the message's protocol role) rather than its structural shape. Sidesteps "envelope" / "kind" baggage. Still TBD whether the field should appear on messages too, or only on events.
- `:signal` (envelope value) → unresolved (see §8).

## Possible directions, none chosen

- **A. Rename fields only.** Leave subjects alone. Envelope-axis filtering stays in-handler. Smallest, most honest move.
- **B. Add envelope to subject when present** (variable-length subjects). `[<event> <role> "scope" ...]` for message events, `[<event> "scope" ...]` for flow events. Honest about the asymmetry; supports role-level routing.
- **C. Top-level prefix split** (`["msg" ...]` vs `["flow" ...]`). First-class dichotomy. Verbose; no attested use case.
- **D. Invent a fourth role value** (`:control`) so every event has one. Phantom value to fake uniformity. Rejected as dishonest.

## What this document is not

Not a plan, not a recommendation. A snapshot of the conceptual obstacles so the next pass at this can start from them rather than rediscover them.
