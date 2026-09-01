# Trigger-surface scope record (the FE-R2 re-scope)

This module's trigger surface was re-scoped during design review (the FE-R2 re-scope decision),
and this file is the standing record of what is promised and what is deliberately NOT promised.
The re-scope is a scope correction, not a gap: the narrower surface is the honest contract, and
anything beyond it is named below so no consumer can silently assume it.

## What this module promises

Automations trigger on **transition and lifecycle events that exist on the staged outbox** — the
integration events watched modules already stage in-transaction (`on_event` rules over an event
pattern, `on_deleted` rules over the deleted-event, `on_time` rules over a watched timestamp
field). If a watched module stages the event, an automation can react to it; if it does not stage
it, no rule can see it. The outbox IS the trigger surface.

## What this module does NOT promise

**Generic field-gated `on_write` automations** — rules of the shape "when field X changes on any
write, do Y" — are NOT provided by this module and are not a near-term addition, because the
data does not exist to gate on: staged transition/lifecycle payloads are deliberately thin
(aggregate id + occurred_at + model identity), which is what makes the outbox contract stable and
module-agnostic. A field-gated trigger would require every producing module to enrich every
payload — a framework-wide contract change, not a reaction-layer feature.

## The path to field-gated automations (demand-gated, separate increment)

When a concrete consumer demand appears for field-gated automations, the increment is a
**changed-fields envelope**: producing modules stage, alongside transition events, a
`<model>.record.changed` event carrying `{ id, changed_fields: [...] }`. That envelope is a new
framework contract with its own register row — this module would then add an `on_write` trigger
kind matching over `changed_fields`, with no other change to the guard architecture (the
recursion guard, dedup, and depth cap are trigger-agnostic). Until that increment is registered
and its producers exist, no `on_write` trigger kind appears in this module's vocabulary — an
absent capability cannot be half-promised.

## Register row (proposed wording)

> `backbone-foundation-ext` automations trigger on transition/lifecycle events staged on the
> transactional outbox (`on_event` / `on_deleted` / `on_time`). Generic field-gated `on_write`
> automations are out of scope: staged payloads are thin (id + occurred_at), and a changed-fields
> envelope is a separate demand-gated framework increment with its own register row when a
> consumer need is demonstrated. This module will never widen its trigger surface without a
> registered envelope contract behind it.

## Why this is the right default

The alternative — enriching every staged payload with full before/after state so automations can
gate on fields — couples every producing module to every consuming rule, grows every outbox row
for a feature nobody has asked for yet, and invites the eval-vocabulary pressure this module
exists to refuse. The thin-payload outbox keeps the reaction layer honest: it reacts to facts
modules declared, on a surface that already exists.
