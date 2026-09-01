# backbone-foundation-ext

Declarative automations as a **reaction layer** over the staged outbox — the framework's port of
record-lifecycle automation (the Odoo `base_automation` concept) under the transactional
outbox/inbox contract. Framework-only: it depends on no domain module. It subscribes to the
integration events other modules already stage; it never writes another module's records directly
— a host-installed **gateway adapter** owns every write.

## What it is

- **Rules, not code.** A rule names a watched model, a trigger (`on_event`, `on_deleted`,
  `on_time`), an event pattern (or a time field + delay), and a body of declarative actions from a
  **closed vocabulary**: `set_fields`, `transition`, `notify`. There is no expression evaluation,
  no stored code, no string-evaluated domain text. Unknown action kinds and unknown fields are
  structurally inexpressible (`serde(deny_unknown_fields)` on a closed enum, no catch-all variant).
- **A reaction, never an invariant.** Automations observe events and react; they never own a
  constraint. Invariants live in the watched modules' database constraints and validated write
  verbs. An automation firing (or not firing) can never make an invalid state valid.
- **In-transaction, at-least-once, exactly-once effect.** A fire stages its effects and its
  `foundation_ext.automation.fired` lifecycle event in the SAME transaction as its run-ledger row,
  via `backbone-outbox`'s `outbox::stage`. Delivery is at-least-once; the effect is exactly-once
  because every consumed envelope is claimed with `inbox::once` (`foundation_ext.reaction`
  consumer) before anything fires.
- **No phantom fires.** An envelope with no matching rule claims nothing; an unwired gateway
  fails LOUDLY (`EventError`) with full rollback — claim included — so the relay's retry re-drives
  it; a corrupted or out-of-vocabulary rule body refuses at fire time as a `refused_body` run row
  instead of executing.

## The recursion guard (three legs)

1. **Self-trigger exclusion.** Every run row carries `effect_event_ids` — the ids of all events
   its fire produced (including its own `automation.fired`, whose id IS the run id). A rule whose
   trigger would be satisfied by a run's own produced event is suppressed
   (`suppressed_self_trigger`), never re-fired.
2. **Inbox dedup.** Redelivery of the same envelope id is refused by `inbox::once` — no second
   run row, no second application.
3. **Causal depth cap.** Every run records `depth` (organic = 0, automation-caused = parent + 1)
   and `parent_run_id`. At the cap (default 5) the chain terminates with a `depth_capped` row.

The fail-hard probe suite proves the composition: a self-triggering rule fires exactly once under
a serial run; a two-rule alternation chain terminates at the cap with an exact ledger.

## The gateway port (the only reach out)

`AutomationGateway` (`src/application/service/gateway_port.rs`) is the module's entire reach into
watched modules: `apply_action` (apply one `ActionSpec` inside the fire's transaction) and
`due_records` (discover time-rule candidates). The module ships the REFUSING default; the
composing host installs its adapter at compose time (`module.install_gateway(...)`), routing each
action through the owning module's validated write services. Unwired = `NotComposed`, fail-closed
(the certification-port precedent used elsewhere in the framework).

## The scheduler (declared posture)

`AutomationScheduler::evaluate_tick(now)` is called by the HOST's timer loop — the module owns no
threads. Posture is *declared*, not ambient:

- No live `on_time` rules → posture `inactive`; a tick is a no-op.
- Any live `on_time` rule → posture `active`; the host should tick every
  `min(max(1, min_delay // 10), 240)` minutes. The interval is RECOMPUTED from live rules on every
  tick and recorded with a deviation note when it changes (in either direction — recompute, not an
  only-ever-shrink ratchet).
- Time fires dedup by construction: each due (rule, record, generation) fires under a
  deterministic UUIDv5 event id claimed through `inbox::once` (`foundation_ext.scheduler`
  consumer), so a re-tick after a partial failure refires nothing already delivered, and a record
  that moves re-arms under a new generation.

## HTTP surface

Every model in this module is `read_only` over HTTP — GET-only list/detail/trash/count routes at
`/automation_rules`, `/automation_runs`, `/scheduler_postures`. There is no write route, no
upsert, and **no webhook intake anywhere** (`/web/hook/<uuid>` and every public automation route
are deliberately absent; external webhook intake belongs to the integrations module under its own
approval). Rules are administered host-side through `RuleService` (create / replace / activate /
retire / list), the only writer of the rules table.

## Composing (host sketch)

```rust
let foundation_ext = FoundationExtModule::builder()
    .with_database(pool.clone())
    .build()?;

// 1. Wire the reach-out (fail-closed until you do):
foundation_ext.install_gateway(Arc::new(MyHostGateway::new(/* watched modules' services */)));

// 2. Register the reaction engine on the HOST's integration bus:
bus.subscribe(foundation_ext.reaction_handler().clone()).await?;

// 3. Drive the scheduler from the host's timer loop:
let report = foundation_ext.scheduler().evaluate_tick(now).await?;

// 4. Mount the read-only surface (writes go through rule_service()):
let router = foundation_ext.readonly_routes();
```

## Schema

| table | role |
|---|---|
| `foundation_ext.foundation_automation_rules` | the declarative rules (soft-deleted, never removed out from under the ledger) |
| `foundation_ext.foundation_automation_runs` | the run ledger — every fire, suppression, cap, skip, and refusal, with `parent_run_id`, `depth`, `effect_event_ids` |
| `foundation_ext.foundation_scheduler_posture` | the scheduler's declared posture + deviation notes (singleton) |
| `foundation_ext.outbox_events` / `inbox_consumed` | this module's transactional outbox/inbox (the framework contract) |

## Events

- Consumes: every integration event (subscribes `*`; matches by rule pattern AND model fence on
  the envelope's source context).
- Emits: `foundation_ext.automation.fired` (id = run id, causal chain via
  `foundation_ext:run:<run_id>` causation tokens) — one per fired run, staged in-transaction.

## Probes

`tests/probes/` (scratch Postgres on 127.0.0.1:5433, one disposable database per test; a missing
scratch server panics the suite rather than skipping): self-trigger exactly-once, inbox dedup,
unwired-gateway rollback, causal depth cap, scheduler posture + ladder + deviation notes,
deleted-trigger guards, fire-time body re-validation, closed-vocabulary refusals, and the
read-only/no-webhook route ban.

See `SPEC.md` for the full contract and `docs/fe-r2-rescope.md` for the trigger-surface scope
record.
