# backbone-foundation-ext — specification

Declarative automations as a reaction layer over the transactional outbox/inbox contract.
Framework-only dependency edge: `backbone-core`, `backbone-orm`, `backbone-auth`,
`backbone-messaging`, `backbone-outbox` (all pinned to the same framework tag). No domain-module
dependencies — the module consumes other modules' events and reaches their records only through
the host-wired gateway port.

## 1. Trigger kinds

| kind | requires | refuses | meaning |
|---|---|---|---|
| `on_event` | `trigger_pattern` | `time_field` | fire when a watched model stages a matching event |
| `on_deleted` | `trigger_pattern` | `time_field` | fire on the watched model's deleted-event; record-targeted actions (`set_fields`, `transition`) are refused — only notifications make sense against a gone record |
| `on_time` | `time_field` (+ optional `delay_minutes >= 0`) | `trigger_pattern` | fire when `time_field + delay` passes on a watched record |

Event patterns are deliberately tiny: an exact event type (`sapiens.user.deactivated`), a
one-level prefix (`sapiens.*`), or the whole model (`*`). Anything else is refused at write time
and re-validated at fire time.

The model fence: a rule's `model` must equal the envelope's source context (`source_context`),
compared case-insensitively, BEFORE any action runs. A `*`-pattern rule watching `sapiens` cannot
fire on a `billing.*` event — the mismatch is recorded as a `skipped_model_mismatch` run row.

## 2. Action vocabulary (closed)

```text
set_fields  { fields: { <name>: <scalar> } }     // scalar = string | number | bool | null
transition  { field, to }
notify      { template, recipients: [address…], context: { <k>: <scalar> } }
```

- At most 10 actions per rule; field names <= 63 chars; template <= 120; address <= 320.
- `serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")` on a closed enum with NO
  catch-all variant: `run_sql`, `eval`, `call`, extra fields, non-scalar values, a non-array body
  — none of these can be parsed, stored, or fired. There is no expression engine and no stored
  code anywhere in the module; no `on_change`/`on_webhook` triggers exist.
- Write-time validation (the suspenders): `RuleService` refuses any draft whose body fails to
  parse or mismatches its trigger shape. Fire-time re-validation (the belt): the engine re-parses
  the stored body before applying; a hand-corrupted row refuses as a `refused_body` run row with
  the reason in `detail`, and nothing is applied.

## 3. Reaction engine

`AutomationReactionHandler` implements the framework's `IntegrationEventHandler` (name
`FoundationExtReactionHandler`, subscribes `*`, retryable). Per envelope:

1. Parse the envelope id (a non-UUID id is warned and skipped — some transports use opaque ids).
2. Load live ACTIVE `on_event` + `on_deleted` rules inside the transaction. No rules of these
   kinds → commit and return (no claim, no phantom rows).
3. Claim the envelope with `inbox::once(consumer = "foundation_ext.reaction")`. Already claimed →
   commit and return (dedup leg).
4. Match rules by pattern. No match → commit (claimed, no run row — a claim is not a fire).
5. For each matched rule, in order: model fence → self-trigger exclusion (any producing run's
   `effect_event_ids` contains the envelope id) → causal depth cap (default 5) → fire-time body
   re-validation. Any failure records its ledger row and moves on.
6. Pass the gate: take the installed gateway (unwired → loud `EventError`, FULL rollback including
   the claim, so the relay's retry re-drives the envelope) and apply each action inside this
   transaction, collecting produced event ids.
7. Insert ONE run row (`fired`): `parent_run_id` + `depth` from the causal chain, the applied
   action summaries, `effect_event_ids` = produced ids + the run id itself.
8. Stage `foundation_ext.automation.fired` with `id = run_id`, a
   `foundation_ext:run:<run_id>` causation token, and the triggering envelope's correlation id
   preserved — in the SAME transaction. Commit.

Because the fired event's id IS the run id and the run's registry contains it, a rule watching
`foundation_ext.*` cannot re-trigger its own producer: the module's own lifecycle echo is
self-suppressing by construction.

## 4. Run ledger statuses

| status | written when |
|---|---|
| `fired` | actions applied + fired event staged in-transaction |
| `suppressed_self_trigger` | the envelope was produced by this very rule's earlier run |
| `depth_capped` | the causal chain reached the cap (default 5) |
| `skipped_model_mismatch` | envelope's source context != rule's watched model |
| `refused_body` | stored body failed fire-time re-validation (e.g. hand-corrupted row) |

## 5. Scheduler

`AutomationScheduler::evaluate_tick(now)` — host-driven, no threads:

- Recomputes from LIVE rules (not stored state): time-rule count, min delay, ladder interval
  `(min_delay.max(0) / 10).clamp(1, 240)` minutes.
- Posture `inactive` (default) when no live time rules exist — no gateway is even required.
- Posture `active` requires a wired gateway (a time-rule module left unwired fails the tick
  loudly rather than silently skipping due records).
- Each due (rule, record, generation) fires in its own transaction under a deterministic UUIDv5
  id (`foundation_ext.scheduler` consumer claim): a re-tick dedups what was already delivered
  (`deduped` count in the report); a record whose watched timestamp moves re-arms under a new
  generation and legitimately fires again.
- The singleton posture row records the recompute and a deviation note on every posture flip or
  interval change (e.g. `interval recomputed from live rules: 5 -> 240 min`) — the interval can
  grow back when rules are retired; it is a recompute, never an only-ever-shrink ratchet.

## 6. Rule administration

`RuleService` is the ONLY writer of `foundation_automation_rules` (all HTTP models are
`read_only`): `create_rule`, `replace_rule` (same validation; id + ledger stay), `set_active`
(retire without deleting history), `delete_rule` (SOFT — audit metadata `deleted_at`), `get_rule`,
`list_rules`. Activation is honored end-to-end: an inactive rule matches nothing and its
envelopes are not claimed by it.

## 7. HTTP surface (deliberately minimal)

GET-only, at plural bases: `/automation_rules`, `/automation_runs`, `/scheduler_postures`
(list / detail / trash / deleted / count variants). No POST/PUT/PATCH/DELETE route exists on any
composable surface (`all_crud_routes()` included — it merges read routes for trusted/admin
readers), and `/web/hook/<uuid>` or any public automation route is absent by design: external
webhook intake is an integrations-module concern under its own approval record. Enforced by the
`no_write_routes` probe.

## 8. Fail-hard probe suite

`tests/probes/` on a scratch Postgres (127.0.0.1:5433; override with
`FOUNDATION_EXT_TEST_ADMIN_URL`), one disposable database per test, migrations applied in sorted
order, teardown leak-guarded. A missing scratch server PANICS the suite — vacuous green is not
green. Coverage: self-trigger exactly-once (serial), own-fired-event echo suppression, inbox
dedup exactly-once effect, unwired-gateway loud failure + rollback + recovery, causal depth cap
on an alternation chain (exact ledger counts), deleted-trigger write-time and fire-time guards,
model fence, closed-vocabulary refusals, activation + soft-delete behavior, scheduler
inactive/active/dedup/grow-back-with-deviation-note, and the read-only/no-webhook route ban.

## 9. Out of scope (recorded, not silently dropped)

- Generic field-gated `on_write` automations. The staged outbox payloads of transition/lifecycle
  events are thin (id + occurred_at + model); a changed-fields envelope is a separate,
  demand-gated framework increment. See `docs/fe-r2-rescope.md`.
- `on_change` / `on_webhook` triggers: not ported (the eval-vocabulary refusal is the point).
- Mail-to-record actions under `on_deleted`: refused — the record is gone.
- Webhook intake of any kind: banned here; lives in the integrations module.
