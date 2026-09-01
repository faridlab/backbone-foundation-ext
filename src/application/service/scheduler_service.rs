//! The declared-posture adaptive scheduler for time rules
//! (hand-written; user-owned; see `metaphor.codegen.yaml`).
//!
//! POSTURE, DECLARED: the scheduler is **inactive by default** — a host
//! that never creates an `on_time` rule runs no ticking at all (the
//! posture row says `inactive`, interval `NULL`). It activates iff at
//! least one live, active time rule exists, and its interval is
//! RECOMPUTED FROM THE LIVE RULES on every evaluation:
//!
//! ```text
//! interval = clamp(min_delay_minutes / 10, 1, 240)   // minutes
//! ```
//!
//! where `min_delay_minutes` is the smallest declared delay across live
//! time rules (a rule with no delay counts as 0 → the floor). The
//! interval may therefore GROW BACK as well as shrink — "only ever
//! shrink" would silently strand a tenant on a cadence its rules no
//! longer justify. Every change is recorded as a **declared deviation**
//! in the posture row (`last_deviation_note`: "interval recomputed from
//! live rules: 5 -> 240 min"), never applied silently.
//!
//! DEDUP-BY-CONSTRUCTION: each due fire's event id is a DETERMINISTIC
//! uuid v5 over `(rule, record, generation)` where `generation` is the
//! watched time field's current value (the gateway reports it). A re-tick
//! over an unchanged field mints the SAME id, the scheduler's own inbox
//! claim (`foundation_ext.scheduler`) refuses the duplicate, and each
//! due record fires exactly once. If the field MOVES, the next tick mints
//! a new id — a legitimately new due, which then fires. No fire-ledger
//! table of its own is needed; the claim is the ledger.
//!
//! Each fire runs in its own transaction (claim → apply through the
//! gateway → run row → stage `foundation_ext.automation.fired`), so one
//! due record's failure cannot roll back another's fire.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use backbone_messaging::EventError;
use backbone_messaging::IntegrationEventEnvelope;
use backbone_outbox::inbox;
use backbone_outbox::outbox;
use backbone_outbox::OutboxRecord;
use sqlx::PgPool;

use crate::domain::entity::AutomationRule;
use crate::domain::entity::AutomationRun;
use crate::domain::entity::FoundationRunStatus;
use crate::domain::entity::FoundationSchedulerPosture;
use crate::domain::entity::FoundationTriggerKind;

use super::action_spec::parse_body;
use super::foundation_error::FoundationError;
use super::foundation_error::FoundationResult;
use super::gateway_port::AutomationGatewaySlot;
use super::gateway_port::FireContext;
use super::gateway_port::TimeWatchSpec;
use super::reaction_engine::CAUSATION_PREFIX;
use super::reaction_engine::FIRED_EVENT_TYPE;
use super::reaction_engine::NIL_COMPANY_ID;
use super::reaction_engine::SCHEMA_SELF;
use super::rule_service::map_rule;
use super::rule_service::RULE_COLUMNS;

/// The inbox consumer identity for scheduler fires (dedup key's
/// consumer half).
pub const CONSUMER_SCHEDULER: &str = "foundation_ext.scheduler";

/// The synthetic source event type recorded on scheduler-fired run rows.
pub const TIME_DUE_EVENT_TYPE: &str = "foundation_ext.automation.time_due";

/// The delay-ladder defaults (mirror `config/application.yml`).
pub const DEFAULT_DIVISOR: i64 = 10;
pub const DEFAULT_FLOOR_MINUTES: i64 = 1;
pub const DEFAULT_CEILING_MINUTES: i64 = 240;

/// The delay ladder: `interval = clamp(min_delay / divisor, floor,
/// ceiling)`, in minutes. Fields are pub so a host config can override
/// them; [`LadderConfig::default`] mirrors the declared module config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LadderConfig {
    /// The divisor (an adaptive scheduler polls a fraction of the
    /// shortest declared delay).
    pub divisor: i64,
    /// The floor (never poll faster than this).
    pub floor_minutes: i64,
    /// The ceiling (never poll slower than this).
    pub ceiling_minutes: i64,
}

impl Default for LadderConfig {
    fn default() -> Self {
        Self {
            divisor: DEFAULT_DIVISOR,
            floor_minutes: DEFAULT_FLOOR_MINUTES,
            ceiling_minutes: DEFAULT_CEILING_MINUTES,
        }
    }
}

impl LadderConfig {
    /// The ladder function. `min_delay_minutes` < 0 is treated as 0; a
    /// divisor below 1 is clamped to 1 (never divide by zero).
    pub fn interval_minutes(&self, min_delay_minutes: i64) -> i64 {
        (min_delay_minutes.max(0) / self.divisor.max(1)).clamp(
            self.floor_minutes.max(0),
            self.ceiling_minutes.max(self.floor_minutes.max(0)),
        )
    }
}

/// The declared default ladder (the config defaults), for hosts and
/// probes that do not carry a `LadderConfig`.
pub fn default_ladder_interval(min_delay_minutes: i64) -> i64 {
    LadderConfig::default().interval_minutes(min_delay_minutes)
}

/// The deterministic due-fire event id: uuid v5 over
/// `foundation_ext.scheduler|{rule}|{record}|{generation}`. Same inputs
/// -> same id (a re-tick dedups); a moved time field changes
/// `generation` -> a new id (a legitimately new due).
pub fn scheduler_event_id(rule_id: Uuid, aggregate_id: &str, generation: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("{CONSUMER_SCHEDULER}|{rule_id}|{aggregate_id}|{generation}").as_bytes(),
    )
}

/// What one evaluation of the scheduler observed and did.
#[derive(Debug, Clone, PartialEq)]
pub struct TickReport {
    /// The posture after this tick.
    pub posture: FoundationSchedulerPosture,
    /// Live, active time rules (0 => inactive posture).
    pub time_rule_count: i32,
    /// The smallest declared delay across live time rules.
    pub min_delay_minutes: Option<i32>,
    /// The recomputed interval (None when inactive).
    pub interval_minutes: Option<i32>,
    /// Fires this tick actually committed (dedup refusals excluded).
    pub fired: u32,
    /// Due fires refused as duplicates by the inbox claim (unchanged
    /// generations re-ticked).
    pub deduped: u32,
    /// The declared deviation note recorded this tick (interval changes,
    /// posture flips), if any.
    pub deviation_note: Option<String>,
}

/// The scheduler. The host calls [`AutomationScheduler::evaluate_tick`]
/// from its own timer loop at the interval the posture row records —
/// the module starts no threads and owns no timers (a library module
/// cannot keep a runtime alive by itself, and the host's loop is where
/// backpressure belongs).
pub struct AutomationScheduler {
    pool: PgPool,
    gateway_slot: std::sync::Arc<AutomationGatewaySlot>,
    ladder: LadderConfig,
}

impl AutomationScheduler {
    /// Build the scheduler against the host pool, the shared gateway
    /// slot, and a ladder config (default mirrors the module config).
    pub fn new(
        pool: PgPool,
        gateway_slot: std::sync::Arc<AutomationGatewaySlot>,
        ladder: LadderConfig,
    ) -> Self {
        Self {
            pool,
            gateway_slot,
            ladder,
        }
    }

    /// The ladder in force.
    pub fn ladder(&self) -> LadderConfig {
        self.ladder
    }

    /// Load the live, active time rules.
    async fn time_rules(&self) -> FoundationResult<Vec<AutomationRule>> {
        let rows = sqlx::query(&format!(
            "SELECT {RULE_COLUMNS} FROM foundation_ext.foundation_automation_rules \
             WHERE active AND metadata->>'deleted_at' IS NULL AND trigger_kind = $1 \
             ORDER BY name"
        ))
        .bind(FoundationTriggerKind::OnTime)
        .fetch_all(&self.pool)
        .await
        .map_err(FoundationError::Db)?;
        Ok(rows.iter().map(map_rule).collect())
    }

    /// Read the stored posture (if a row exists yet).
    async fn stored_posture(&self) -> FoundationResult<Option<(FoundationSchedulerPosture, Option<i32>)>> {
        let row: Option<(FoundationSchedulerPosture, Option<i32>)> = sqlx::query_as(
            "SELECT posture, interval_minutes FROM foundation_ext.foundation_scheduler_posture \
             WHERE singleton LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(FoundationError::Db)?;
        Ok(row)
    }

    /// Upsert the singleton posture row, returning the deviation note
    /// that was recorded (if the observation changed anything).
    async fn record_posture(
        &self,
        posture: FoundationSchedulerPosture,
        time_rule_count: i32,
        min_delay_minutes: Option<i32>,
        interval_minutes: Option<i32>,
        last_fired_count: u32,
        now: DateTime<Utc>,
    ) -> FoundationResult<Option<String>> {
        let (previous_posture, previous_interval) =
            self.stored_posture().await?.unwrap_or((FoundationSchedulerPosture::Inactive, None));
        let mut notes: Vec<String> = Vec::new();
        if previous_posture != posture {
            notes.push(format!("posture {} -> {}", previous_posture, posture));
        }
        if previous_interval != interval_minutes {
            notes.push(format!(
                "interval recomputed from live rules: {} -> {} min",
                previous_interval
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                interval_minutes
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "none".to_string()),
            ));
        }
        let note = if notes.is_empty() {
            None
        } else {
            Some(notes.join("; "))
        };
        sqlx::query(
            "INSERT INTO foundation_ext.foundation_scheduler_posture \
                 (id, singleton, posture, time_rule_count, min_delay_minutes, \
                  interval_minutes, last_evaluated_at, last_fired_count, last_deviation_note) \
             VALUES ($1, TRUE, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (singleton) DO UPDATE SET \
                 posture = EXCLUDED.posture, \
                 time_rule_count = EXCLUDED.time_rule_count, \
                 min_delay_minutes = EXCLUDED.min_delay_minutes, \
                 interval_minutes = EXCLUDED.interval_minutes, \
                 last_evaluated_at = EXCLUDED.last_evaluated_at, \
                 last_fired_count = EXCLUDED.last_fired_count, \
                 last_deviation_note = EXCLUDED.last_deviation_note",
        )
        .bind(Uuid::new_v4())
        .bind(posture)
        .bind(time_rule_count)
        .bind(min_delay_minutes)
        .bind(interval_minutes)
        .bind(now)
        .bind(last_fired_count as i32)
        .bind(&note)
        .execute(&self.pool)
        .await
        .map_err(FoundationError::Db)?;
        Ok(note)
    }

    /// Evaluate the scheduler once: recompute posture + interval from the
    /// LIVE rules, fire every due record (deduped by construction), and
    /// record the posture — with any deviation declared in the note.
    pub async fn evaluate_tick(&self, now: DateTime<Utc>) -> FoundationResult<TickReport> {
        let rules = self.time_rules().await?;

        // INACTIVE BY DEFAULT: no live time rules -> the scheduler does
        // not fire anything and says so in its posture row.
        if rules.is_empty() {
            let note = self
                .record_posture(FoundationSchedulerPosture::Inactive, 0, None, None, 0, now)
                .await?;
            return Ok(TickReport {
                posture: FoundationSchedulerPosture::Inactive,
                time_rule_count: 0,
                min_delay_minutes: None,
                interval_minutes: None,
                fired: 0,
                deduped: 0,
                deviation_note: note,
            });
        }

        let min_delay = rules
            .iter()
            .map(|r| r.delay_minutes.unwrap_or(0))
            .min()
            .unwrap_or(0);
        let interval = self.ladder.interval_minutes(min_delay as i64) as i32;

        let gateway = self.gateway_slot.take().ok_or_else(|| {
            FoundationError::NotComposed(
                "automation gateway not composed: time rules exist but no host adapter is \
                 installed (the tick fired nothing; fail-closed)",
            )
        })?;

        let mut fired: u32 = 0;
        let mut deduped: u32 = 0;
        for rule in &rules {
            let time_field = match &rule.time_field {
                Some(field) => field.clone(),
                None => {
                    // A live on_time rule without a time field cannot be
                    // authored through the guarded service; a corrupted
                    // row is skipped loudly, not executed.
                    tracing::warn!(
                        rule_id = %rule.id,
                        "foundation_ext: on_time rule without time_field skipped"
                    );
                    continue;
                }
            };
            let spec = TimeWatchSpec {
                rule_id: rule.id,
                model: rule.model.clone(),
                time_field,
                delay_minutes: rule.delay_minutes.unwrap_or(0),
            };
            let due = gateway
                .due_records(&self.pool, &spec, now)
                .await
                .map_err(|e| FoundationError::Tick(format!("due_records failed: {e}")))?;

            for record in due {
                let event_id =
                    scheduler_event_id(rule.id, &record.aggregate_id, &record.generation);
                let envelope = IntegrationEventEnvelope {
                    id: event_id.to_string(),
                    event_type: TIME_DUE_EVENT_TYPE.to_string(),
                    source_context: rule.model.clone(),
                    aggregate_id: record.aggregate_id.clone(),
                    occurred_at: record.due_at,
                    published_at: now,
                    version: 1,
                    correlation_id: None,
                    causation_id: None,
                    payload: serde_json::json!({
                        "rule_id": rule.id,
                        "time_field": spec.time_field,
                        "generation": record.generation,
                        "company_id": record.company_id,
                        "due_at": record.due_at,
                    }),
                };
                match self.fire_due(rule, &envelope, event_id, &gateway, now).await {
                    Ok(true) => fired += 1,
                    Ok(false) => deduped += 1,
                    Err(e) => return Err(e),
                }
            }
        }

        let note = self
            .record_posture(
                FoundationSchedulerPosture::Active,
                rules.len() as i32,
                Some(min_delay),
                Some(interval),
                fired,
                now,
            )
            .await?;
        Ok(TickReport {
            posture: FoundationSchedulerPosture::Active,
            time_rule_count: rules.len() as i32,
            min_delay_minutes: Some(min_delay),
            interval_minutes: Some(interval),
            fired,
            deduped,
            deviation_note: note,
        })
    }

    /// Fire ONE due record in its own transaction. `Ok(true)` = fired
    /// (committed); `Ok(false)` = the inbox claim refused a re-tick
    /// duplicate; `Err` = the fire rolled back.
    async fn fire_due(
        &self,
        rule: &AutomationRule,
        envelope: &IntegrationEventEnvelope,
        event_id: Uuid,
        gateway: &std::sync::Arc<dyn super::gateway_port::AutomationGateway>,
        now: DateTime<Utc>,
    ) -> FoundationResult<bool> {
        let mut tx = self.pool.begin().await.map_err(FoundationError::Db)?;
        let claimed = inbox::once(&mut *tx, SCHEMA_SELF, CONSUMER_SCHEDULER, event_id)
            .await
            .map_err(|e| {
                FoundationError::Tick(format!("scheduler inbox claim failed: {e}"))
            })?;
        if !claimed {
            tx.commit().await.map_err(FoundationError::Db)?;
            return Ok(false);
        }

        // Fire-time body re-validation, same as the event engine.
        let actions = match parse_body(&rule.actions) {
            Ok(actions) => actions,
            Err(reason) => {
                let run = AutomationRun {
                    id: Uuid::new_v4(),
                    automation_id: rule.id,
                    trigger: rule.trigger_kind,
                    status: FoundationRunStatus::RefusedBody,
                    envelope_id: envelope.id.clone(),
                    source_event_type: Some(TIME_DUE_EVENT_TYPE.to_string()),
                    aggregate_id: Some(envelope.aggregate_id.clone()),
                    parent_run_id: None,
                    depth: 0,
                    actions_applied: rule.actions.clone(),
                    effect_event_ids: serde_json::json!([]),
                    detail: Some(format!("fire-time body re-validation refused: {reason}")),
                    occurred_at: now,
                    metadata: Default::default(),
                };
                record_run(&mut tx, &run).await?;
                tx.commit().await.map_err(FoundationError::Db)?;
                return Ok(false);
            }
        };

        let run_id = Uuid::new_v4();
        let ctx = FireContext {
            rule_id: rule.id,
            run_id,
            model: rule.model.clone(),
            trigger_kind: rule.trigger_kind.to_string(),
            envelope_id: envelope.id.clone(),
            event_type: envelope.event_type.clone(),
            aggregate_id: envelope.aggregate_id.clone(),
            company_id: rule_company_or_nil(envelope).unwrap_or(NIL_COMPANY_ID),
            now,
        };
        let mut produced_event_ids: Vec<Uuid> = Vec::new();
        let mut summaries: Vec<String> = Vec::new();
        for action in &actions {
            let effect = gateway
                .apply_action(&mut tx, &ctx, action)
                .await
                .map_err(|e| {
                    FoundationError::Tick(format!(
                        "gateway refused scheduler action '{}': {e} (fire rolled back)",
                        action.kind()
                    ))
                })?;
            produced_event_ids.extend(effect.produced_event_ids);
            summaries.push(effect.summary);
        }
        let mut detail = summaries.join("; ");
        if detail.len() > 500 {
            detail.truncate(500);
        }

        // The AutomationFired event id IS the run id: the run's own
        // lifecycle echo lands in the recursion registry by construction
        // (a rule watching foundation_ext cannot re-trigger itself on its
        // own fired event).
        produced_event_ids.push(run_id);
        let run = AutomationRun {
            id: run_id,
            automation_id: rule.id,
            trigger: rule.trigger_kind,
            status: FoundationRunStatus::Fired,
            envelope_id: envelope.id.clone(),
            source_event_type: Some(TIME_DUE_EVENT_TYPE.to_string()),
            aggregate_id: Some(envelope.aggregate_id.clone()),
            parent_run_id: None,
            depth: 0,
            actions_applied: rule.actions.clone(),
            effect_event_ids: serde_json::json!(produced_event_ids),
            detail: if detail.is_empty() { None } else { Some(detail) },
            occurred_at: now,
            metadata: Default::default(),
        };
        record_run(&mut tx, &run).await?;

        let mut fired_record = OutboxRecord::new(
            FIRED_EVENT_TYPE,
            "AutomationRun",
            run_id.to_string(),
            ctx.company_id,
            serde_json::json!({
                "run_id": run_id,
                "automation_id": rule.id,
                "rule_name": rule.name,
                "trigger": rule.trigger_kind.to_string(),
                "source_event_type": TIME_DUE_EVENT_TYPE,
                "aggregate_id": envelope.aggregate_id,
                "produced_event_ids": produced_event_ids,
                "occurred_at": now,
            }),
            now,
        )
        .with_id(run_id);
        fired_record.causation_id = Some(format!("{CAUSATION_PREFIX}{run_id}"));
        outbox::stage(&mut *tx, SCHEMA_SELF, &fired_record)
            .await
            .map_err(|e| {
                FoundationError::Tick(format!(
                    "AutomationFired staging failed: {e} (scheduler fire rolled back)"
                ))
            })?;

        tx.commit().await.map_err(FoundationError::Db)?;
        Ok(true)
    }
}

/// The tenant from the synthetic envelope's payload, if the gateway
/// reported one.
fn rule_company_or_nil(envelope: &IntegrationEventEnvelope) -> Option<Uuid> {
    envelope
        .payload
        .get("company_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
}

/// The scheduler's run-ledger write (same shape as the engine's).
async fn record_run(
    tx: &mut sqlx::PgConnection,
    run: &AutomationRun,
) -> FoundationResult<()> {
    sqlx::query(
        "INSERT INTO foundation_ext.foundation_automation_runs \
             (id, automation_id, trigger, status, envelope_id, source_event_type, \
              aggregate_id, parent_run_id, depth, actions_applied, effect_event_ids, \
              detail, occurred_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
    )
    .bind(run.id)
    .bind(run.automation_id)
    .bind(run.trigger)
    .bind(run.status)
    .bind(&run.envelope_id)
    .bind(&run.source_event_type)
    .bind(&run.aggregate_id)
    .bind(run.parent_run_id)
    .bind(run.depth)
    .bind(&run.actions_applied)
    .bind(&run.effect_event_ids)
    .bind(&run.detail)
    .bind(run.occurred_at)
    .execute(&mut *tx)
    .await
    .map_err(FoundationError::Db)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ladder_math_is_the_declared_formula() {
        // min(max(1, min_delay // 10), 240) minutes.
        assert_eq!(default_ladder_interval(0), 1);
        assert_eq!(default_ladder_interval(5), 1);
        assert_eq!(default_ladder_interval(9), 1);
        assert_eq!(default_ladder_interval(10), 1);
        assert_eq!(default_ladder_interval(50), 5);
        assert_eq!(default_ladder_interval(1200), 120);
        assert_eq!(default_ladder_interval(2400), 240);
        assert_eq!(default_ladder_interval(1_000_000), 240);
        assert_eq!(default_ladder_interval(-7), 1);
    }

    #[test]
    fn ladder_grows_back_not_only_shrinks() {
        // The formula is a pure function of the LIVE rules — nothing
        // ratchets. 5 now, 240 later, 5 again after.
        assert_eq!(default_ladder_interval(50), 5);
        assert_eq!(default_ladder_interval(2400), 240);
        assert_eq!(default_ladder_interval(50), 5);
    }

    #[test]
    fn scheduler_event_ids_are_deterministic_per_generation() {
        let rule = Uuid::new_v4();
        let a = scheduler_event_id(rule, "rec-1", "2026-09-01T00:00:00Z");
        assert_eq!(a, scheduler_event_id(rule, "rec-1", "2026-09-01T00:00:00Z"));
        // A different rule, record, or generation is a different id.
        assert_ne!(a, scheduler_event_id(Uuid::new_v4(), "rec-1", "2026-09-01T00:00:00Z"));
        assert_ne!(a, scheduler_event_id(rule, "rec-2", "2026-09-01T00:00:00Z"));
        assert_ne!(a, scheduler_event_id(rule, "rec-1", "2026-10-01T00:00:00Z"));
    }

    #[test]
    fn tick_report_is_constructible_without_a_db() {
        // Presence/shape check for the report type (probe suites assert
        // its fields against the posture row).
        let report = TickReport {
            posture: FoundationSchedulerPosture::Inactive,
            time_rule_count: 0,
            min_delay_minutes: None,
            interval_minutes: None,
            fired: 0,
            deduped: 0,
            deviation_note: None,
        };
        assert_eq!(report.posture, FoundationSchedulerPosture::Inactive);
        let _: Option<EventError> = None; // the engine maps errors; the scheduler does not
    }
}
