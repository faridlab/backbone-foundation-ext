//! The reaction engine — automations as a REACTION layer over the staged
//! outbox (hand-written; user-owned; see `metaphor.codegen.yaml`).
//!
//! THE CONTRACT THIS ENGINE KEEPS:
//!
//! - **Subscription, never interrogation.** The engine implements
//!   `IntegrationEventHandler` with pattern `"*"`; the HOST registers it
//!   on its integration bus, and events reach it only after a producer
//!   staged them in ITS outbox and the relay drained them. The engine
//!   never polls other modules' tables and is never called in-process
//!   from a producer's write path.
//! - **In-transaction fire.** For one envelope: claim the inbox slot,
//!   match rules, apply every action of every matching rule through the
//!   host-wired gateway, write the run rows, and stage this module's own
//!   `foundation_ext.automation.fired` event — ALL IN ONE TRANSACTION. A
//!   failed fire rolls back completely: no run row says `fired` for an
//!   effect that did not commit (**no phantom fires**).
//! - **At-least-once in, exactly-once effect.** The relay may redeliver;
//!   the `inbox::once` claim on `(foundation_ext.reaction, envelope id)`
//!   makes the second delivery a no-op.
//! - **The recursion guard, three legs** (a rule's writes never satisfy
//!   its own trigger; a chain cannot run away):
//!   1. *Self-exclusion* — the gateway reports the event ids each action
//!      staged (`produced_event_ids`); they are stored on the run row as
//!      `effect_event_ids` (the recursion registry). When an envelope
//!      carrying one of those ids comes back around, the rule that
//!      produced it is suppressed (`suppressed_self_trigger` — audited,
//!      not silent). Other rules may chain on it, at parent depth + 1.
//!   2. *Dedup* — the inbox claim above.
//!   3. *Causal depth cap* — a chain of automation-caused fires is
//!      capped (`depth_capped` beyond the ceiling; organic fires are
//!      depth 0, each automation-caused hop +1).
//! - **No invariants here.** The engine never validates domain state; it
//!   pattern-matches, guards recursion, applies declarative actions
//!   through the watched module's own validated write service (the
//!   gateway), and records what happened. Invariants live in DB
//!   constraints and verbs — always.
//!
//! PARENT RESOLUTION (how an envelope is known to be automation-caused):
//! primary — its id appears in some run's `effect_event_ids`; secondary —
//! its `causation_id` carries this module's run token
//! (`foundation_ext:run:<run_id>`), which survives even when an adapter
//! could not report produced ids.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::{Uuid, uuid};

use backbone_messaging::EventError;
use backbone_messaging::IntegrationEventEnvelope;
use backbone_messaging::IntegrationEventHandler;
use backbone_outbox::inbox;
use backbone_outbox::outbox;
use backbone_outbox::OutboxRecord;
use sqlx::PgConnection;
use sqlx::PgPool;
use sqlx::Row;

use crate::domain::entity::AutomationRule;
use crate::domain::entity::AutomationRun;
use crate::domain::entity::FoundationRunStatus;
use crate::domain::entity::FoundationTriggerKind;

use super::action_spec::parse_body;
use super::action_spec::pattern_matches;
use super::action_spec::validate_body_for_trigger;
use super::gateway_port::AutomationGatewaySlot;
use super::gateway_port::FireContext;
use super::rule_service::map_rule;
use super::rule_service::RULE_COLUMNS;

/// This module's own schema (its outbox/inbox + tables).
pub const SCHEMA_SELF: &str = "foundation_ext";

/// The inbox consumer identity for event reaction (the dedup key's
/// consumer half).
pub const CONSUMER_REACTION: &str = "foundation_ext.reaction";

/// The causation token prefix stamped on every event this module stages:
/// `foundation_ext:run:<run_id>` — the secondary parent-resolution path.
pub const CAUSATION_PREFIX: &str = "foundation_ext:run:";

/// The event this module stages for every fired run (its own lifecycle
/// event — itself subscribable, under the same recursion guard).
pub const FIRED_EVENT_TYPE: &str = "foundation_ext.automation.fired";

/// The nil-tenant sentinel used when the triggering payload carried no
/// tenant (matches the outbox table's own default posture).
pub const NIL_COMPANY_ID: Uuid = uuid!("00000000-0000-0000-0000-000000000000");

/// The default causal depth cap (mirrors `config/application.yml`).
pub const DEFAULT_CAUSAL_DEPTH_CAP: i32 = 5;

/// The `detail` column bound (gateway summaries are truncated to it).
const MAX_DETAIL: usize = 500;

/// The handler's bus name (also its error attribution).
const NAME: &str = "FoundationExtReactionHandler";

/// The handler the host registers on its integration bus.
pub struct AutomationReactionHandler {
    pool: PgPool,
    gateway_slot: std::sync::Arc<AutomationGatewaySlot>,
    causal_depth_cap: i32,
}

impl AutomationReactionHandler {
    /// Build the handler against the host pool and the shared gateway
    /// slot. `causal_depth_cap` comes from module config (default 5;
    /// values below 1 are clamped to 1).
    pub fn new(
        pool: PgPool,
        gateway_slot: std::sync::Arc<AutomationGatewaySlot>,
        causal_depth_cap: i32,
    ) -> Self {
        Self {
            pool,
            gateway_slot,
            causal_depth_cap: causal_depth_cap.max(1),
        }
    }

    /// The configured causal depth cap.
    pub fn causal_depth_cap(&self) -> i32 {
        self.causal_depth_cap
    }

    /// Extract the tenant from the envelope payload when the producer
    /// staged one (`payload.company_id`, the convention for fenced
    /// producers). The gateway adapter re-fences host-side regardless.
    fn company_id_of(envelope: &IntegrationEventEnvelope) -> Uuid {
        envelope
            .payload
            .get("company_id")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or(NIL_COMPANY_ID)
    }

    /// Resolve the automation-caused parent of an envelope, if any:
    /// `(parent run id, parent depth)`. Primary: the envelope id appears
    /// in a run's `effect_event_ids` (the recursion registry). Secondary:
    /// the envelope carries this module's causation token.
    async fn resolve_parent(
        tx: &mut PgConnection,
        envelope: &IntegrationEventEnvelope,
    ) -> Result<Option<(Uuid, i32)>, EventError> {
        let via_registry: Option<(Uuid, i32)> = sqlx::query_as(
            "SELECT id, depth FROM foundation_ext.foundation_automation_runs \
             WHERE effect_event_ids @> to_jsonb(ARRAY[$1::text]) LIMIT 1",
        )
        .bind(envelope.id.clone())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| {
            EventError::handler(NAME, format!("recursion registry lookup failed: {e}"))
        })?;
        if via_registry.is_some() {
            return Ok(via_registry);
        }
        if let Some(token) = envelope
            .causation_id
            .as_ref()
            .and_then(|c| c.strip_prefix(CAUSATION_PREFIX))
        {
            if let Ok(run_id) = Uuid::parse_str(token) {
                let via_token: Option<(Uuid, i32)> = sqlx::query_as(
                    "SELECT id, depth FROM foundation_ext.foundation_automation_runs \
                     WHERE id = $1 LIMIT 1",
                )
                .bind(run_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| {
                    EventError::handler(NAME, format!("causation parent lookup failed: {e}"))
                })?;
                return Ok(via_token);
            }
        }
        Ok(None)
    }

    /// Whether `rule_id`'s own write produced the envelope carrying
    /// `envelope_id` (the self-exclusion leg's registry check).
    async fn rule_produced_event(
        tx: &mut PgConnection,
        rule_id: Uuid,
        envelope_id: &str,
    ) -> Result<bool, EventError> {
        let produced: Option<bool> = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM foundation_ext.foundation_automation_runs \
             WHERE automation_id = $1 \
               AND effect_event_ids @> to_jsonb(ARRAY[$2::text]))",
        )
        .bind(rule_id)
        .bind(envelope_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| {
            EventError::handler(NAME, format!("self-exclusion lookup failed: {e}"))
        })?;
        Ok(produced.unwrap_or(false))
    }

    /// Insert one run row (the ledger write shared by every disposition).
    async fn record_run(tx: &mut PgConnection, run: &AutomationRun) -> Result<(), EventError> {
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
        .map_err(|e| {
            EventError::handler(NAME, format!("run ledger write failed: {e}"))
        })?;
        Ok(())
    }

    /// A not-fired run row: every disposition other than `fired` echoes
    /// the rule's raw action body, records no effects, and carries its
    /// reason in `detail`.
    #[allow(clippy::too_many_arguments)]
    fn disposition_row(
        rule: &AutomationRule,
        envelope: &IntegrationEventEnvelope,
        parent_run_id: Option<Uuid>,
        depth: i32,
        status: FoundationRunStatus,
        detail: String,
        now: DateTime<Utc>,
    ) -> AutomationRun {
        AutomationRun {
            id: Uuid::new_v4(),
            automation_id: rule.id,
            trigger: rule.trigger_kind,
            status,
            envelope_id: envelope.id.clone(),
            source_event_type: Some(envelope.event_type.clone()),
            aggregate_id: Some(envelope.aggregate_id.clone()),
            parent_run_id,
            depth,
            actions_applied: rule.actions.clone(),
            effect_event_ids: serde_json::json!([]),
            detail: Some(detail),
            occurred_at: now,
            metadata: Default::default(),
        }
    }
}

#[async_trait]
impl IntegrationEventHandler for AutomationReactionHandler {
    async fn handle(&self, envelope: IntegrationEventEnvelope) -> Result<(), EventError> {
        let event_id = match Uuid::parse_str(&envelope.id) {
            Ok(id) => id,
            Err(e) => {
                // The outbox contract makes envelope ids uuids; a producer
                // that stages something else cannot be deduped, so it is
                // skipped loudly rather than claimed opaquely.
                tracing::warn!(
                    event_type = %envelope.event_type,
                    envelope_id = %envelope.id,
                    error = %e,
                    "foundation_ext: envelope id is not a uuid; skipping (not claimable)"
                );
                return Ok(());
            }
        };
        let now = Utc::now();

        let mut tx = self.pool.begin().await.map_err(|e| {
            EventError::handler(NAME, format!("fire transaction begin failed: {e}"))
        })?;

        // Load the live, active event-side rules (both trigger kinds match
        // by pattern; on_deleted differs only in body legality).
        let mut rules: Vec<AutomationRule> = Vec::new();
        for kind in [FoundationTriggerKind::OnEvent, FoundationTriggerKind::OnDeleted] {
            let rows = sqlx::query(&format!(
                "SELECT {RULE_COLUMNS} FROM foundation_ext.foundation_automation_rules \
                 WHERE active AND metadata->>'deleted_at' IS NULL AND trigger_kind = $1"
            ))
            .bind(kind)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| EventError::handler(NAME, format!("rule load failed: {e}")))?;
            rules.extend(rows.iter().map(map_rule));
        }

        if rules.is_empty() {
            // No live rules: nothing could have fired. Do not even claim
            // the inbox slot — an event that predates all rules must not
            // be consumed either way.
            tx.commit().await.map_err(|e| {
                EventError::handler(NAME, format!("empty-rules commit failed: {e}"))
            })?;
            return Ok(());
        }

        // DEDUP LEG: claim (consumer, envelope id). A redelivery claims
        // false and is a full no-op — exactly-once effect.
        let claimed = inbox::once(&mut *tx, SCHEMA_SELF, CONSUMER_REACTION, event_id)
            .await
            .map_err(|e| EventError::handler(NAME, format!("inbox claim failed: {e}")))?;
        if !claimed {
            tx.commit().await.map_err(|e| {
                EventError::handler(NAME, format!("redelivery commit failed: {e}"))
            })?;
            return Ok(());
        }

        // Pattern match (the WHEN arm's event side).
        let matching: Vec<&AutomationRule> = rules
            .iter()
            .filter(|r| {
                r.trigger_pattern
                    .as_deref()
                    .is_some_and(|p| pattern_matches(p, &envelope.event_type))
            })
            .collect();
        if matching.is_empty() {
            // A non-match is the common case, not a disposition: the slot
            // is claimed (this event is done, forever) and no run row is
            // written.
            tx.commit().await.map_err(|e| {
                EventError::handler(NAME, format!("non-match commit failed: {e}"))
            })?;
            return Ok(());
        }

        // Recursion context: is this envelope an automation's own write?
        // Organic triggers are depth 0; an automation-caused envelope
        // fires at parent depth + 1.
        let (parent_run_id, depth) = match Self::resolve_parent(&mut tx, &envelope).await? {
            Some((run_id, parent_depth)) => (Some(run_id), parent_depth + 1),
            None => (None, 0),
        };

        // The gateway is required from here on. Unwired = loud failure
        // that rolls the whole claim back (fail-closed: better a visible
        // error than a silent miss or a phantom row).
        let gateway = self.gateway_slot.take().ok_or_else(|| {
            EventError::handler(
                NAME,
                "automation gateway not composed: install a host adapter before events \
                 reach this handler (fail-closed; the fire transaction rolled back)",
            )
        })?;

        for rule in matching {
            // MODEL FENCE: the pattern matched the event type; the watched
            // model must match the producing context.
            if !envelope.source_context.eq_ignore_ascii_case(&rule.model) {
                let run = Self::disposition_row(
                    rule,
                    &envelope,
                    parent_run_id,
                    depth,
                    FoundationRunStatus::SkippedModelMismatch,
                    format!(
                        "pattern matched {:?} but envelope source_context is {:?}",
                        rule.trigger_pattern.as_deref().unwrap_or("*"),
                        envelope.source_context
                    ),
                    now,
                );
                Self::record_run(&mut tx, &run).await?;
                continue;
            }

            // SELF-EXCLUSION LEG: this rule's own write produced the
            // envelope — it never fires on its own effects.
            if Self::rule_produced_event(&mut tx, rule.id, &envelope.id).await? {
                let run = Self::disposition_row(
                    rule,
                    &envelope,
                    parent_run_id,
                    depth,
                    FoundationRunStatus::SuppressedSelfTrigger,
                    "the triggering envelope was produced by this rule's own write \
                     (recursion guard: self-exclusion)"
                        .to_string(),
                    now,
                );
                Self::record_run(&mut tx, &run).await?;
                continue;
            }

            // DEPTH-CAP LEG: automation-caused chains stop at the ceiling.
            if depth > self.causal_depth_cap {
                let run = Self::disposition_row(
                    rule,
                    &envelope,
                    parent_run_id,
                    depth,
                    FoundationRunStatus::DepthCapped,
                    format!(
                        "causal depth {depth} exceeds the configured cap {}",
                        self.causal_depth_cap
                    ),
                    now,
                );
                Self::record_run(&mut tx, &run).await?;
                continue;
            }

            // FIRE-TIME BODY RE-VALIDATION (the belt): a hand-corrupted or
            // drifted row refuses here instead of executing.
            let actions = match parse_body(&rule.actions)
                .and_then(|parsed| {
                    validate_body_for_trigger(
                        rule.trigger_kind == FoundationTriggerKind::OnDeleted,
                        &parsed,
                    )
                    .map(|_| parsed)
                }) {
                Ok(actions) => actions,
                Err(reason) => {
                    let run = Self::disposition_row(
                        rule,
                        &envelope,
                        parent_run_id,
                        depth,
                        FoundationRunStatus::RefusedBody,
                        format!("fire-time body re-validation refused: {reason}"),
                        now,
                    );
                    Self::record_run(&mut tx, &run).await?;
                    continue;
                }
            };

            // FIRE: apply every action through the gateway in THIS
            // transaction; collect the ids of the events the writes
            // staged (the recursion registry this run records).
            let run_id = Uuid::new_v4();
            let ctx = FireContext {
                rule_id: rule.id,
                run_id,
                model: rule.model.clone(),
                trigger_kind: rule.trigger_kind.to_string(),
                envelope_id: envelope.id.clone(),
                event_type: envelope.event_type.clone(),
                aggregate_id: envelope.aggregate_id.clone(),
                company_id: Self::company_id_of(&envelope),
                now,
            };
            let mut produced_event_ids: Vec<Uuid> = Vec::new();
            let mut summaries: Vec<String> = Vec::new();
            for action in &actions {
                let effect = gateway
                    .apply_action(&mut tx, &ctx, action)
                    .await
                    .map_err(|e| {
                        EventError::handler(
                            NAME,
                            format!(
                                "gateway refused action '{}' for rule {}: {e} \
                                 (the fire transaction rolled back)",
                                action.kind(),
                                rule.id
                            ),
                        )
                    })?;
                produced_event_ids.extend(effect.produced_event_ids);
                summaries.push(effect.summary);
            }

            let mut detail = summaries.join("; ");
            if detail.len() > MAX_DETAIL {
                detail.truncate(MAX_DETAIL);
            }

            // This module's own lifecycle event id IS the run id: the
            // echo lands in the recursion registry by construction, so
            // even a rule watching `foundation_ext.*` cannot re-trigger
            // itself on its own fired event (self-exclusion holds for
            // the module's own writes, not just the gateway's).
            produced_event_ids.push(run_id);

            // The ledger row: committed iff everything above committed.
            let run = AutomationRun {
                id: run_id,
                automation_id: rule.id,
                trigger: rule.trigger_kind,
                status: FoundationRunStatus::Fired,
                envelope_id: envelope.id.clone(),
                source_event_type: Some(envelope.event_type.clone()),
                aggregate_id: Some(envelope.aggregate_id.clone()),
                parent_run_id,
                depth,
                actions_applied: rule.actions.clone(),
                effect_event_ids: serde_json::json!(produced_event_ids),
                detail: if detail.is_empty() { None } else { Some(detail) },
                occurred_at: now,
                metadata: Default::default(),
            };
            Self::record_run(&mut tx, &run).await?;

            // The lifecycle event itself, staged in the same transaction
            // as the run row: observers (digests, audit feeds) see fired
            // automations through the same bus, and the causation token
            // marks the recursion chain.
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
                    "source_event_type": envelope.event_type,
                    "aggregate_id": envelope.aggregate_id,
                    "produced_event_ids": produced_event_ids,
                    "occurred_at": now,
                }),
                now,
            )
            .with_id(run_id);
            fired_record.correlation_id = envelope.correlation_id.clone();
            fired_record.causation_id = Some(format!("{CAUSATION_PREFIX}{run_id}"));
            outbox::stage(&mut *tx, SCHEMA_SELF, &fired_record)
                .await
                .map_err(|e| {
                    EventError::handler(
                        NAME,
                        format!("AutomationFired staging failed: {e} (fire rolled back)"),
                    )
                })?;
        }

        tx.commit().await.map_err(|e| {
            EventError::handler(NAME, format!("fire commit failed: {e}"))
        })?;
        Ok(())
    }

    fn event_patterns(&self) -> Vec<&'static str> {
        // The engine does its own pattern matching against rule bodies;
        // the bus subscription is intentionally everything, because rule
        // patterns are data and must not require a code change to
        // broaden. The model fence + recursion guard stand behind it.
        vec!["*"]
    }

    fn name(&self) -> &'static str {
        NAME
    }

    fn should_retry(&self) -> bool {
        // A rolled-back fire is safe to retry (the inbox claim rolls back
        // with it); a committed one never errors.
        true
    }
}
