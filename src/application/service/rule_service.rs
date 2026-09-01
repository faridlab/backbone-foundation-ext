//! The guarded rule administration service — the ONLY writer of
//! automation rules (hand-written; user-owned; see `metaphor.codegen.yaml`).
//!
//! Every generated route surface in this module is read-only (all three
//! models are `read_only: true`), so the only path that can create or
//! change a rule is this service, called host-side by whatever
//! administration surface the host chooses to mount. It refuses loudly
//! and types every reason ([`FoundationError::Validation`]) — a rule that
//! would parse as code, carry an out-of-vocabulary action, or mismatch
//! its trigger shape never reaches the table.
//!
//! The write-time checks are the SUSPENDERS; the engine re-runs the same
//! parse + trigger-shape validation at fire time as the BELT (a
//! hand-corrupted row refuses as a `refused_body` run row instead of
//! executing).
//!
//! Deletion is SOFT (metadata `deleted_at`), matching the module's audit
//! posture: a rule with a run history is retired, never removed out from
//! under its ledger.

use uuid::Uuid;

use sqlx::PgExecutor;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::Row;

use crate::domain::entity::FoundationTriggerKind;
use crate::domain::entity::AutomationRule;

use super::action_spec::validate_body_for_trigger;
use super::action_spec::validate_pattern;
use super::action_spec::parse_body;
use super::foundation_error::FoundationError;
use super::foundation_error::FoundationResult;

/// Bounds shared by the draft validators.
const MAX_NAME: usize = 120;
const MAX_DESCRIPTION: usize = 2000;
const MAX_MODEL: usize = 63;
const MAX_TIME_FIELD: usize = 63;

/// A validated rule body, ready to store. Built by the host adapter
/// (request DTO -> `RuleDraft`); validated by this service before any
/// write, so a host DTO cannot smuggle an invalid rule past a forgotten
/// check.
#[derive(Debug, Clone)]
pub struct RuleDraft {
    /// Human-readable rule name (1..=120).
    pub name: String,
    /// Optional long description (<= 2000).
    pub description: Option<String>,
    /// The watched model key — matched against the envelope's
    /// `source_context` at fire time (the model fence).
    pub model: String,
    /// The WHEN arm.
    pub trigger_kind: FoundationTriggerKind,
    /// Event pattern for `on_event`/`on_deleted` rules (required there,
    /// refused for `on_time`).
    pub trigger_pattern: Option<String>,
    /// The watched timestamp field for `on_time` rules (required there,
    /// refused for event triggers).
    pub time_field: Option<String>,
    /// The declared delay in minutes for `on_time` rules (>= 0; feeds
    /// the scheduler's ladder).
    pub delay_minutes: Option<i32>,
    /// The declarative action body (the closed vocabulary).
    pub actions: serde_json::Value,
    /// Who/what created this rule (a free-form source label for the
    /// audit trail; there is no actor stamp on the shared metadata).
    pub created_by_source: Option<String>,
}

impl RuleDraft {
    /// Run EVERY draft check: name/model bounds, the closed-vocabulary
    /// body parse, pattern syntax, and the trigger-shape matrix
    /// (pattern for event triggers, time field for time triggers,
    /// no record-targeted actions under `on_deleted`).
    pub fn validate(&self) -> FoundationResult<()> {
        if self.name.trim().is_empty() || self.name.len() > MAX_NAME {
            return Err(FoundationError::Validation(format!(
                "rule name out of bounds (1..={MAX_NAME}, after trim)"
            )));
        }
        if let Some(desc) = &self.description {
            if desc.len() > MAX_DESCRIPTION {
                return Err(FoundationError::Validation(format!(
                    "rule description out of bounds (<={MAX_DESCRIPTION})"
                )));
            }
        }
        if self.model.trim().is_empty() || self.model.len() > MAX_MODEL {
            return Err(FoundationError::Validation(format!(
                "watched model key out of bounds (1..={MAX_MODEL}, after trim)"
            )));
        }
        if let Some(source) = &self.created_by_source {
            if source.len() > MAX_MODEL {
                return Err(FoundationError::Validation(format!(
                    "created_by_source out of bounds (<={MAX_MODEL})"
                )));
            }
        }
        if let Some(delay) = self.delay_minutes {
            if delay < 0 {
                return Err(FoundationError::Validation(
                    "delay_minutes must be >= 0".to_string(),
                ));
            }
        }
        if let Some(pattern) = &self.trigger_pattern {
            validate_pattern(pattern)?;
        }
        if let Some(field) = &self.time_field {
            if field.trim().is_empty() || field.len() > MAX_TIME_FIELD {
                return Err(FoundationError::Validation(format!(
                    "time_field out of bounds (1..={MAX_TIME_FIELD}, after trim)"
                )));
            }
        }
        let kind = self.trigger_kind;
        match self.trigger_kind {
            FoundationTriggerKind::OnEvent | FoundationTriggerKind::OnDeleted => {
                let Some(pattern) = &self.trigger_pattern else {
                    return Err(FoundationError::Validation(format!(
                        "{kind} rules must declare trigger_pattern \
                         (an event type the watched module stages)"
                    )));
                };
                validate_pattern(pattern)?;
                if self.time_field.is_some() {
                    return Err(FoundationError::Validation(format!(
                        "{kind} rules must not declare time_field"
                    )));
                }
            }
            FoundationTriggerKind::OnTime => {
                if self.time_field.is_none() {
                    return Err(FoundationError::Validation(
                        "on_time rules must declare time_field".to_string(),
                    ));
                }
                if self.trigger_pattern.is_some() {
                    return Err(FoundationError::Validation(
                        "on_time rules must not declare trigger_pattern".to_string(),
                    ));
                }
            }
        }
        let actions = parse_body(&self.actions)?;
        validate_body_for_trigger(self.trigger_kind == FoundationTriggerKind::OnDeleted, &actions)?;
        Ok(())
    }
}

/// Row mapper shared by every rule read in this module (column order
/// matches [`RULE_COLUMNS`] everywhere it is used).
pub(crate) fn map_rule(row: &sqlx::postgres::PgRow) -> AutomationRule {
    AutomationRule {
        id: row.get("id"),
        name: row.get("name"),
        description: row.get("description"),
        model: row.get("model"),
        trigger_kind: row.get("trigger_kind"),
        trigger_pattern: row.get("trigger_pattern"),
        time_field: row.get("time_field"),
        delay_minutes: row.get("delay_minutes"),
        actions: row.get("actions"),
        active: row.get("active"),
        created_by_source: row.get("created_by_source"),
        metadata: serde_json::from_value(row.get("metadata")).unwrap_or_default(),
    }
}

/// The SELECT list, pinned once so mapper and query cannot drift.
pub(crate) const RULE_COLUMNS: &str =
    "id, name, description, model, trigger_kind, trigger_pattern, time_field, \
     delay_minutes, actions, active, created_by_source, metadata";

/// The guarded rule writer/reader. Constructed with the host pool by the
/// module builder.
pub struct RuleService {
    pool: PgPool,
}

impl RuleService {
    /// Bind the service to a pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create a rule from a validated draft. Returns the stored row.
    pub async fn create_rule(&self, draft: &RuleDraft) -> FoundationResult<AutomationRule> {
        draft.validate()?;
        let id = Uuid::new_v4();
        let row = sqlx::query(&format!(
            "INSERT INTO foundation_ext.foundation_automation_rules \
                 (id, name, description, model, trigger_kind, trigger_pattern, time_field, \
                  delay_minutes, actions, active, created_by_source) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,TRUE,$10) \
             RETURNING {RULE_COLUMNS}"
        ))
        .bind(id)
        .bind(draft.name.trim())
        .bind(&draft.description)
        .bind(draft.model.trim())
        .bind(draft.trigger_kind)
        .bind(&draft.trigger_pattern)
        .bind(&draft.time_field)
        .bind(draft.delay_minutes)
        .bind(&draft.actions)
        .bind(&draft.created_by_source)
        .fetch_one(&self.pool)
        .await
        .map_err(FoundationError::Db)?;
        Ok(map_rule(&row))
    }

    /// Replace an existing rule's full body (name/model/trigger/actions)
    /// under the same validation. The id and the run ledger stay.
    pub async fn replace_rule(&self, id: Uuid, draft: &RuleDraft) -> FoundationResult<AutomationRule> {
        draft.validate()?;
        let result = sqlx::query(&format!(
            "UPDATE foundation_ext.foundation_automation_rules SET \
                 name=$2, description=$3, model=$4, trigger_kind=$5, trigger_pattern=$6, \
                 time_field=$7, delay_minutes=$8, actions=$9, created_by_source=$10, \
                 metadata = jsonb_set(metadata, '{{updated_at}}', to_jsonb(NOW())) \
             WHERE id=$1 AND metadata->>'deleted_at' IS NULL \
             RETURNING {RULE_COLUMNS}"
        ))
        .bind(id)
        .bind(draft.name.trim())
        .bind(&draft.description)
        .bind(draft.model.trim())
        .bind(draft.trigger_kind)
        .bind(&draft.trigger_pattern)
        .bind(&draft.time_field)
        .bind(draft.delay_minutes)
        .bind(&draft.actions)
        .bind(&draft.created_by_source)
        .fetch_optional(&self.pool)
        .await
        .map_err(FoundationError::Db)?;
        match result {
            Some(row) => Ok(map_rule(&row)),
            None => Err(FoundationError::NotFound(id)),
        }
    }

    /// Activate or deactivate a rule (retire without deleting history).
    pub async fn set_active(&self, id: Uuid, active: bool) -> FoundationResult<AutomationRule> {
        let result = sqlx::query(&format!(
            "UPDATE foundation_ext.foundation_automation_rules SET active=$2 \
                 , metadata = jsonb_set(metadata, '{{updated_at}}', to_jsonb(NOW())) \
             WHERE id=$1 AND metadata->>'deleted_at' IS NULL \
             RETURNING {RULE_COLUMNS}"
        ))
        .bind(id)
        .bind(active)
        .fetch_optional(&self.pool)
        .await
        .map_err(FoundationError::Db)?;
        match result {
            Some(row) => Ok(map_rule(&row)),
            None => Err(FoundationError::NotFound(id)),
        }
    }

    /// Soft-delete a rule (metadata `deleted_at`): the row and its run
    /// history stay queryable; the engine stops matching it.
    pub async fn delete_rule(&self, id: Uuid) -> FoundationResult<()> {
        let result = sqlx::query(
            "UPDATE foundation_ext.foundation_automation_rules \
             SET metadata = jsonb_set( \
                     jsonb_set(metadata, '{deleted_at}', to_jsonb(NOW())), \
                     '{updated_at}', to_jsonb(NOW())) \
             WHERE id=$1 AND metadata->>'deleted_at' IS NULL",
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(FoundationError::Db)?;
        if result.rows_affected() == 0 {
            return Err(FoundationError::NotFound(id));
        }
        Ok(())
    }

    /// Fetch one live rule.
    pub async fn get_rule(&self, id: Uuid) -> FoundationResult<AutomationRule> {
        let row = sqlx::query(&format!(
            "SELECT {RULE_COLUMNS} FROM foundation_ext.foundation_automation_rules \
             WHERE id=$1 AND metadata->>'deleted_at' IS NULL"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(FoundationError::Db)?;
        match row {
            Some(row) => Ok(map_rule(&row)),
            None => Err(FoundationError::NotFound(id)),
        }
    }

    /// List live rules, optionally filtered to one trigger kind. Ordered
    /// by creation (the audit trigger fills `metadata.created_at` on
    /// INSERT) then name, so administrative listings are stable.
    pub async fn list_rules(
        &self,
        kind: Option<FoundationTriggerKind>,
    ) -> FoundationResult<Vec<AutomationRule>> {
        let rows = match kind {
            Some(kind) => {
                sqlx::query(&format!(
                    "SELECT {RULE_COLUMNS} FROM foundation_ext.foundation_automation_rules \
                     WHERE metadata->>'deleted_at' IS NULL AND trigger_kind=$1 \
                     ORDER BY (metadata->>'created_at') NULLS LAST, name"
                ))
                .bind(kind)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query(&format!(
                    "SELECT {RULE_COLUMNS} FROM foundation_ext.foundation_automation_rules \
                     WHERE metadata->>'deleted_at' IS NULL \
                     ORDER BY (metadata->>'created_at') NULLS LAST, name"
                ))
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(FoundationError::Db)?;
        Ok(rows.iter().map(map_rule).collect())
    }

    /// The engine's rule loader: every live ACTIVE rule of the given
    /// kind. (Public only for the engine + probes; hosts administer via
    /// the verbs above.)
    pub async fn active_rules_of_kind(
        &self,
        executor: impl PgExecutor<'_, Database = Postgres>,
        kind: FoundationTriggerKind,
    ) -> FoundationResult<Vec<AutomationRule>> {
        let rows = sqlx::query(&format!(
            "SELECT {RULE_COLUMNS} FROM foundation_ext.foundation_automation_rules \
             WHERE active AND metadata->>'deleted_at' IS NULL AND trigger_kind=$1"
        ))
        .bind(kind)
        .fetch_all(executor)
        .await
        .map_err(FoundationError::Db)?;
        Ok(rows.iter().map(map_rule).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entity::FoundationTriggerKind;
    use serde_json::json;

    fn event_draft() -> RuleDraft {
        RuleDraft {
            name: "Archive deactivated users".to_string(),
            description: None,
            model: "sapiens".to_string(),
            trigger_kind: FoundationTriggerKind::OnEvent,
            trigger_pattern: Some("sapiens.user.deactivated".to_string()),
            time_field: None,
            delay_minutes: None,
            actions: json!([{"kind": "set_fields", "fields": {"archived": true}}]),
            created_by_source: Some("test".to_string()),
        }
    }

    #[test]
    fn valid_draft_passes() {
        assert!(event_draft().validate().is_ok());
    }

    #[test]
    fn event_rules_require_pattern() {
        let mut draft = event_draft();
        draft.trigger_pattern = None;
        assert!(matches!(
            draft.validate(),
            Err(FoundationError::Validation(_))
        ));
    }

    #[test]
    fn time_rules_require_time_field_and_refuse_pattern() {
        let mut draft = event_draft();
        draft.trigger_kind = FoundationTriggerKind::OnTime;
        draft.trigger_pattern = None;
        draft.time_field = Some("termination_date".to_string());
        draft.delay_minutes = Some(30);
        assert!(draft.validate().is_ok());

        draft.trigger_pattern = Some("*".to_string());
        assert!(draft.validate().is_err());
    }

    #[test]
    fn deleted_rules_reject_record_targets() {
        let mut draft = event_draft();
        draft.trigger_kind = FoundationTriggerKind::OnDeleted;
        assert!(draft.validate().is_err());

        draft.actions = json!([
            {"kind": "notify", "template": "user.gone", "recipients": ["ops@example.test"], "context": {}}
        ]);
        assert!(draft.validate().is_ok());
    }

    #[test]
    fn out_of_vocabulary_body_refuses_at_write_time() {
        let mut draft = event_draft();
        draft.actions = json!([{"kind": "run_sql", "sql": "DELETE FROM x"}]);
        assert!(matches!(draft.validate(), Err(FoundationError::Validation(_))));
    }

    #[test]
    fn name_and_model_bounds() {
        let mut draft = event_draft();
        draft.name = "   ".to_string();
        assert!(draft.validate().is_err());
        let mut draft = event_draft();
        draft.model = "a".repeat(MAX_MODEL + 1);
        assert!(draft.validate().is_err());
        let mut draft = event_draft();
        draft.delay_minutes = Some(-1);
        assert!(draft.validate().is_err());
    }
}
