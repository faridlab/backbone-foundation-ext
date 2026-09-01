use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::FoundationTriggerKind;
use super::FoundationRunStatus;
use super::AuditMetadata;

/// Strongly-typed ID for AutomationRun
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AutomationRunId(pub Uuid);

impl AutomationRunId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for AutomationRunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for AutomationRunId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for AutomationRunId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<AutomationRunId> for Uuid {
    fn from(id: AutomationRunId) -> Self { id.0 }
}

impl AsRef<Uuid> for AutomationRunId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for AutomationRunId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AutomationRun {
    pub id: Uuid,
    pub automation_id: Uuid,
    pub trigger: FoundationTriggerKind,
    pub status: FoundationRunStatus,
    pub envelope_id: String,
    pub source_event_type: Option<String>,
    pub aggregate_id: Option<String>,
    pub parent_run_id: Option<Uuid>,
    pub depth: i32,
    pub actions_applied: serde_json::Value,
    pub effect_event_ids: serde_json::Value,
    pub detail: Option<String>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl AutomationRun {
    /// Create a builder for AutomationRun
    pub fn builder() -> AutomationRunBuilder {
        <AutomationRunBuilder as Default>::default()
    }

    /// Create a new AutomationRun with required fields
    pub fn new(automation_id: Uuid, trigger: FoundationTriggerKind, status: FoundationRunStatus, envelope_id: String, depth: i32, actions_applied: serde_json::Value, effect_event_ids: serde_json::Value, occurred_at: DateTime<Utc>) -> Self {
        Self {
            id: Uuid::new_v4(),
            automation_id,
            trigger,
            status,
            envelope_id,
            source_event_type: None,
            aggregate_id: None,
            parent_run_id: None,
            depth,
            actions_applied,
            effect_event_ids,
            detail: None,
            occurred_at,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> AutomationRunId {
        AutomationRunId(self.id)
    }

    /// Get when this entity was created
    pub fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.created_at.as_ref()
    }

    /// Get when this entity was last updated
    pub fn updated_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.updated_at.as_ref()
    }

    /// Check if this entity is soft deleted
    pub fn is_deleted(&self) -> bool {
        self.metadata.deleted_at.is_some()
    }

    /// Check if this entity is active (not deleted)
    pub fn is_active(&self) -> bool {
        self.metadata.deleted_at.is_none()
    }

    /// Get when this entity was deleted
    pub fn deleted_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.deleted_at.as_ref()
    }

    /// Get who created this entity
    pub fn created_by(&self) -> Option<&Uuid> {
        self.metadata.created_by.as_ref()
    }

    /// Get who last updated this entity
    pub fn updated_by(&self) -> Option<&Uuid> {
        self.metadata.updated_by.as_ref()
    }

    /// Get who deleted this entity
    pub fn deleted_by(&self) -> Option<&Uuid> {
        self.metadata.deleted_by.as_ref()
    }

    /// Get the current status
    pub fn status(&self) -> &FoundationRunStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the source_event_type field (chainable)
    pub fn with_source_event_type(mut self, value: String) -> Self {
        self.source_event_type = Some(value);
        self
    }

    /// Set the aggregate_id field (chainable)
    pub fn with_aggregate_id(mut self, value: String) -> Self {
        self.aggregate_id = Some(value);
        self
    }

    /// Set the parent_run_id field (chainable)
    pub fn with_parent_run_id(mut self, value: Uuid) -> Self {
        self.parent_run_id = Some(value);
        self
    }

    /// Set the detail field (chainable)
    pub fn with_detail(mut self, value: String) -> Self {
        self.detail = Some(value);
        self
    }

    // ==========================================================
    // Partial Update
    // ==========================================================

    /// Apply partial updates from a map of field name to JSON value
    pub fn apply_patch(&mut self, fields: std::collections::HashMap<String, serde_json::Value>) {
        for (key, value) in fields {
            match key.as_str() {
                "automation_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.automation_id = v; }
                }
                "trigger" => {
                    if let Ok(v) = serde_json::from_value(value) { self.trigger = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                "envelope_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.envelope_id = v; }
                }
                "source_event_type" => {
                    if let Ok(v) = serde_json::from_value(value) { self.source_event_type = v; }
                }
                "aggregate_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.aggregate_id = v; }
                }
                "parent_run_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.parent_run_id = v; }
                }
                "depth" => {
                    if let Ok(v) = serde_json::from_value(value) { self.depth = v; }
                }
                "actions_applied" => {
                    if let Ok(v) = serde_json::from_value(value) { self.actions_applied = v; }
                }
                "effect_event_ids" => {
                    if let Ok(v) = serde_json::from_value(value) { self.effect_event_ids = v; }
                }
                "detail" => {
                    if let Ok(v) = serde_json::from_value(value) { self.detail = v; }
                }
                "occurred_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.occurred_at = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for AutomationRun {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "AutomationRun"
    }
}

impl backbone_core::PersistentEntity for AutomationRun {
    fn entity_id(&self) -> String {
        self.id.to_string()
    }
    fn set_entity_id(&mut self, id: String) {
        if let Ok(uuid) = uuid::Uuid::parse_str(&id) {
            self.id = uuid;
        }
    }
    fn created_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.created_at
    }
    fn set_created_at(&mut self, ts: chrono::DateTime<chrono::Utc>) {
        self.metadata.created_at = Some(ts);
    }
    fn updated_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.updated_at
    }
    fn set_updated_at(&mut self, ts: chrono::DateTime<chrono::Utc>) {
        self.metadata.updated_at = Some(ts);
    }
    fn deleted_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.deleted_at
    }
    fn set_deleted_at(&mut self, ts: Option<chrono::DateTime<chrono::Utc>>) {
        self.metadata.deleted_at = ts;
    }
}

impl backbone_orm::EntityRepoMeta for AutomationRun {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("automation_id".to_string(), "uuid".to_string());
        m.insert("parent_run_id".to_string(), "uuid".to_string());
        m.insert("trigger".to_string(), "foundation_trigger_kind".to_string());
        m.insert("status".to_string(), "foundation_run_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &["envelope_id"]
    }
}

/// Builder for AutomationRun entity
///
/// Provides a fluent API for constructing AutomationRun instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct AutomationRunBuilder {
    automation_id: Option<Uuid>,
    trigger: Option<FoundationTriggerKind>,
    status: Option<FoundationRunStatus>,
    envelope_id: Option<String>,
    source_event_type: Option<String>,
    aggregate_id: Option<String>,
    parent_run_id: Option<Uuid>,
    depth: Option<i32>,
    actions_applied: Option<serde_json::Value>,
    effect_event_ids: Option<serde_json::Value>,
    detail: Option<String>,
    occurred_at: Option<DateTime<Utc>>,
}

impl AutomationRunBuilder {
    /// Set the automation_id field (required)
    pub fn automation_id(mut self, value: Uuid) -> Self {
        self.automation_id = Some(value);
        self
    }

    /// Set the trigger field (required)
    pub fn trigger(mut self, value: FoundationTriggerKind) -> Self {
        self.trigger = Some(value);
        self
    }

    /// Set the status field (default: `FoundationRunStatus::default()`)
    pub fn status(mut self, value: FoundationRunStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Set the envelope_id field (required)
    pub fn envelope_id(mut self, value: String) -> Self {
        self.envelope_id = Some(value);
        self
    }

    /// Set the source_event_type field (optional)
    pub fn source_event_type(mut self, value: String) -> Self {
        self.source_event_type = Some(value);
        self
    }

    /// Set the aggregate_id field (optional)
    pub fn aggregate_id(mut self, value: String) -> Self {
        self.aggregate_id = Some(value);
        self
    }

    /// Set the parent_run_id field (optional)
    pub fn parent_run_id(mut self, value: Uuid) -> Self {
        self.parent_run_id = Some(value);
        self
    }

    /// Set the depth field (default: `0`)
    pub fn depth(mut self, value: i32) -> Self {
        self.depth = Some(value);
        self
    }

    /// Set the actions_applied field (required)
    pub fn actions_applied(mut self, value: serde_json::Value) -> Self {
        self.actions_applied = Some(value);
        self
    }

    /// Set the effect_event_ids field (required)
    pub fn effect_event_ids(mut self, value: serde_json::Value) -> Self {
        self.effect_event_ids = Some(value);
        self
    }

    /// Set the detail field (optional)
    pub fn detail(mut self, value: String) -> Self {
        self.detail = Some(value);
        self
    }

    /// Set the occurred_at field (required)
    pub fn occurred_at(mut self, value: DateTime<Utc>) -> Self {
        self.occurred_at = Some(value);
        self
    }

    /// Build the AutomationRun entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<AutomationRun, String> {
        let automation_id = self.automation_id.ok_or_else(|| "automation_id is required".to_string())?;
        let trigger = self.trigger.ok_or_else(|| "trigger is required".to_string())?;
        let envelope_id = self.envelope_id.ok_or_else(|| "envelope_id is required".to_string())?;
        let actions_applied = self.actions_applied.ok_or_else(|| "actions_applied is required".to_string())?;
        let effect_event_ids = self.effect_event_ids.ok_or_else(|| "effect_event_ids is required".to_string())?;
        let occurred_at = self.occurred_at.ok_or_else(|| "occurred_at is required".to_string())?;

        Ok(AutomationRun {
            id: Uuid::new_v4(),
            automation_id,
            trigger,
            status: self.status.unwrap_or_default(),
            envelope_id,
            source_event_type: self.source_event_type,
            aggregate_id: self.aggregate_id,
            parent_run_id: self.parent_run_id,
            depth: self.depth.unwrap_or(0),
            actions_applied,
            effect_event_ids,
            detail: self.detail,
            occurred_at,
            metadata: AuditMetadata::default(),
        })
    }
}
