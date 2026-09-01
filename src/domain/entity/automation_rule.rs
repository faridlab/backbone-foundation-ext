use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::FoundationTriggerKind;
use super::AuditMetadata;

/// Strongly-typed ID for AutomationRule
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AutomationRuleId(pub Uuid);

impl AutomationRuleId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for AutomationRuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for AutomationRuleId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for AutomationRuleId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<AutomationRuleId> for Uuid {
    fn from(id: AutomationRuleId) -> Self { id.0 }
}

impl AsRef<Uuid> for AutomationRuleId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for AutomationRuleId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AutomationRule {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub model: String,
    pub trigger_kind: FoundationTriggerKind,
    pub trigger_pattern: Option<String>,
    pub time_field: Option<String>,
    pub delay_minutes: Option<i32>,
    pub actions: serde_json::Value,
    pub active: bool,
    pub created_by_source: Option<String>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl AutomationRule {
    /// Create a builder for AutomationRule
    pub fn builder() -> AutomationRuleBuilder {
        <AutomationRuleBuilder as Default>::default()
    }

    /// Create a new AutomationRule with required fields
    pub fn new(name: String, model: String, trigger_kind: FoundationTriggerKind, actions: serde_json::Value, active: bool) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            description: None,
            model,
            trigger_kind,
            trigger_pattern: None,
            time_field: None,
            delay_minutes: None,
            actions,
            active,
            created_by_source: None,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> AutomationRuleId {
        AutomationRuleId(self.id)
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


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the description field (chainable)
    pub fn with_description(mut self, value: String) -> Self {
        self.description = Some(value);
        self
    }

    /// Set the trigger_pattern field (chainable)
    pub fn with_trigger_pattern(mut self, value: String) -> Self {
        self.trigger_pattern = Some(value);
        self
    }

    /// Set the time_field field (chainable)
    pub fn with_time_field(mut self, value: String) -> Self {
        self.time_field = Some(value);
        self
    }

    /// Set the delay_minutes field (chainable)
    pub fn with_delay_minutes(mut self, value: i32) -> Self {
        self.delay_minutes = Some(value);
        self
    }

    /// Set the created_by_source field (chainable)
    pub fn with_created_by_source(mut self, value: String) -> Self {
        self.created_by_source = Some(value);
        self
    }

    // ==========================================================
    // Partial Update
    // ==========================================================

    /// Apply partial updates from a map of field name to JSON value
    pub fn apply_patch(&mut self, fields: std::collections::HashMap<String, serde_json::Value>) {
        for (key, value) in fields {
            match key.as_str() {
                "name" => {
                    if let Ok(v) = serde_json::from_value(value) { self.name = v; }
                }
                "description" => {
                    if let Ok(v) = serde_json::from_value(value) { self.description = v; }
                }
                "model" => {
                    if let Ok(v) = serde_json::from_value(value) { self.model = v; }
                }
                "trigger_kind" => {
                    if let Ok(v) = serde_json::from_value(value) { self.trigger_kind = v; }
                }
                "trigger_pattern" => {
                    if let Ok(v) = serde_json::from_value(value) { self.trigger_pattern = v; }
                }
                "time_field" => {
                    if let Ok(v) = serde_json::from_value(value) { self.time_field = v; }
                }
                "delay_minutes" => {
                    if let Ok(v) = serde_json::from_value(value) { self.delay_minutes = v; }
                }
                "actions" => {
                    if let Ok(v) = serde_json::from_value(value) { self.actions = v; }
                }
                "active" => {
                    if let Ok(v) = serde_json::from_value(value) { self.active = v; }
                }
                "created_by_source" => {
                    if let Ok(v) = serde_json::from_value(value) { self.created_by_source = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for AutomationRule {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "AutomationRule"
    }
}

impl backbone_core::PersistentEntity for AutomationRule {
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

impl backbone_orm::EntityRepoMeta for AutomationRule {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("trigger_kind".to_string(), "foundation_trigger_kind".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &["name", "model"]
    }
}

/// Builder for AutomationRule entity
///
/// Provides a fluent API for constructing AutomationRule instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct AutomationRuleBuilder {
    name: Option<String>,
    description: Option<String>,
    model: Option<String>,
    trigger_kind: Option<FoundationTriggerKind>,
    trigger_pattern: Option<String>,
    time_field: Option<String>,
    delay_minutes: Option<i32>,
    actions: Option<serde_json::Value>,
    active: Option<bool>,
    created_by_source: Option<String>,
}

impl AutomationRuleBuilder {
    /// Set the name field (required)
    pub fn name(mut self, value: String) -> Self {
        self.name = Some(value);
        self
    }

    /// Set the description field (optional)
    pub fn description(mut self, value: String) -> Self {
        self.description = Some(value);
        self
    }

    /// Set the model field (required)
    pub fn model(mut self, value: String) -> Self {
        self.model = Some(value);
        self
    }

    /// Set the trigger_kind field (default: `FoundationTriggerKind::default()`)
    pub fn trigger_kind(mut self, value: FoundationTriggerKind) -> Self {
        self.trigger_kind = Some(value);
        self
    }

    /// Set the trigger_pattern field (optional)
    pub fn trigger_pattern(mut self, value: String) -> Self {
        self.trigger_pattern = Some(value);
        self
    }

    /// Set the time_field field (optional)
    pub fn time_field(mut self, value: String) -> Self {
        self.time_field = Some(value);
        self
    }

    /// Set the delay_minutes field (optional)
    pub fn delay_minutes(mut self, value: i32) -> Self {
        self.delay_minutes = Some(value);
        self
    }

    /// Set the actions field (required)
    pub fn actions(mut self, value: serde_json::Value) -> Self {
        self.actions = Some(value);
        self
    }

    /// Set the active field (default: `true`)
    pub fn active(mut self, value: bool) -> Self {
        self.active = Some(value);
        self
    }

    /// Set the created_by_source field (optional)
    pub fn created_by_source(mut self, value: String) -> Self {
        self.created_by_source = Some(value);
        self
    }

    /// Build the AutomationRule entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<AutomationRule, String> {
        let name = self.name.ok_or_else(|| "name is required".to_string())?;
        let model = self.model.ok_or_else(|| "model is required".to_string())?;
        let actions = self.actions.ok_or_else(|| "actions is required".to_string())?;

        Ok(AutomationRule {
            id: Uuid::new_v4(),
            name,
            description: self.description,
            model,
            trigger_kind: self.trigger_kind.unwrap_or_default(),
            trigger_pattern: self.trigger_pattern,
            time_field: self.time_field,
            delay_minutes: self.delay_minutes,
            actions,
            active: self.active.unwrap_or(true),
            created_by_source: self.created_by_source,
            metadata: AuditMetadata::default(),
        })
    }
}
