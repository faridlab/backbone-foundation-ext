use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::FoundationSchedulerPosture;
use super::AuditMetadata;

/// Strongly-typed ID for SchedulerPosture
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchedulerPostureId(pub Uuid);

impl SchedulerPostureId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for SchedulerPostureId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for SchedulerPostureId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for SchedulerPostureId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<SchedulerPostureId> for Uuid {
    fn from(id: SchedulerPostureId) -> Self { id.0 }
}

impl AsRef<Uuid> for SchedulerPostureId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for SchedulerPostureId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct SchedulerPosture {
    pub id: Uuid,
    pub singleton: bool,
    pub posture: FoundationSchedulerPosture,
    pub time_rule_count: i32,
    pub min_delay_minutes: Option<i32>,
    pub interval_minutes: Option<i32>,
    pub last_evaluated_at: Option<DateTime<Utc>>,
    pub last_fired_count: i32,
    pub last_deviation_note: Option<String>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl SchedulerPosture {
    /// Create a builder for SchedulerPosture
    pub fn builder() -> SchedulerPostureBuilder {
        <SchedulerPostureBuilder as Default>::default()
    }

    /// Create a new SchedulerPosture with required fields
    pub fn new(singleton: bool, posture: FoundationSchedulerPosture, time_rule_count: i32, last_fired_count: i32) -> Self {
        Self {
            id: Uuid::new_v4(),
            singleton,
            posture,
            time_rule_count,
            min_delay_minutes: None,
            interval_minutes: None,
            last_evaluated_at: None,
            last_fired_count,
            last_deviation_note: None,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> SchedulerPostureId {
        SchedulerPostureId(self.id)
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

    /// Set the min_delay_minutes field (chainable)
    pub fn with_min_delay_minutes(mut self, value: i32) -> Self {
        self.min_delay_minutes = Some(value);
        self
    }

    /// Set the interval_minutes field (chainable)
    pub fn with_interval_minutes(mut self, value: i32) -> Self {
        self.interval_minutes = Some(value);
        self
    }

    /// Set the last_evaluated_at field (chainable)
    pub fn with_last_evaluated_at(mut self, value: DateTime<Utc>) -> Self {
        self.last_evaluated_at = Some(value);
        self
    }

    /// Set the last_deviation_note field (chainable)
    pub fn with_last_deviation_note(mut self, value: String) -> Self {
        self.last_deviation_note = Some(value);
        self
    }

    // ==========================================================
    // Partial Update
    // ==========================================================

    /// Apply partial updates from a map of field name to JSON value
    pub fn apply_patch(&mut self, fields: std::collections::HashMap<String, serde_json::Value>) {
        for (key, value) in fields {
            match key.as_str() {
                "singleton" => {
                    if let Ok(v) = serde_json::from_value(value) { self.singleton = v; }
                }
                "posture" => {
                    if let Ok(v) = serde_json::from_value(value) { self.posture = v; }
                }
                "time_rule_count" => {
                    if let Ok(v) = serde_json::from_value(value) { self.time_rule_count = v; }
                }
                "min_delay_minutes" => {
                    if let Ok(v) = serde_json::from_value(value) { self.min_delay_minutes = v; }
                }
                "interval_minutes" => {
                    if let Ok(v) = serde_json::from_value(value) { self.interval_minutes = v; }
                }
                "last_evaluated_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.last_evaluated_at = v; }
                }
                "last_fired_count" => {
                    if let Ok(v) = serde_json::from_value(value) { self.last_fired_count = v; }
                }
                "last_deviation_note" => {
                    if let Ok(v) = serde_json::from_value(value) { self.last_deviation_note = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for SchedulerPosture {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "SchedulerPosture"
    }
}

impl backbone_core::PersistentEntity for SchedulerPosture {
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

impl backbone_orm::EntityRepoMeta for SchedulerPosture {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("posture".to_string(), "foundation_scheduler_posture".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &[]
    }
}

/// Builder for SchedulerPosture entity
///
/// Provides a fluent API for constructing SchedulerPosture instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct SchedulerPostureBuilder {
    singleton: Option<bool>,
    posture: Option<FoundationSchedulerPosture>,
    time_rule_count: Option<i32>,
    min_delay_minutes: Option<i32>,
    interval_minutes: Option<i32>,
    last_evaluated_at: Option<DateTime<Utc>>,
    last_fired_count: Option<i32>,
    last_deviation_note: Option<String>,
}

impl SchedulerPostureBuilder {
    /// Set the singleton field (default: `true`)
    pub fn singleton(mut self, value: bool) -> Self {
        self.singleton = Some(value);
        self
    }

    /// Set the posture field (default: `FoundationSchedulerPosture::default()`)
    pub fn posture(mut self, value: FoundationSchedulerPosture) -> Self {
        self.posture = Some(value);
        self
    }

    /// Set the time_rule_count field (default: `0`)
    pub fn time_rule_count(mut self, value: i32) -> Self {
        self.time_rule_count = Some(value);
        self
    }

    /// Set the min_delay_minutes field (optional)
    pub fn min_delay_minutes(mut self, value: i32) -> Self {
        self.min_delay_minutes = Some(value);
        self
    }

    /// Set the interval_minutes field (optional)
    pub fn interval_minutes(mut self, value: i32) -> Self {
        self.interval_minutes = Some(value);
        self
    }

    /// Set the last_evaluated_at field (optional)
    pub fn last_evaluated_at(mut self, value: DateTime<Utc>) -> Self {
        self.last_evaluated_at = Some(value);
        self
    }

    /// Set the last_fired_count field (default: `0`)
    pub fn last_fired_count(mut self, value: i32) -> Self {
        self.last_fired_count = Some(value);
        self
    }

    /// Set the last_deviation_note field (optional)
    pub fn last_deviation_note(mut self, value: String) -> Self {
        self.last_deviation_note = Some(value);
        self
    }

    /// Build the SchedulerPosture entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<SchedulerPosture, String> {

        Ok(SchedulerPosture {
            id: Uuid::new_v4(),
            singleton: self.singleton.unwrap_or(true),
            posture: self.posture.unwrap_or_default(),
            time_rule_count: self.time_rule_count.unwrap_or(0),
            min_delay_minutes: self.min_delay_minutes,
            interval_minutes: self.interval_minutes,
            last_evaluated_at: self.last_evaluated_at,
            last_fired_count: self.last_fired_count.unwrap_or(0),
            last_deviation_note: self.last_deviation_note,
            metadata: AuditMetadata::default(),
        })
    }
}
