use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "foundation_trigger_kind", rename_all = "snake_case")]
pub enum FoundationTriggerKind {
    OnEvent,
    OnDeleted,
    OnTime,
}

impl std::fmt::Display for FoundationTriggerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OnEvent => write!(f, "on_event"),
            Self::OnDeleted => write!(f, "on_deleted"),
            Self::OnTime => write!(f, "on_time"),
        }
    }
}

impl FromStr for FoundationTriggerKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "on_event" => Ok(Self::OnEvent),
            "on_deleted" => Ok(Self::OnDeleted),
            "on_time" => Ok(Self::OnTime),
            _ => Err(format!("Unknown FoundationTriggerKind variant: {}", s)),
        }
    }
}

impl Default for FoundationTriggerKind {
    fn default() -> Self {
        Self::OnEvent
    }
}
