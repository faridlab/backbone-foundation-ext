use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "foundation_scheduler_posture", rename_all = "snake_case")]
pub enum FoundationSchedulerPosture {
    Inactive,
    Active,
}

impl std::fmt::Display for FoundationSchedulerPosture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inactive => write!(f, "inactive"),
            Self::Active => write!(f, "active"),
        }
    }
}

impl FromStr for FoundationSchedulerPosture {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "inactive" => Ok(Self::Inactive),
            "active" => Ok(Self::Active),
            _ => Err(format!("Unknown FoundationSchedulerPosture variant: {}", s)),
        }
    }
}

impl Default for FoundationSchedulerPosture {
    fn default() -> Self {
        Self::Inactive
    }
}
