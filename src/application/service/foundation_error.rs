//! The typed error surface for foundation_ext's hand services
//! (hand-written; user-owned; see `metaphor.codegen.yaml`).
//!
//! One enum shared by the guarded rule service, the reaction engine, and
//! the scheduler. Every refusal is TYPED and carries its reason — this
//! module never fails silently (a disposition that is not a fired run is
//! an audited run row, and an operational failure is one of these variants
//! in the caller's logs).

use thiserror::Error;
use uuid::Uuid;

/// Errors from the foundation_ext reaction layer.
#[derive(Debug, Error)]
pub enum FoundationError {
    /// A rule body or trigger shape failed validation (the closed
    /// vocabulary / trigger-syntax checks). The message names the exact
    /// defect; nothing is written.
    #[error("rule validation refused: {0}")]
    Validation(String),

    /// No row exists for the given rule id (or it is soft-deleted).
    #[error("automation rule {0} not found")]
    NotFound(Uuid),

    /// The host never wired the gateway — the reaction layer's only reach
    /// into other modules refuses loudly rather than pretending to act
    /// (the survey certification-port precedent: unwired = NotComposed,
    /// never a silent no-op).
    #[error("automation gateway not composed: {0}")]
    NotComposed(&'static str),

    /// A database failure underneath a hand verb.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    /// The scheduler tick failed while resolving due records (the gateway
    /// refused or the watch spec is unresolvable host-side).
    #[error("scheduler tick failed: {0}")]
    Tick(String),
}

impl FoundationError {
    /// A coarse HTTP-mapping hint for host-side adapters that surface
    /// these verbs. This module mounts no write routes of its own; the
    /// hint exists so the HOST maps refusals uniformly when it chooses to
    /// expose rule administration.
    pub fn status_hint(&self) -> u16 {
        match self {
            Self::Validation(_) => 422,
            Self::NotFound(_) => 404,
            Self::NotComposed(_) => 503,
            Self::Db(_) | Self::Tick(_) => 500,
        }
    }
}

/// Result alias for the hand services.
pub type FoundationResult<T> = Result<T, FoundationError>;
