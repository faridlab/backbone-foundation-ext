//! The fail-closed gateway port — this module's ONLY reach into other
//! modules' records (hand-written; user-owned; see `metaphor.codegen.yaml`).
//!
//! Automations are a REACTION layer: they must read other modules'
//! transition/lifecycle events and (for field-set/transition actions)
//! write other modules' records — but this crate has NO cargo edge to any
//! domain module and NO cross-schema FK. Every such reach goes through
//! this port trait, which the COMPOSING HOST wires at startup to an
//! adapter that calls the watched module's own validated write service.
//!
//! Fail-closed (the survey certification-port / portal credential-port
//! precedent): the module owns the trait, a refusing default, and this
//! install slot. An unwired gateway is `NotComposed` — a LOUD refusal
//! that rolls the fire transaction back, never a silent no-op that would
//! write a `fired` run row for an effect that did not happen (no phantom
//! fires). A host that composes foundation_ext without wiring a gateway
//! discovers that at the first fire attempt, in its logs.
//!
//! Why the port reports `produced_event_ids`: the gateway applies an
//! action through the owning module's write service, which stages that
//! module's events in ITS outbox in the same transaction. The adapter
//! returns those staged event ids, the engine records them on the run row
//! (`effect_event_ids` — the recursion registry), and the producing rule
//! then never fires on their envelopes. That is the recursion guard's
//! self-exclusion leg, and only the gateway can supply it: the engine
//! cannot know which events a foreign module's write produced.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::action_spec::ActionSpec;

/// Everything one action application needs to know about the fire. The
/// gateway resolves `aggregate_id` against the watched model — the engine
/// only carries keys, never foreign records (thin staged payloads carry
/// id + occurred_at; the C11 FE-R2 record documents that boundary).
#[derive(Debug, Clone)]
pub struct FireContext {
    /// The rule being applied.
    pub rule_id: Uuid,
    /// The run row this fire will commit (the engine mints it first so
    /// the gateway can stamp causation on anything it logs host-side).
    pub run_id: Uuid,
    /// The watched model key (the rule's `model`, matched against the
    /// envelope's `source_context` before the gateway was called).
    pub model: String,
    /// The rule's WHEN arm (`on_event` / `on_deleted` / `on_time`), as
    /// the engine saw it.
    pub trigger_kind: String,
    /// The triggering envelope id (the end-to-end dedup key).
    pub envelope_id: String,
    /// The triggering event type.
    pub event_type: String,
    /// The watched record's id (the action's target key).
    pub aggregate_id: String,
    /// The owning tenant the action must fence to.
    pub company_id: Uuid,
    /// The deterministic fire timestamp.
    pub now: DateTime<Utc>,
}

/// What one applied action produced. `produced_event_ids` is the load-
/// bearing half: the ids of the events the owning module's write service
/// staged in the SAME transaction — the engine stores them on the run row
/// as the recursion registry.
#[derive(Debug, Clone, Default)]
pub struct AppliedEffect {
    /// Ids of events this action's write staged in-transaction (empty for
    /// effect-free actions, e.g. a notification whose module stages
    /// nothing observable).
    pub produced_event_ids: Vec<Uuid>,
    /// A short human-readable summary of what was applied (ledger
    /// `detail`; bounded by the caller, not the vocabulary).
    pub summary: String,
}

/// One time-rule's watch: the scheduler asks the gateway which records of
/// `model` are due at `now`, where "due" = `time_field` value (plus the
/// rule's delay) has passed and has not been acted on host-side. The
/// adapter resolves `model.time_field` through the watched module's own
/// tables — this module stores no foreign column references.
#[derive(Debug, Clone)]
pub struct TimeWatchSpec {
    /// The owning rule.
    pub rule_id: Uuid,
    /// The watched model key.
    pub model: String,
    /// The timestamp field to watch on that model.
    pub time_field: String,
    /// The rule's declared delay, in minutes (0 = fire at the field's
    /// time).
    pub delay_minutes: i32,
}

/// One record the scheduler found due.
#[derive(Debug, Clone)]
pub struct DueRecord {
    /// The watched record's id (the fire's target key).
    pub aggregate_id: String,
    /// The record's owning tenant.
    pub company_id: Uuid,
    /// When the record came due.
    pub due_at: DateTime<Utc>,
    /// The generation token — the current value of the watched time
    /// field (string form). The scheduler's deterministic event id is
    /// minted over (rule, record, generation): if the field moves, the
    /// next tick mints a NEW id (a legitimately new due); if it does not,
    /// a re-tick mints the SAME id and the inbox claim dedups it.
    pub generation: String,
}

/// Gateway failures. `NotComposed` is the fail-closed default (nothing
/// installed); `Refused` is a host adapter declining a specific action
/// (its module's validation said no — invariants live in the watched
/// module, never here); `Db` is the shared transaction failing, which
/// rolls the whole fire back by design.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// No gateway was installed — the host never wired the port. Loud by
    /// design.
    #[error("automation gateway not composed: {0}")]
    NotComposed(&'static str),
    /// The host adapter refused this action for this record (watched-
    /// module validation, missing record, tenant fence). The fire
    /// transaction rolls back; nothing fires, nothing is recorded as
    /// fired.
    #[error("automation gateway refused: {0}")]
    Refused(String),
    /// The shared transaction failed underneath the adapter.
    #[error("automation gateway database failure: {0}")]
    Db(#[from] sqlx::Error),
}

/// The port. Implemented HOST-SIDE (one adapter per watched module, or
/// one routing adapter for all); this crate ships only the trait, the
/// slot below, and the tests' in-memory gateways.
#[async_trait]
pub trait AutomationGateway: Send + Sync {
    /// Apply ONE declarative action to the watched record, inside the
    /// fire transaction. The adapter must route through the watched
    /// module's validated write service (invariants stay there) and
    /// report the ids of any events the write staged.
    async fn apply_action(
        &self,
        tx: &mut PgConnection,
        ctx: &FireContext,
        action: &ActionSpec,
    ) -> Result<AppliedEffect, GatewayError>;

    /// Resolve the due records for one time-rule at `now`. Read-only;
    /// the scheduler fires each due record through [`Self::apply_action`]
    /// in its own transaction afterwards.
    async fn due_records(
        &self,
        pool: &PgPool,
        spec: &TimeWatchSpec,
        now: DateTime<Utc>,
    ) -> Result<Vec<DueRecord>, GatewayError>;
}

/// The install slot: the module owns it, the host fills it once at
/// compose time. `Mutex<Option<Arc<...>>>` so install can happen after
/// `build()` without `&mut` on the shared module struct (the portal
/// credential-port pattern).
///
/// The lock is held only for the nanoseconds of cloning the `Arc` —
/// never across an `await` — so the fire path stays contention-free.
#[derive(Default)]
pub struct AutomationGatewaySlot {
    gateway: Mutex<Option<Arc<dyn AutomationGateway>>>,
}

impl AutomationGatewaySlot {
    /// An empty (unwired, fail-closed) slot.
    pub const fn new() -> Self {
        Self {
            gateway: Mutex::new(None),
        }
    }

    /// Install the host adapter. Idempotent-last-wins within tests;
    /// production hosts install once at compose time.
    pub fn install(&self, gateway: Arc<dyn AutomationGateway>) {
        if let Ok(mut slot) = self.gateway.lock() {
            *slot = Some(gateway);
        }
        // A poisoned lock means a panic elsewhere in the process left the
        // mutex unusable; leaving the slot unwired keeps this fail-closed.
    }

    /// Whether an adapter is installed (compose-time diagnostics: warn
    /// loudly when a module that has rules is unwired).
    pub fn is_wired(&self) -> bool {
        self.gateway.lock().map(|slot| slot.is_some()).unwrap_or(false)
    }

    /// Clone the installed adapter, if any. Callers treat `None` as
    /// `NotComposed` — the loud refusal.
    pub fn take(&self) -> Option<Arc<dyn AutomationGateway>> {
        self.gateway.lock().ok().and_then(|slot| slot.clone())
    }
}
