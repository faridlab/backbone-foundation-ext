//! The fail-hard probe modules (one scratch database per test).
//!
//! - `self_trigger` — THE W7 recursion-guard probe: a self-triggering
//!   rule fires exactly once under a serial run (own-write echo and own
//!   `AutomationFired` echo are both suppressed).
//! - `inbox_dedup` — at-least-once delivery becomes exactly-once effect;
//!   non-matches claim without ledger rows; an unwired gateway fails
//!   LOUD and rolls the claim back.
//! - `depth_cap` — a two-rule alternation chain terminates at the causal
//!   depth cap (12 fires, then `depth_capped`, then silence).
//! - `deleted_guards` — the on_deleted trigger's body bans, the
//!   fire-time re-validation belt, and the model fence.
//! - `scheduler_posture` — inactive by default; the delay ladder;
//!   deterministic re-tick dedup; the grow-back interval with its
//!   declared deviation note.
//! - `vocabulary` — the closed action vocabulary enforced end-to-end
//!   through the guarded rule service.
//! - `no_write_routes` — every composable route surface is read-only,
//!   and the banned webhook route family is absent.

pub mod common;
pub mod deleted_guards;
pub mod depth_cap;
pub mod inbox_dedup;
pub mod no_write_routes;
pub mod scheduler_posture;
pub mod self_trigger;
pub mod vocabulary;
