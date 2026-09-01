//! The foundation_ext module's behavioral gate.
//!
//! Eight probe classes, each against its own disposable scratch Postgres
//! (port 5433 — never a live service database):
//!
//! - `self_trigger` — THE W7 condition probe: a self-triggering rule
//!   fires exactly once under a serial run (own-write echo and own
//!   `AutomationFired` echo both suppressed, audited, never applied).
//! - `inbox_dedup` — at-least-once delivery becomes an exactly-once
//!   effect; non-matches claim without ledger rows; an unwired gateway
//!   fails LOUD with a full rollback (a retried envelope still fires).
//! - `depth_cap` — a two-rule alternation chain terminates at the causal
//!   depth cap (12 fires, 2 depth_capped, then silence).
//! - `deleted_guards` — the on_deleted body bans at write time AND at
//!   fire time (the belt over a hand-corrupted row), plus the model
//!   fence.
//! - `scheduler_posture` — inactive by default; the delay ladder
//!   `clamp(min_delay/10, 1, 240)`; deterministic re-tick dedup; the
//!   grow-back interval with its declared deviation note.
//! - `vocabulary` — the closed action vocabulary end-to-end through the
//!   guarded rule service (no code, no eval, no smuggled fields, no
//!   non-scalars — and the three legal actions store and retire).
//! - `no_write_routes` — every composable route surface is read-only
//!   and the banned `/web/hook/*` family is absent.
//!
//! Fail-hard contract: see `probes/common/mod.rs` — a skipped probe is a
//! failed probe.

mod probes;
