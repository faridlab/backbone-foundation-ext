//! Shared harness: one DISPOSABLE scratch database per test, FAIL-HARD,
//! plus the recording gateway every probe drives the module through.
//!
//! The suite never runs against a shared DB (and NEVER against any live
//! service database — port 5433 is the pinned scratch container, 5432 is
//! off-limits): each test mints `foundation_seat_<marker>_<hex>`,
//! applies this module's migrations with a raw SQL file runner, runs,
//! and drops the database.
//!
//! FAIL-HARD CONTRACT (the portal/survey harness class): a test that
//! cannot reach its scratch database PANICS — [`TestDb::new`] refuses to
//! return `None`, and [`skipped`] panics on principle. A skipped probe
//! is a FAILED probe: a green suite means the behaviors were exercised,
//! not that they were unreachable.
//!
//! The [`RecordingGateway`] is the probe-side host: it "applies" each
//! declarative action by recording it, and it stages one synthetic
//! watched-module write event per action (id deterministic in the run,
//! event type `<model>.record.updated`) so probes can feed the echo
//! envelope back through the handler and watch the recursion guard
//! answer. It never touches a foreign schema — the engine cannot tell,
//! which is the point of the port.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use backbone_foundation_ext::application::service::action_spec::ActionSpec;
use backbone_foundation_ext::application::service::gateway_port::{
    AppliedEffect, AutomationGateway, DueRecord, FireContext, GatewayError, TimeWatchSpec,
};
use backbone_foundation_ext::domain::entity::FoundationRunStatus;
use backbone_foundation_ext::FoundationExtModule;
use backbone_messaging::IntegrationEventEnvelope;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// The scratch Postgres every test database is born on and dropped from.
/// Port 5433 is the pinned scratch container — NEVER a live service
/// database.
pub const SCRATCH_ADMIN_URL: &str = "postgres://postgres:postgres@localhost:5433/postgres";

fn admin_url() -> String {
    std::env::var("FOUNDATION_EXT_TEST_ADMIN_URL").unwrap_or_else(|_| SCRATCH_ADMIN_URL.into())
}

/// The fail-hard skip: reaching this is a FAILURE, never a green tick.
pub fn skipped(reason: &str) -> ! {
    panic!("VACUOUS SKIP IS A FAILURE: {reason}");
}

/// Assert-or-panic with the probe banner (the suite has no quiet
/// assertions — every failure names what it expected).
pub fn check(condition: bool, what: &str) {
    if !condition {
        panic!("PROBE-FAIL: {what}");
    }
}

/// One disposable scratch database, migrations applied. Panics (never
/// returns `None`) when the scratch Postgres is unreachable.
pub struct TestDb {
    pub pool: PgPool,
    name: String,
    admin: PgPool,
}

impl TestDb {
    pub async fn new(marker: &str) -> Self {
        let url = admin_url();
        let admin = match PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&url)
            .await
        {
            Ok(a) => a,
            Err(e) => {
                eprintln!("PROBE-FAIL: {marker}: admin connect to {url} failed: {e}");
                skipped(&format!("scratch Postgres unreachable: {e}"));
            }
        };
        let suffix: String = Uuid::new_v4().simple().to_string().chars().take(8).collect();
        let name = format!("foundation_seat_{marker}_{suffix}");
        // Disposable by construction: a stale DB of the same name goes first.
        if let Err(e) =
            sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
                .execute(&admin)
                .await
        {
            eprintln!("PROBE-FAIL: {marker}: pre-drop of {name} failed: {e}");
            skipped(&format!("scratch pre-drop failed: {e}"));
        }
        if let Err(e) = sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(&admin).await {
            eprintln!("PROBE-FAIL: {marker}: create database {name} failed: {e}");
            skipped(&format!("scratch create failed: {e}"));
        }
        // Splice ONLY the trailing path segment.
        let db_url = match url.rfind('/') {
            Some(i) => format!("{}{}", &url[..=i], name),
            None => url.clone(),
        };
        let pool = match PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&db_url)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                eprintln!("PROBE-FAIL: {marker}: connect to {db_url} failed: {e}");
                skipped(&format!("scratch connect failed: {e}"));
            }
        };
        if let Err(what) = apply_module_migrations(&pool, marker).await {
            skipped(&what);
        }
        Self { pool, name, admin }
    }

    /// Explicit teardown: drop the scratch database entirely.
    pub async fn dispose(self) {
        self.drop_db().await;
    }

    async fn drop_db(&self) {
        // FORCE: connected test pool may still hold an idle session.
        let _ = sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#, self.name))
            .execute(&self.admin)
            .await;
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let name = self.name.clone();
        let url = admin_url();
        // Leak-guard teardown for panicking tests; dispose() is the happy path.
        std::thread::spawn(move || {
            if let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
                rt.block_on(async move {
                    if let Ok(admin) = sqlx::PgPool::connect(&url).await {
                        let _ = sqlx::query(&format!(
                            r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#
                        ))
                        .execute(&admin)
                        .await;
                    }
                });
            }
        });
    }
}

/// Apply this module's migrations with a raw SQL file runner (sorted
/// `.up.sql` order — the module's files are self-contained).
async fn apply_module_migrations(pool: &PgPool, marker: &str) -> Result<(), String> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = format!("{manifest}/migrations");
    let mut files: Vec<std::path::PathBuf> = match std::fs::read_dir(&dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with(".up.sql"))
                    .unwrap_or(false)
            })
            .collect(),
        Err(e) => return Err(format!("PROBE-FAIL: {marker}: cannot read {dir}: {e}")),
    };
    files.sort();
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| format!("PROBE-FAIL: {marker}: cannot acquire pool conn: {e}"))?;
    for file in files {
        let sql = std::fs::read_to_string(&file)
            .map_err(|e| format!("PROBE-FAIL: {marker}: cannot read {}: {e}", file.display()))?;
        if let Err(e) = sqlx::raw_sql(&sql).execute(&mut *conn).await {
            return Err(format!("PROBE-FAIL: {marker}: migration {} failed: {e}", file.display()));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Module + envelope helpers
// ---------------------------------------------------------------------------

/// Build the module against a scratch pool (all hand services wired,
/// gateway deliberately NOT installed — probes install the recording
/// gateway themselves so the fail-closed path stays probeable).
pub async fn module_on(pool: &PgPool) -> FoundationExtModule {
    FoundationExtModule::builder()
        .with_database(pool.clone())
        .build()
        .unwrap_or_else(|e| panic!("PROBE-FAIL: module build failed: {e}"))
}

/// A synthetic integration envelope (the shape the relay hands the
/// handler after draining a producer's outbox).
pub fn envelope(id: Uuid, event_type: &str, source_context: &str, aggregate_id: &str) -> IntegrationEventEnvelope {
    IntegrationEventEnvelope {
        id: id.to_string(),
        event_type: event_type.to_string(),
        source_context: source_context.to_string(),
        aggregate_id: aggregate_id.to_string(),
        occurred_at: Utc::now(),
        published_at: Utc::now(),
        version: 1,
        correlation_id: None,
        causation_id: None,
        payload: serde_json::json!({"company_id": "00000000-0000-0000-0000-000000000001"}),
    }
}

/// One ledger row as the probes read it (status as text for messages).
#[derive(Debug, sqlx::FromRow)]
pub struct RunRow {
    pub id: Uuid,
    pub automation_id: Uuid,
    pub status: FoundationRunStatus,
    pub depth: i32,
    pub envelope_id: String,
    pub parent_run_id: Option<Uuid>,
    pub detail: Option<String>,
    pub effect_event_ids: serde_json::Value,
}

impl RunRow {
    /// Whether this run's recursion registry contains `event_id`.
    pub fn produced(&self, event_id: &str) -> bool {
        self.effect_event_ids
            .as_array()
            .map(|a| a.iter().any(|v| v.as_str() == Some(event_id)))
            .unwrap_or(false)
    }
}

/// The whole fire ledger, in ledger order.
pub async fn all_runs(pool: &PgPool) -> Vec<RunRow> {
    sqlx::query_as(
        "SELECT id, automation_id, status, depth, envelope_id, parent_run_id, detail, \
                effect_event_ids \
         FROM foundation_ext.foundation_automation_runs \
         ORDER BY occurred_at, id",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("PROBE-FAIL: run ledger read failed: {e}"))
}

/// Ledger counts by status, as (status_text, n) pairs.
pub async fn status_counts(pool: &PgPool) -> Vec<(String, i64)> {
    sqlx::query_as(
        "SELECT status::text AS status, count(*)::bigint AS n \
         FROM foundation_ext.foundation_automation_runs GROUP BY 1 ORDER BY 1",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("PROBE-FAIL: status count read failed: {e}"))
}

/// The count for one status (0 when absent).
pub async fn count_status(pool: &PgPool, status: &str) -> i64 {
    let n: Option<i64> = sqlx::query_scalar(
        "SELECT count(*) FROM foundation_ext.foundation_automation_runs WHERE status::text = $1",
    )
    .bind(status)
    .fetch_optional(pool)
    .await
    .unwrap_or_else(|e| panic!("PROBE-FAIL: status count read failed: {e}"));
    n.unwrap_or(0)
}

// ---------------------------------------------------------------------------
// The recording gateway (the probe-side host)
// ---------------------------------------------------------------------------

/// One recorded action application.
#[derive(Debug, Clone)]
pub struct AppliedCall {
    pub rule_id: Uuid,
    pub run_id: Uuid,
    pub kind: String,
    pub aggregate_id: String,
    pub model: String,
}

/// The synthetic watched-module write event the gateway "stages" per
/// action: `(event id, event type)`. Probes feed these back as envelope
/// ids to drive chains and self-echoes deterministically.
#[derive(Debug, Clone)]
pub struct ProducedEvent {
    pub id: Uuid,
    pub event_type: String,
    pub source_context: String,
    pub aggregate_id: String,
}

/// The echo envelope for a produced event — what the relay would hand
/// back after draining the watched module's outbox.
pub fn echo_envelope(produced: &ProducedEvent) -> IntegrationEventEnvelope {
    envelope(
        produced.id,
        &produced.event_type,
        &produced.source_context,
        &produced.aggregate_id,
    )
}

/// The probe-side host adapter: records applications, "stages" one write
/// event per action (deterministic id per run), and serves whatever due
/// records the probe loaded. It applies NO invariants — the engine's
/// contract is that invariants live elsewhere, so the probe must not
/// accidentally prove a gateway-side check the real host would not have.
#[derive(Clone, Default)]
pub struct RecordingGateway {
    state: Arc<GatewayState>,
}

#[derive(Default)]
struct GatewayState {
    applied: std::sync::Mutex<Vec<AppliedCall>>,
    produced: std::sync::Mutex<Vec<ProducedEvent>>,
    due: std::sync::Mutex<Vec<DueRecord>>,
}

impl RecordingGateway {
    pub fn new() -> Self {
        Self::default()
    }

    /// The recorded applications, in fire order.
    pub fn applied(&self) -> Vec<AppliedCall> {
        self.state.applied.lock().map(|g| g.clone()).unwrap_or_default()
    }

    /// The synthetic write events "staged" so far, in fire order.
    pub fn produced(&self) -> Vec<ProducedEvent> {
        self.state.produced.lock().map(|g| g.clone()).unwrap_or_default()
    }

    /// Load the due records the next `due_records` call returns.
    pub fn set_due(&self, due: Vec<DueRecord>) {
        if let Ok(mut guard) = self.state.due.lock() {
            *guard = due;
        }
    }
}

#[async_trait]
impl AutomationGateway for RecordingGateway {
    async fn apply_action(
        &self,
        _tx: &mut sqlx::PgConnection,
        ctx: &FireContext,
        action: &ActionSpec,
    ) -> Result<AppliedEffect, GatewayError> {
        let seq = self.state.applied.lock().map(|g| g.len()).unwrap_or(0);
        if let Ok(mut guard) = self.state.applied.lock() {
            guard.push(AppliedCall {
                rule_id: ctx.rule_id,
                run_id: ctx.run_id,
                kind: action.kind().to_string(),
                aggregate_id: ctx.aggregate_id.clone(),
                model: ctx.model.clone(),
            });
        }
        // The watched module's write stages one event in its own outbox;
        // this adapter reports its id (the contract that powers the
        // recursion registry).
        let produced_id = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("probe-produced|{}|{seq}", ctx.run_id).as_bytes(),
        );
        let event_type = format!("{}.record.updated", ctx.model);
        if let Ok(mut guard) = self.state.produced.lock() {
            guard.push(ProducedEvent {
                id: produced_id,
                event_type: event_type.clone(),
                source_context: ctx.model.clone(),
                aggregate_id: ctx.aggregate_id.clone(),
            });
        }
        Ok(AppliedEffect {
            produced_event_ids: vec![produced_id],
            summary: format!("{} on {} {}", action.kind(), ctx.model, ctx.aggregate_id),
        })
    }

    async fn due_records(
        &self,
        _pool: &PgPool,
        _spec: &TimeWatchSpec,
        _now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<DueRecord>, GatewayError> {
        Ok(self.state.due.lock().map(|g| g.clone()).unwrap_or_default())
    }
}
