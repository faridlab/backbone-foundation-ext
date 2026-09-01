//! The declared-posture scheduler probes: INACTIVE by default (no time
//! rules -> no ticking, no gateway required); activation + the delay
//! ladder `clamp(min_delay/10, 1, 240)`; deterministic per-(rule, record,
//! generation) ids that make re-ticks exactly-once; and the GROW-BACK
//! interval recomputed from live rules with every change recorded as a
//! declared deviation (never an only-ever-shrink ratchet).

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use uuid::Uuid;

use backbone_foundation_ext::application::service::gateway_port::DueRecord;
use backbone_foundation_ext::application::service::rule_service::RuleDraft;
use backbone_foundation_ext::application::service::scheduler_service::scheduler_event_id;
use backbone_foundation_ext::domain::entity::FoundationSchedulerPosture;
use backbone_foundation_ext::domain::entity::FoundationTriggerKind;

use super::common::{check, count_status, module_on, TestDb};

async fn time_rule(delay: Option<i32>) -> RuleDraft {
    RuleDraft {
        name: "follow up stale deals".to_string(),
        description: None,
        model: "crm".to_string(),
        trigger_kind: FoundationTriggerKind::OnTime,
        trigger_pattern: None,
        time_field: Some("follow_up_at".to_string()),
        delay_minutes: delay,
        actions: serde_json::json!([
            {"kind": "notify", "template": "deal.followup",
             "recipients": ["sales@example.test"], "context": {}}
        ]),
        created_by_source: Some("scheduler probe".to_string()),
    }
}

fn due(aggregate: &str, generation: &str) -> DueRecord {
    DueRecord {
        aggregate_id: aggregate.to_string(),
        company_id: Uuid::new_v4(),
        due_at: Utc.with_ymd_and_hms(2026, 9, 1, 9, 0, 0).unwrap(),
        generation: generation.to_string(),
    }
}

/// No time rules: inactive posture, no interval, no gateway needed — the
/// declared default is DO NOTHING, visibly.
#[tokio::test]
async fn inactive_without_time_rules() {
    let db = TestDb::new("schedinactive").await;
    // Deliberately NO gateway installed: an inactive scheduler must not
    // need one.
    let module = module_on(&db.pool).await;

    let report = module
        .scheduler()
        .evaluate_tick(Utc::now())
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: inactive tick errored: {e}"));
    check(
        report.posture == FoundationSchedulerPosture::Inactive,
        "with no time rules the posture is inactive"
    );
    check(report.interval_minutes.is_none(), "an inactive scheduler has no interval");
    check(report.time_rule_count == 0, "zero time rules counted");
    check(report.fired == 0, "an inactive scheduler fires nothing");

    let row: Option<(FoundationSchedulerPosture, Option<i32>, Option<i32>)> = sqlx::query_as(
        "SELECT posture, min_delay_minutes, interval_minutes \
         FROM foundation_ext.foundation_scheduler_posture WHERE singleton",
    )
    .fetch_optional(&db.pool)
    .await
    .unwrap_or(None);
    let Some((posture, min_delay, interval)) = row else {
        panic!("PROBE-FAIL: the inactive tick still recorded its posture row");
    };
    check(posture == FoundationSchedulerPosture::Inactive, "the posture row says inactive");
    check(min_delay.is_none() && interval.is_none(), "the posture row carries no interval");

    db.dispose().await;
}

/// Activation: the ladder over the smallest declared delay, due fires
/// with DETERMINISTIC ids, re-ticks deduped, and a moved time field
/// (generation change) is a legitimately new due.
#[tokio::test]
async fn activates_fires_and_retick_dedups() {
    let db = TestDb::new("schedtick").await;
    let module = module_on(&db.pool).await;
    let gateway = super::common::RecordingGateway::new();
    module.install_gateway(Arc::new(gateway.clone()));

    let rule = module
        .rule_service()
        .create_rule(&time_rule(Some(50)).await)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: time rule refused: {e}"));

    gateway.set_due(vec![due("deal-9", "2026-09-01T09:00:00Z")]);
    let report = module
        .scheduler()
        .evaluate_tick(Utc::now())
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: active tick errored: {e}"));
    check(report.posture == FoundationSchedulerPosture::Active, "a live time rule activates");
    check(report.time_rule_count == 1, "one time rule counted");
    check(report.min_delay_minutes == Some(50), "the smallest declared delay is 50");
    check(
        report.interval_minutes == Some(5),
        "the ladder: clamp(50/10, 1, 240) = 5 minutes"
    );
    check(report.fired == 1, "the due record fired");
    check(gateway.applied().len() == 1, "the fire applied through the gateway");

    // The fired run carries the DETERMINISTIC id for (rule, record, gen).
    let expected_id = scheduler_event_id(rule.id, "deal-9", "2026-09-01T09:00:00Z");
    let run: Option<(String, i32, Option<String>)> = sqlx::query_as(
        "SELECT envelope_id, depth, source_event_type \
         FROM foundation_ext.foundation_automation_runs WHERE status = 'fired'",
    )
    .fetch_optional(&db.pool)
    .await
    .unwrap_or(None);
    let Some((envelope_id, depth, source_type)) = run else {
        panic!("PROBE-FAIL: the scheduler fire wrote no run row");
    };
    check(
        envelope_id == expected_id.to_string(),
        "the run's envelope id is the deterministic (rule, record, generation) uuid"
    );
    check(depth == 0, "a scheduler fire is causal depth 0 (organic)");
    check(
        source_type.as_deref() == Some("foundation_ext.automation.time_due"),
        "the run row names the synthetic time-due source"
    );

    // RE-TICK over the unchanged generation: same id, claim refuses, no
    // new fire — dedup without a fire-ledger of its own.
    let report = module
        .scheduler()
        .evaluate_tick(Utc::now())
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: re-tick errored: {e}"));
    check(report.fired == 0, "the re-tick fired nothing");
    check(report.deduped == 1, "the re-tick's due was refused as a duplicate");
    check(count_status(&db.pool, "fired").await == 1, "still exactly one fired scheduler run");
    check(gateway.applied().len() == 1, "still exactly one application");

    // The time field MOVES (a new generation): a legitimately new due.
    gateway.set_due(vec![due("deal-9", "2026-10-01T09:00:00Z")]);
    let report = module
        .scheduler()
        .evaluate_tick(Utc::now())
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: moved-generation tick errored: {e}"));
    check(report.fired == 1, "the moved time field fired again");
    check(count_status(&db.pool, "fired").await == 2, "two fired scheduler runs total");

    // Unwired gateway WITH live time rules: the tick fails loudly, and
    // the previous posture row is untouched (fail-closed).
    let bare = module_on(&db.pool).await;
    // `bare` shares nothing with `module` (its own slot, unwired).
    let outcome = bare.scheduler().evaluate_tick(Utc::now()).await;
    check(outcome.is_err(), "a tick with live time rules but no gateway FAILS LOUD");

    db.dispose().await;
}

/// The interval is RECOMPUTED from the live rules every tick and may
/// GROW BACK (50 -> 2400 min delay => 5 -> 240 min interval), with the
/// change recorded as a declared deviation in the posture row.
#[tokio::test]
async fn interval_grows_back_with_declared_deviation() {
    let db = TestDb::new("schedgrow").await;
    let module = module_on(&db.pool).await;
    let gateway = super::common::RecordingGateway::new();
    module.install_gateway(Arc::new(gateway.clone()));

    let rule = module
        .rule_service()
        .create_rule(&time_rule(Some(50)).await)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: time rule refused: {e}"));
    gateway.set_due(vec![]);

    let first = module
        .scheduler()
        .evaluate_tick(Utc::now())
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: first tick errored: {e}"));
    check(first.interval_minutes == Some(5), "first tick: interval 5 (from delay 50)");
    check(
        first.deviation_note.is_some(),
        "the activation itself was a declared deviation"
    );

    // The live rule's delay grows: the interval recomputes UPWARD.
    module
        .rule_service()
        .replace_rule(
            rule.id,
            &time_rule(Some(2400)).await,
        )
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: rule replace refused: {e}"));
    let second = module
        .scheduler()
        .evaluate_tick(Utc::now())
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: second tick errored: {e}"));
    check(second.interval_minutes == Some(240), "second tick: interval grew to 240 (ceiling)");
    check(
        second.deviation_note.as_deref().is_some_and(|n| n.contains("5 -> 240")),
        "the grow-back is recorded as a declared deviation (5 -> 240)"
    );

    let row: Option<(Option<i32>, Option<String>)> = sqlx::query_as(
        "SELECT interval_minutes, last_deviation_note \
         FROM foundation_ext.foundation_scheduler_posture WHERE singleton",
    )
    .fetch_optional(&db.pool)
    .await
    .unwrap_or(None);
    let Some((interval, note)) = row else {
        panic!("PROBE-FAIL: posture row missing after grow-back");
    };
    check(interval == Some(240), "the posture row persists the recomputed interval");
    check(
        note.as_deref().is_some_and(|n| n.contains("5 -> 240")),
        "the posture row carries the deviation note"
    );

    db.dispose().await;
}
