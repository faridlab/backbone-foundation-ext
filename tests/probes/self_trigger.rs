//! THE recursion-guard probe: a self-triggering rule fires EXACTLY ONCE
//! under a serial run.
//!
//! The rule watches `sapiens.*` and its action "writes" a
//! `sapiens.record.updated` event — which matches its own trigger. The
//! serial drive:
//!
//! ```text
//! E1 (organic sapiens.user.deactivated)  -> rule FIRES (depth 0), write stages W1
//! W1 (sapiens.record.updated, its own)   -> rule SUPPRESSED (own write), no application
//! W1 redelivered                          -> inbox dedup, nothing
//! ```
//!
//! Expected ledger: exactly ONE `fired` row, one `suppressed_self_trigger`
//! row, exactly ONE gateway application. A second `fired` row here would
//! mean the guard failed and the loop was live.

use std::sync::Arc;

use uuid::Uuid;

use backbone_foundation_ext::application::service::rule_service::RuleDraft;
use backbone_foundation_ext::domain::entity::FoundationTriggerKind;
use backbone_messaging::IntegrationEventHandler;

use super::common::{
    all_runs, check, count_status, echo_envelope, envelope, module_on, TestDb,
};

#[tokio::test]
async fn self_triggering_rule_fires_exactly_once_serial() {
    let db = TestDb::new("selftrig").await;
    let module = module_on(&db.pool).await;
    let gateway = super::common::RecordingGateway::new();
    module.install_gateway(Arc::new(gateway.clone()));

    let rule = module
        .rule_service()
        .create_rule(&RuleDraft {
            name: "archive on any sapiens change".to_string(),
            description: Some("watches every sapiens event; its own write matches".to_string()),
            model: "sapiens".to_string(),
            trigger_kind: FoundationTriggerKind::OnEvent,
            trigger_pattern: Some("sapiens.*".to_string()),
            time_field: None,
            delay_minutes: None,
            actions: serde_json::json!([
                {"kind": "set_fields", "fields": {"archived": true}}
            ]),
            created_by_source: Some("self-trigger probe".to_string()),
        })
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: rule create refused: {e}"));

    let handler = module.reaction_handler().clone();

    // 1. The organic trigger fires once.
    let e1 = envelope(Uuid::new_v4(), "sapiens.user.deactivated", "sapiens", "user-1");
    handler
        .handle(e1.clone())
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: organic fire errored: {e}"));
    let runs = all_runs(&db.pool).await;
    check(runs.len() == 1, "organic fire wrote exactly one run row");
    check(
        runs[0].status == backbone_foundation_ext::domain::entity::FoundationRunStatus::Fired,
        "organic fire's run row is fired",
    );
    check(runs[0].depth == 0, "organic fire is causal depth 0");
    check(runs[0].envelope_id == e1.id, "run row names the triggering envelope");
    check(gateway.applied().len() == 1, "gateway applied exactly one action");

    // 2. The rule's OWN write (which matches its own trigger pattern)
    //    comes back around: suppressed, never applied.
    let produced = gateway.produced();
    check(produced.len() == 1, "the fire staged exactly one watched-module write");
    check(
        produced[0].event_type == "sapiens.record.updated",
        "the probe gateway's write event matches the rule's own pattern"
    );
    handler
        .handle(echo_envelope(&produced[0]))
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: own-write echo errored: {e}"));

    check(
        count_status(&db.pool, "fired").await == 1,
        "EXACTLY ONE fired run after the own-write echo (the guard held)"
    );
    check(
        count_status(&db.pool, "suppressed_self_trigger").await == 1,
        "the own-write echo produced a suppressed_self_trigger row"
    );
    check(gateway.applied().len() == 1, "the gateway applied NOTHING for the echo");

    // 3. The suppressed row is a real ledger row naming both sides.
    let runs = all_runs(&db.pool).await;
    check(runs.len() == 2, "two ledger rows total (fired + suppressed)");
    check(
        runs.iter().all(|r| r.automation_id == rule.id),
        "both rows belong to the probing rule"
    );
    check(
        runs[0].produced(&produced[0].id.to_string()),
        "the fired run's registry names the produced write id (the suppression evidence)"
    );

    // 4. Redelivery of the echo changes nothing (dedup leg).
    handler
        .handle(echo_envelope(&produced[0]))
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: echo redelivery errored: {e}"));
    check(
        all_runs(&db.pool).await.len() == 2,
        "echo redelivery added no rows (inbox dedup)"
    );
    check(gateway.applied().len() == 1, "still exactly one application after redelivery");

    db.dispose().await;
}

/// The module's OWN lifecycle event (`foundation_ext.automation.fired`,
/// id = run id) is in the producing run's registry by construction: a
/// rule watching `foundation_ext.*` cannot re-trigger itself on its own
/// fired event either.
#[tokio::test]
async fn own_fired_event_echo_is_suppressed() {
    let db = TestDb::new("selfecho").await;
    let module = module_on(&db.pool).await;
    let gateway = super::common::RecordingGateway::new();
    module.install_gateway(Arc::new(gateway.clone()));

    module
        .rule_service()
        .create_rule(&RuleDraft {
            name: "watch automations themselves".to_string(),
            description: None,
            model: "foundation_ext".to_string(),
            trigger_kind: FoundationTriggerKind::OnEvent,
            trigger_pattern: Some("*".to_string()),
            time_field: None,
            delay_minutes: None,
            actions: serde_json::json!([
                {"kind": "notify", "template": "automation.fired",
                 "recipients": ["ops@example.test"], "context": {}}
            ]),
            created_by_source: Some("self-echo probe".to_string()),
        })
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: rule create refused: {e}"));

    let handler = module.reaction_handler().clone();

    // An organic foundation_ext event fires the rule once.
    let organic = envelope(
        Uuid::new_v4(),
        "foundation_ext.something.happened",
        "foundation_ext",
        "agg-1",
    );
    handler
        .handle(organic)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: organic fire errored: {e}"));
    check(count_status(&db.pool, "fired").await == 1, "one organic fired run");

    // The module staged its AutomationFired event with id = run id.
    let runs = all_runs(&db.pool).await;
    let run_id = runs[0].id;
    let staged: Vec<(uuid::Uuid, String)> = sqlx::query_as(
        "SELECT id, event_type FROM foundation_ext.outbox_events ORDER BY occurred_at",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap_or_else(|e| panic!("PROBE-FAIL: outbox read failed: {e}"));
    check(
        staged.iter().any(|(id, ty)| {
            *id == run_id && ty == "foundation_ext.automation.fired"
        }),
        "the AutomationFired event was staged in-transaction with id = run id"
    );

    // That event echoes back around the bus (it matches the rule's '*'):
    // the producing rule must be suppressed, not re-fired.
    let echo = envelope(
        run_id,
        "foundation_ext.automation.fired",
        "foundation_ext",
        &run_id.to_string(),
    );
    handler
        .handle(echo)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: fired-echo errored: {e}"));
    check(
        count_status(&db.pool, "fired").await == 1,
        "the module's own fired event did NOT re-fire its producing rule"
    );
    check(
        count_status(&db.pool, "suppressed_self_trigger").await == 1,
        "the fired-echo produced a suppressed_self_trigger row"
    );
    check(gateway.applied().len() == 1, "still exactly one application");

    db.dispose().await;
}
