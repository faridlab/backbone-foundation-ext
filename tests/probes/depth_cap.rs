//! The causal-depth-cap probe: a two-rule alternation that WOULD run
//! forever (every fire produces an event both rules watch) terminates at
//! the cap, with the last refused hops audited as `depth_capped`.
//!
//! Two rules, both watching `chain.*` on model `chain`; every applied
//! action stages a `chain.record.updated` event. Serial drive:
//!
//! ```text
//! E0 organic            -> R1 fires d0, R2 fires d0        (2 fires)
//! each produced echo    -> the producer suppresses itself,
//!                           the other fires at parent.depth + 1
//! ```
//!
//! Depth pairs up 0,0,1,1,2,2,3,3,4,4,5,5 — twelve fires — and the
//! echoes past depth 5 are refused as `depth_capped`, after which NO new
//! events are produced and the queue drains: the chain TERMINATES. A
//! loop that escaped the cap would keep the queue non-empty forever and
//! trip the drive guard.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use uuid::Uuid;

use backbone_foundation_ext::application::service::rule_service::RuleDraft;
use backbone_foundation_ext::domain::entity::FoundationTriggerKind;
use backbone_messaging::IntegrationEventHandler;

use super::common::{check, echo_envelope, envelope, module_on, TestDb};

fn chain_rule(name: &str, source: &str) -> RuleDraft {
    RuleDraft {
        name: name.to_string(),
        description: Some("alternation chain probe rule".to_string()),
        model: "chain".to_string(),
        trigger_kind: FoundationTriggerKind::OnEvent,
        trigger_pattern: Some("chain.*".to_string()),
        time_field: None,
        delay_minutes: None,
        actions: serde_json::json!([
            {"kind": "set_fields", "fields": {"hopped": true}}
        ]),
        created_by_source: Some(source.to_string()),
    }
}

#[tokio::test]
async fn alternation_chain_terminates_at_the_cap() {
    let db = TestDb::new("depthcap").await;
    let module = module_on(&db.pool).await;
    let gateway = super::common::RecordingGateway::new();
    module.install_gateway(Arc::new(gateway.clone()));
    module
        .rule_service()
        .create_rule(&chain_rule("chain hopper one", "depth-cap probe"))
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: rule create refused: {e}"));
    module
        .rule_service()
        .create_rule(&chain_rule("chain hopper two", "depth-cap probe"))
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: rule create refused: {e}"));

    let handler = module.reaction_handler().clone();

    // The organic kick (depth 0 for both rules).
    handler
        .handle(envelope(Uuid::new_v4(), "chain.kick", "chain", "rec-1"))
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: organic kick errored: {e}"));

    // Drive every produced echo exactly once, serially, until the queue
    // drains. The guard makes drain inevitable; without it this loop
    // would never end.
    let mut fed: HashSet<Uuid> = HashSet::new();
    let mut guard = 0;
    loop {
        guard += 1;
        check(guard < 80, "the chain TERMINATED (drive guard not exceeded)");
        let pending: VecDeque<_> = gateway
            .produced()
            .into_iter()
            .filter(|p| !fed.contains(&p.id))
            .collect();
        if pending.is_empty() {
            break;
        }
        for produced in pending {
            fed.insert(produced.id);
            handler
                .handle(echo_envelope(&produced))
                .await
                .unwrap_or_else(|e| panic!("PROBE-FAIL: chain echo errored: {e}"));
        }
    }

    // The ledger: exactly the expected alternation, then the cap.
    let counts = super::common::status_counts(&db.pool).await;
    eprintln!("depth-cap ledger: {counts:?}");
    let get = |status: &str| -> i64 {
        counts
            .iter()
            .find(|(s, _)| s == status)
            .map(|(_, n)| *n)
            .unwrap_or(0)
    };
    check(
        get("fired") == 12,
        "twelve fired runs (depths 0,0,1,1,2,2,3,3,4,4,5,5) — the cap allowed exactly these"
    );
    check(
        get("suppressed_self_trigger") == 12,
        "twelve suppressed rows (every echo suppressed its producer)"
    );
    check(
        get("depth_capped") == 2,
        "two depth_capped rows (the two echoes past the ceiling)"
    );
    check(gateway.applied().len() == 12, "exactly twelve applications — no extra fires");

    // No fired run exceeded the cap, and every fired run beyond the kick
    // carries an automation-caused parent (depth >= 1 implies a parent).
    let runs = super::common::all_runs(&db.pool).await;
    let max_depth = runs
        .iter()
        .filter(|r| {
            r.status == backbone_foundation_ext::domain::entity::FoundationRunStatus::Fired
        })
        .map(|r| r.depth)
        .max()
        .unwrap_or(-1);
    check(max_depth == 5, "the deepest fired run is exactly depth 5 (the cap)");
    check(
        runs.iter()
            .filter(|r| r.depth >= 1)
            .all(|r| r.parent_run_id.is_some()),
        "every automation-caused run names its parent run (the chain is legible)"
    );
    check(
        runs.iter()
            .filter(|r| r.depth == 0)
            .all(|r| r.parent_run_id.is_none()),
        "organic runs carry no parent"
    );

    db.dispose().await;
}
