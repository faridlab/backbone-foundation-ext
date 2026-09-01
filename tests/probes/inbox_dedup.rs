//! The dedup + fail-closed probes: at-least-once delivery becomes an
//! exactly-once effect; non-matches claim without ledger rows; an
//! unwired gateway fails LOUD and rolls the claim back so a later,
//! wired retry can still fire.

use std::sync::Arc;

use uuid::Uuid;

use backbone_foundation_ext::application::service::rule_service::RuleDraft;
use backbone_foundation_ext::domain::entity::FoundationTriggerKind;
use backbone_messaging::IntegrationEventHandler;

use super::common::{all_runs, check, count_status, envelope, module_on, TestDb};

async fn notify_rule() -> RuleDraft {
    RuleDraft {
        name: "notify on sapiens lifecycle".to_string(),
        description: None,
        model: "sapiens".to_string(),
        trigger_kind: FoundationTriggerKind::OnEvent,
        trigger_pattern: Some("sapiens.*".to_string()),
        time_field: None,
        delay_minutes: None,
        actions: serde_json::json!([
            {"kind": "notify", "template": "user.lifecycle",
             "recipients": ["ops@example.test"], "context": {"watch": "sapiens"}}
        ]),
        created_by_source: Some("dedup probe".to_string()),
    }
}

/// The relay may redeliver; the inbox claim makes the second delivery a
/// full no-op (exactly-once effect), and the claim itself is visible.
#[tokio::test]
async fn redelivery_is_exactly_once_effect() {
    let db = TestDb::new("dedup").await;
    let module = module_on(&db.pool).await;
    let gateway = super::common::RecordingGateway::new();
    module.install_gateway(Arc::new(gateway.clone()));
    module
        .rule_service()
        .create_rule(&notify_rule().await)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: rule create refused: {e}"));

    let handler = module.reaction_handler().clone();
    let e1_id = Uuid::new_v4();
    let e1 = envelope(e1_id, "sapiens.user.created", "sapiens", "user-9");
    for i in 0..3 {
        handler
            .handle(e1.clone())
            .await
            .unwrap_or_else(|e| panic!("PROBE-FAIL: delivery #{i} errored: {e}"));
    }
    check(
        count_status(&db.pool, "fired").await == 1,
        "three deliveries of the same envelope -> exactly one fired run"
    );
    check(gateway.applied().len() == 1, "exactly one application across all deliveries");

    // The claim is the evidence: (foundation_ext.reaction, envelope id).
    let claimed: Option<i64> = sqlx::query_scalar(
        "SELECT count(*) FROM foundation_ext.inbox_consumed \
         WHERE consumer = 'foundation_ext.reaction' AND event_id = $1",
    )
    .bind(e1_id)
    .fetch_optional(&db.pool)
    .await
    .unwrap_or(None);
    check(claimed == Some(1), "the inbox holds exactly one claim row for the envelope");

    db.dispose().await;
}

/// A non-match is the COMMON case, not a disposition: the slot is
/// claimed (this event is done, forever) and NO run row is written.
#[tokio::test]
async fn non_match_claims_but_records_nothing() {
    let db = TestDb::new("nonmatch").await;
    let module = module_on(&db.pool).await;
    module.install_gateway(Arc::new(super::common::RecordingGateway::new()));
    module
        .rule_service()
        .create_rule(&notify_rule().await)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: rule create refused: {e}"));

    let handler = module.reaction_handler().clone();
    let other_id = Uuid::new_v4();
    let other = envelope(other_id, "billing.invoice.settled", "billing", "inv-1");
    handler
        .handle(other.clone())
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: non-match delivery errored: {e}"));

    check(all_runs(&db.pool).await.is_empty(), "a non-match wrote no run rows");
    let claimed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM foundation_ext.inbox_consumed \
         WHERE consumer = 'foundation_ext.reaction' AND event_id = $1",
    )
    .bind(other_id)
    .fetch_one(&db.pool)
    .await
    .unwrap_or(0);
    check(claimed == 1, "the non-match still claimed its inbox slot (done, forever)");

    // And its redelivery stays nothing.
    handler
        .handle(other)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: non-match redelivery errored: {e}"));
    check(all_runs(&db.pool).await.is_empty(), "non-match redelivery wrote no rows");

    db.dispose().await;
}

/// Unwired gateway = LOUD failure with a FULL rollback (claim included),
/// so a retry after wiring still fires — never a silent miss, never a
/// phantom row.
#[tokio::test]
async fn unwired_gateway_fails_loud_and_rolls_back() {
    let db = TestDb::new("failclosed").await;
    // Deliberately NO install_gateway: the module composes unwired.
    let module = module_on(&db.pool).await;
    check(!module.gateway_is_wired(), "a fresh module reports its gateway unwired");
    module
        .rule_service()
        .create_rule(&notify_rule().await)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: rule create refused: {e}"));

    let handler = module.reaction_handler().clone();
    let e1_id = Uuid::new_v4();
    let e1 = envelope(e1_id, "sapiens.user.created", "sapiens", "user-2");
    let outcome = handler.handle(e1.clone()).await;
    check(outcome.is_err(), "an unwired gateway FAILS LOUD (no silent no-op)");
    check(
        all_runs(&db.pool).await.is_empty(),
        "no run rows survived the unwired failure (no phantom fires)"
    );
    let claimed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM foundation_ext.inbox_consumed \
         WHERE consumer = 'foundation_ext.reaction' AND event_id = $1",
    )
    .bind(e1_id)
    .fetch_one(&db.pool)
    .await
    .unwrap_or(0);
    check(claimed == 0, "the inbox claim rolled back with the failed fire");

    // Wiring the gateway afterwards makes the SAME envelope fire: the
    // loud failure was a retry, not a loss.
    module.install_gateway(Arc::new(super::common::RecordingGateway::new()));
    check(module.gateway_is_wired(), "the module reports the gateway after install");
    handler
        .handle(e1)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: post-wiring fire errored: {e}"));
    check(
        count_status(&db.pool, "fired").await == 1,
        "the retried envelope fired after wiring (rollback made it retryable)"
    );

    // An event reaching the handler with a non-uuid id cannot be claimed;
    // it is skipped loudly, not recorded as anything.
    let junk = backbone_messaging::IntegrationEventEnvelope {
        id: "not-a-uuid".to_string(),
        event_type: "sapiens.user.junk".to_string(),
        source_context: "sapiens".to_string(),
        aggregate_id: "user-3".to_string(),
        occurred_at: chrono::Utc::now(),
        published_at: chrono::Utc::now(),
        version: 1,
        correlation_id: None,
        causation_id: None,
        payload: serde_json::json!({}),
    };
    handler
        .handle(junk)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: junk-id delivery errored: {e}"));
    check(
        count_status(&db.pool, "fired").await == 1,
        "a non-uuid envelope id fired nothing"
    );

    db.dispose().await;
}
