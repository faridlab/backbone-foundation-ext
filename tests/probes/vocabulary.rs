//! The closed vocabulary, enforced END-TO-END through the guarded rule
//! service against a scratch database: out-of-vocabulary bodies,
//! smuggled fields, non-scalar values, and trigger-shape violations are
//! all refused at write time — and the three legal actions store, retire
//! (deactivate), and stay retireable (soft delete) without losing the
//! ledger.

use std::sync::Arc;

use uuid::Uuid;

use backbone_foundation_ext::application::service::rule_service::RuleDraft;
use backbone_foundation_ext::domain::entity::FoundationTriggerKind;
use backbone_foundation_ext::application::service::FoundationError;
use backbone_messaging::IntegrationEventHandler;

use super::common::{check, module_on, TestDb};

fn draft_with(actions: serde_json::Value) -> RuleDraft {
    RuleDraft {
        name: "vocabulary probe".to_string(),
        description: None,
        model: "sapiens".to_string(),
        trigger_kind: FoundationTriggerKind::OnEvent,
        trigger_pattern: Some("sapiens.*".to_string()),
        time_field: None,
        delay_minutes: None,
        actions,
        created_by_source: Some("vocabulary probe".to_string()),
    }
}

async fn refused_because_invalid(
    module: &backbone_foundation_ext::FoundationExtModule,
    actions: serde_json::Value,
) {
    let outcome = module.rule_service().create_rule(&draft_with(actions)).await;
    check(
        matches!(outcome, Err(FoundationError::Validation(_))),
        "the guarded service refused the draft"
    );
}

#[tokio::test]
async fn the_vocabulary_is_closed_end_to_end() {
    let db = TestDb::new("vocab").await;
    let module = module_on(&db.pool).await;

    // No code, no sql, no eval — unknown kinds are structurally
    // unparseable, whatever they call themselves.
    refused_because_invalid(
        &module,
        serde_json::json!([{"kind": "run_sql", "sql": "DELETE FROM everything"}]),
    )
    .await;
    refused_because_invalid(
        &module,
        serde_json::json!([{"kind": "eval", "expr": "1 == 1"}]),
    )
    .await;
    refused_because_invalid(
        &module,
        serde_json::json!([{"kind": "call", "verb": "whatever"}]),
    )
    .await;

    // No smuggled parameters on KNOWN kinds.
    refused_because_invalid(
        &module,
        serde_json::json!([{"kind": "set_fields", "fields": {"a": 1}, "then": "eval"}]),
    )
    .await;
    refused_because_invalid(
        &module,
        serde_json::json!([{"kind": "notify", "template": "t", "recipients": ["a@b.test"], "context": {}, "cc_record": true}]),
    )
    .await;

    // Scalars only — no nested expression objects riding as values.
    refused_because_invalid(
        &module,
        serde_json::json!([{"kind": "set_fields", "fields": {"stage": {"$expr": "anything"}}}]),
    )
    .await;
    refused_because_invalid(
        &module,
        serde_json::json!([{"kind": "set_fields", "fields": {"tags": ["a", "b"]}}]),
    )
    .await;

    // Bodies must be non-empty arrays within the action cap.
    refused_because_invalid(&module, serde_json::json!([])).await;
    refused_because_invalid(&module, serde_json::json!({"kind": "notify"})).await;

    // Trigger-shape violations.
    let mut no_pattern = draft_with(serde_json::json!([
        {"kind": "notify", "template": "t", "recipients": ["a@b.test"], "context": {}}
    ]));
    no_pattern.trigger_pattern = None;
    check(
        matches!(
            module.rule_service().create_rule(&no_pattern).await,
            Err(FoundationError::Validation(_))
        ),
        "an event rule without a pattern is refused"
    );
    let mut bad_pattern = draft_with(serde_json::json!([
        {"kind": "notify", "template": "t", "recipients": ["a@b.test"], "context": {}}
    ]));
    bad_pattern.trigger_pattern = Some("*; DROP TABLE users; --".to_string());
    check(
        matches!(
            module.rule_service().create_rule(&bad_pattern).await,
            Err(FoundationError::Validation(_))
        ),
        "non-structural pattern syntax is refused"
    );

    // Nothing was stored by any refusal.
    let n: i64 =
        sqlx::query_scalar("SELECT count(*) FROM foundation_ext.foundation_automation_rules")
            .fetch_one(&db.pool)
            .await
            .unwrap_or(0);
    check(n == 0, "no refused draft was stored");

    // The three legal actions store.
    let legal = draft_with(serde_json::json!([
        {"kind": "set_fields", "fields": {"stage": "done", "score": 3, "flag": true}},
        {"kind": "transition", "field": "state", "to": "settled"},
        {"kind": "notify", "template": "record.settled",
         "recipients": ["ops@example.test", "audit@example.test"], "context": {"why": "probe"}}
    ]));
    let rule = module
        .rule_service()
        .create_rule(&legal)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: the legal three-action body was refused: {e}"));
    check(rule.active, "a stored rule is active by default");

    // Deactivate: the engine no longer matches it (and does not even
    // claim events when nothing else is live).
    let gateway = super::common::RecordingGateway::new();
    module.install_gateway(Arc::new(gateway.clone()));
    module
        .rule_service()
        .set_active(rule.id, false)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: set_active refused: {e}"));
    let handler = module.reaction_handler().clone();
    let id = Uuid::new_v4();
    handler
        .handle(super::common::envelope(id, "sapiens.user.created", "sapiens", "u-1"))
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: delivery errored: {e}"));
    check(gateway.applied().is_empty(), "an inactive rule fires nothing");
    let claimed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM foundation_ext.inbox_consumed WHERE event_id = $1",
    )
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap_or(0);
    check(claimed == 0, "with no live rules the engine does not even claim the event");

    // Soft delete: the row retires, reads stop, history stays.
    module
        .rule_service()
        .set_active(rule.id, true)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: reactivation refused: {e}"));
    module
        .rule_service()
        .delete_rule(rule.id)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: delete_rule refused: {e}"));
    check(
        matches!(
            module.rule_service().get_rule(rule.id).await,
            Err(FoundationError::NotFound(_))
        ),
        "a soft-deleted rule no longer reads"
    );
    let still_there: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM foundation_ext.foundation_automation_rules WHERE id = $1",
    )
    .bind(rule.id)
    .fetch_one(&db.pool)
    .await
    .unwrap_or(0);
    check(still_there == 1, "soft-deleted rows stay (the audit posture)");
    check(
        module.rule_service().list_rules(None).await.unwrap_or_default().is_empty(),
        "list_rules excludes retired rules"
    );

    db.dispose().await;
}
