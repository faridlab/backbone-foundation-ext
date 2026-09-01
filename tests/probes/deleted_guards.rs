//! The deleted-trigger guards (the on_unlink port): a `on_deleted` rule
//! carries NO record-targeted actions (the record is gone; only
//! explicit-recipient notifications remain), the fire-time re-validation
//! belt refuses a hand-corrupted body instead of executing it, and the
//! model fence skips pattern matches from other modules.

use std::sync::Arc;

use uuid::Uuid;

use backbone_foundation_ext::application::service::rule_service::RuleDraft;
use backbone_foundation_ext::domain::entity::FoundationRunStatus;
use backbone_foundation_ext::domain::entity::FoundationTriggerKind;
use backbone_foundation_ext::application::service::FoundationError;
use backbone_messaging::IntegrationEventHandler;

use super::common::{all_runs, check, count_status, envelope, module_on, TestDb};

/// The legal deleted-event rule: notify only, explicit recipients.
async fn deleted_rule() -> RuleDraft {
    RuleDraft {
        name: "flag deleted users".to_string(),
        description: None,
        model: "sapiens".to_string(),
        trigger_kind: FoundationTriggerKind::OnDeleted,
        trigger_pattern: Some("sapiens.user.deleted".to_string()),
        time_field: None,
        delay_minutes: None,
        actions: serde_json::json!([
            {"kind": "notify", "template": "user.deleted",
             "recipients": ["ops@example.test"], "context": {}}
        ]),
        created_by_source: Some("deleted-guards probe".to_string()),
    }
}

/// The WRITE-TIME ban: the guarded service refuses a deleted-event rule
/// whose body writes the (gone) record. Nothing is stored.
#[tokio::test]
async fn write_time_refuses_record_targets_on_deleted_rules() {
    let db = TestDb::new("delwrite").await;
    let module = module_on(&db.pool).await;

    let mut draft = deleted_rule().await;
    draft.actions = serde_json::json!([
        {"kind": "set_fields", "fields": {"archived": true}}
    ]);
    let refused = module.rule_service().create_rule(&draft).await;
    check(
        matches!(refused, Err(FoundationError::Validation(_))),
        "the guarded service refuses a deleted-event rule carrying set_fields"
    );

    let mut draft = deleted_rule().await;
    draft.actions = serde_json::json!([
        {"kind": "transition", "field": "state", "to": "gone"}
    ]);
    let refused = module.rule_service().create_rule(&draft).await;
    check(
        matches!(refused, Err(FoundationError::Validation(_))),
        "the guarded service refuses a deleted-event rule carrying transition"
    );

    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM foundation_ext.foundation_automation_rules")
        .fetch_one(&db.pool)
        .await
        .unwrap_or(0);
    check(n == 0, "no refused draft was stored");

    db.dispose().await;
}

/// The FIRE-TIME belt: a row hand-corrupted past the service (direct SQL)
/// refuses at fire time as a `refused_body` ledger row and applies
/// NOTHING — the vocabulary holds even when the table is lied to.
#[tokio::test]
async fn corrupted_deleted_body_refuses_at_fire_time() {
    let db = TestDb::new("delbelt").await;
    let module = module_on(&db.pool).await;
    let gateway = super::common::RecordingGateway::new();
    module.install_gateway(Arc::new(gateway.clone()));

    let rule = module
        .rule_service()
        .create_rule(&deleted_rule().await)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: legal deleted rule refused: {e}"));

    let handler = module.reaction_handler().clone();

    // The legal body fires: notify applied once.
    handler
        .handle(envelope(Uuid::new_v4(), "sapiens.user.deleted", "sapiens", "user-1"))
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: legal deleted fire errored: {e}"));
    check(count_status(&db.pool, "fired").await == 1, "the legal deleted rule fired once");
    check(gateway.applied().len() == 1, "one notify application");
    check(
        gateway.applied()[0].kind == "notify",
        "the applied action was the notification"
    );

    // Hand-corrupt the row straight in the table (past the service).
    sqlx::query("UPDATE foundation_ext.foundation_automation_rules \
                 SET actions = $1::jsonb WHERE id = $2")
        .bind(serde_json::json!([{"kind": "set_fields", "fields": {"archived": true}}]))
        .bind(rule.id)
        .execute(&db.pool)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: corruption update failed: {e}"));

    handler
        .handle(envelope(Uuid::new_v4(), "sapiens.user.deleted", "sapiens", "user-2"))
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: corrupted-body delivery errored: {e}"));
    check(
        count_status(&db.pool, "fired").await == 1,
        "the corrupted body did NOT fire"
    );
    check(
        count_status(&db.pool, "refused_body").await == 1,
        "the corrupted body produced a refused_body ledger row"
    );
    check(gateway.applied().len() == 1, "NOTHING was applied for the corrupted body");
    let runs = all_runs(&db.pool).await;
    let refused = runs
        .iter()
        .find(|r| r.status == FoundationRunStatus::RefusedBody)
        .unwrap_or_else(|| panic!("PROBE-FAIL: no refused_body row found"));
    check(
        refused.detail.as_deref().is_some_and(|d| d.contains("fire-time")),
        "the refusal row carries its reason"
    );

    db.dispose().await;
}

/// The model fence: the pattern matched, but the producing module is not
/// the watched model — a `skipped_model_mismatch` row, no application.
#[tokio::test]
async fn model_fence_skips_foreign_sources() {
    let db = TestDb::new("modelfence").await;
    let module = module_on(&db.pool).await;
    let gateway = super::common::RecordingGateway::new();
    module.install_gateway(Arc::new(gateway.clone()));

    let mut draft = deleted_rule().await;
    draft.trigger_pattern = Some("*".to_string()); // matches everything…
    module
        .rule_service()
        .create_rule(&draft)
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: rule create refused: {e}"));

    let handler = module.reaction_handler().clone();
    // …but this event comes from a module the rule does not watch.
    handler
        .handle(envelope(Uuid::new_v4(), "sapiens.user.deleted", "billing", "inv-7"))
        .await
        .unwrap_or_else(|e| panic!("PROBE-FAIL: foreign-source delivery errored: {e}"));

    check(count_status(&db.pool, "fired").await == 0, "the foreign-source event did not fire");
    check(
        count_status(&db.pool, "skipped_model_mismatch").await == 1,
        "the model fence recorded a skipped_model_mismatch row"
    );
    check(gateway.applied().is_empty(), "nothing was applied through the gateway");

    db.dispose().await;
}
