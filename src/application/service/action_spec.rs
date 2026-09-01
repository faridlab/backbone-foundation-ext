//! The CLOSED declarative action vocabulary
//! (hand-written; user-owned; see `metaphor.codegen.yaml`).
//!
//! An automation body is DATA, never code. This module is the vocabulary:
//! a serde enum tagged by `kind`, built with `deny_unknown_fields` and
//! WITHOUT a catch-all variant, plus per-variant semantic validation. The
//! consequences, by construction:
//!
//! - An action kind outside the vocabulary (`run_sql`, `eval`, anything)
//!   **cannot be parsed** — `parse_body` is a refusal, so it can never be
//!   stored through the guarded rule service, and a hand-corrupted row
//!   fails at fire time as a `refused_body` run row instead of executing.
//! - Unknown fields inside a known action are unparseable (no extra
//!   smuggled parameter can ride along).
//! - Field values are SCALARS only (string / number / bool / null) — an
//!   action cannot carry a nested expression object for a later eval.
//!
//! There is no `safe_eval`, no stored code, no `literal_eval`'d domain
//! TEXT anywhere in this module — the ECR-4 discipline applied to
//! automation bodies. The reaction engine interprets this enum
//! structurally (it hands each variant to the host-wired gateway); it
//! never interprets strings as code.
//!
//! THE VOCABULARY (three actions, the W7 ruling's "field sets,
//! transitions, notifications"):
//!
//! | kind        | fields                                        | effect (gateway-applied) |
//! |-------------|-----------------------------------------------|--------------------------|
//! | set_fields  | `fields: {name: scalar}`                       | write the scalars onto the watched record |
//! | transition  | `field`, `to`                                  | move the watched record's state field to `to` |
//! | notify      | `template`, `recipients: [address]`, `context` | emit a notification through the host's composed path |
//!
//! The target of `set_fields`/`transition` is ALWAYS the watched record
//! (the event's aggregate) — the only target today's thin staged payloads
//! can resolve. A deleted-event rule (`on_deleted`) may not carry either:
//! the record is gone. `notify` addresses EXPLICIT recipients only; there
//! is no mail-to-record action in the vocabulary at all, so a deleted
//! record can never be "written to".

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::foundation_error::FoundationError;

/// The maximum number of actions one rule body may carry (the write-time
/// cap; the engine re-checks it at fire time as the belt).
pub const MAX_ACTIONS_PER_RULE: usize = 10;

/// Longest allowed field/template/address lengths (vocabulary bounds — a
/// body cannot smuggle a novel data structure past them).
pub const MAX_FIELD_NAME: usize = 63;
pub const MAX_TEMPLATE: usize = 120;
pub const MAX_ADDRESS: usize = 320;

/// One declarative action. `deny_unknown_fields` + no catch-all variant =
/// the closed vocabulary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum ActionSpec {
    /// Set scalar fields on the watched record (the event's aggregate).
    /// Applied by the host-wired gateway through the owning module's
    /// validated write service — never by this module.
    SetFields {
        /// Field name -> scalar value. Names are bounded; values are
        /// scalars only.
        fields: BTreeMap<String, Value>,
    },
    /// Move the watched record's state field to a declared value (a typed
    /// field set: audited as a transition, so run ledgers read as
    /// transitions, not as anonymous writes).
    Transition {
        /// The state field to move.
        field: String,
        /// The declared target value.
        to: String,
    },
    /// Emit a notification through the host's composed notification path.
    /// `recipients` are EXPLICIT declared addresses resolved by the host
    /// adapter — there is no action that attaches a message to a record
    /// (no mail-to-record, by absence).
    Notify {
        /// The declared template key the host resolves.
        template: String,
        /// Explicit recipient addresses (never derived from the watched
        /// record — payloads are thin and the deleted-trigger ban would
        /// make that a trap).
        recipients: Vec<String>,
        /// Scalar context values handed to the template resolution.
        context: BTreeMap<String, Value>,
    },
}

impl ActionSpec {
    /// The vocabulary kind name (audit labels).
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SetFields { .. } => "set_fields",
            Self::Transition { .. } => "transition",
            Self::Notify { .. } => "notify",
        }
    }

    /// Whether this action targets the watched RECORD (field writes). A
    /// `on_deleted` rule may not carry record-targeted actions — the
    /// record is gone (the on_unlink port's no-mail-to-record ruling
    /// covers this class: nothing may write to or through the deleted
    /// record).
    pub fn targets_record(&self) -> bool {
        matches!(self, Self::SetFields { .. } | Self::Transition { .. })
    }
}

/// A scalar value (string / number / bool / null). Arrays and objects are
/// outside the vocabulary — an action body cannot carry a nested
/// expression structure.
pub fn is_scalar(value: &Value) -> bool {
    matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null)
}

/// Parse and validate a stored rule body. This is the single parse point:
/// the guarded rule service calls it at write time, and the reaction
/// engine calls it again at fire time (the belt — a hand-corrupted row
/// refuses as `refused_body`, never executes).
pub fn parse_body(actions: &Value) -> Result<Vec<ActionSpec>, FoundationError> {
    let raw = actions
        .as_array()
        .ok_or_else(|| FoundationError::Validation("rule actions must be a JSON array".to_string()))?;
    if raw.is_empty() {
        return Err(FoundationError::Validation(
            "rule actions must contain at least one action".to_string(),
        ));
    }
    if raw.len() > MAX_ACTIONS_PER_RULE {
        return Err(FoundationError::Validation(format!(
            "rule actions exceed the cap ({MAX_ACTIONS_PER_RULE})"
        )));
    }
    let mut parsed = Vec::with_capacity(raw.len());
    for (idx, entry) in raw.iter().enumerate() {
        let spec: ActionSpec = serde_json::from_value(entry.clone()).map_err(|e| {
            FoundationError::Validation(format!(
                "action #{idx} is outside the closed vocabulary \
                 (kind must be one of set_fields/transition/notify, no unknown fields): {e}"
            ))
        })?;
        validate_action(&spec).map_err(|e| FoundationError::Validation(format!("action #{idx}: {e}")))?;
        parsed.push(spec);
    }
    Ok(parsed)
}

/// Per-variant semantic validation (bounds + scalar discipline).
pub fn validate_action(spec: &ActionSpec) -> Result<(), FoundationError> {
    match spec {
        ActionSpec::SetFields { fields } => {
            if fields.is_empty() {
                return Err(FoundationError::Validation(
                    "set_fields must name at least one field".to_string(),
                ));
            }
            for (name, value) in fields {
                if name.is_empty() || name.len() > MAX_FIELD_NAME {
                    return Err(FoundationError::Validation(format!(
                        "set_fields field name out of bounds (1..={MAX_FIELD_NAME}): {name:?}"
                    )));
                }
                if !is_scalar(value) {
                    return Err(FoundationError::Validation(format!(
                        "set_fields field {name:?} carries a non-scalar value — \
                         only string/number/bool/null are in the vocabulary"
                    )));
                }
            }
            Ok(())
        }
        ActionSpec::Transition { field, to } => {
            if field.is_empty() || field.len() > MAX_FIELD_NAME {
                return Err(FoundationError::Validation(format!(
                    "transition field out of bounds (1..={MAX_FIELD_NAME}): {field:?}"
                )));
            }
            if to.is_empty() {
                return Err(FoundationError::Validation(
                    "transition target value must not be empty".to_string(),
                ));
            }
            Ok(())
        }
        ActionSpec::Notify { template, recipients, context } => {
            if template.is_empty() || template.len() > MAX_TEMPLATE {
                return Err(FoundationError::Validation(format!(
                    "notify template key out of bounds (1..={MAX_TEMPLATE}): {template:?}"
                )));
            }
            if recipients.is_empty() {
                return Err(FoundationError::Validation(
                    "notify must declare at least one explicit recipient".to_string(),
                ));
            }
            for address in recipients {
                if address.is_empty() || address.len() > MAX_ADDRESS {
                    return Err(FoundationError::Validation(format!(
                        "notify recipient out of bounds (1..={MAX_ADDRESS})"
                    )));
                }
            }
            for (key, value) in context {
                if key.len() > MAX_FIELD_NAME {
                    return Err(FoundationError::Validation(format!(
                        "notify context key out of bounds: {key:?}"
                    )));
                }
                if !is_scalar(value) {
                    return Err(FoundationError::Validation(format!(
                        "notify context {key:?} carries a non-scalar value"
                    )));
                }
            }
            Ok(())
        }
    }
}

/// Validate a trigger-pattern string (the WHEN arm's event matcher).
///
/// Pattern semantics (mirroring the integration envelope matcher):
/// exact match (`UserDeactivated`), suffix wildcard (`sapiens.*`), or the
/// global wildcard (`*`). A `*` anywhere else is a REFUSAL — the pattern
/// is matched structurally, never evaluated, but the syntax check keeps
/// stored config honest and greppable.
pub fn validate_pattern(pattern: &str) -> Result<(), FoundationError> {
    let invalid = || {
        FoundationError::Validation(format!(
            "trigger pattern {pattern:?} is not valid syntax: \
             exact event type, 'prefix.*' suffix wildcard, or '*' only"
        ))
    };
    if pattern.is_empty() || pattern.len() > 160 {
        return Err(FoundationError::Validation(
            "trigger pattern out of bounds (1..=160)".to_string(),
        ));
    }
    if pattern == "*" {
        return Ok(());
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        if prefix.is_empty() || prefix.contains('*') {
            return Err(invalid());
        }
        return Ok(());
    }
    if pattern.contains('*') {
        return Err(invalid());
    }
    Ok(())
}

/// Structural pattern match, identical in semantics to the integration
/// envelope's matcher (exact / `prefix.*` / `*`).
pub fn pattern_matches(pattern: &str, event_type: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        return event_type.starts_with(prefix);
    }
    pattern == event_type
}

/// Validate that a body is legal for its trigger kind: a `on_deleted`
/// rule (the on_unlink port) may not carry record-targeted actions — the
/// record is gone; only explicit-recipient notifications remain.
pub fn validate_body_for_trigger(kind_on_deleted: bool, actions: &[ActionSpec]) -> Result<(), FoundationError> {
    if !kind_on_deleted {
        return Ok(());
    }
    for spec in actions {
        if spec.targets_record() {
            return Err(FoundationError::Validation(format!(
                "a deleted-event rule (on_unlink port) may not carry the record-targeted \
                 action '{}' — the watched record no longer exists; only explicit-recipient \
                 notifications are in the vocabulary for this trigger",
                spec.kind()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn closed_vocabulary_rejects_unknown_kind() {
        let body = json!([{"kind": "run_sql", "sql": "DELETE FROM everything"}]);
        let err = parse_body(&body).unwrap_err();
        assert!(err.to_string().contains("closed vocabulary"), "{err}");
    }

    #[test]
    fn closed_vocabulary_rejects_unknown_fields_on_known_kind() {
        let body = json!([{"kind": "set_fields", "fields": {"stage": "done"}, "also": "eval payload"}]);
        assert!(parse_body(&body).is_err());
    }

    #[test]
    fn closed_vocabulary_rejects_non_scalar_values() {
        let body = json!([{"kind": "set_fields", "fields": {"stage": {"$expr": "1 == 1"}}}]);
        assert!(parse_body(&body).is_err());
    }

    #[test]
    fn deleted_trigger_rejects_record_targeted_actions() {
        let body = json!([
            {"kind": "notify", "template": "record.gone", "recipients": ["ops@example.test"], "context": {}}
        ]);
        let actions = parse_body(&body).unwrap();
        assert!(validate_body_for_trigger(true, &actions).is_ok());

        let body = json!([
            {"kind": "set_fields", "fields": {"archived": true}}
        ]);
        let actions = parse_body(&body).unwrap();
        assert!(validate_body_for_trigger(true, &actions).is_err());
    }

    #[test]
    fn pattern_syntax_is_structural() {
        assert!(validate_pattern("UserDeactivated").is_ok());
        assert!(validate_pattern("sapiens.*").is_ok());
        assert!(validate_pattern("*").is_ok());
        assert!(validate_pattern("*; DROP TABLE users; --").is_err());
        assert!(validate_pattern("a.*.b").is_err());
        assert!(validate_pattern("x*y").is_err());
        assert!(validate_pattern("").is_err());
    }

    #[test]
    fn pattern_match_semantics() {
        assert!(pattern_matches("UserDeactivated", "UserDeactivated"));
        assert!(!pattern_matches("UserDeactivated", "UserDeleted"));
        assert!(pattern_matches("sapiens.*", "sapiens.user.created"));
        assert!(!pattern_matches("sapiens.*", "users.user.created"));
        assert!(pattern_matches("*", "anything.at.all"));
    }

    #[test]
    fn empty_and_oversized_bodies_refuse() {
        assert!(parse_body(&json!([])).is_err());
        let big: Vec<Value> = (0..MAX_ACTIONS_PER_RULE + 1)
            .map(|_| json!({"kind": "notify", "template": "t", "recipients": ["a@b.test"], "context": {}}))
            .collect();
        assert!(parse_body(&Value::Array(big)).is_err());
        assert!(parse_body(&json!({"kind": "notify"})).is_err());
    }
}
