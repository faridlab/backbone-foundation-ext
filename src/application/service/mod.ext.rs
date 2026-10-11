// This directory's extension (hand-written; ADR-0031). The generated mod.rs (lib.rs for
// src/) pulls it in with include!, so its lines resolve against this directory.

// Hand-written, user-owned services (declared in metaphor.codegen.yaml):
// the typed error surface, the closed action vocabulary, the fail-closed
// gateway port, the guarded rule administration service, the reaction
// engine (the IntegrationEventHandler the host registers on its bus),
// and the declared-posture scheduler.
pub mod foundation_error;
pub mod action_spec;
pub mod gateway_port;
pub mod rule_service;
pub mod reaction_engine;
pub mod scheduler_service;
pub use foundation_error::{FoundationError, FoundationResult};
pub use action_spec::{
    ActionSpec, MAX_ACTIONS_PER_RULE, parse_body, pattern_matches, validate_action,
    validate_body_for_trigger, validate_pattern,
};
pub use gateway_port::{
    AppliedEffect, AutomationGateway, AutomationGatewaySlot, DueRecord, FireContext,
    GatewayError, TimeWatchSpec,
};
pub use rule_service::{RuleDraft, RuleService};
pub use reaction_engine::{
    AutomationReactionHandler, CAUSATION_PREFIX, CONSUMER_REACTION, DEFAULT_CAUSAL_DEPTH_CAP,
    FIRED_EVENT_TYPE, NIL_COMPANY_ID, SCHEMA_SELF,
};
pub use scheduler_service::{
    AutomationScheduler, CONSUMER_SCHEDULER, LadderConfig, TickReport, TIME_DUE_EVENT_TYPE,
    default_ladder_interval, scheduler_event_id,
};
