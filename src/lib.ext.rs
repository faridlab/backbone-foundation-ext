//! The module's extension (hand-written; ADR-0031): what the module adds beside its
//! generated services. The generated module and builder carry `ModuleExt` and
//! `ModuleBuilderExt` and dereference to them, so their fields read as `module.field`.

#[allow(unused_imports)]
use super::*;

/// State the module adds; read through the generated module's `Deref`.
pub struct ModuleExt {
    /// The fail-closed gateway slot the host fills at compose time (the
    /// module's ONLY reach into watched modules' records).
    pub(crate) gateway_slot: Arc<application::service::gateway_port::AutomationGatewaySlot>,
    /// The guarded rule administration service (the only rule writer).
    pub(crate) rule_service: Arc<application::service::rule_service::RuleService>,
    /// The reaction engine — register on the HOST's integration bus; this
    /// module never self-registers.
    pub(crate) reaction_handler:
        Arc<application::service::reaction_engine::AutomationReactionHandler>,
    /// The declared-posture scheduler — the host's timer loop calls
    /// `evaluate_tick`; the module owns no threads.
    pub(crate) scheduler:
        Arc<application::service::scheduler_service::AutomationScheduler>,
}

/// State the builder adds.
pub struct ModuleBuilderExt {
}

impl Default for ModuleBuilderExt {
    fn default() -> Self {
        Self {
        }
    }
}

impl ModuleBuilderExt {
    /// Build the extension's state from what the generated build made.
    #[allow(unused_variables, clippy::redundant_clone)]
    pub(crate) fn build(self, parts: &ModuleParts<'_>) -> anyhow::Result<ModuleExt> {
        let db_pool = parts.db_pool.clone();
        // Hand services share one fail-closed gateway slot: the reaction
        // engine and the scheduler both reach watched modules only through
        // it, and the host installs its adapter once at compose time.
        let gateway_slot = Arc::new(
            application::service::gateway_port::AutomationGatewaySlot::new(),
        );
        let rule_service = Arc::new(application::service::rule_service::RuleService::new(
            db_pool.clone(),
        ));
        let reaction_handler =
            Arc::new(application::service::reaction_engine::AutomationReactionHandler::new(
                db_pool.clone(),
                gateway_slot.clone(),
                application::service::reaction_engine::DEFAULT_CAUSAL_DEPTH_CAP,
            ));
        let scheduler = Arc::new(
            application::service::scheduler_service::AutomationScheduler::new(
                db_pool.clone(),
                gateway_slot.clone(),
                application::service::scheduler_service::LadderConfig::default(),
            ),
        );
        Ok(ModuleExt {
            gateway_slot,
            rule_service,
            reaction_handler,
            scheduler,
        })
    }
}

impl crate::ThisModule {
    /// The shared fail-closed gateway slot.
    pub fn gateway_slot(
        &self,
    ) -> &Arc<application::service::gateway_port::AutomationGatewaySlot> {
        &self.gateway_slot
    }
    /// Install the HOST's gateway adapter (compose time). Until this is
    /// called, every fire refuses loudly as NotComposed — fail-closed by
    /// design (the survey certification-port precedent).
    pub fn install_gateway(
        &self,
        gateway: Arc<dyn application::service::gateway_port::AutomationGateway>,
    ) {
        self.gateway_slot.install(gateway);
    }
    /// Whether a gateway adapter is installed (compose-time diagnostics:
    /// warn loudly when a module that has rules is unwired).
    pub fn gateway_is_wired(&self) -> bool {
        self.gateway_slot.is_wired()
    }
    /// The guarded rule administration service (create/replace/activate/
    /// retire/list — the only writer of automation rules; all generated
    /// route surfaces are read-only).
    pub fn rule_service(&self) -> &application::service::rule_service::RuleService {
        &self.rule_service
    }
    /// The reaction engine. The HOST registers this handler on its
    /// integration bus (`bus.subscribe(module.reaction_handler())`) and
    /// relays its own outbox onto that bus; the handler subscribes with
    /// pattern `"*"` and does its own rule-pattern matching.
    pub fn reaction_handler(
        &self,
    ) -> &Arc<application::service::reaction_engine::AutomationReactionHandler> {
        &self.reaction_handler
    }
    /// The declared-posture scheduler. The host's timer loop calls
    /// `scheduler().evaluate_tick(now)` at the interval the posture row
    /// records; with no live time rules the posture is `inactive` and a
    /// tick is a no-op.
    pub fn scheduler(
        &self,
    ) -> &Arc<application::service::scheduler_service::AutomationScheduler> {
        &self.scheduler
    }
}
