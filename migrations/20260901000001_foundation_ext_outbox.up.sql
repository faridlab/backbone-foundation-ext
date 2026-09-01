-- The transactional outbox + inbox dedup for this module's own schema
-- (mirrors backbone_outbox::outbox::migrate, the sapiens precedent).
--
-- OUTBOX: the engine stages one "AutomationFired" record per fired run,
-- INSIDE the same transaction as the run row and the applied actions — a
-- crash between the effect and any downstream publish can never drop the
-- fire, and no fire is emitted for a rolled-back effect (no phantom
-- fires). The host's relay drains it at-least-once onto its integration
-- bus; downstream consumers dedup via their own inbox_consumed.
--
-- INBOX: the consumer side of the same contract. The reaction engine
-- claims every delivered envelope id here, IN the fire transaction, so
-- the relay's at-least-once delivery becomes an exactly-once effect. The
-- scheduler shares the table with DETERMINISTIC uuid-v5 ids over
-- (rule, record, time-field generation) — a re-tick mints the same id
-- and dedups.
--
-- Automation config is platform-scoped (no company dimension), so the
-- NOT NULL company_id column carries the nil sentinel uuid and NO
-- row-level-security fence — the same posture as the sapiens platform
-- events. Consumers key on the event type and aggregate id, never on
-- company_id.
--
-- This file runs FIRST in the module's migration order, so the schema
-- itself is created here (the generated table migrations later in the
-- order repeat the statement idempotently).
CREATE SCHEMA IF NOT EXISTS foundation_ext;

CREATE TABLE IF NOT EXISTS foundation_ext.outbox_events (
  id             uuid PRIMARY KEY,
  event_type     text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id   text NOT NULL,
  company_id     uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
  payload        jsonb NOT NULL,
  occurred_at    timestamptz NOT NULL,
  correlation_id text,
  causation_id   text,
  version        int NOT NULL DEFAULT 1,
  created_at     timestamptz NOT NULL DEFAULT now(),
  published_at   timestamptz
);
CREATE INDEX IF NOT EXISTS idx_foundation_ext_outbox_unpublished
  ON foundation_ext.outbox_events (occurred_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS foundation_ext.inbox_consumed (
  consumer    text NOT NULL,
  event_id    uuid NOT NULL,
  consumed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
