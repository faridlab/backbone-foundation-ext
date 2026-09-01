-- Down: drop foundation_ext.foundation_scheduler_posture table
DROP TABLE IF EXISTS foundation_ext.foundation_scheduler_posture CASCADE;
DROP FUNCTION IF EXISTS foundation_ext.foundation_scheduler_posture_audit_timestamp() CASCADE;
