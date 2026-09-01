-- Down: drop foundation_ext.foundation_automation_runs table
DROP TABLE IF EXISTS foundation_ext.foundation_automation_runs CASCADE;
DROP FUNCTION IF EXISTS foundation_ext.foundation_automation_runs_audit_timestamp() CASCADE;
