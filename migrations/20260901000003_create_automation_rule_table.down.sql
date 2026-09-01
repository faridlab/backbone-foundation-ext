-- Down: drop foundation_ext.foundation_automation_rules table
DROP TABLE IF EXISTS foundation_ext.foundation_automation_rules CASCADE;
DROP FUNCTION IF EXISTS foundation_ext.foundation_automation_rules_audit_timestamp() CASCADE;
