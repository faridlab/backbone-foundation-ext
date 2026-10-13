-- Down: drop the declared foreign keys for foundation_ext module
ALTER TABLE IF EXISTS foundation_ext.foundation_automation_runs DROP CONSTRAINT IF EXISTS fk_foundation_automation_runs_parent_run_id;
ALTER TABLE IF EXISTS foundation_ext.foundation_automation_runs DROP CONSTRAINT IF EXISTS fk_foundation_automation_runs_automation_id;
