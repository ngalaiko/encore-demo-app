ALTER TABLE jobs DROP CONSTRAINT jobs_mode_check;
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;

UPDATE jobs SET mode = 'normal' WHERE mode = 'ok';
UPDATE jobs SET status = 'completed' WHERE status = 'done';

ALTER TABLE jobs ADD CONSTRAINT jobs_mode_check
    CHECK (mode IN ('normal', 'slow', 'retry-once'));
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('queued', 'processing', 'completed'));
ALTER TABLE jobs RENAME COLUMN result_received TO completion_confirmed;
