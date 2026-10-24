-- Ingestion gate: the job's first step.
-- Fails if any pipe is not running, still has files pending,
-- or still has S3 notifications waiting in its queue.
WITH pipe_status AS (
    SELECT 'STORES_PIPE' AS pipe_name, PARSE_JSON(SYSTEM$PIPE_STATUS('WALMART.RAW.STORES_PIPE')) AS status
    UNION ALL SELECT 'DEPARTMENT_PIPE', PARSE_JSON(SYSTEM$PIPE_STATUS('WALMART.RAW.DEPARTMENT_PIPE'))
    UNION ALL SELECT 'FACT_PIPE', PARSE_JSON(SYSTEM$PIPE_STATUS('WALMART.RAW.FACT_PIPE'))
)
SELECT pipe_name,
       status:executionState::STRING AS execution_state,
       status:pendingFileCount::INT AS pending_files,
       status:numOutstandingMessagesOnChannel::INT AS queued_notifications
FROM pipe_status
WHERE status:executionState::STRING <> 'RUNNING'
   OR status:pendingFileCount::INT > 0
   OR status:numOutstandingMessagesOnChannel::INT > 0
