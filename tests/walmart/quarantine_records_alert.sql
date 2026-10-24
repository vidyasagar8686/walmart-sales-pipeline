{{ config(severity='warn') }}

-- Warn only on bad rows that nobody has fixed yet, in any feed
SELECT 'department' AS feed, file_name, file_row_number, error_reason, first_seen_at
FROM {{ ref('quarantine_department') }}
WHERE quarantine_status = 'OPEN'

UNION ALL

SELECT 'fact', file_name, file_row_number, error_reason, first_seen_at
FROM {{ ref('quarantine_fact') }}
WHERE quarantine_status = 'OPEN'
