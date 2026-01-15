-- Secure view for recent signup events
-- Filters PARENT_EVENTS to show only signup events from the last 7 days
-- Flattens the VARIANT column for easier querying

CREATE OR REPLACE SECURE VIEW STAGING_DB.PUBLIC.RECENT_SIGNUPS AS
SELECT 
    id,
    name,
    country,
    event_type,
    event_date,
    event_metadata,
    event_metadata:user_id::INT AS user_id,
    event_metadata:session_duration::INT AS session_duration
FROM STAGING_DB.PUBLIC.PARENT_EVENTS
WHERE event_type = 'signup'
  AND event_date >= DATEADD('day', -7, CURRENT_DATE());
