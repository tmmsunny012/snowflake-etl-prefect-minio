-- Secure view for German events only
-- Filters PARENT_EVENTS to show only records where country = 'DE'
-- Flattens the VARIANT column for easier querying

CREATE OR REPLACE SECURE VIEW STAGING_DB.PUBLIC.GERMANY_EVENTS AS
SELECT 
    id,
    name,
    country,
    event_type,
    event_date,
    event_metadata,
    event_metadata:user_id::INT AS user_id,
    event_metadata:session_duration::INT AS session_duration,
    event_metadata:amount::FLOAT AS amount
FROM STAGING_DB.PUBLIC.PARENT_EVENTS
WHERE country = 'DE';
