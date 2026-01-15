-- Create internal stage for file uploads
CREATE STAGE IF NOT EXISTS STAGING_DB.PUBLIC.ETL_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        NULL_IF = ('', 'NULL', 'null')
    )
    COMMENT = 'Internal stage for ETL file uploads';

-- Verify stage was created
SHOW STAGES IN STAGING_DB.PUBLIC;
