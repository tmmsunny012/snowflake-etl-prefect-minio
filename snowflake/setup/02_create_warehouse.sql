-- Create the ETL warehouse
CREATE WAREHOUSE IF NOT EXISTS ETL_WAREHOUSE
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for ETL pipeline operations';

-- Set the warehouse as default
USE WAREHOUSE ETL_WAREHOUSE;
