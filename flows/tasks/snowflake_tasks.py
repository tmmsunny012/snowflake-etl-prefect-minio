"""
Snowflake Tasks for Prefect ETL Flow

Handles all Snowflake operations:
- Connection management with QUERY_TAG
- Table creation from detected schema
- Staging table operations
- MERGE for incremental updates
- Secure view deployment
"""

import os
from contextlib import contextmanager
from typing import Generator

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from prefect import task, get_run_logger


@contextmanager
def get_snowflake_connection() -> Generator[SnowflakeConnection, None, None]:
    """
    Create Snowflake connection from environment variables.
    
    Sets QUERY_TAG for cost tracking.
    """
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ETL_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "STAGING_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    
    try:
        # Set query tag for cost tracking
        cursor = conn.cursor()
        cursor.execute("ALTER SESSION SET QUERY_TAG = 'prefect_etl_pipeline'")
        cursor.close()
        
        yield conn
    finally:
        conn.close()


@task(name="Create Parent Table")
def create_parent_table(ddl: str) -> bool:
    """
    Execute CREATE TABLE DDL for the parent table.
    
    Args:
        ddl: CREATE TABLE SQL statement
    
    Returns:
        True if successful
    """
    logger = get_run_logger()
    logger.info("Creating parent table...")
    
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(ddl)
            logger.info("Parent table created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
        finally:
            cursor.close()


@task(name="Load to Staging")
def load_to_staging(
    csv_path: str,
    staging_table: str = "STAGING_EVENTS",
    stage_name: str = "ETL_STAGE",
) -> int:
    """
    Load CSV data into a staging table via Snowflake stage.
    
    1. Upload file to internal stage
    2. Create staging table
    3. COPY INTO staging table
    
    Args:
        csv_path: Path to the CSV file
        staging_table: Name for the staging table
        stage_name: Name of the Snowflake stage
    
    Returns:
        Number of rows loaded
    """
    logger = get_run_logger()
    logger.info(f"Loading {csv_path} to staging table {staging_table}")
    
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        
        try:
            # Upload file to stage using PUT
            put_sql = f"PUT 'file://{csv_path}' @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            logger.info(f"Uploading file to stage: {put_sql}")
            cursor.execute(put_sql)
            
            # Drop and recreate staging table (clean slate)
            drop_sql = f"DROP TABLE IF EXISTS {staging_table}"
            cursor.execute(drop_sql)
            
            # Create staging table with all VARCHAR columns
            # (we'll cast properly during MERGE)
            create_staging_sql = f"""
            CREATE TABLE {staging_table} (
                ID VARCHAR,
                NAME VARCHAR,
                COUNTRY VARCHAR,
                EVENT_TYPE VARCHAR,
                EVENT_DATE VARCHAR,
                EVENT_METADATA VARCHAR
            )
            """
            cursor.execute(create_staging_sql)
            logger.info(f"Created staging table: {staging_table}")
            
            # COPY INTO staging table
            file_name = os.path.basename(csv_path)
            copy_sql = f"""
            COPY INTO {staging_table}
            FROM @{stage_name}/{file_name}.gz
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                NULL_IF = ('', 'NULL', 'null')
            )
            ON_ERROR = 'CONTINUE'
            """
            logger.info("Copying data from stage to staging table...")
            cursor.execute(copy_sql)
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {staging_table}")
            row_count = cursor.fetchone()[0]
            logger.info(f"Loaded {row_count} rows to staging table")
            
            return row_count
            
        except Exception as e:
            logger.error(f"Failed to load staging: {e}")
            raise
        finally:
            cursor.close()


@task(name="Merge to Parent")
def merge_to_parent(
    staging_table: str = "STAGING_EVENTS",
    parent_table: str = "PARENT_EVENTS",
    key_column: str = "ID",
) -> dict:
    """
    Execute MERGE statement for upsert logic.
    
    - Matched rows: UPDATE all columns
    - Unmatched rows: INSERT new records
    - JSON columns: Parse with PARSE_JSON()
    
    Args:
        staging_table: Source staging table
        parent_table: Target parent table
        key_column: Primary key column for matching
    
    Returns:
        Dictionary with insert_count and update_count
    """
    logger = get_run_logger()
    logger.info(f"Merging {staging_table} into {parent_table}")
    
    merge_sql = f"""
    MERGE INTO {parent_table} AS target
    USING (
        SELECT 
            ID::INTEGER AS ID,
            NAME::VARCHAR AS NAME,
            COUNTRY::VARCHAR AS COUNTRY,
            EVENT_TYPE::VARCHAR AS EVENT_TYPE,
            TRY_TO_DATE(EVENT_DATE) AS EVENT_DATE,
            TRY_PARSE_JSON(EVENT_METADATA) AS EVENT_METADATA
        FROM {staging_table}
    ) AS source
    ON target.{key_column} = source.{key_column}
    WHEN MATCHED THEN
        UPDATE SET 
            NAME = source.NAME,
            COUNTRY = source.COUNTRY,
            EVENT_TYPE = source.EVENT_TYPE,
            EVENT_DATE = source.EVENT_DATE,
            EVENT_METADATA = source.EVENT_METADATA
    WHEN NOT MATCHED THEN
        INSERT (ID, NAME, COUNTRY, EVENT_TYPE, EVENT_DATE, EVENT_METADATA)
        VALUES (source.ID, source.NAME, source.COUNTRY, source.EVENT_TYPE, 
                source.EVENT_DATE, source.EVENT_METADATA)
    """
    
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        
        try:
            cursor.execute(merge_sql)
            
            # Get counts from MERGE result
            result = cursor.fetchone()
            insert_count = result[0] if result else 0
            
            # Query to get actual counts
            cursor.execute(f"SELECT COUNT(*) FROM {parent_table}")
            total_count = cursor.fetchone()[0]
            
            logger.info(f"MERGE complete. Total rows in parent table: {total_count}")
            
            return {
                "merge_result": insert_count,
                "total_rows": total_count,
            }
            
        except Exception as e:
            logger.error(f"Failed to merge: {e}")
            raise
        finally:
            cursor.close()


@task(name="Create Secure Views")
def create_secure_views(views_dir: str = None) -> list[str]:
    """
    Deploy secure views from SQL files.
    
    Args:
        views_dir: Directory containing view SQL files
                   (default: snowflake/views/)
    
    Returns:
        List of created view names
    """
    logger = get_run_logger()
    
    # Default views embedded in code (can also read from files)
    views = {
        "GERMANY_EVENTS": """
        CREATE OR REPLACE SECURE VIEW GERMANY_EVENTS AS
        SELECT 
            ID,
            NAME,
            COUNTRY,
            EVENT_TYPE,
            EVENT_DATE,
            EVENT_METADATA:user_id::INTEGER AS USER_ID,
            EVENT_METADATA:session_duration::INTEGER AS SESSION_DURATION,
            EVENT_METADATA:amount::FLOAT AS AMOUNT,
            EVENT_METADATA
        FROM PARENT_EVENTS
        WHERE COUNTRY = 'DE'
        """,
        "RECENT_SIGNUPS": """
        CREATE OR REPLACE SECURE VIEW RECENT_SIGNUPS AS
        SELECT 
            ID,
            NAME,
            COUNTRY,
            EVENT_DATE,
            EVENT_METADATA:user_id::INTEGER AS USER_ID,
            EVENT_METADATA:session_duration::INTEGER AS SESSION_DURATION,
            EVENT_METADATA
        FROM PARENT_EVENTS
        WHERE EVENT_TYPE = 'signup'
        """,
    }
    
    created_views = []
    
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        
        for view_name, view_sql in views.items():
            try:
                logger.info(f"Creating secure view: {view_name}")
                cursor.execute(view_sql)
                created_views.append(view_name)
                logger.info(f"Successfully created view: {view_name}")
            except Exception as e:
                logger.error(f"Failed to create view {view_name}: {e}")
                raise
        
        cursor.close()
    
    return created_views


@task(name="Cleanup Staging")
def cleanup_staging(staging_table: str = "STAGING_EVENTS") -> bool:
    """
    Drop the staging table after successful merge.
    
    Args:
        staging_table: Name of staging table to drop
    
    Returns:
        True if successful
    """
    logger = get_run_logger()
    logger.info(f"Cleaning up staging table: {staging_table}")
    
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
            logger.info(f"Dropped staging table: {staging_table}")
            return True
        except Exception as e:
            logger.warning(f"Failed to cleanup staging: {e}")
            return False
        finally:
            cursor.close()


@task(name="Execute SQL File")
def execute_sql_file(sql_file_path: str) -> bool:
    """
    Execute a SQL file using Snowflake connection.
    
    Args:
        sql_file_path: Path to the SQL file
    
    Returns:
        True if successful
    """
    logger = get_run_logger()
    logger.info(f"Executing SQL file: {sql_file_path}")
    
    with open(sql_file_path, "r") as f:
        sql_content = f.read()
    
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        try:
            # Execute each statement (split by semicolon)
            for statement in sql_content.split(";"):
                statement = statement.strip()
                if statement and not statement.startswith("--"):
                    cursor.execute(statement)
            
            logger.info(f"Successfully executed: {sql_file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to execute SQL file: {e}")
            raise
        finally:
            cursor.close()
