"""
Data Validation Tasks for ETL Pipeline

Provides data quality checks:
- Row count validation
- Sample data verification
- Schema validation
"""

import os
from typing import Any

import snowflake.connector
from prefect import task, get_run_logger


def get_connection():
    """Create Snowflake connection."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ETL_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "STAGING_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


@task(name="Validate Row Counts")
def validate_row_counts(
    expected_min_rows: int = 1,
    tables: list[str] = None,
) -> dict[str, int]:
    """
    Validate that tables have the expected minimum row counts.
    
    Args:
        expected_min_rows: Minimum expected rows per table
        tables: List of tables to check (default: PARENT_EVENTS and views)
    
    Returns:
        Dictionary of table names to row counts
    """
    logger = get_run_logger()
    
    if tables is None:
        tables = ["PARENT_EVENTS", "GERMANY_EVENTS", "RECENT_SIGNUPS"]
    
    results = {}
    errors = []
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                results[table] = count
                
                if count < expected_min_rows:
                    errors.append(
                        f"{table}: Expected at least {expected_min_rows} rows, got {count}"
                    )
                    logger.warning(f"[!] {table}: {count} rows (below minimum)")
                else:
                    logger.info(f"[OK] {table}: {count} rows")
                    
            except Exception as e:
                logger.error(f"Failed to count {table}: {e}")
                results[table] = -1
                errors.append(f"{table}: Query failed - {e}")
        
        if errors:
            logger.warning(f"Validation completed with {len(errors)} issue(s)")
        else:
            logger.info("All row count validations passed!")
            
    finally:
        cursor.close()
        conn.close()
    
    return results


@task(name="Validate Sample Data")
def validate_sample_data(
    table_name: str = "PARENT_EVENTS",
    sample_size: int = 5,
) -> list[dict[str, Any]]:
    """
    Fetch and display sample data for manual verification.
    
    Args:
        table_name: Table to sample from
        sample_size: Number of rows to return
    
    Returns:
        List of sample rows as dictionaries
    """
    logger = get_run_logger()
    logger.info(f"Fetching {sample_size} sample rows from {table_name}")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {sample_size}")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        samples = []
        for row in rows:
            sample = dict(zip(columns, row))
            samples.append(sample)
            logger.info(f"  Sample: {sample}")
        
        return samples
        
    finally:
        cursor.close()
        conn.close()


@task(name="Validate VARIANT Column")
def validate_variant_column(
    table_name: str = "PARENT_EVENTS",
    variant_column: str = "EVENT_METADATA",
) -> dict:
    """
    Validate that VARIANT column contains proper JSON data.
    
    Args:
        table_name: Table to validate
        variant_column: Name of VARIANT column
    
    Returns:
        Validation results including sample extractions
    """
    logger = get_run_logger()
    logger.info(f"Validating VARIANT column {variant_column} in {table_name}")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Check that we can extract JSON keys
        validation_sql = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT({variant_column}) as non_null_json,
            COUNT({variant_column}:user_id) as has_user_id
        FROM {table_name}
        """
        cursor.execute(validation_sql)
        result = cursor.fetchone()
        
        validation_result = {
            "table": table_name,
            "column": variant_column,
            "total_rows": result[0],
            "non_null_json": result[1],
            "has_user_id": result[2],
            "valid": result[1] > 0,
        }
        
        if validation_result["valid"]:
            logger.info(f"[OK] VARIANT column is valid. JSON extraction working.")
            logger.info(f"  Total rows: {result[0]}, With JSON: {result[1]}, With user_id: {result[2]}")
        else:
            logger.warning(f"[!] VARIANT column may have issues")
        
        return validation_result
        
    finally:
        cursor.close()
        conn.close()


@task(name="Generate Validation Report")
def generate_validation_report(
    row_counts: dict[str, int],
    sample_data: list[dict],
    variant_validation: dict,
) -> str:
    """
    Generate a human-readable validation report.
    
    Args:
        row_counts: Results from validate_row_counts
        sample_data: Results from validate_sample_data
        variant_validation: Results from validate_variant_column
    
    Returns:
        Formatted report string
    """
    logger = get_run_logger()
    
    report_lines = [
        "=" * 60,
        "ETL Pipeline Validation Report",
        "=" * 60,
        "",
                "Row Counts:",
    ]
    
    for table, count in row_counts.items():
        status = "[OK]" if count > 0 else "[X]"
        report_lines.append(f"  {status} {table}: {count} rows")
    
    report_lines.extend([
        "",
                "VARIANT Column Validation:",
        f"  Table: {variant_validation.get('table')}",
        f"  Column: {variant_validation.get('column')}",
        f"  Non-null JSON: {variant_validation.get('non_null_json')}",
        f"  Valid: {'Yes' if variant_validation.get('valid') else 'No'}",
        "",
                "Sample Data (first 3 rows):",
    ])
    
    for i, sample in enumerate(sample_data[:3], 1):
        report_lines.append(f"  Row {i}: {sample}")
    
    report_lines.extend([
        "",
        "=" * 60,
        "Validation Complete",
        "=" * 60,
    ])
    
    report = "\n".join(report_lines)
    logger.info("\n" + report)
    
    return report
