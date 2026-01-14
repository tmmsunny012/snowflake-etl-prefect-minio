"""
Schema Detection Module for Dynamic DDL Generation

Analyzes CSV data to detect column types including:
- INTEGER
- FLOAT 
- DATE
- VARCHAR
- VARIANT (for JSON columns)
"""

import json
import re
from datetime import datetime
from typing import Any

import pandas as pd
from prefect import task, get_run_logger


# Type mapping for Snowflake DDL
SNOWFLAKE_TYPE_MAP = {
    "integer": "INTEGER",
    "float": "FLOAT",
    "date": "DATE",
    "string": "VARCHAR",
    "variant": "VARIANT",
}


def is_json_string(value: Any) -> bool:
    """Check if a value is a valid JSON string (object or array)."""
    if not isinstance(value, str):
        return False
    
    value = value.strip()
    if not value:
        return False
    
    # Quick check for JSON-like structure
    if not (value.startswith("{") or value.startswith("[")):
        return False
    
    try:
        parsed = json.loads(value)
        # Must be dict or list, not primitive
        return isinstance(parsed, (dict, list))
    except (json.JSONDecodeError, TypeError):
        return False


def is_json_column(series: pd.Series) -> bool:
    """
    Determine if a column contains JSON data.
    
    Returns True if >80% of non-null values are valid JSON objects/arrays.
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    
    json_count = sum(1 for val in non_null if is_json_string(str(val)))
    return (json_count / len(non_null)) >= 0.8


def is_date_column(series: pd.Series) -> bool:
    """
    Check if a column contains date values.
    
    Supports common formats: YYYY-MM-DD, MM/DD/YYYY, etc.
    """
    date_patterns = [
        r"^\d{4}-\d{2}-\d{2}$",  # YYYY-MM-DD
        r"^\d{2}/\d{2}/\d{4}$",  # MM/DD/YYYY
        r"^\d{2}-\d{2}-\d{4}$",  # DD-MM-YYYY
    ]
    
    non_null = series.dropna().astype(str)
    if len(non_null) == 0:
        return False
    
    for pattern in date_patterns:
        matches = sum(1 for val in non_null if re.match(pattern, val.strip()))
        if (matches / len(non_null)) >= 0.8:
            return True
    
    # Try pandas datetime parsing
    try:
        pd.to_datetime(non_null, errors="raise")
        return True
    except (ValueError, TypeError):
        return False


def is_integer_column(series: pd.Series) -> bool:
    """Check if a column contains integer values."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    
    try:
        # Check if all values can be converted to int without loss
        as_float = pd.to_numeric(non_null, errors="coerce")
        if as_float.isna().any():
            return False
        return (as_float == as_float.astype(int)).all()
    except (ValueError, TypeError):
        return False


def is_float_column(series: pd.Series) -> bool:
    """Check if a column contains float/numeric values."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    
    try:
        pd.to_numeric(non_null, errors="raise")
        return True
    except (ValueError, TypeError):
        return False


def detect_column_type(series: pd.Series, column_name: str) -> str:
    """
    Detect the Snowflake data type for a pandas Series.
    
    Priority order:
    1. VARIANT (JSON) - if column contains JSON objects
    2. DATE - if column contains date strings
    3. INTEGER - if column contains whole numbers
    4. FLOAT - if column contains decimal numbers
    5. VARCHAR - fallback for everything else
    
    Args:
        series: Pandas Series to analyze
        column_name: Name of the column (for logging)
    
    Returns:
        Snowflake type string (e.g., "INTEGER", "VARIANT")
    """
    # Check JSON first  (my goal)
    if is_json_column(series):
        return "variant"
    
    # Check date
    if is_date_column(series):
        return "date"
    
    # Check integer
    if is_integer_column(series):
        return "integer"
    
    # Check float
    if is_float_column(series):
        return "float"
    
    # Default to string
    return "string"


@task(name="Detect Schema")
def detect_schema(csv_path: str) -> dict[str, str]:
    """
    Analyze a CSV file and detect Snowflake column types.
    
    Args:
        csv_path: Path to the CSV file
    
    Returns:
        Dictionary mapping column names to Snowflake types
        Example: {"id": "INTEGER", "name": "VARCHAR", "data": "VARIANT"}
    """
    logger = get_run_logger()
    logger.info(f"Detecting schema for: {csv_path}")
    
    # Read CSV with all columns as strings initially
    df = pd.read_csv(csv_path, dtype=str)
    
    schema = {}
    for column in df.columns:
        detected_type = detect_column_type(df[column], column)
        snowflake_type = SNOWFLAKE_TYPE_MAP[detected_type]
        schema[column] = snowflake_type
        logger.info(f"  Column '{column}': {snowflake_type}")
    
    return schema


@task(name="Generate DDL")
def generate_ddl(
    schema: dict[str, str],
    table_name: str,
    database: str = "STAGING_DB",
    schema_name: str = "PUBLIC",
    if_not_exists: bool = True,
) -> str:
    """
    Generate CREATE TABLE DDL from detected schema.
    
    Args:
        schema: Dictionary of column names to Snowflake types
        table_name: Name for the table
        database: Target database
        schema_name: Target schema
        if_not_exists: Add IF NOT EXISTS clause
    
    Returns:
        CREATE TABLE SQL statement
    """
    logger = get_run_logger()
    
    full_table_name = f"{database}.{schema_name}.{table_name}"
    
    # Build column definitions
    columns = []
    for col_name, col_type in schema.items():
        # Quote column names to handle special characters
        safe_name = f'"{col_name.upper()}"'
        columns.append(f"    {safe_name} {col_type}")
    
    columns_sql = ",\n".join(columns)
    
    # Build CREATE TABLE statement
    exists_clause = "IF NOT EXISTS " if if_not_exists else ""
    ddl = f"""CREATE TABLE {exists_clause}{full_table_name} (
{columns_sql}
)"""
    
    logger.info(f"Generated DDL for {full_table_name}")
    return ddl
