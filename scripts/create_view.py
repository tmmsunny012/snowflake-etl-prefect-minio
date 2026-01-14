#!/usr/bin/env python
"""
Dynamic Secure View Generator CLI

Creates custom filtered secure views on PARENT_EVENTS table.

Usage:
    python scripts/create_view.py --name US_EVENTS --filter "country='US'"
    python scripts/create_view.py --name HIGH_VALUE --filter "event_metadata:amount > 100"
    python scripts/create_view.py --name RECENT --filter "event_date >= '2025-01-05'"
    python scripts/create_view.py --list  # Show all custom views
    python scripts/create_view.py --drop US_EVENTS  # Remove a view
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


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


def create_secure_view(
    view_name: str,
    filter_condition: str,
    source_table: str = "PARENT_EVENTS",
    flatten_json: bool = True,
) -> bool:
    """
    Create a secure view with custom filter.
    
    Args:
        view_name: Name for the new view (will be uppercased)
        filter_condition: SQL WHERE clause condition
        source_table: Source table (default: PARENT_EVENTS)
        flatten_json: Whether to extract common JSON fields
    
    Returns:
        True if successful
    """
    view_name = view_name.upper()
    
    # Build column list
    if flatten_json:
        columns = """
            ID,
            NAME,
            COUNTRY,
            EVENT_TYPE,
            EVENT_DATE,
            EVENT_METADATA:user_id::INTEGER AS USER_ID,
            EVENT_METADATA:session_duration::INTEGER AS SESSION_DURATION,
            EVENT_METADATA:amount::FLOAT AS AMOUNT,
            EVENT_METADATA"""
    else:
        columns = "*"
    
    # Build CREATE VIEW SQL
    sql = f"""
    CREATE OR REPLACE SECURE VIEW {view_name} AS
    SELECT {columns}
    FROM {source_table}
    WHERE {filter_condition}
    """
    
    print(f"\n>> Creating secure view: {view_name}")
    print(f"   Filter: {filter_condition}")
    print(f"   SQL:\n{sql}")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql)
        print(f"\n[OK] View '{view_name}' created successfully!")
        
        # Show sample data
        cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
        count = cursor.fetchone()[0]
        print(f"   Rows matching filter: {count}")
        
        if count > 0:
            print(f"\n   Sample data (first 3 rows):")
            cursor.execute(f"SELECT * FROM {view_name} LIMIT 3")
            for row in cursor.fetchall():
                print(f"   {row}")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Error creating view: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def list_views() -> list:
    """List all views in the current schema."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT TABLE_NAME, IS_SECURE, CREATED
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
            ORDER BY CREATED DESC
        """)
        
        views = cursor.fetchall()
        
        print("\nCurrent Views in Schema:")
        print("-" * 50)
        
        if not views:
            print("   No views found.")
        else:
            for view in views:
                secure_badge = "[S]" if view[1] == "YES" else "   "
                print(f"   {secure_badge} {view[0]:<25} (created: {view[2]})")
        
        return views
        
    finally:
        cursor.close()
        conn.close()


def drop_view(view_name: str) -> bool:
    """Drop a view."""
    view_name = view_name.upper()
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
        print(f"\n[OK] View '{view_name}' dropped successfully!")
        return True
    except Exception as e:
        print(f"\n[ERROR] Error dropping view: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def show_examples():
    """Show usage examples."""
    examples = """
+-----------------------------------------------------------------------+
|                    Dynamic View Generator - Examples                   |
+-----------------------------------------------------------------------+

>> Filter by Country:
   python scripts/create_view.py --name US_EVENTS --filter "country='US'"
   python scripts/create_view.py --name FR_EVENTS --filter "country='FR'"

>> Filter by Event Type:
   python scripts/create_view.py --name ALL_PURCHASES --filter "event_type='purchase'"
   python scripts/create_view.py --name ALL_SIGNUPS --filter "event_type='signup'"

>> Filter by Date Range:
   python scripts/create_view.py --name RECENT_EVENTS --filter "event_date >= '2025-01-05'"
   python scripts/create_view.py --name JAN_EVENTS --filter "event_date BETWEEN '2025-01-01' AND '2025-01-31'"

>> Filter by JSON Fields (VARIANT extraction):
   python scripts/create_view.py --name HIGH_VALUE --filter "event_metadata:amount > 100"
   python scripts/create_view.py --name LONG_SESSIONS --filter "event_metadata:session_duration > 30"

>> Combined Filters:
   python scripts/create_view.py --name DE_SIGNUPS --filter "country='DE' AND event_type='signup'"
   python scripts/create_view.py --name US_HIGH_VALUE --filter "country='US' AND event_metadata:amount > 50"

>> List All Views:
   python scripts/create_view.py --list

>> Drop a View:
   python scripts/create_view.py --drop US_EVENTS
"""
    print(examples)


def main():
    parser = argparse.ArgumentParser(
        description="Create custom secure views with dynamic filters",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Run with --examples for usage examples.",
    )
    
    parser.add_argument(
        "--name", "-n",
        help="Name for the new secure view (e.g., US_EVENTS)",
    )
    parser.add_argument(
        "--filter", "-f",
        help="SQL WHERE clause filter (e.g., \"country='US'\")",
    )
    parser.add_argument(
        "--table", "-t",
        default="PARENT_EVENTS",
        help="Source table (default: PARENT_EVENTS)",
    )
    parser.add_argument(
        "--no-flatten",
        action="store_true",
        help="Don't extract JSON fields (just use SELECT *)",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List all existing views",
    )
    parser.add_argument(
        "--drop", "-d",
        metavar="VIEW_NAME",
        help="Drop an existing view",
    )
    parser.add_argument(
        "--examples", "-e",
        action="store_true",
        help="Show usage examples",
    )
    
    args = parser.parse_args()
    
    # Handle different commands
    if args.examples:
        show_examples()
        return
    
    if args.list:
        list_views()
        return
    
    if args.drop:
        drop_view(args.drop)
        return
    
    # Create view requires both name and filter
    if args.name and args.filter:
        create_secure_view(
            view_name=args.name,
            filter_condition=args.filter,
            source_table=args.table,
            flatten_json=not args.no_flatten,
        )
    else:
        parser.print_help()
        print("\nTip: Run with --examples to see usage examples!")


if __name__ == "__main__":
    main()
