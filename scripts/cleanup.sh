#!/bin/bash
#
# Cleanup/Teardown Script - Remove all resources
#
# This script removes all resources created by the ETL pipeline:
# - Stops and removes Docker containers
# - Removes Docker volumes (MinIO data, Prefect data)
# - Drops Snowflake database, warehouse, and stage
#
# Use with caution! This is destructive.
#
# Usage:
#   ./scripts/cleanup.sh
#   ./scripts/cleanup.sh --skip-snowflake  # Keep Snowflake resources
#   ./scripts/cleanup.sh --skip-docker     # Keep Docker resources
#   ./scripts/cleanup.sh --force           # Skip confirmation
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

info() { echo -e "${CYAN}  $1${NC}"; }
success() { echo -e "${GREEN}[OK] $1${NC}"; }
warn() { echo -e "${YELLOW}[!] $1${NC}"; }

# Parse arguments
SKIP_DOCKER=false
SKIP_SNOWFLAKE=false
FORCE=false

for arg in "$@"; do
    case $arg in
        --skip-docker)
            SKIP_DOCKER=true
            ;;
        --skip-snowflake)
            SKIP_SNOWFLAKE=true
            ;;
        --force|-f)
            FORCE=true
            ;;
    esac
done

# Get project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo ""
echo -e "${RED}==============================================================${NC}"
echo -e "${RED}                    CLEANUP / TEARDOWN                        ${NC}"
echo -e "${RED}==============================================================${NC}"
echo ""
echo -e "${YELLOW}This will remove:${NC}"
if [ "$SKIP_DOCKER" = false ]; then
    echo "  - Docker containers (etl-minio, etl-prefect-server, etl-prefect-worker, etl-file-watcher)"
    echo "  - Docker volumes (minio-data, prefect-data)"
    echo "  - Docker images"
fi
if [ "$SKIP_SNOWFLAKE" = false ]; then
    echo "  - Snowflake database: STAGING_DB"
    echo "  - Snowflake warehouse: ETL_WAREHOUSE"
    echo "  - Snowflake stage: ETL_STAGE"
fi
echo ""

# Confirmation prompt
if [ "$FORCE" = false ]; then
    read -p "Are you sure you want to proceed? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo -e "${YELLOW}Cleanup cancelled.${NC}"
        exit 0
    fi
fi

echo ""

# ===========================================================================
# STEP 1: Stop and Remove Docker Resources
# ===========================================================================
if [ "$SKIP_DOCKER" = false ]; then
    echo -e "${YELLOW}[1/2] Cleaning up Docker resources...${NC}"
    
    cd "$PROJECT_ROOT"
    
    # Clear MinIO tracker first (if containers are running)
    info "Clearing file tracker..."
    docker-compose exec -T prefect-worker python -c "from flows.tasks.minio_tasks import get_minio_client; c = get_minio_client(); c.remove_object('etl-bucket', 'metadata/processed_files.json')" > /dev/null 2>&1 || true
    
    # Stop containers AND remove volumes
    info "Stopping containers and removing volumes..."
    docker-compose down -v > /dev/null 2>&1 || true
    
    # Remove images
    info "Removing images..."
    docker rmi snowflake-etl-prefect-minio-prefect-worker > /dev/null 2>&1 || true
    docker rmi snowflake-etl-prefect-minio-file-watcher > /dev/null 2>&1 || true
    
    success "Docker resources removed"
else
    echo -e "${YELLOW}[1/2] Skipping Docker cleanup${NC}"
fi

# ===========================================================================
# STEP 2: Drop Snowflake Resources
# ===========================================================================
if [ "$SKIP_SNOWFLAKE" = false ]; then
    echo ""
    echo -e "${YELLOW}[2/2] Cleaning up Snowflake resources...${NC}"
    
    # Check for Snowflake CLI
    if ! command -v snow &> /dev/null; then
        warn "Snowflake CLI not found. Skipping Snowflake cleanup."
        echo "  You can manually drop resources with:"
        echo "    DROP DATABASE IF EXISTS STAGING_DB;"
        echo "    DROP WAREHOUSE IF EXISTS ETL_WAREHOUSE;"
    else
        # Drop views first
        info "Dropping views..."
        snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.GERMANY_EVENTS" > /dev/null 2>&1 || true
        snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.RECENT_SIGNUPS" > /dev/null 2>&1 || true
        snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.US_EVENTS" > /dev/null 2>&1 || true
        snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.HIGH_VALUE_PURCHASES" > /dev/null 2>&1 || true
        snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.FR_SIGNUPS" > /dev/null 2>&1 || true
        snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.HIGH_VALUE" > /dev/null 2>&1 || true
        snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.CUSTOM_VIEW" > /dev/null 2>&1 || true
        
        # Drop tables
        info "Dropping tables..."
        snow sql -q "DROP TABLE IF EXISTS STAGING_DB.PUBLIC.PARENT_EVENTS" > /dev/null 2>&1 || true
        snow sql -q "DROP TABLE IF EXISTS STAGING_DB.PUBLIC.STAGING_EVENTS" > /dev/null 2>&1 || true
        
        # Drop stage
        info "Dropping stage..."
        snow sql -q "DROP STAGE IF EXISTS STAGING_DB.PUBLIC.ETL_STAGE" > /dev/null 2>&1 || true
        
        # Drop database
        info "Dropping database..."
        snow sql -q "DROP DATABASE IF EXISTS STAGING_DB" > /dev/null 2>&1 || true
        
        # Drop warehouse
        info "Dropping warehouse..."
        snow sql -q "DROP WAREHOUSE IF EXISTS ETL_WAREHOUSE" > /dev/null 2>&1 || true
        
        success "Snowflake resources removed"
    fi
else
    echo ""
    echo -e "${YELLOW}[2/2] Skipping Snowflake cleanup${NC}"
fi

# ===========================================================================
# SUMMARY
# ===========================================================================
echo ""
echo -e "${GREEN}==============================================================${NC}"
echo -e "${GREEN}                    Cleanup Complete!                         ${NC}"
echo -e "${GREEN}==============================================================${NC}"
echo ""
echo -e "${YELLOW}  To set up again from scratch, run:${NC}"
echo -e "${CYAN}    ./scripts/setup.sh${NC}"
echo ""
