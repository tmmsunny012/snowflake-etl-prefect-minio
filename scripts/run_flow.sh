#!/bin/bash
#
# Run the ETL Flow via Docker
#
# This script runs the Snowflake ETL pipeline inside the Docker container.
# No local Python dependencies required!
#
# Usage:
#   ./scripts/run_flow.sh
#   ./scripts/run_flow.sh --local  # Run locally instead of Docker
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Get project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo ""
echo -e "${CYAN}============================================${NC}"
echo -e "${CYAN}   Snowflake ETL Pipeline - Run Flow       ${NC}"
echo -e "${CYAN}============================================${NC}"
echo ""

# Load .env file
if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(cat "$PROJECT_ROOT/.env" | grep -v '^#' | xargs)
    echo -e "${GREEN}[OK] Loaded environment from .env${NC}"
else
    echo -e "${RED}[ERROR] No .env file found. Run setup.sh first!${NC}"
    exit 1
fi

# Check for --local flag
if [ "$1" = "--local" ]; then
    # Run locally with Python (requires dependencies)
    echo ""
    echo -e "${YELLOW}Running locally with Python...${NC}"
    echo "(This requires: pip install -r requirements.txt)"
    echo ""
    
    cd "$PROJECT_ROOT"
    PYTHONPATH="$PROJECT_ROOT" python flows/etl_flow.py
else
    # Run via Docker (default - no local deps needed)
    echo -e "${YELLOW}Running via Docker container...${NC}"
    echo ""
    
    # Check if Docker is running
    if ! docker ps > /dev/null 2>&1; then
        echo -e "${RED}[ERROR] Docker is not running. Please start Docker.${NC}"
        exit 1
    fi
    
    # Check if containers are running
    if ! docker ps --format "{{.Names}}" 2>/dev/null | grep -q "etl-prefect-worker"; then
        echo -e "${YELLOW}Docker containers not running. Starting...${NC}"
        cd "$PROJECT_ROOT"
        docker-compose up -d > /dev/null 2>&1
        echo "Waiting for services to be healthy..."
        sleep 15
    fi
    
    echo -e "${CYAN}Executing ETL flow inside container...${NC}"
    echo ""
    
    # Execute flow in the worker container
    cd "$PROJECT_ROOT"
    docker-compose exec -T prefect-worker python /app/flows/etl_flow.py
fi

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}       Flow Execution Complete!            ${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo -e "${YELLOW}Verify results with:${NC}"
echo '  snow sql -q "SELECT * FROM STAGING_DB.PUBLIC.PARENT_EVENTS"'
echo '  snow sql -q "SELECT * FROM STAGING_DB.PUBLIC.GERMANY_EVENTS"'
echo '  snow sql -q "SELECT * FROM STAGING_DB.PUBLIC.RECENT_SIGNUPS"'
echo ""
