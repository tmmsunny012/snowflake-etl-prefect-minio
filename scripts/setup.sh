#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════
# Snowflake ETL Pipeline - Setup Script (Linux/Mac)
# ═══════════════════════════════════════════════════════════════════════════
# Usage: ./scripts/setup.sh
#        ./scripts/setup.sh --skip-docker
#        ./scripts/setup.sh --skip-snowflake

set -e

# Parse arguments
SKIP_DOCKER=false
SKIP_SNOWFLAKE=false

for arg in "$@"; do
    case $arg in
        --skip-docker) SKIP_DOCKER=true ;;
        --skip-snowflake) SKIP_SNOWFLAKE=true ;;
    esac
done

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

success() { echo -e "${GREEN}[OK] $1${NC}"; }
info() { echo -e "${CYAN}  $1${NC}"; }
warn() { echo -e "${YELLOW}⚠ $1${NC}"; }
error() { echo -e "${RED}✗ $1${NC}"; }

echo ""
echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║       Snowflake ETL Pipeline - Setup Script                ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Get project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 0: Check Prerequisites
# ═══════════════════════════════════════════════════════════════════════════
echo -e "${YELLOW}[0/4] Checking prerequisites...${NC}"

# Check Docker (unless skipping)
if [ "$SKIP_DOCKER" = false ]; then
    if ! command -v docker &> /dev/null; then
        echo ""
        echo -e "${RED}  +-------------------------------------------------------------+${NC}"
        echo -e "${RED}  |                    DOCKER NOT FOUND                         |${NC}"
        echo -e "${RED}  +-------------------------------------------------------------+${NC}"
        echo ""
        echo -e "${YELLOW}  Docker is required for MinIO and Prefect services.${NC}"
        echo ""
        echo -e "${CYAN}  To install Docker:${NC}"
        echo "    • Mac: brew install --cask docker"
        echo "    • Ubuntu: sudo apt-get install docker.io docker-compose"
        echo "    • Or download from: https://www.docker.com/products/docker-desktop/"
        echo ""
        echo -e "${CYAN}  Alternatively, run with --skip-docker to set up Snowflake only:${NC}"
        echo "    ./scripts/setup.sh --skip-docker"
        echo ""
        exit 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        echo ""
        echo -e "${RED}  +-------------------------------------------------------------+${NC}"
        echo -e "${RED}  |               DOCKER IS NOT RUNNING                         |${NC}"
        echo -e "${RED}  +-------------------------------------------------------------+${NC}"
        echo ""
        echo -e "${YELLOW}  Please start Docker and try again.${NC}"
        echo ""
        exit 1
    fi
    
    success "Docker is installed and running"
fi

# Check Snowflake CLI
if ! command -v snow &> /dev/null; then
    echo ""
    echo -e "${RED}  +-------------------------------------------------------------+${NC}"
    echo -e "${RED}  |              SNOWFLAKE CLI NOT FOUND                        |${NC}"
    echo -e "${RED}  +-------------------------------------------------------------+${NC}"
    echo ""
    echo -e "${YELLOW}  Install with: pip install snowflake-cli-labs${NC}"
    echo ""
    exit 1
fi
success "Snowflake CLI found"

# Test Snowflake CLI connection
info "Testing Snowflake CLI connection..."
if ! snow sql -q "SELECT 1" > /dev/null 2>&1; then
    echo ""
    echo -e "${RED}  +-------------------------------------------------------------+${NC}"
    echo -e "${RED}  |          SNOWFLAKE CLI NOT CONFIGURED                       |${NC}"
    echo -e "${RED}  +-------------------------------------------------------------+${NC}"
    echo ""
    echo -e "${YELLOW}  The Snowflake CLI is installed but not configured.${NC}"
    echo ""
    echo -e "${CYAN}  Please create ~/.snowflake/config.toml with your credentials:${NC}"
    echo ""
    echo "    [connections.default]"
    echo '    account = "your_account.region"'
    echo '    user = "your_username"'
    echo '    password = "your_password"'
    echo '    warehouse = "ETL_WAREHOUSE"'
    echo '    database = "STAGING_DB"'
    echo '    role = "ACCOUNTADMIN"'
    echo ""
    echo -e "${CYAN}  Or copy the example:${NC}"
    echo "    cp snowflake/connections/config.toml.example ~/.snowflake/config.toml"
    echo ""
    exit 1
fi
success "Snowflake CLI connection verified"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: Environment File
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${YELLOW}[1/4] Checking environment configuration...${NC}"

ENV_FILE="$PROJECT_ROOT/.env"
ENV_EXAMPLE="$PROJECT_ROOT/.env.example"

if [ ! -f "$ENV_FILE" ]; then
    warn "No .env file found. Creating from template..."
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    echo ""
    echo -e "${YELLOW}  ┌─────────────────────────────────────────────────────────────┐${NC}"
    echo -e "${YELLOW}  │  IMPORTANT: Edit .env with your Snowflake credentials!     │${NC}"
    echo -e "${YELLOW}  │                                                             │${NC}"
    echo -e "${YELLOW}  │  Required values:                                           │${NC}"
    echo -e "${YELLOW}  │    - SNOWFLAKE_ACCOUNT  (e.g., abc12345.us-east-1)         │${NC}"
    echo -e "${YELLOW}  │    - SNOWFLAKE_USER     (your username)                    │${NC}"
    echo -e "${YELLOW}  │    - SNOWFLAKE_PASSWORD (your password)                    │${NC}"
    echo -e "${YELLOW}  └─────────────────────────────────────────────────────────────┘${NC}"
    echo ""
    
    # Try to open in editor
    if command -v nano &> /dev/null; then
        nano "$ENV_FILE"
    elif command -v vim &> /dev/null; then
        vim "$ENV_FILE"
    else
        echo "Please edit $ENV_FILE manually and run this script again."
        exit 1
    fi
fi

# Load .env file
info "Loading .env file..."
set -a
source "$ENV_FILE"
set +a

# Validate required variables
MISSING=""
for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD; do
    value="${!var}"
    if [ -z "$value" ] || [[ "$value" == *"your-"* ]] || [[ "$value" == *"<"* ]]; then
        MISSING="$MISSING $var"
    fi
done

if [ -n "$MISSING" ]; then
    error "Missing or invalid credentials in .env file:$MISSING"
    echo ""
    echo "Please edit .env with your actual Snowflake credentials and run again."
    exit 1
fi

success ".env file configured"
info "Account: $SNOWFLAKE_ACCOUNT"
info "User: $SNOWFLAKE_USER"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: Snowflake CLI Configuration
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${YELLOW}[2/4] Configuring Snowflake CLI...${NC}"

# Check if snow CLI is installed
if ! command -v snow &> /dev/null; then
    error "Snowflake CLI not found!"
    echo ""
    echo "  Install with: pip install snowflake-cli-labs"
    echo "  Then run this script again."
    exit 1
fi

SNOW_VERSION=$(snow --version 2>&1)
info "Found: $SNOW_VERSION"

# Create config directory and file
CONFIG_DIR="$HOME/.snowflake"
CONFIG_FILE="$CONFIG_DIR/config.toml"

mkdir -p "$CONFIG_DIR"

# Write config.toml
cat > "$CONFIG_FILE" << EOF
[default]
account = "$SNOWFLAKE_ACCOUNT"
user = "$SNOWFLAKE_USER"
password = "$SNOWFLAKE_PASSWORD"
warehouse = "${SNOWFLAKE_WAREHOUSE:-ETL_WAREHOUSE}"
database = "${SNOWFLAKE_DATABASE:-STAGING_DB}"
schema = "${SNOWFLAKE_SCHEMA:-PUBLIC}"
role = "${SNOWFLAKE_ROLE:-ACCOUNTADMIN}"
EOF

success "Snowflake CLI configured at: $CONFIG_FILE"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: Create Snowflake Infrastructure
# ═══════════════════════════════════════════════════════════════════════════
if [ "$SKIP_SNOWFLAKE" = false ]; then
    echo ""
    echo -e "${YELLOW}[3/4] Creating Snowflake infrastructure...${NC}"
    
    # Test connection
    info "Testing connection..."
    if ! snow sql -q "SELECT CURRENT_USER()" > /dev/null 2>&1; then
        error "Connection failed!"
        echo "Please check your credentials in .env and try again."
        exit 1
    fi
    success "Connection successful"
    
    # Create database
    info "Creating database STAGING_DB..."
    snow sql -q "CREATE DATABASE IF NOT EXISTS STAGING_DB" > /dev/null 2>&1
    success "Database created"
    
    # Create warehouse
    info "Creating warehouse ETL_WAREHOUSE..."
    snow sql -q "CREATE WAREHOUSE IF NOT EXISTS ETL_WAREHOUSE WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE" > /dev/null 2>&1
    success "Warehouse created"
    
    # Create stage
    info "Creating internal stage ETL_STAGE..."
    snow sql -q "USE DATABASE STAGING_DB; CREATE STAGE IF NOT EXISTS ETL_STAGE" > /dev/null 2>&1
    success "Stage created"
    
    # Verify
    echo ""
    info "Verifying infrastructure..."
    snow connection test
    
else
    echo ""
    echo -e "${YELLOW}[3/4] Skipping Snowflake infrastructure (--skip-snowflake)${NC}"
fi

# ═══════════════════════════════════════════════════════════════════════════
# STEP 4: Docker Services
# ═══════════════════════════════════════════════════════════════════════════
if [ "$SKIP_DOCKER" = false ]; then
    echo ""
    echo -e "${YELLOW}[4/4] Starting Docker services...${NC}"
    
    cd "$PROJECT_ROOT"
    info "Building and starting containers (this may take a few minutes on first run)..."
    info "Pulling images: MinIO, Prefect Server, Prefect Worker..."
    
    # Run docker-compose and suppress output
    docker-compose up -d --build > /dev/null 2>&1
    
    # Check if containers are running
    sleep 3
    if docker ps --format "{{.Names}}" 2>/dev/null | grep -q "etl-minio" && \
       docker ps --format "{{.Names}}" 2>/dev/null | grep -q "etl-prefect"; then
        success "Docker services started successfully!"
        echo ""
        echo -e "${CYAN}  Services available at:${NC}"
        echo "    • MinIO Console:  http://localhost:9001  (user: minioadmin / pass: minioadmin)"
        echo "    • Prefect UI:     http://localhost:4200"
    else
        warn "Docker containers may still be starting..."
        echo "  Check status with: docker ps"
    fi
else
    echo ""
    echo -e "${YELLOW}[4/4] Skipping Docker (--skip-docker)${NC}"
fi

# ═══════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${GREEN}+------------------------------------------------------------+${NC}"
echo -e "${GREEN}|                    Setup Complete!                         |${NC}"
echo -e "${GREEN}+------------------------------------------------------------+${NC}"
echo ""
echo -e "${YELLOW}  NEXT STEP: Run the ETL pipeline:${NC}"
echo ""
echo -e "${CYAN}    ./scripts/run_flow.sh${NC}"
echo ""
echo -e "  This will (inside Docker, no local Python required):"
echo "    1. Upload sample CSV to MinIO"
echo "    2. Detect schema (including VARIANT for JSON)"
echo "    3. Create PARENT_EVENTS table in Snowflake"
echo "    4. Load data via staging + MERGE"
echo "    5. Create secure views (GERMANY_EVENTS, RECENT_SIGNUPS)"
echo ""
echo -e "${YELLOW}  File Watcher is enabled by default:${NC}"
echo "    - Polls MinIO every 60s for new CSV files"
echo "    - Automatically triggers ETL pipeline"
echo "    - View logs: docker-compose logs -f file-watcher"
echo ""
echo -e "${YELLOW}  After running, verify with:${NC}"
echo '    snow sql -q "SELECT * FROM STAGING_DB.PUBLIC.PARENT_EVENTS"'
echo ""


