#!/bin/bash

# ============================================================================
# Apache Cloudberry (Incubating) - Airline Demo Runner
# ============================================================================
# This script provides multiple ways to run the airline reservations demo
# 
# Usage:
#   ./run-demo.sh [method]
# 
# Methods:
#   enhanced    - Use real-world OpenFlights data (recommended)
#   sql-only    - Self-contained SQL demo with generated data
#   csv         - Generate CSV files for manual loading
#   clean       - Clean up generated files
# ============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default database connection parameters
DB_HOST=${CLOUDBERRY_HOST:-localhost}
DB_PORT=${CLOUDBERRY_PORT:-5432}
DB_NAME=${CLOUDBERRY_DB:-airline_demo}
DB_USER=${CLOUDBERRY_USER:-postgres}

print_banner() {
    echo -e "${BLUE}"
    echo "============================================================================"
    echo "Apache Cloudberry (Incubating) - Airline Reservations Demo"
    echo "============================================================================"
    echo -e "${NC}"
    echo "Demonstrating MPP capabilities with realistic airline data"
    echo ""
}

print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "OPTIONS:"
    echo "  -m, --method METHOD    Demo method (required)"
    echo "  -s, --scale SCALE      Scale factor (optional, default: 1)"
    echo "  -y, --yes              Skip confirmation prompts (optional)"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Available methods:"
    echo "  enhanced     - Generate realistic data from OpenFlights + Faker (recommended)"
    echo "  sql-only     - Run self-contained SQL demo with embedded data generation"
    echo "  csv          - Generate CSV files for manual loading"
    echo "  clean        - Remove generated files"
    echo ""
    echo "Scale factor options:"
    echo "  1            - Standard demo (10K passengers, 1K flights, ~28K bookings)"
    echo "  5            - Medium scale (50K passengers, 5K flights, ~125K bookings)"
    echo "  25           - Large scale (250K passengers, 25K flights, ~625K bookings)"
    echo "  100          - Enterprise scale (1M passengers, 100K flights, ~2.5M bookings)"
    echo ""
    echo "Environment variables (optional):"
    echo "  CLOUDBERRY_HOST - Database host (default: localhost)"
    echo "  CLOUDBERRY_PORT - Database port (default: 5432)"
    echo "  CLOUDBERRY_DB   - Database name (default: airline_demo)"
    echo "  CLOUDBERRY_USER - Database user (default: postgres)"
    echo "  DEMO_SCALE      - Scale factor override"
    echo ""
    echo "Examples:"
    echo "  $0 --method enhanced                          # Standard demo"
    echo "  $0 -m enhanced --scale 5                      # 5x scale for performance testing"
    echo "  $0 --method enhanced --yes                    # Skip confirmation prompts"
    echo "  $0 -m enhanced -s 5 --yes                     # Automated execution with scaling"
    echo "  $0 --method sql-only -s 25 -y                 # Large SQL demo, automated"
    echo "  $0 -m csv --scale 10 --yes                    # Generate CSV at 10x scale, automated"
    echo "  DEMO_SCALE=15 $0 --method enhanced --yes      # Environment override, automated"
    echo "  CLOUDBERRY_HOST=myhost $0 -m enhanced -s 5 -y # Remote with scaling, automated"
    echo ""
}

check_dependencies() {
    local method=$1
    
    if [ "$method" = "enhanced" ]; then
        echo -e "${YELLOW}Checking Python dependencies...${NC}"
        
        if ! command -v python3 &> /dev/null; then
            echo -e "${RED}Error: python3 not found. Please install Python 3.7+${NC}"
            exit 1
        fi
        
        # Check if pip packages are available
        if ! python3 -c "import requests, pandas, faker" &> /dev/null; then
            echo -e "${YELLOW}Installing required Python packages...${NC}"
            pip3 install -r requirements.txt || {
                echo -e "${RED}Error: Failed to install Python dependencies${NC}"
                echo "Please run: pip3 install -r requirements.txt"
                exit 1
            }
        fi
        echo -e "${GREEN}✓ Python dependencies OK${NC}"
    fi
    
    # Check psql availability
    if ! command -v psql &> /dev/null; then
        echo -e "${RED}Error: psql not found. Please install PostgreSQL client tools${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ PostgreSQL client OK${NC}"
}

test_connection() {
    echo -e "${YELLOW}Testing connection to Apache Cloudberry...${NC}"
    if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT version();" &> /dev/null; then
        echo -e "${GREEN}✓ Connected to Apache Cloudberry at $DB_HOST:$DB_PORT${NC}"
    else
        echo -e "${RED}Error: Cannot connect to Apache Cloudberry${NC}"
        echo "Please check your connection parameters:"
        echo "  Host: $DB_HOST"
        echo "  Port: $DB_PORT"
        echo "  Database: $DB_NAME"
        echo "  User: $DB_USER"
        echo ""
        echo "Set environment variables if needed:"
        echo "  export CLOUDBERRY_HOST=your_host"
        echo "  export CLOUDBERRY_PORT=5432"
        echo "  export CLOUDBERRY_DB=your_database"
        echo "  export CLOUDBERRY_USER=your_user"
        exit 1
    fi
}

run_enhanced_demo() {
    local scale_factor=$1
    local auto_yes=$2
    echo -e "${GREEN}Running Enhanced Demo with OpenFlights Data (Scale: ${scale_factor}x)${NC}"
    echo "This will:"
    echo "1. Download real airport/route data from OpenFlights.org"
    echo "2. Generate synthetic passenger data with Faker"
    echo "3. Create realistic booking patterns"
    echo "4. Load data into Apache Cloudberry"
    echo ""
    echo "Data scale: $((scale_factor * 10))K passengers, $((scale_factor * 1))K flights, ~$((scale_factor * 28))K bookings"
    echo ""
    
    if [ "$auto_yes" != "true" ]; then
        read -p "Continue? [Y/n] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            echo "Demo cancelled."
            exit 0
        fi
    else
        echo "Auto-confirming: Yes"
    fi
    
    # Generate enhanced data with scale factor
    echo -e "${YELLOW}Generating realistic data at ${scale_factor}x scale...${NC}"
    python3 enhanced-data-loader.py --scale "$scale_factor"
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: Data generation failed${NC}"
        exit 1
    fi
    
    # Create schema only (no embedded data)
    echo -e "${YELLOW}Creating schema in Apache Cloudberry...${NC}"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
         -f airline-schema-only.sql \
         -v ON_ERROR_STOP=1
    
    # Clean existing data to prevent duplicates
    echo -e "${YELLOW}Cleaning existing data...${NC}"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
         -c "TRUNCATE TABLE booking, flights, passenger CASCADE;" \
         -v ON_ERROR_STOP=1
    
    if [ -f "load_passengers.sql" ] && [ -f "load_flights.sql" ] && [ -f "load_bookings.sql" ]; then
        echo -e "${YELLOW}Loading realistic data...${NC}"
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
             -f load_passengers.sql \
             -v ON_ERROR_STOP=1
        
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
             -f load_flights.sql \
             -v ON_ERROR_STOP=1
        
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
             -f load_bookings.sql \
             -v ON_ERROR_STOP=1
        
        echo -e "${GREEN}✓ Enhanced demo completed successfully!${NC}"
        echo ""
        echo "The demo is now ready. Connect to your Apache Cloudberry instance to explore:"
        echo "  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"
        echo ""
        echo "Sample queries to try:"
        echo "  SELECT COUNT(*) FROM passenger;"
        echo "  SELECT COUNT(*) FROM flights;"
        echo "  SELECT COUNT(*) FROM booking;"
    else
        echo -e "${RED}Error: Data loading files not found${NC}"
        exit 1
    fi
}

run_sql_demo() {
    local scale_factor=$1
    local auto_yes=$2
    echo -e "${GREEN}Running Self-Contained SQL Demo (Scale: ${scale_factor}x)${NC}"
    echo "This will run the complete demo with embedded data generation."
    echo ""
    echo "Data scale: $((scale_factor * 10))K passengers, ~$((scale_factor * 1))K flights"
    echo ""
    
    if [ "$auto_yes" != "true" ]; then
        read -p "Continue? [Y/n] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            echo "Demo cancelled."
            exit 0
        fi
    else
        echo "Auto-confirming: Yes"
    fi
    
    # Create a temporary SQL file with scaled parameters
    echo -e "${YELLOW}Preparing SQL demo with ${scale_factor}x scale...${NC}"
    
    # Generate scaled SQL demo
    sed "s/FROM generate_series(1, 10000)/FROM generate_series(1, $((scale_factor * 10000)))/g; \
         s/FROM generate_series(1, 750)/FROM generate_series(1, $((scale_factor * 750)))/g" \
         airline-reservations-demo.sql > airline-reservations-demo-scaled.sql
    
    echo -e "${YELLOW}Running scaled SQL demo...${NC}"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
         -f airline-reservations-demo-scaled.sql \
         -v ON_ERROR_STOP=1
    
    # Clean up temporary file
    rm -f airline-reservations-demo-scaled.sql
    
    echo -e "${GREEN}✓ SQL demo completed successfully!${NC}"
}

generate_csv() {
    local scale_factor=$1
    local auto_yes=$2
    echo -e "${GREEN}Generating CSV Files (Scale: ${scale_factor}x)${NC}"
    echo "This will create CSV files that you can load manually."
    echo ""
    echo "Data scale: $((scale_factor * 10))K passengers, ~$((scale_factor * 1))K flights"
    echo ""
    
    python3 data-generator.py --scale "$scale_factor"
    
    echo -e "${GREEN}✓ CSV files generated!${NC}"
    echo ""
    echo "To load into Apache Cloudberry:"
    echo "1. Create schema: psql -f airline-reservations-demo.sql"
    echo "2. Load data:"
    echo "   \\COPY passenger FROM 'passengers.csv' CSV HEADER;"
    echo "   \\COPY flights FROM 'flights.csv' CSV HEADER;"
    echo "   \\COPY booking FROM 'bookings.csv' CSV HEADER;"
}

clean_files() {
    echo -e "${YELLOW}Cleaning up generated files...${NC}"
    
    # Remove generated data files
    rm -f load_passengers.sql load_flights.sql load_bookings.sql
    rm -f passengers.csv flights.csv bookings.csv
    
    echo -e "${GREEN}✓ Cleanup completed${NC}"
}


main() {
    # Handle help first, before banner
    if [ $# -eq 1 ] && [[ "$1" == "help" || "$1" == "-h" || "$1" == "--help" ]]; then
        print_banner
        print_usage
        exit 0
    fi
    
    print_banner
    
    # Parse named arguments
    local method=""
    local scale_factor="${DEMO_SCALE:-1}"
    local auto_yes=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            -m|--method)
                if [ -z "$2" ] || [[ "$2" =~ ^- ]]; then
                    echo -e "${RED}Error: --method requires a value${NC}"
                    exit 1
                fi
                method="$2"
                shift 2
                ;;
            -s|--scale)
                if [ -z "$2" ] || [[ "$2" =~ ^- ]]; then
                    echo -e "${RED}Error: --scale requires a value${NC}"
                    exit 1
                fi
                scale_factor="$2"
                shift 2
                ;;
            -y|--yes)
                auto_yes=true
                shift
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                echo -e "${RED}Error: Unknown option '$1'${NC}"
                echo ""
                print_usage
                exit 1
                ;;
        esac
    done
    
    # Validate scale factor
    if ! [[ "$scale_factor" =~ ^[0-9]+$ ]] || [ "$scale_factor" -lt 1 ] || [ "$scale_factor" -gt 1000 ]; then
        echo -e "${RED}Error: Scale factor must be a number between 1 and 1000 (got: '$scale_factor')${NC}"
        echo "Recommended values: 1 (standard), 5 (medium), 25 (large), 100 (enterprise)"
        exit 1
    fi
    
    # Validate method
    if [ -z "$method" ]; then
        echo -e "${RED}Error: Method is required${NC}"
        echo ""
        print_usage
        exit 1
    fi
    
    case $method in
        "enhanced")
            check_dependencies enhanced
            test_connection
            run_enhanced_demo "$scale_factor" "$auto_yes"
            ;;
        "sql-only")
            check_dependencies sql-only
            test_connection
            run_sql_demo "$scale_factor" "$auto_yes"
            ;;
        "csv")
            check_dependencies csv
            generate_csv "$scale_factor" "$auto_yes"
            ;;
        "clean")
            clean_files
            ;;
        *)
            echo -e "${RED}Error: Unknown method '$method'${NC}"
            echo "Valid methods: enhanced, sql-only, csv, clean"
            echo ""
            print_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"