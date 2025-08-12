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
DB_NAME=${CLOUDBERRY_DB:-postgres}
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
    echo "Usage: $0 [method]"
    echo ""
    echo "Available methods:"
    echo "  enhanced    - Generate realistic data from OpenFlights + Faker (recommended)"
    echo "  sql-only    - Run self-contained SQL demo with embedded data generation"
    echo "  csv         - Generate CSV files for manual loading"
    echo "  clean       - Remove generated files"
    echo ""
    echo "Environment variables (optional):"
    echo "  CLOUDBERRY_HOST - Database host (default: localhost)"
    echo "  CLOUDBERRY_PORT - Database port (default: 5432)"
    echo "  CLOUDBERRY_DB   - Database name (default: postgres)"
    echo "  CLOUDBERRY_USER - Database user (default: postgres)"
    echo ""
    echo "Examples:"
    echo "  $0 enhanced                    # Best option - realistic data"
    echo "  $0 sql-only                    # Quick demo with embedded data"
    echo "  CLOUDBERRY_HOST=myhost $0 enhanced   # Connect to remote Cloudberry"
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
    echo -e "${GREEN}Running Enhanced Demo with OpenFlights Data${NC}"
    echo "This will:"
    echo "1. Download real airport/route data from OpenFlights.org"
    echo "2. Generate synthetic passenger data with Faker"
    echo "3. Create realistic booking patterns"
    echo "4. Load data into Apache Cloudberry"
    echo ""
    
    read -p "Continue? [Y/n] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Demo cancelled."
        exit 0
    fi
    
    # Generate enhanced data
    echo -e "${YELLOW}Generating realistic data...${NC}"
    python3 enhanced-data-loader.py
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: Data generation failed${NC}"
        exit 1
    fi
    
    # Create schema and load data
    echo -e "${YELLOW}Creating schema in Apache Cloudberry...${NC}"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
         -f airline-reservations-demo.sql \
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
    echo -e "${GREEN}Running Self-Contained SQL Demo${NC}"
    echo "This will run the complete demo with embedded data generation."
    echo ""
    
    read -p "Continue? [Y/n] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Demo cancelled."
        exit 0
    fi
    
    echo -e "${YELLOW}Running SQL demo...${NC}"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
         -f airline-reservations-demo.sql \
         -v ON_ERROR_STOP=1
    
    echo -e "${GREEN}✓ SQL demo completed successfully!${NC}"
}

generate_csv() {
    echo -e "${GREEN}Generating CSV Files${NC}"
    echo "This will create CSV files that you can load manually."
    echo ""
    
    python3 data-generator.py
    
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
    print_banner
    
    local method=${1:-}
    
    if [ -z "$method" ]; then
        print_usage
        exit 1
    fi
    
    case $method in
        "enhanced")
            check_dependencies enhanced
            test_connection
            run_enhanced_demo
            ;;
        "sql-only")
            check_dependencies sql-only
            test_connection
            run_sql_demo
            ;;
        "csv")
            check_dependencies csv
            generate_csv
            ;;
        "clean")
            clean_files
            ;;
        "help"|"-h"|"--help")
            print_usage
            ;;
        *)
            echo -e "${RED}Error: Unknown method '$method'${NC}"
            echo ""
            print_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"