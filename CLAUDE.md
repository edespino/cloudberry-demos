# Apache Cloudberry (Incubating) Airline Demo - Project Context

## Project Overview
This directory contains a comprehensive demonstration of **Apache Cloudberry (Incubating)** MPP (Massively Parallel Processing) database capabilities using a realistic airline reservations system. The demo showcases advanced query processing, parallel execution, and analytical workload performance.

## Key Objectives
1. **Educational**: Teach Apache Cloudberry MPP concepts through practical examples
2. **Realistic**: Use real-world datasets (OpenFlights.org) and synthetic data (Faker)
3. **Comprehensive**: Cover all major MPP features including ORCA optimizer, Motion operations, and distributed processing
4. **Accessible**: Multiple usage methods from simple to advanced

## Architecture & Design Decisions

### MPP Table Design
- **Strategic distribution keys**: Each table distributed by its primary key for optimal parallelism
- **Appendonly storage**: Optimized for analytical workloads with high compression
- **zstd compression level 5**: Balance between compression ratio and performance
- **Row orientation**: Suitable for mixed query patterns in airline operations

### Data Generation Strategy
1. **Real-world foundation**: OpenFlights.org provides authentic airport codes, airlines, and routes
2. **Privacy compliance**: Faker generates synthetic passenger data with zero PII concerns
3. **Realistic patterns**: Hub-weighted flight frequency, business hours scheduling, traveler behavior modeling
4. **Fallback resilience**: Hardcoded data ensures demo works even without internet connectivity

### Query Demonstrations
- **Motion operations**: Show data redistribution between segments
- **Parallel aggregation**: Demonstrate distributed GROUP BY processing
- **Window functions**: Complex analytical queries across segments
- **Join optimization**: Various join strategies and performance characteristics
- **Optimizer hints**: Control ORCA behavior for educational purposes

## File Structure & Purpose

### Core Demo Files
- `airline-reservations-demo.sql` - Self-contained demo with embedded data generation
- `enhanced-data-loader.py` - Advanced data generator using OpenFlights + Faker
- `data-generator.py` - Basic CSV generator with hardcoded realistic data
- `run-demo.sh` - One-command demo runner with multiple modes

### Supporting Files
- `requirements.txt` - Python dependencies for enhanced data generation
- `README.md` - Comprehensive documentation and usage guide
- `CLAUDE.md` - This context file

## Usage Patterns

### Target Audiences
1. **Database Engineers**: Evaluating Apache Cloudberry for analytical workloads
2. **Data Architects**: Understanding MPP design patterns and optimization
3. **Educators**: Teaching distributed database concepts
4. **Sales Engineers**: Demonstrating Apache Cloudberry capabilities

### Deployment Scenarios
- **Development**: Local Cloudberry instances for learning
- **Evaluation**: Production-like environments for performance testing  
- **Training**: Classroom or workshop environments
- **Sales**: Customer demonstrations and POCs

## Technical Specifications

### Data Scale
- **Passengers**: 10,000 synthetic records
- **Flights**: 750-1,000 realistic flight schedules
- **Bookings**: ~15,000 records with intelligent booking patterns
- **Total size**: ~100MB compressed with zstd level 5

### Performance Characteristics
- **Distribution**: Even spread across Cloudberry segments
- **Query complexity**: From simple selects to complex multi-table analytical queries
- **Execution patterns**: Demonstrates both OLTP-style lookups and OLAP aggregations
- **Compression**: 60-80% space savings with appendonly + zstd

### System Requirements
- **Apache Cloudberry**: Latest stable release
- **Memory**: 4GB+ recommended for full dataset
- **Storage**: 500MB for demo files and temporary data
- **Python**: 3.7+ for enhanced data generation
- **Network**: Optional for OpenFlights data download

### Environment Setup
- **Development Environment**: Use gpdemo setup with port 7000
  - Source environment: `source ../cloudberry/gpAux/gpdemo/gpdemo-env.sh`
  - Connection parameters: localhost:7000, database=airline_demo, user=cbadmin
  - Database: Uses dedicated `airline_demo` database for clean separation
- **Production Environment**: Use standard port 5432 with custom settings

### Database Strategy
- **Dedicated Database**: Uses `airline_demo` database instead of default `postgres`
- **Benefits**: Clean separation, easy cleanup, no interference with other work
- **Setup**: Auto-created by demo script or manually with `CREATE DATABASE airline_demo;`

## Educational Value & Learning Outcomes

### MPP Concepts Covered
1. **Data Distribution**: Hash distribution strategies and segment balancing
2. **Query Processing**: ORCA optimizer behavior and execution plans
3. **Parallel Execution**: Segment-parallel operations and coordination
4. **Data Movement**: Motion operations and network optimization
5. **Storage Optimization**: Compression and appendonly table benefits

### Practical Skills Developed
- **Schema Design**: Optimal table distribution for MPP environments
- **Query Optimization**: Using EXPLAIN and optimizer hints effectively
- **Performance Analysis**: Understanding execution plans and bottlenecks
- **Data Loading**: Bulk loading strategies for analytical workloads
- **Monitoring**: System views and performance metrics

## Integration Notes

### Apache Cloudberry Compatibility
- **Explicitly branded**: All references use "Apache Cloudberry (Incubating)"
- **Feature alignment**: Uses only supported Cloudberry features
- **Version compatibility**: Works with current and future Cloudberry releases
- **Extension ready**: Easy to add Cloudberry-specific features as they're released

### External Dependencies
- **OpenFlights.org**: Public domain airport/airline/route data
- **Faker library**: Industry-standard synthetic data generation
- **No licensing issues**: All data sources are open-source or public domain
- **Privacy compliant**: Zero real PII in any generated data

## Maintenance & Updates

### Data Freshness
- OpenFlights data is relatively static (airports/routes don't change frequently)
- Flight schedules generated for current date + 30 days
- Passenger data is completely synthetic and timeless
- Easy to regenerate data for different time periods

### Code Maintenance
- **Modular design**: Each component can be updated independently  
- **Error handling**: Graceful degradation when external services unavailable
- **Documentation**: Comprehensive inline comments and external docs
- **Testing**: Multiple usage paths ensure reliability

### Future Enhancements
- **Columnar tables**: Demonstrate column-oriented storage for wide tables
- **Partitioning**: Time-based partitioning for historical data
- **External tables**: Connect to cloud storage or other data sources
- **User-defined functions**: Custom analytical functions for domain-specific queries
- **Resource management**: Workload management and query prioritization

## Success Metrics

### Demo Effectiveness
- **Completion rate**: Users successfully run demo end-to-end
- **Learning outcomes**: Understanding of MPP concepts demonstrated
- **Engagement**: Time spent exploring queries and results
- **Adoption**: Decision to evaluate/adopt Apache Cloudberry

### Technical Quality
- **Performance**: Queries execute efficiently on target hardware
- **Reliability**: Demo runs consistently across different environments  
- **Scalability**: Data generation scales to larger/smaller datasets
- **Maintainability**: Easy to update and extend

## Common Setup Issues & Solutions

### Connection Problems
1. **"Cannot connect to Apache Cloudberry"** - Check these settings:
   - For gpdemo: Use port 7000, not 5432
   - Ensure Cloudberry is running: `gpstate -s`
   - Source environment: `source ../cloudberry/gpAux/gpdemo/gpdemo-env.sh`
   - Use correct user (typically system username, e.g., cbadmin)

2. **Database not found** - demo uses dedicated 'airline_demo' database
   - Connect to: `psql -h localhost -p 7000 -d airline_demo`
   - Create if missing: `psql -d postgres -c "CREATE DATABASE airline_demo;"`

3. **Authentication failed** - Use system username, not 'postgres'
   - For gpdemo: `CLOUDBERRY_USER=cbadmin` (or current username)

### Quick Verification Commands
```bash
# Check if Cloudberry is running
source ../cloudberry/gpAux/gpdemo/gpdemo-env.sh
gpstate -s

# Test connection
psql -d postgres -c "SELECT version();"

# Create demo database
psql -d postgres -c "CREATE DATABASE airline_demo;"

# Run demo with correct settings
source ../cloudberry/gpAux/gpdemo/gpdemo-env.sh && CLOUDBERRY_PORT=7000 CLOUDBERRY_DB=airline_demo CLOUDBERRY_USER=cbadmin ./run-demo.sh enhanced
```

## ORCA Optimizer Guidance for Claude

### Validated Best Practices
**What Works with ORCA:**
1. **ANALYZE is critical** - ORCA relies heavily on accurate statistics
2. **Schema design matters** - Distribution keys, compression, storage format
3. **Query structure optimization** - Clear joins, early filtering, proper GROUP BY
4. **Trust ORCA's decisions** - Advanced cost-based optimizer usually chooses optimally

**What Doesn't Work:**
1. **Manual optimizer hints** - `enable_hashjoin=off` largely ignored by ORCA
2. **PostgreSQL-style tuning** - Traditional hint methods don't apply
3. **Forcing execution plans** - ORCA overrides manual controls with cost-based decisions

### Troubleshooting Query Performance
**Step 1: Check Statistics**
```sql
ANALYZE table_name;
SELECT last_analyze FROM pg_stat_user_tables WHERE relname = 'table_name';
```

**Step 2: Compare Optimizers**
```sql
SET optimizer = on;   -- ORCA (test first)
SET optimizer = off;  -- PostgreSQL planner (for comparison)
```

**Step 3: Analyze Execution Plans**
- Look for appropriate Motion operations (Broadcast vs Redistribute)
- Check slice allocation and memory usage
- Verify row estimates are realistic

### When to Recommend What
- **Complex analytics**: Always recommend ORCA
- **Simple OLTP queries**: Either optimizer works well
- **Performance issues**: First check ANALYZE, then schema design
- **Manual tuning**: Focus on data distribution and query structure, not optimizer hints

## Notes for Claude
- This demo showcases **defensive** database technology for analytics
- All code is educational and demonstrates legitimate MPP database concepts
- Data generation is synthetic/public domain with no privacy concerns
- Focus on helping users understand Apache Cloudberry's capabilities
- Encourage exploration of query optimization and performance tuning
- Always verify Cloudberry environment setup before troubleshooting demo issues
- **ORCA optimization**: Emphasize ANALYZE and schema design over manual controls

---

**Last Updated**: August 2025  
**Purpose**: Apache Cloudberry MPP demonstration and education  
**License**: Open source components, synthetic data, no licensing restrictions