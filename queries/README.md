# Apache Cloudberry Demo Queries

This directory contains ready-to-run SQL query collections that demonstrate various Apache Cloudberry MPP features. Each file focuses on a specific aspect of distributed database processing.

## Quick Start

After setting up your demo data:
```bash
# Run the demo setup first
./run-demo.sh --method enhanced

# Connect to the demo database
psql -h localhost -p 7000 -d airline_demo  # gpdemo
# OR
psql -h localhost -p 5432 -d airline_demo  # production

# Run any query file
\i queries/01-mpp-joins.sql
```

## Query Files Overview

### 📊 **01-mpp-joins.sql** - MPP Join Processing
**Purpose**: Demonstrates distributed join operations and Motion operations  
**Key Features**:
- 3-table joins with execution plan analysis
- Motion operations (Broadcast vs Redistribute)
- Parallel join processing across segments
- Distributed GROUP BY aggregations

**Sample Usage**:
```sql
\i queries/01-mpp-joins.sql
```

### 📈 **02-window-functions.sql** - Analytics & Window Functions  
**Purpose**: Advanced analytical processing with window functions  
**Key Features**:
- Passenger booking rankings and percentiles
- Route popularity analysis with RANK() and DENSE_RANK()
- Running totals and moving averages
- Complex passenger journey analysis

**Best For**: Business intelligence, customer analytics, trend analysis

### ⚡ **03-optimizer-comparison.sql** - ORCA vs PostgreSQL
**Purpose**: Compare ORCA and PostgreSQL query optimizers  
**Key Features**:
- Side-by-side execution plan comparison
- Performance timing measurements
- Complex analytical query optimization
- Optimizer recommendations and best practices

**Best For**: Performance tuning, understanding optimizer behavior

### 🔍 **04-performance-analysis.sql** - System Performance  
**Purpose**: Analyze database performance and health  
**Key Features**:
- Table size and compression analysis
- Data distribution across segments
- Column statistics quality assessment
- Query performance benchmarks
- Memory and resource usage analysis

**Best For**: Performance monitoring, capacity planning, troubleshooting

### 🧠 **05-advanced-analytics.sql** - Business Intelligence
**Purpose**: Complex analytical queries for business insights  
**Key Features**:
- Flight network and hub analysis
- Advanced passenger behavior segmentation
- Route profitability and demand analysis
- Time-based booking pattern analysis
- Multi-leg journey and connection analysis

**Best For**: Business analytics, operational insights, strategic planning

### 🎯 **06-orca-optimizer.sql** - ORCA Optimizer Control
**Purpose**: Demonstrate ORCA optimizer features and controls  
**Key Features**:
- ORCA vs PostgreSQL planner comparison
- Motion operations analysis and statistics quality
- Best practices for ORCA optimization
- Segment distribution analysis
- Configuration settings and statistics

**Best For**: Performance tuning, troubleshooting specific queries

### 🔧 **07-troubleshooting.sql** - Diagnostics & Health
**Purpose**: Diagnose issues and monitor system health  
**Key Features**:
- Segment health and configuration checks
- Statistics currency and quality analysis
- Data distribution balance assessment
- Memory and resource configuration review
- Common performance issue detection

**Best For**: System maintenance, issue diagnosis, health monitoring

## Usage Patterns

### For Learning Apache Cloudberry:
```bash
# Start with basic concepts
\i queries/01-mpp-joins.sql

# Explore analytical capabilities  
\i queries/02-window-functions.sql

# Understand optimizer behavior
\i queries/03-optimizer-comparison.sql
```

### For Performance Testing:
```bash
# Generate larger dataset first
./run-demo.sh --method enhanced --scale 10

# Analyze performance
\i queries/04-performance-analysis.sql

# Test optimization techniques
\i queries/06-orca-optimizer.sql
```

### For Business Analytics:
```bash
# Run comprehensive analytics
\i queries/05-advanced-analytics.sql

# Monitor ongoing performance
\i queries/07-troubleshooting.sql
```

### For Troubleshooting:
```bash
# Quick health check
\i queries/07-troubleshooting.sql

# Deep performance analysis
\i queries/04-performance-analysis.sql

# Optimizer comparison
\i queries/03-optimizer-comparison.sql
```

## Key Features Demonstrated

### MPP Architecture:
- **Data Distribution**: Hash distribution across segments
- **Motion Operations**: Broadcast vs Redistribute strategies
- **Parallel Processing**: Segment-parallel query execution
- **Load Balancing**: Even data distribution analysis

### Query Optimization:
- **ORCA Optimizer**: Cost-based optimization for analytical workloads
- **PostgreSQL Planner**: Comparison and use cases
- **pg_hint_plan**: Manual query optimization techniques
- **Statistics**: Importance of ANALYZE for optimal plans

### Analytical Processing:
- **Window Functions**: Advanced analytics with partitioning
- **Complex Aggregations**: Multi-level GROUP BY and ROLLUP
- **Business Intelligence**: Real-world analytical patterns
- **Performance Monitoring**: System health and optimization

### Data Management:
- **Compression**: zstd compression effectiveness
- **Storage**: Appendonly table benefits
- **Distribution**: Strategic distribution key choices
- **Maintenance**: Statistics and health monitoring

## Tips for Best Results

### Before Running Queries:
1. **Ensure fresh statistics**: `ANALYZE passenger; ANALYZE flights; ANALYZE booking;`
2. **Check ORCA is enabled**: `SHOW optimizer;` (should be 'on')
3. **Verify data is loaded**: `SELECT COUNT(*) FROM booking;`

### For Performance Testing:
1. **Use appropriate scale**: Start with scale 1, increase to 5-25 for testing
2. **Enable timing**: `\timing on` before running performance queries
3. **Clear caches**: Restart database between major tests if needed

### For Learning:
1. **Read execution plans**: Focus on Motion operations and costs
2. **Compare optimizers**: Run same query with both ORCA and PostgreSQL
3. **Experiment with hints**: Try different pg_hint_plan options

### For Production Use:
1. **Monitor regularly**: Use troubleshooting queries for health checks
2. **Update statistics**: Schedule regular ANALYZE operations
3. **Watch for skew**: Monitor data distribution balance

## Environment-Specific Notes

### For gpdemo (Development):
- Use port 7000: `psql -h localhost -p 7000 -d airline_demo`
- Smaller scale factors (1-5) work well
- Limited memory settings - adjust work_mem if needed

### For Production:
- Use port 5432: `psql -h localhost -p 5432 -d airline_demo` 
- Scale factors 10-100 for serious performance testing
- Monitor resource usage with larger datasets

## Integration with Demo Scripts

These query files complement the main demo scripts:

```bash
# Generate data at desired scale
./run-demo.sh --method enhanced --scale 5

# Run specific query collections
psql -d airline_demo -f queries/01-mpp-joins.sql
psql -d airline_demo -f queries/05-advanced-analytics.sql

# Or interactive exploration
psql -d airline_demo
\i queries/02-window-functions.sql
```

## Contributing

When adding new query files:
1. Follow the naming convention: `NN-category-name.sql`
2. Include comprehensive `\echo` statements for section headers
3. Add explanatory comments for complex queries
4. Update this README with the new file description
5. Test with multiple scale factors to ensure robustness

## Support

For questions about these queries or Apache Cloudberry:
- Review the main README.md for setup instructions
- Check CLAUDE.md for development environment guidance
- Use the troubleshooting queries for diagnostic help