# Apache Cloudberry (Incubating) - Airline Reservations Demo

This comprehensive demo showcases **Apache Cloudberry (Incubating)** MPP capabilities using a realistic airline reservations system with three interconnected tables.

## Overview

**Apache Cloudberry (Incubating)** is a next-generation MPP (Massively Parallel Processing) database designed for analytical workloads. This demo highlights its key features:

- **MPP Architecture**: Distributed query execution across multiple segments
- **ORCA Optimizer**: Advanced cost-based query optimization
- **Motion Operations**: Automatic data redistribution for joins and aggregations
- **Parallel Execution**: Segment-parallel processing of complex analytical queries
- **Compression**: Built-in zstd compression for optimal storage efficiency

## Schema Design

### Tables with MPP Optimization

```sql
-- Passengers: 10,000 rows, distributed by passenger_id
passenger(passenger_id, first_name, last_name, email, phone)

-- Flights: 750 rows, distributed by flight_id  
flights(flight_id, flight_number, origin, destination, departure_time, arrival_time)

-- Bookings: ~15,000 rows, distributed by booking_id
booking(booking_id, passenger_id, flight_id, booking_date, seat_number)
```

All tables use:
- **appendonly=true**: Optimized for analytical workloads
- **compresstype=zstd, compresslevel=5**: High compression efficiency
- **row orientation**: Suitable for mixed analytical queries
- **Strategic distribution keys**: Balanced data distribution across segments

## Database Setup

The demo uses a dedicated `airline_demo` database to keep the demonstration isolated from your main work. This provides:

- **Clean separation** from system databases
- **Easy cleanup** - drop entire database when done
- **No interference** with other work
- **Clear demo environment** for testing and learning

## Quick Start

### 🚀 One-Command Demo (Recommended)
```bash
# Make the runner executable
chmod +x run-demo.sh

# Run enhanced demo with real-world data
./run-demo.sh enhanced

# OR run self-contained SQL demo
./run-demo.sh sql-only

# OR generate CSV files
./run-demo.sh csv

# Clean up generated files
./run-demo.sh clean
```

### Manual Methods

#### Method 1: Enhanced Realistic Data
```bash
# Install required packages
pip install -r requirements.txt

# Generate data from real-world sources
python enhanced-data-loader.py

# Connect to Apache Cloudberry and create schema
# For gpdemo: psql -h localhost -p 7000 -d airline_demo
# For production: psql -h localhost -p 5432 -d your_database
psql -h localhost -p 7000 -d airline_demo
\i airline-reservations-demo.sql

# Load the realistic data  
\i load_passengers.sql
\i load_flights.sql
\i load_bookings.sql
```

#### Method 2: Self-Contained SQL Demo
```bash
# Connect to your Apache Cloudberry instance
# For gpdemo: psql -h localhost -p 7000 -d airline_demo
# For production: psql -h localhost -p 5432 -d your_database
psql -h localhost -p 7000 -d airline_demo

# Run the complete demo with generated data
\i airline-reservations-demo.sql
```

#### Method 3: Basic CSV Generation
```bash
# Generate simple CSV files
python data-generator.py

# Load data into Cloudberry
# For gpdemo: psql -h localhost -p 7000 -d airline_demo
# For production: psql -h localhost -p 5432 -d your_database
psql -h localhost -p 7000 -d airline_demo
\COPY passenger FROM 'passengers.csv' CSV HEADER;
\COPY flights FROM 'flights.csv' CSV HEADER; 
\COPY booking FROM 'bookings.csv' CSV HEADER;
```

### Environment Configuration

#### For Cloudberry Development Environment (gpdemo)
```bash
# Source the Cloudberry demo environment
source ../cloudberry/gpAux/gpdemo/gpdemo-env.sh

# Set correct connection parameters for gpdemo
export CLOUDBERRY_HOST=localhost
export CLOUDBERRY_PORT=7000  # gpdemo uses port 7000, not 5432
export CLOUDBERRY_DB=airline_demo  # dedicated database for demo
export CLOUDBERRY_USER=cbadmin  # or your system username

# Create the database (first time only)
psql -d postgres -c "CREATE DATABASE airline_demo;"

# Then run demo
./run-demo.sh enhanced
```

#### For Production/Custom Cloudberry Installation
```bash
# Set connection parameters for your installation
export CLOUDBERRY_HOST=your-host
export CLOUDBERRY_PORT=5432
export CLOUDBERRY_DB=your-database
export CLOUDBERRY_USER=your-user

# Then run demo
./run-demo.sh enhanced
```

#### Quick Start with gpdemo
```bash
# Create database and run demo in one command
source ../cloudberry/gpAux/gpdemo/gpdemo-env.sh && psql -d postgres -c "CREATE DATABASE airline_demo;" && CLOUDBERRY_PORT=7000 CLOUDBERRY_DB=airline_demo CLOUDBERRY_USER=cbadmin ./run-demo.sh enhanced
```

## Key Demonstrations

### 1. MPP Join Processing
```sql
-- Shows Motion operations and parallel execution
EXPLAIN (COSTS OFF, VERBOSE)
SELECT b.booking_id, p.first_name || ' ' || p.last_name as passenger_name,
       f.flight_number, f.origin, f.destination, f.departure_time
FROM booking b
JOIN passenger p ON b.passenger_id = p.passenger_id  
JOIN flights f ON b.flight_id = f.flight_id
ORDER BY f.departure_time LIMIT 10;
```

**Expected Output Features:**
- **Redistribute Motion**: Shows data movement between segments
- **Broadcast Motion**: Efficient small table distribution
- **Gather Motion**: Result collection at query dispatcher

### 2. Parallel Aggregation
```sql
-- Distributed GROUP BY with segment-parallel execution
SELECT f.destination, COUNT(*) as total_bookings,
       COUNT(DISTINCT b.passenger_id) as unique_passengers
FROM flights f JOIN booking b ON f.flight_id = b.flight_id
GROUP BY f.destination
HAVING COUNT(*) > 10
ORDER BY total_bookings DESC;
```

### 3. Window Functions & Analytics
```sql
-- Complex analytical processing across segments
WITH passenger_flight_summary AS (
    SELECT p.passenger_id, COUNT(*) as flight_count
    FROM passenger p
    JOIN booking b ON p.passenger_id = b.passenger_id
    GROUP BY p.passenger_id
)
SELECT passenger_id, flight_count,
       RANK() OVER (ORDER BY flight_count DESC) as booking_rank,
       PERCENT_RANK() OVER (ORDER BY flight_count) as percentile_rank
FROM passenger_flight_summary
WHERE flight_count > 1
ORDER BY flight_count DESC;
```

### 4. Optimizer Control & Comparison
```sql
-- Compare ORCA vs PostgreSQL optimizer behavior
-- First update table statistics (CRITICAL for accurate optimization)
ANALYZE passenger;
ANALYZE flights; 
ANALYZE booking;

-- ORCA optimizer (Apache Cloudberry default)
SET optimizer = on;
EXPLAIN SELECT b.booking_id, b.booking_date, b.seat_number,
       f.origin, f.destination, f.departure_time
FROM booking b JOIN flights f ON b.flight_id = f.flight_id;

-- PostgreSQL optimizer 
SET optimizer = off;
EXPLAIN SELECT b.booking_id, b.booking_date, b.seat_number,
       f.origin, f.destination, f.departure_time
FROM booking b JOIN flights f ON b.flight_id = f.flight_id;
```

**Key Findings from Testing:**
- **ORCA**: Uses Broadcast Motion for small flights table (~720 rows), sophisticated cost-based decisions
- **PostgreSQL**: Similar strategy but different cost estimates, more responsive to manual hints
- **Both**: Accurate row estimates (30,000 final rows) ONLY after ANALYZE
- **Critical**: Without ANALYZE, ORCA shows unrealistic "rows=1" estimates leading to poor plans

**ORCA vs Manual Control:**
```sql
-- Traditional hints work with PostgreSQL planner
SET optimizer = off;
SET enable_hashjoin = off;  -- ✅ Forces Nested Loop
SET enable_nestloop = on;

-- But are largely ignored by ORCA  
SET optimizer = on;
SET enable_hashjoin = off;  -- ❌ Still uses Hash Join
-- ORCA trusts its cost model over manual overrides
```

## Apache Cloudberry MPP Features Highlighted

### Critical: Table Statistics for Optimization
```sql
-- ALWAYS run ANALYZE after loading data for accurate query optimization
ANALYZE passenger;
ANALYZE flights; 
ANALYZE booking;

-- Check current statistics
SELECT schemaname, tablename, n_distinct, correlation 
FROM pg_stats 
WHERE tablename IN ('passenger', 'flights', 'booking') 
AND attname LIKE '%_id';
```

**Impact of ANALYZE:**
- **Before ANALYZE**: ORCA shows "rows=1" estimates (inaccurate)
- **After ANALYZE**: Both optimizers show realistic estimates (30,000 rows)
- **Join Strategy**: Enables proper Motion operation selection (Broadcast vs Redistribute)

### Motion Operation Strategies
Apache Cloudberry automatically chooses optimal data movement patterns:

**Broadcast Motion** (small table to all segments):
```
Broadcast Motion 3:3  -- Sends flights (720 rows) to all segments
```
- Used when one table is significantly smaller
- Avoids redistributing large booking table (30,000 rows)
- Optimal for small dimension tables joining to large fact tables

**Redistribute Motion** (hash-based data movement):
```  
Redistribute Motion 3:3  -- Moves data by hash(join_key)
```
- Used when both tables are large
- Ensures matching join keys end up on same segment
- Required when tables have different distribution keys

### Distribution Strategy Impact
- **Hash Distribution**: Even data spread across segments via hash functions
- **Join Strategies**: ORCA automatically chooses optimal join algorithms
- **Motion Minimization**: Smart redistribution reduces network overhead

### Query Optimization
- **Cost-Based Decisions**: ORCA uses statistics for optimal execution plans
- **Parallel Execution**: Operations execute simultaneously across segments
- **Dynamic Optimization**: Runtime adaptation based on data characteristics

### Storage Efficiency
- **zstd Compression**: ~60-80% space savings with level 5 compression
- **Appendonly Tables**: Optimized for analytical insert patterns
- **Segment-Local Storage**: Data locality reduces I/O overhead

## Performance Insights

### Data Distribution Quality
```sql
-- Check distribution evenness across segments
SELECT 'passenger' as table_name, gp_segment_id, count(*) as row_count
FROM gp_dist_random('passenger')
GROUP BY gp_segment_id ORDER BY gp_segment_id;
```

### Compression Effectiveness  
```sql
-- View compressed vs uncompressed sizes
SELECT tablename,
       pg_size_pretty(pg_total_relation_size(tablename)) as compressed_size,
       pg_size_pretty(pg_relation_size(tablename)) as table_size
FROM pg_tables WHERE schemaname = 'public';
```

## Sample Query Results

### Route Analysis
```
route                  | total_bookings | unique_passengers | avg_flight_hours
-----------------------|----------------|-------------------|------------------
LAX → JFK             |             45 |                42 |             5.75
ATL → ORD             |             38 |                35 |             2.25
DFW → LAX             |             33 |                31 |             3.50
SFO → JFK             |             31 |                29 |             6.00
```

### Frequent Travelers
```
passenger_name        | flight_count | booking_rank | percentile_rank
----------------------|--------------|--------------|----------------
Sarah Johnson         |            3 |            1 |           0.998
Michael Smith         |            3 |            1 |           0.998
David Wilson          |            3 |            1 |           0.998
Lisa Anderson         |            2 |           47 |           0.953
```

## Data Sources & Realism

### Enhanced Data Loader Features
- **OpenFlights.org**: Real airport codes, airlines, and route data
- **Realistic Flight Patterns**: Hub-weighted frequency (ATL, ORD, LAX busiest)  
- **Authentic Scheduling**: Business hours weighting, realistic durations
- **Synthetic Passengers**: Faker-generated data with no privacy concerns
- **Smart Booking Patterns**: Frequent vs leisure traveler behaviors
- **Lead Time Modeling**: Realistic 1-60 day booking windows

### Data Quality Benefits
- **No Licensing Issues**: All data sources are open-source/public domain
- **Privacy Compliant**: Zero real PII in passenger data
- **Operationally Accurate**: Based on actual US domestic flight patterns
- **Scalable**: Easy to adjust volumes for different demo sizes

## Educational Value

This demo serves as a comprehensive introduction to:

1. **MPP Database Design**: Optimal table distribution strategies
2. **Query Optimization**: Understanding ORCA optimizer decisions  
3. **Parallel Processing**: How Apache Cloudberry distributes work
4. **Performance Tuning**: Using hints and analyzing execution plans
5. **Analytical Workloads**: Complex joins, aggregations, and window functions
6. **Real-World Data Handling**: Processing datasets from external sources

## System Requirements

- **Apache Cloudberry (Incubating)**: Latest stable release
- **Memory**: 4GB+ recommended for demo dataset
- **Storage**: ~100MB for compressed demo data
- **Segments**: Works with default configuration (adjust for larger deployments)

## Best Practices for Apache Cloudberry

### Essential Query Optimization Steps
```sql
-- 1. Always analyze tables after data loads
ANALYZE passenger;
ANALYZE flights;
ANALYZE booking;

-- 2. Monitor query execution plans
EXPLAIN (ANALYZE, COSTS, VERBOSE) 
SELECT * FROM booking b JOIN flights f ON b.flight_id = f.flight_id;

-- 3. Compare optimizers for complex queries
SET optimizer = on;   -- ORCA (default)
SET optimizer = off;  -- PostgreSQL planner
```

### Understanding Motion Operations
- **Broadcast Motion**: Best for small dimension tables (<1000 rows)
- **Redistribute Motion**: Required when joining tables with different distribution keys  
- **Gather Motion**: Always present - collects final results to master node

## ORCA Optimizer Best Practices

### What Works: Data-Driven Optimization
```sql
-- 1. ALWAYS update statistics - Most critical for ORCA performance
ANALYZE passenger;
ANALYZE flights; 
ANALYZE booking;

-- 2. Check statistics quality
SELECT schemaname, tablename, attname, n_distinct, correlation
FROM pg_stats 
WHERE tablename IN ('passenger', 'flights', 'booking') 
AND attname IN ('passenger_id', 'flight_id', 'booking_id');

-- 3. Monitor optimizer decisions
EXPLAIN (ANALYZE, VERBOSE) your_query_here;
```

### What Doesn't Work: Manual Optimizer Controls
**ORCA largely ignores traditional PostgreSQL optimizer hints:**
```sql
-- These have LIMITED effect with ORCA (optimizer = on)
SET enable_hashjoin = off;  -- ❌ Usually ignored
SET enable_nestloop = on;   -- ❌ Usually ignored  
SET enable_seqscan = off;   -- ❌ Usually ignored

-- ORCA trusts its cost-based decisions over manual overrides
```

### Schema Design for ORCA Optimization

#### 1. Distribution Key Selection
```sql
-- ✅ Good: Distribute by join keys
CREATE TABLE booking (...) DISTRIBUTED BY (flight_id);  -- Joins with flights
CREATE TABLE passenger (...) DISTRIBUTED BY (passenger_id);  -- Joins with booking

-- ❌ Bad: Random distribution for frequently joined tables  
CREATE TABLE booking (...) DISTRIBUTED RANDOMLY;
```

#### 2. Table Storage Optimization
```sql
-- ✅ Appendonly + compression for analytical workloads
CREATE TABLE large_fact_table (...) 
WITH (appendonly=true, compresstype=zstd, compresslevel=5)
DISTRIBUTED BY (primary_key);

-- ✅ Heap tables for frequent updates
CREATE TABLE lookup_table (...) 
WITH (appendonly=false)  
DISTRIBUTED BY (lookup_key);
```

#### 3. Index Strategy
```sql
-- ✅ Index on distribution key + filter columns
CREATE INDEX idx_passenger_name ON passenger (passenger_id, last_name);

-- ✅ Partial indexes for selective filters
CREATE INDEX idx_premium_bookings ON booking (passenger_id) 
WHERE seat_number LIKE 'A%';
```

### Query Structure for MPP Performance

#### 1. Write MPP-Friendly Queries
```sql
-- ✅ Good: Filters applied early, clear join conditions
SELECT p.first_name, p.last_name, COUNT(*) as flight_count
FROM passenger p
JOIN booking b ON p.passenger_id = b.passenger_id  -- Clear equi-join
JOIN flights f ON b.flight_id = f.flight_id        -- Clear equi-join  
WHERE f.departure_time >= CURRENT_DATE              -- Early filtering
GROUP BY p.passenger_id, p.first_name, p.last_name -- Group by distribution key
HAVING COUNT(*) > 2;                                -- Post-aggregation filter

-- ❌ Bad: Complex expressions in joins, late filtering
SELECT p.first_name, p.last_name, COUNT(*)
FROM passenger p, booking b, flights f
WHERE p.passenger_id::text = b.passenger_id::text   -- Type conversion in join
AND b.flight_id = f.flight_id
AND EXTRACT(year FROM f.departure_time) = 2025;     -- Function in WHERE
```

#### 2. Leverage ORCA's Strengths
```sql
-- ✅ Complex analytical queries - ORCA excels here
WITH passenger_stats AS (
  SELECT passenger_id, COUNT(*) as booking_count,
         AVG(EXTRACT(epoch FROM f.departure_time - b.booking_date)/86400) as avg_lead_days
  FROM booking b 
  JOIN flights f ON b.flight_id = f.flight_id
  GROUP BY passenger_id
),
route_popularity AS (
  SELECT origin, destination, COUNT(*) as route_bookings
  FROM flights f JOIN booking b ON f.flight_id = b.flight_id
  GROUP BY origin, destination  
)
SELECT p.first_name, p.last_name, ps.booking_count, ps.avg_lead_days
FROM passenger p
JOIN passenger_stats ps ON p.passenger_id = ps.passenger_id
WHERE ps.booking_count > (SELECT AVG(booking_count) FROM passenger_stats);
```

### Monitoring ORCA Performance

#### 1. Execution Plan Analysis
```sql
-- Check for optimal Motion operations
EXPLAIN (ANALYZE, COSTS OFF, VERBOSE)
SELECT f.origin, COUNT(*) 
FROM flights f JOIN booking b ON f.flight_id = b.flight_id
GROUP BY f.origin;

-- Look for:
-- ✅ Broadcast Motion for small tables (flights: 720 rows)
-- ✅ Redistribute Motion for balanced joins
-- ✅ Efficient slice allocation and memory usage
```

#### 2. Statistics Health Check  
```sql
-- Verify statistics are current
SELECT schemaname, tablename, 
       pg_stat_get_last_analyze_time(c.oid) as last_analyze
FROM pg_class c 
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_tables t ON c.relname = t.tablename AND n.nspname = t.schemaname
WHERE schemaname = 'public'
ORDER BY last_analyze DESC;
```

#### 3. Query Performance Comparison
```sql
-- Compare ORCA vs PostgreSQL planner for complex queries
SET optimizer = on;   -- ORCA
EXPLAIN (ANALYZE) your_complex_query;

SET optimizer = off;  -- PostgreSQL planner  
EXPLAIN (ANALYZE) your_complex_query;

-- ORCA typically wins on:
-- • Complex joins (3+ tables)
-- • Advanced analytics (window functions, CTEs)
-- • Large data aggregations
```

### Advanced MPP Query Demonstrations

#### 1. Multi-Level Aggregation with GROUPING SETS
```sql
-- Shows complex cross-segment aggregation at multiple granularities
EXPLAIN (ANALYZE, COSTS OFF)
SELECT 
    COALESCE(f.origin, 'ALL ORIGINS') as origin,
    COALESCE(f.destination, 'ALL DESTINATIONS') as destination,
    COUNT(*) as bookings,
    AVG(EXTRACT(epoch FROM (f.arrival_time - f.departure_time))/3600) as avg_flight_hours
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id
GROUP BY GROUPING SETS ((f.origin, f.destination), (f.origin), ())
HAVING COUNT(*) > 100
ORDER BY bookings DESC
LIMIT 10;
```

**Key MPP Features Demonstrated:**
- **Multiple Redistribute Motions**: Data reshuffled for different aggregation levels
- **Shared Scans**: Same data read once, used for multiple aggregations
- **Parallel GROUP BY**: Each segment processes partial aggregations
- **Finalize Aggregate**: Master node combines segment results

#### 2. Complex Window Functions with Multiple Partitions
```sql
-- Shows cross-segment window function processing with multiple redistribute operations
EXPLAIN (ANALYZE, COSTS OFF)
SELECT 
    p.passenger_id,
    p.first_name || ' ' || p.last_name as passenger_name,
    COUNT(*) OVER (PARTITION BY p.passenger_id) as total_flights,
    ROW_NUMBER() OVER (PARTITION BY f.origin ORDER BY f.departure_time) as departure_sequence,
    LAG(f.destination) OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as previous_destination
FROM passenger p
JOIN booking b ON p.passenger_id = b.passenger_id
JOIN flights f ON b.flight_id = f.flight_id
ORDER BY p.passenger_id, f.departure_time
LIMIT 20;
```

**Key MPP Features Demonstrated:**
- **Multiple Window Functions**: Each requiring different data partitioning
- **Cascade of Redistribute Motions**: Data moved multiple times for different partitions
- **Segment-Parallel Sorting**: Each segment sorts its portion independently
- **Memory Management**: Work_mem allocation across multiple operations

#### 3. Global Ranking Across All Segments
```sql
-- Demonstrates global ordering and ranking across distributed data
EXPLAIN (ANALYZE, COSTS OFF)
SELECT origin, destination, flight_count, 
       DENSE_RANK() OVER (ORDER BY flight_count DESC) as popularity_rank,
       PERCENT_RANK() OVER (ORDER BY flight_count) as percentile
FROM (
    SELECT f.origin, f.destination, COUNT(*) as flight_count
    FROM flights f
    JOIN booking b ON f.flight_id = b.flight_id  
    GROUP BY f.origin, f.destination
    HAVING COUNT(*) > 50
) route_stats
ORDER BY popularity_rank, flight_count DESC
LIMIT 20;
```

**Key MPP Features Demonstrated:**
- **Global Sorting**: Gather Motion with merge keys for distributed sorting
- **Window Functions on Aggregates**: Ranking calculated after cross-segment aggregation
- **Streaming Partial Aggregates**: Segments compute partial results, master finalizes

### Performance Monitoring
```sql
-- Check data distribution across segments  
SELECT gp_segment_id, count(*) 
FROM gp_dist_random('booking') 
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

-- View table compression effectiveness
SELECT schemaname, tablename,
       pg_size_pretty(pg_total_relation_size(tablename)) as total_size,
       pg_size_pretty(pg_relation_size(tablename)) as table_size
FROM pg_tables WHERE schemaname = 'public';

-- Monitor slice execution and memory usage
EXPLAIN (ANALYZE, VERBOSE) 
SELECT f.origin, COUNT(*) 
FROM flights f 
JOIN booking b ON f.flight_id = b.flight_id 
GROUP BY f.origin 
ORDER BY COUNT(*) DESC;
```

## Advanced Query Optimization with pg_hint_plan

### pg_hint_plan Extension Support (✅ Confirmed Working)

Apache Cloudberry includes **pg_hint_plan v1.3.9** extension support for fine-grained query optimization control:

**Version Details:**
- **pg_hint_plan**: 1.3.9 (PostgreSQL 14 compatible)
- **Apache Cloudberry**: 2.0.0-incubating 
- **PostgreSQL Base**: 14.4
- **Installation**: Pre-installed and configured in shared_preload_libraries

#### Key Features Verified:
- **Scan Method Hints**: `SeqScan`, `IndexScan`, `BitmapScan` - ✅ Working
- **Join Method Hints**: `NestLoop`, `HashJoin`, `MergeJoin` - ✅ Working  
- **Both Optimizers**: Works with ORCA and PostgreSQL planner - ✅ Working
- **Error Handling**: Clear syntax error messages - ✅ Working
- **Debug Output**: Available via `pg_hint_plan.debug_print` - ✅ Working

#### Basic Usage Examples (CRITICAL: Single-line format required):
```sql
-- ✅ CORRECT: Single-line format
/*+ SeqScan(passenger) */ SELECT * FROM passenger WHERE passenger_id = 1000;

-- ✅ CORRECT: Join hints  
/*+ NestLoop(b f) */ SELECT * FROM booking b JOIN flights f ON b.flight_id = f.flight_id;

-- ❌ INCORRECT: Multi-line format fails in psql -c
/*+ SeqScan(passenger) */ 
SELECT * FROM passenger WHERE passenger_id = 1000;
```

#### Advanced Usage Patterns:

**Force Specific Scan Methods:**
```sql
-- Force sequential scan on flights table
/*+ SeqScan(f) */ 
SELECT * FROM booking b JOIN flights f ON b.flight_id = f.flight_id;

-- Force index scan (when available and beneficial)
/*+ IndexScan(f) */ 
SELECT * FROM flights f WHERE flight_id = 100;
```

**Control Join Algorithms:**
```sql
-- Force Hash Join (good for large datasets)
/*+ HashJoin(b f) */ 
SELECT * FROM booking b JOIN flights f ON b.flight_id = f.flight_id;

-- Force Merge Join (when inputs can be sorted)  
/*+ MergeJoin(b f) */ 
SELECT * FROM booking b JOIN flights f ON b.flight_id = f.flight_id;
```

**Steer Join Order (Critical for Multi-table BI Queries):**
```sql
-- Force booking-flights join first, then passenger
/*+ Leading((b f)) */ 
SELECT b.booking_id, f.flight_number, p.first_name 
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id 
JOIN passenger p ON b.passenger_id = p.passenger_id;

-- Natural order vs forced order comparison shows dramatic plan differences
```

**Subquery-Specific Optimization:**
```sql
-- Apply hints to specific parts of complex queries
/*+ NestLoop(b f) */ 
SELECT * FROM (
  SELECT * FROM booking b 
  WHERE booking_date >= CURRENT_DATE - INTERVAL '30 days'
) recent 
JOIN flights f ON recent.flight_id = f.flight_id;
```

#### Comprehensive Demonstration Commands:
```bash
# Test scan method hints
psql -d airline_demo -c "/*+ SeqScan(passenger) */ EXPLAIN SELECT * FROM passenger WHERE passenger_id = 1000;"

# Test join method hints  
psql -d airline_demo -c "/*+ NestLoop(b f) */ EXPLAIN SELECT b.booking_id, f.flight_number FROM booking b JOIN flights f ON b.flight_id = f.flight_id LIMIT 5;"

# Test join order control (Leading hint)
psql -d airline_demo -c "/*+ Leading((b f)) */ EXPLAIN (COSTS OFF) SELECT b.booking_id, f.flight_number, p.first_name FROM booking b JOIN flights f ON b.flight_id = f.flight_id JOIN passenger p ON b.passenger_id = p.passenger_id LIMIT 5;"

# Test subquery optimization  
psql -d airline_demo -c "/*+ NestLoop(b f) */ EXPLAIN (COSTS OFF) SELECT * FROM (SELECT * FROM booking b WHERE booking_date >= CURRENT_DATE - INTERVAL '30 days') recent JOIN flights f ON recent.flight_id = f.flight_id LIMIT 5;"

# Compare optimizers with same hint
psql -d airline_demo -c "SET optimizer = on; /*+ HashJoin(b f) */ EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id;"
psql -d airline_demo -c "SET optimizer = off; /*+ HashJoin(b f) */ EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id;"

# Test error handling
psql -d airline_demo -c "/*+ InvalidHint(b f) */ EXPLAIN SELECT * FROM booking b JOIN flights f ON b.flight_id = f.flight_id;" 2>&1
```

#### Configuration Parameters:
```sql
-- Core settings (current values in Apache Cloudberry)
SHOW pg_hint_plan.enable_hint;                -- on (hints are active)
SHOW pg_hint_plan.debug_print;                -- off (can be enabled for debugging)
SHOW pg_hint_plan.enable_hint_table;          -- off (hint table feature disabled)
SHOW pg_hint_plan.message_level;              -- log (debug message level)  
SHOW pg_hint_plan.parse_messages;             -- info (parse error message level)

-- Enable debug output (shows hint processing in server logs)
SET pg_hint_plan.debug_print = on;

-- Verify extension is loaded and check version
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_hint_plan';
-- Returns: pg_hint_plan | 1.3.9
```

#### Key Findings from Testing:

**✅ What Works Exceptionally Well:**
- **Scan Method Control**: SeqScan, IndexScan hints work reliably
- **Join Algorithm Control**: HashJoin, NestLoop, MergeJoin hints effective
- **Join Order Control**: Leading hints provide powerful multi-table query optimization
- **Cross-Optimizer Support**: Same hints work with both ORCA and PostgreSQL planner
- **Subquery Scope**: Hints can target specific parts of complex nested queries
- **Error Reporting**: Clear, helpful syntax error messages

**❌ Limitations Discovered:**
- **Execution Format**: MUST use single-line format for psql -c commands
- **Rows Hint**: Row count estimation hints may use different syntax in v1.3.9
- **Hint Table**: Hint table feature disabled by default (enable_hint_table = off)

**🎯 Best Use Cases:**
- **Complex BI Queries**: Multi-table joins where join order matters significantly
- **Performance Troubleshooting**: Force different execution strategies for comparison  
- **Workload Tuning**: Override optimizer decisions for specific query patterns
- **ETL Optimization**: Control resource usage in data processing pipelines

**Performance Impact**: pg_hint_plan provides surgical control over execution plans, especially useful for:
- Complex analytical queries where ORCA needs guidance
- Workload-specific optimizations  
- Performance troubleshooting and plan comparison
- Critical production queries requiring consistent execution plans

## Next Steps

After running this demo, explore:
- **pg_hint_plan**: Advanced query optimization with surgical hint control
- **Columnar Storage**: Try `orientation=column` for wider analytical tables
- **Partitioning**: Implement date-based partitioning for time-series data  
- **External Tables**: Connect to external data sources
- **User-Defined Functions**: Create custom analytical functions
- **Resource Management**: Configure resource queues for workload management

## Resources

- **Apache Cloudberry Website**: https://cloudberry.apache.org/
- **Documentation**: https://cloudberry.apache.org/docs/
- **GitHub Repository**: https://github.com/apache/cloudberry
- **Community**: https://cloudberry.apache.org/community/

---

**Apache Cloudberry (Incubating)** - Next-generation MPP analytics database  
*Disclaimer: Apache Cloudberry is an effort undergoing incubation at The Apache Software Foundation (ASF)*