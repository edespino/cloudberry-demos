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

## Performance Benchmarking & Metrics

### Query Performance Comparison

#### ANALYZE Impact Measurements
```sql
-- Measure performance before ANALYZE
\timing on
SET optimizer = on;
EXPLAIN (ANALYZE, BUFFERS) 
SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id;

-- Run ANALYZE and measure again
ANALYZE booking; ANALYZE flights;
EXPLAIN (ANALYZE, BUFFERS) 
SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id;
```

**Expected Results:**
- **Before ANALYZE**: Row estimates ~1, poor join strategy selection
- **After ANALYZE**: Accurate estimates (~30,000 rows), optimal Motion operations
- **Performance Improvement**: 2-5x faster execution, better memory usage

#### ORCA vs PostgreSQL Planner Performance
```sql
-- Complex analytical query benchmark
WITH passenger_metrics AS (
  SELECT p.passenger_id, COUNT(*) as flight_count,
         AVG(EXTRACT(epoch FROM f.departure_time - b.booking_date)/86400) as avg_lead_days
  FROM passenger p 
  JOIN booking b ON p.passenger_id = b.passenger_id
  JOIN flights f ON b.flight_id = f.flight_id
  GROUP BY p.passenger_id
)
SELECT flight_count, COUNT(*) as passenger_count, AVG(avg_lead_days) as avg_planning_days
FROM passenger_metrics 
GROUP BY flight_count 
ORDER BY flight_count DESC;

-- Test with both optimizers
SET optimizer = on;   -- ORCA
\timing on
[run query above]

SET optimizer = off;  -- PostgreSQL
\timing on  
[run query above]
```

**Typical Performance Characteristics:**
- **ORCA**: Excels at complex joins (3+ tables), window functions, sophisticated analytics
- **PostgreSQL**: Faster for simple queries, more predictable for OLTP patterns
- **Sweet Spot**: ORCA shows 20-40% improvement on complex analytical workloads

#### pg_hint_plan Overhead Measurements
```sql
-- Measure hint processing overhead
\timing on

-- Without hints
EXPLAIN (ANALYZE) SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id;

-- With hints
/*+ HashJoin(b f) */ EXPLAIN (ANALYZE) SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id;
```

**Overhead Analysis:**
- **Planning Time**: <1ms additional overhead for hint parsing
- **Execution Time**: No runtime overhead, only planning phase impact
- **Memory Usage**: Negligible impact on query memory footprint

### Compression Effectiveness Metrics

#### zstd Level 5 Results
```sql
-- Measure actual compression ratios
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as compressed_size,
  pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
  ROUND(
    (1 - pg_relation_size(schemaname||'.'||tablename)::numeric / 
     (pg_total_relation_size(schemaname||'.'||tablename) * 0.25)) * 100, 1
  ) as compression_ratio_estimate
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

**Expected Compression Results:**
- **Passenger Table**: 60-70% space savings (text fields compress well)
- **Flights Table**: 65-75% space savings (timestamps and codes)
- **Booking Table**: 50-60% space savings (mixed data types)
- **Overall**: ~100MB uncompressed → ~35MB compressed

### Scaling Characteristics

#### Segment Count Impact
```sql
-- Check current segment configuration
SELECT gp_segment_id, count(*) as row_count
FROM gp_dist_random('booking') 
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

-- Expected distribution:
-- 3 segments: ~5,000 rows per segment (current demo)
-- 6 segments: ~2,500 rows per segment  
-- 12 segments: ~1,250 rows per segment
```

**Performance Scaling Patterns:**
- **Linear Scaling**: Most aggregations scale linearly with segment count
- **Motion Overhead**: Join performance optimal at 3-6 segments for demo size
- **Memory Distribution**: Better parallelization with more segments
- **Optimal Configuration**: 3-4 segments per CPU core for analytical workloads

#### Data Volume Impact Analysis
```sql
-- Memory usage by query complexity
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT f.origin, f.destination, COUNT(*) as bookings,
       AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_hours
FROM flights f 
JOIN booking b ON f.flight_id = b.flight_id
GROUP BY ROLLUP(f.origin, f.destination)
ORDER BY COUNT(*) DESC;
```

**Memory Usage Patterns:**
- **Simple Queries**: 2-4MB work_mem per segment sufficient
- **Complex Aggregations**: 16-32MB work_mem for optimal performance  
- **Window Functions**: 32-64MB work_mem for large result sets
- **Multi-table Joins**: Memory usage scales with number of join operations

### Monitoring & Metrics Collection
```sql
-- Query performance monitoring
CREATE OR REPLACE VIEW query_performance_metrics AS
SELECT 
  schemaname,
  tablename,
  seq_scan,
  seq_tup_read,
  idx_scan,
  idx_tup_fetch,
  n_tup_ins,
  n_tup_upd,
  n_tup_del,
  last_analyze,
  analyze_count
FROM pg_stat_user_tables
WHERE schemaname = 'public';

-- View current metrics
SELECT * FROM query_performance_metrics;
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

## Troubleshooting Playbook

### Common Query Performance Problems

#### 1. Slow Aggregations
**Symptoms:**
- Long execution times on GROUP BY queries
- High CPU usage across segments
- Excessive Motion operations

**Diagnostic Queries:**
```sql
-- Check data distribution for aggregation columns
SELECT gp_segment_id, COUNT(*) 
FROM gp_dist_random('booking') 
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

-- Analyze distribution key effectiveness
SELECT attname, n_distinct, correlation 
FROM pg_stats 
WHERE tablename = 'booking' AND attname = 'booking_id';
```

**Solutions:**
1. **Verify ANALYZE status**: `ANALYZE booking; ANALYZE flights;`
2. **Check distribution key alignment**: Ensure GROUP BY columns match distribution keys
3. **Consider re-distribution**: Use `DISTRIBUTED BY (group_column)` for frequently grouped columns
4. **Increase work_mem**: `SET work_mem = '64MB';` for complex aggregations

#### 2. High Motion Operation Costs
**Symptoms:**
- Multiple Redistribute Motion operations in execution plan
- Network I/O bottlenecks
- Uneven segment utilization

**Diagnostic Queries:**
```sql
-- Identify Motion-heavy queries
EXPLAIN (ANALYZE, VERBOSE) 
SELECT b.booking_id, f.flight_number, p.first_name 
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id 
JOIN passenger p ON b.passenger_id = p.passenger_id;

-- Check join key distribution alignment
SELECT 'booking.flight_id' as column, gp_segment_id, count(*) 
FROM gp_dist_random('booking') 
GROUP BY gp_segment_id 
UNION ALL
SELECT 'flights.flight_id', gp_segment_id, count(*) 
FROM gp_dist_random('flights') 
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;
```

**Solutions:**
1. **Align distribution keys**: `DISTRIBUTED BY (flight_id)` for both tables in frequent joins
2. **Use pg_hint_plan**: `/*+ Leading((smaller_table larger_table)) */` to optimize join order
3. **Consider Broadcast Motion**: Small tables (<1000 rows) benefit from broadcasting
4. **Denormalization**: Pre-join frequently accessed tables

#### 3. Memory Errors and Segment Failures
**Symptoms:**
- "out of memory" errors
- Segment process crashes
- Query cancellations

**Diagnostic Queries:**
```sql
-- Check current memory settings
SHOW work_mem;
SHOW shared_buffers;
SHOW max_connections;

-- Monitor memory usage during queries
EXPLAIN (ANALYZE, BUFFERS) 
SELECT origin, destination, COUNT(*) 
FROM flights f 
JOIN booking b ON f.flight_id = b.flight_id 
GROUP BY CUBE(origin, destination);
```

**Solutions:**
1. **Increase work_mem**: `SET work_mem = '128MB';` (per segment)
2. **Optimize query structure**: Break complex queries into simpler parts
3. **Use LIMIT strategically**: Add `LIMIT` clauses to large result sets
4. **Check segment resource limits**: `gpconfig -s gp_vmem_protect_limit`

#### 4. Query Plan Instability
**Symptoms:**
- Same query produces different execution plans
- Performance varies significantly between runs
- ORCA chooses suboptimal plans intermittently

**Diagnostic Queries:**
```sql
-- Check statistics currency
SELECT schemaname, tablename, 
       pg_stat_get_last_analyze_time(c.oid) as last_analyze,
       age(now(), pg_stat_get_last_analyze_time(c.oid)) as time_since_analyze
FROM pg_class c 
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_tables t ON c.relname = t.tablename AND n.nspname = t.schemaname
WHERE schemaname = 'public'
ORDER BY time_since_analyze DESC;

-- Compare optimizer plan consistency
SET optimizer = on;
EXPLAIN your_unstable_query;
SET optimizer = off; 
EXPLAIN your_unstable_query;
```

**Solutions:**
1. **Regular ANALYZE schedule**: Set up automated `ANALYZE` jobs
2. **Use pg_hint_plan**: Force consistent execution plans with hints
3. **Pin to specific optimizer**: `SET optimizer = off;` for problematic queries
4. **Update statistics target**: `ALTER TABLE booking ALTER COLUMN passenger_id SET STATISTICS 1000;`

### ORCA-Specific Troubleshooting

#### When ORCA Produces Worse Plans
**Common Scenarios:**
- Small datasets (<10,000 rows total)
- Queries with many OR conditions
- Highly selective WHERE clauses
- Complex nested subqueries

**Quick Fixes:**
```sql
-- Switch to PostgreSQL planner for specific queries
SET optimizer = off;
your_problematic_query;

-- Or use session-level setting
SET SESSION optimizer = off;
```

**Permanent Solutions:**
```sql
-- Create function wrapper to force PostgreSQL planner
CREATE OR REPLACE FUNCTION run_with_pg_planner(query_text TEXT) 
RETURNS SETOF RECORD AS $$
BEGIN
  SET LOCAL optimizer = off;
  RETURN QUERY EXECUTE query_text;
END;
$$ LANGUAGE plpgsql;
```

#### Handling Queries with Many Joins (>5 tables)
**ORCA Challenges:**
- Exponential planning time growth
- Suboptimal join order selection
- Memory exhaustion during planning

**Solutions:**
```sql
-- Use Leading hints to control join order
/*+ Leading((((a b) c) d) e) */ 
SELECT ... FROM a, b, c, d, e WHERE ...;

-- Break into CTEs to simplify planning
WITH first_join AS (
  SELECT ... FROM table1 t1 JOIN table2 t2 ON ...
),
second_join AS (
  SELECT ... FROM first_join fj JOIN table3 t3 ON ...
)
SELECT ... FROM second_join ...;

-- Use temporary tables for complex intermediate results
CREATE TEMP TABLE intermediate_result AS 
SELECT ... FROM complex_multi_table_join;

ANALYZE intermediate_result;

SELECT ... FROM intermediate_result ir JOIN final_table ft ON ...;
```

### Performance Debugging Workflow

#### Step 1: Gather Baseline Metrics
```sql
-- Enable timing and detailed output
\timing on
SET log_statement_stats = on;
SET log_duration = on;

-- Run problematic query with full analysis
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS) 
your_problematic_query;
```

#### Step 2: Isolate the Bottleneck
```sql
-- Test individual table scans
EXPLAIN ANALYZE SELECT COUNT(*) FROM booking;
EXPLAIN ANALYZE SELECT COUNT(*) FROM flights;
EXPLAIN ANALYZE SELECT COUNT(*) FROM passenger;

-- Test individual joins
EXPLAIN ANALYZE SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id;
EXPLAIN ANALYZE SELECT COUNT(*) FROM booking b JOIN passenger p ON b.passenger_id = p.passenger_id;
```

#### Step 3: Apply Targeted Fixes
```sql
-- Fix 1: Statistics
ANALYZE booking; ANALYZE flights; ANALYZE passenger;

-- Fix 2: Memory allocation  
SET work_mem = '128MB';

-- Fix 3: Query hints
/*+ HashJoin(b f) SeqScan(p) */ 
your_query_with_hints;

-- Fix 4: Optimizer selection
SET optimizer = off; -- or on
your_query;
```

#### Step 4: Validate Improvements
```sql
-- Compare before/after metrics
EXPLAIN (ANALYZE, BUFFERS) 
original_query;

-- Document the solution
COMMENT ON TABLE booking IS 'Requires ANALYZE after bulk loads for optimal ORCA plans';
```

### Environment-Specific Issues

#### gpdemo Development Environment
**Common Issues:**
- Limited memory allocation
- Single-node segment configuration
- Development vs production behavior differences

**Fixes:**
```sql
-- Adjust for development limitations
SET work_mem = '32MB';  -- Lower than production
SET enable_nestloop = on;  -- Sometimes faster on small datasets
SET optimizer = off;  -- More predictable for development
```

#### Production Deployment Issues
**Common Issues:**
- Segment failures under load
- Resource queue exhaustion
- Statistics staleness

**Monitoring Queries:**
```sql
-- Check segment health
SELECT gp_segment_id, hostname, port, status 
FROM gp_configuration 
WHERE content >= 0 
ORDER BY gp_segment_id;

-- Monitor resource queue usage
SELECT rsqname, rsqcountlimit, rsqcostlimit, rsqmemorylimit, rsqwaiters
FROM pg_resqueue_status;

-- Check for stale statistics
SELECT tablename, last_analyze, analyze_count 
FROM pg_stat_user_tables 
WHERE analyze_count = 0 OR age(now(), last_analyze) > interval '1 day';
```

## Advanced Use Cases & Real-World Scenarios

### ETL Pipeline Optimization

#### Bulk Loading Strategies for Airline Data
```sql
-- Optimized bulk loading with staging tables
CREATE TABLE staging_bookings (
  booking_id INT,
  passenger_id INT,
  flight_id INT,
  booking_date TIMESTAMP,
  seat_number TEXT
) WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=5)
DISTRIBUTED RANDOMLY;  -- Random distribution for fastest loading

-- Fast parallel loading from multiple files
COPY staging_bookings FROM '/data/bookings_part1.csv' CSV HEADER;
COPY staging_bookings FROM '/data/bookings_part2.csv' CSV HEADER;
COPY staging_bookings FROM '/data/bookings_part3.csv' CSV HEADER;

-- Redistribute to final table with optimal distribution key
INSERT INTO booking 
SELECT * FROM staging_bookings;

-- Clean up staging
DROP TABLE staging_bookings;
```

#### Optimal UPDATE/DELETE Patterns for MPP
```sql
-- ❌ AVOID: Row-by-row updates (very slow in MPP)
UPDATE booking SET seat_number = 'A15' WHERE booking_id = 12345;

-- ✅ PREFER: Batch updates with distribution key in WHERE
UPDATE booking 
SET seat_number = CASE 
  WHEN flight_id < 100 THEN 'A' || (ROW_NUMBER() OVER (ORDER BY booking_id))::text
  ELSE seat_number 
END
WHERE flight_id BETWEEN 50 AND 150;  -- Targets specific segments

-- ✅ OPTIMAL: Recreate table for large changes
CREATE TABLE booking_updated AS 
SELECT booking_id, passenger_id, flight_id, booking_date,
       CASE WHEN seat_number LIKE 'A%' THEN 'Premium' ELSE seat_number END as seat_number
FROM booking
DISTRIBUTED BY (booking_id);

DROP TABLE booking;
ALTER TABLE booking_updated RENAME TO booking;
```

#### Partition Pruning for Time-Series Flight Data
```sql
-- Create partitioned flights table by date
CREATE TABLE flights_partitioned (
  flight_id SERIAL,
  flight_number TEXT NOT NULL,
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  departure_time TIMESTAMP NOT NULL,
  arrival_time TIMESTAMP NOT NULL
)
WITH (appendonly=true, compresstype=zstd, compresslevel=5)
DISTRIBUTED BY (flight_id)
PARTITION BY RANGE(departure_time) (
  START ('2025-01-01'::timestamp) END ('2026-01-01'::timestamp) 
  EVERY (INTERVAL '1 month')
);

-- Queries automatically use partition pruning
EXPLAIN (ANALYZE, COSTS OFF)
SELECT COUNT(*) FROM flights_partitioned 
WHERE departure_time >= '2025-08-01' AND departure_time < '2025-09-01';
-- Result: Only scans August 2025 partition, dramatic performance improvement
```

### BI/Analytics Workloads

#### Dashboard Query Optimization Patterns
```sql
-- Pattern 1: Pre-aggregated summary tables for dashboards
CREATE TABLE daily_booking_summary AS
SELECT 
  DATE(booking_date) as booking_day,
  f.origin,
  f.destination,
  COUNT(*) as total_bookings,
  COUNT(DISTINCT b.passenger_id) as unique_passengers,
  AVG(EXTRACT(epoch FROM f.departure_time - b.booking_date)/3600) as avg_lead_hours
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id
GROUP BY DATE(booking_date), f.origin, f.destination
DISTRIBUTED BY (booking_day);

-- Dashboard queries become lightning fast
SELECT origin, destination, SUM(total_bookings) as weekly_bookings
FROM daily_booking_summary 
WHERE booking_day >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY origin, destination
ORDER BY weekly_bookings DESC
LIMIT 10;
```

#### Real-time vs Batch Processing Trade-offs
```sql
-- Real-time view: Fast approximate results
CREATE VIEW real_time_metrics AS
SELECT 
  COUNT(*) as current_bookings,
  COUNT(DISTINCT passenger_id) as active_passengers,
  (SELECT COUNT(*) FROM flights WHERE departure_time >= CURRENT_DATE) as upcoming_flights
FROM booking 
WHERE booking_date >= CURRENT_DATE - INTERVAL '1 hour';

-- Batch view: Accurate historical analysis (runs hourly)
CREATE MATERIALIZED VIEW hourly_booking_metrics AS
SELECT 
  DATE_TRUNC('hour', booking_date) as booking_hour,
  COUNT(*) as bookings_count,
  COUNT(DISTINCT passenger_id) as unique_passengers,
  AVG(EXTRACT(epoch FROM f.departure_time - b.booking_date)) as avg_lead_time_seconds
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id
GROUP BY DATE_TRUNC('hour', booking_date)
DISTRIBUTED BY (booking_hour);

-- Refresh strategy
REFRESH MATERIALIZED VIEW hourly_booking_metrics;
```

#### Materialized View Strategies for Common Aggregations
```sql
-- High-frequency query optimization with materialized views
CREATE MATERIALIZED VIEW passenger_flight_profiles AS
SELECT 
  p.passenger_id,
  p.first_name || ' ' || p.last_name as full_name,
  COUNT(*) as total_flights,
  COUNT(DISTINCT f.origin) as origins_visited,
  COUNT(DISTINCT f.destination) as destinations_visited,
  MIN(f.departure_time) as first_flight,
  MAX(f.departure_time) as last_flight,
  ARRAY_AGG(DISTINCT f.origin ORDER BY f.origin) as all_origins,
  AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_flight_duration_hours
FROM passenger p
JOIN booking b ON p.passenger_id = b.passenger_id  
JOIN flights f ON b.flight_id = f.flight_id
GROUP BY p.passenger_id, p.first_name, p.last_name
DISTRIBUTED BY (passenger_id);

-- Complex analytics become simple lookups
SELECT full_name, total_flights, origins_visited, destinations_visited
FROM passenger_flight_profiles
WHERE total_flights > 2
ORDER BY total_flights DESC;
```

### Operational Analytics

#### Route Network Analysis
```sql
-- Hub connectivity analysis
WITH route_metrics AS (
  SELECT 
    f.origin,
    f.destination,
    COUNT(*) as flight_frequency,
    COUNT(DISTINCT b.passenger_id) as passenger_count,
    AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_duration_hours
  FROM flights f
  JOIN booking b ON f.flight_id = b.flight_id
  GROUP BY f.origin, f.destination
),
hub_analysis AS (
  SELECT 
    origin as airport,
    COUNT(*) as outbound_routes,
    SUM(flight_frequency) as total_outbound_flights,
    AVG(avg_duration_hours) as avg_flight_duration
  FROM route_metrics
  GROUP BY origin
  UNION ALL
  SELECT 
    destination as airport,
    COUNT(*) as inbound_routes,  
    SUM(flight_frequency) as total_inbound_flights,
    AVG(avg_duration_hours) as avg_flight_duration
  FROM route_metrics
  GROUP BY destination
)
SELECT 
  airport,
  SUM(outbound_routes + inbound_routes) as total_connectivity,
  SUM(total_outbound_flights + total_inbound_flights) as total_traffic,
  AVG(avg_flight_duration) as avg_duration
FROM hub_analysis
GROUP BY airport
HAVING SUM(total_outbound_flights + total_inbound_flights) > 100
ORDER BY total_traffic DESC;
```

#### Passenger Journey Mapping
```sql
-- Multi-leg journey analysis
WITH passenger_journeys AS (
  SELECT 
    p.passenger_id,
    p.first_name || ' ' || p.last_name as passenger_name,
    f.departure_time,
    f.origin,
    f.destination,
    LAG(f.destination) OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as previous_destination,
    LAG(f.departure_time) OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as previous_departure,
    LEAD(f.origin) OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as next_origin
  FROM passenger p
  JOIN booking b ON p.passenger_id = b.passenger_id
  JOIN flights f ON b.flight_id = f.flight_id
)
SELECT 
  passenger_name,
  COUNT(*) as total_segments,
  COUNT(CASE WHEN origin = previous_destination THEN 1 END) as connecting_flights,
  STRING_AGG(origin || '->' || destination, ' | ' ORDER BY departure_time) as complete_journey
FROM passenger_journeys
GROUP BY passenger_id, passenger_name
HAVING COUNT(*) > 1
ORDER BY connecting_flights DESC, total_segments DESC;
```

### Data Quality & Monitoring

#### Automated Data Quality Checks
```sql
-- Comprehensive data quality monitoring
CREATE OR REPLACE VIEW data_quality_report AS
WITH booking_quality AS (
  SELECT 
    'booking' as table_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN booking_id IS NULL THEN 1 END) as null_booking_ids,
    COUNT(CASE WHEN passenger_id IS NULL THEN 1 END) as null_passenger_ids,
    COUNT(CASE WHEN flight_id IS NULL THEN 1 END) as null_flight_ids,
    COUNT(CASE WHEN booking_date > CURRENT_TIMESTAMP THEN 1 END) as future_bookings,
    COUNT(CASE WHEN seat_number IS NULL OR seat_number = '' THEN 1 END) as missing_seats
  FROM booking
),
flights_quality AS (
  SELECT 
    'flights' as table_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN flight_id IS NULL THEN 1 END) as null_flight_ids,
    COUNT(CASE WHEN departure_time >= arrival_time THEN 1 END) as invalid_duration,
    COUNT(CASE WHEN origin = destination THEN 1 END) as same_origin_destination,
    COUNT(CASE WHEN origin IS NULL OR destination IS NULL THEN 1 END) as missing_airports,
    0 as placeholder_col1, 0 as placeholder_col2
  FROM flights
),
passenger_quality AS (
  SELECT 
    'passenger' as table_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN passenger_id IS NULL THEN 1 END) as null_passenger_ids,
    COUNT(CASE WHEN first_name IS NULL OR first_name = '' THEN 1 END) as missing_first_names,
    COUNT(CASE WHEN last_name IS NULL OR last_name = '' THEN 1 END) as missing_last_names,
    COUNT(CASE WHEN email NOT LIKE '%@%' THEN 1 END) as invalid_emails,
    0 as placeholder_col1, 0 as placeholder_col2
  FROM passenger
)
SELECT * FROM booking_quality
UNION ALL SELECT * FROM flights_quality  
UNION ALL SELECT * FROM passenger_quality;

-- Run quality checks
SELECT * FROM data_quality_report;
```

#### Performance Monitoring Automation
```sql
-- Query performance tracking
CREATE TABLE query_performance_log (
  log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  query_type TEXT,
  execution_time_ms INT,
  rows_processed BIGINT,
  segments_used INT,
  optimizer_used TEXT,
  query_hash TEXT
) DISTRIBUTED BY (log_timestamp);

-- Function to log query performance
CREATE OR REPLACE FUNCTION log_query_performance(
  p_query_type TEXT,
  p_execution_time_ms INT,
  p_rows_processed BIGINT,
  p_segments_used INT,
  p_optimizer_used TEXT,
  p_query_hash TEXT
) RETURNS VOID AS $$
BEGIN
  INSERT INTO query_performance_log 
  VALUES (CURRENT_TIMESTAMP, p_query_type, p_execution_time_ms, p_rows_processed, p_segments_used, p_optimizer_used, p_query_hash);
END;
$$ LANGUAGE plpgsql;
```

## Framework Integration & Automation

### Apache Airflow DAGs for Database Operations

#### Automated ANALYZE Scheduling
```python
# airflow_cloudberry_maintenance.py
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'cloudberry_maintenance',
    default_args=default_args,
    description='Apache Cloudberry maintenance and optimization',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False
)

# Update table statistics
analyze_tables = PostgresOperator(
    task_id='analyze_airline_tables',
    postgres_conn_id='cloudberry_airline_demo',
    sql="""
        ANALYZE passenger;
        ANALYZE flights;
        ANALYZE booking;
        
        -- Log statistics update
        INSERT INTO maintenance_log (operation, table_name, execution_time)
        SELECT 'ANALYZE', unnest(ARRAY['passenger', 'flights', 'booking']), CURRENT_TIMESTAMP;
    """,
    dag=dag
)

# Data quality validation
data_quality_check = PostgresOperator(
    task_id='data_quality_validation',
    postgres_conn_id='cloudberry_airline_demo',
    sql="""
        -- Run data quality checks and alert on issues
        WITH quality_issues AS (
            SELECT * FROM data_quality_report 
            WHERE total_rows = 0 OR null_passenger_ids > 0 OR null_flight_ids > 0
        )
        SELECT CASE 
            WHEN EXISTS (SELECT 1 FROM quality_issues) 
            THEN 'QUALITY_ISSUES_DETECTED'
            ELSE 'QUALITY_OK'
        END as status;
    """,
    dag=dag
)

# Performance monitoring
performance_snapshot = PostgresOperator(
    task_id='capture_performance_metrics',
    postgres_conn_id='cloudberry_airline_demo',
    sql="""
        INSERT INTO daily_performance_snapshot (
            snapshot_date,
            total_bookings,
            total_flights,
            total_passengers,
            avg_query_time_ms,
            segment_utilization
        )
        SELECT 
            CURRENT_DATE,
            (SELECT COUNT(*) FROM booking),
            (SELECT COUNT(*) FROM flights),
            (SELECT COUNT(*) FROM passenger),
            COALESCE((SELECT AVG(execution_time_ms) FROM query_performance_log 
                     WHERE log_timestamp >= CURRENT_DATE - INTERVAL '1 day'), 0),
            (SELECT COUNT(DISTINCT gp_segment_id) FROM gp_dist_random('booking'))
    """,
    dag=dag
)

# Define task dependencies
analyze_tables >> data_quality_check >> performance_snapshot
```

#### ETL Pipeline with Performance Optimization
```python
# airflow_cloudberry_etl.py
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def extract_booking_data(**context):
    """Extract booking data from external source with optimal formatting for Cloudberry"""
    # Simulate data extraction
    booking_data = pd.DataFrame({
        'booking_id': range(15001, 16001),
        'passenger_id': np.random.randint(1, 10001, 1000),
        'flight_id': np.random.randint(1, 751, 1000),
        'booking_date': pd.date_range(start='2025-08-01', periods=1000, freq='H'),
        'seat_number': [f"{random.choice(['A','B','C','D','E','F'])}{random.randint(1,35)}" for _ in range(1000)]
    })
    
    # Save to staging area optimized for Cloudberry bulk loading
    booking_data.to_csv('/staging/booking_incremental.csv', index=False, header=True)
    return '/staging/booking_incremental.csv'

etl_dag = DAG(
    'cloudberry_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline optimized for Apache Cloudberry',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    catchup=False
)

extract_data = PythonOperator(
    task_id='extract_booking_data',
    python_callable=extract_booking_data,
    dag=etl_dag
)

# Fast bulk loading with staging table approach
bulk_load = PostgresOperator(
    task_id='bulk_load_bookings',
    postgres_conn_id='cloudberry_airline_demo',
    sql="""
        -- Create staging table for fast parallel loading
        CREATE TEMP TABLE staging_bookings (
            booking_id INT,
            passenger_id INT,
            flight_id INT,
            booking_date TIMESTAMP,
            seat_number TEXT
        ) DISTRIBUTED RANDOMLY;
        
        -- Bulk load with optimal settings
        SET gp_autostats_mode = NONE;  -- Disable auto-stats during load
        COPY staging_bookings FROM '/staging/booking_incremental.csv' CSV HEADER;
        
        -- Insert with distribution key optimization
        INSERT INTO booking 
        SELECT * FROM staging_bookings
        WHERE NOT EXISTS (
            SELECT 1 FROM booking b WHERE b.booking_id = staging_bookings.booking_id
        );
        
        -- Re-enable stats and update
        SET gp_autostats_mode = ON_CHANGE;
        ANALYZE booking;
    """,
    dag=etl_dag
)

extract_data >> bulk_load
```

### Python Integration with psycopg2 and pandas

#### Optimized Data Analysis Pipeline
```python
# cloudberry_analytics.py
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

class CloudberryAnalytics:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self.connection = None
    
    def connect(self):
        """Establish connection with optimal settings for analytics"""
        self.connection = psycopg2.connect(
            host='localhost',
            port=7000,
            database='airline_demo',
            user='cbadmin'
        )
        
        # Optimize connection for analytical workloads
        with self.connection.cursor() as cursor:
            cursor.execute("SET work_mem = '256MB'")
            cursor.execute("SET optimizer = on")  # Use ORCA
            cursor.execute("SET enable_nestloop = off")  # Prefer hash joins for large datasets
        
        self.connection.commit()
    
    def get_booking_trends(self, days=30):
        """Analyze booking trends with MPP-optimized query"""
        query = """
        WITH daily_bookings AS (
            SELECT 
                DATE(booking_date) as booking_day,
                COUNT(*) as bookings_count,
                COUNT(DISTINCT passenger_id) as unique_passengers,
                AVG(EXTRACT(epoch FROM f.departure_time - b.booking_date)/86400) as avg_lead_days
            FROM booking b
            JOIN flights f ON b.flight_id = f.flight_id
            WHERE booking_date >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY DATE(booking_date)
        )
        SELECT * FROM daily_bookings ORDER BY booking_day;
        """
        
        return pd.read_sql_query(query, self.engine, params=(days,))
    
    def get_route_analysis(self):
        """Complex route analysis leveraging MPP parallelism"""
        query = """
        /*+ HashJoin(f b) Leading((f b)) */
        SELECT 
            f.origin || ' → ' || f.destination as route,
            COUNT(*) as total_bookings,
            COUNT(DISTINCT b.passenger_id) as unique_passengers,
            AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_flight_hours,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
                EXTRACT(epoch FROM f.departure_time - b.booking_date)/86400
            ) as median_lead_days
        FROM flights f
        JOIN booking b ON f.flight_id = b.flight_id
        GROUP BY f.origin, f.destination
        HAVING COUNT(*) >= 10
        ORDER BY total_bookings DESC;
        """
        
        return pd.read_sql_query(query, self.engine)
    
    def optimize_query_performance(self, query):
        """Test query with different optimizers and return best performing"""
        results = {}
        
        # Test with ORCA
        with self.connection.cursor() as cursor:
            cursor.execute("SET optimizer = on")
            start_time = time.time()
            cursor.execute(query)
            results['orca'] = time.time() - start_time
        
        # Test with PostgreSQL planner
        with self.connection.cursor() as cursor:
            cursor.execute("SET optimizer = off")
            start_time = time.time()
            cursor.execute(query)
            results['postgresql'] = time.time() - start_time
        
        # Return optimal setting
        best_optimizer = min(results, key=results.get)
        return best_optimizer, results
    
    def create_performance_dashboard(self):
        """Generate performance visualization using MPP queries"""
        # Get booking trends
        trends = self.get_booking_trends(30)
        
        # Get route performance
        routes = self.get_route_analysis()
        
        # Create visualizations
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        # Daily booking trends
        ax1.plot(trends['booking_day'], trends['bookings_count'])
        ax1.set_title('Daily Booking Volume')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Bookings')
        
        # Lead time distribution
        ax2.hist(trends['avg_lead_days'], bins=20)
        ax2.set_title('Booking Lead Time Distribution')
        ax2.set_xlabel('Average Lead Days')
        ax2.set_ylabel('Frequency')
        
        # Top routes by volume
        top_routes = routes.head(10)
        ax3.barh(top_routes['route'], top_routes['total_bookings'])
        ax3.set_title('Top Routes by Booking Volume')
        ax3.set_xlabel('Total Bookings')
        
        # Flight duration vs bookings
        ax4.scatter(routes['avg_flight_hours'], routes['total_bookings'], alpha=0.6)
        ax4.set_title('Flight Duration vs Booking Volume')
        ax4.set_xlabel('Average Flight Hours')
        ax4.set_ylabel('Total Bookings')
        
        plt.tight_layout()
        plt.savefig('cloudberry_analytics_dashboard.png', dpi=300)
        return fig

# Usage example
analytics = CloudberryAnalytics('postgresql://cbadmin@localhost:7000/airline_demo')
analytics.connect()

# Generate insights
booking_trends = analytics.get_booking_trends()
route_analysis = analytics.get_route_analysis()

# Create performance dashboard
dashboard = analytics.create_performance_dashboard()
```

### Jupyter Notebook Integration

#### Interactive Query Analysis Notebook
```python
# cloudberry_exploration.ipynb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

# Connect to Apache Cloudberry
engine = create_engine('postgresql://cbadmin@localhost:7000/airline_demo')

# Set optimal session parameters for analysis
with engine.connect() as conn:
    conn.execute("SET work_mem = '512MB'")
    conn.execute("SET optimizer = on")
    
print("Connected to Apache Cloudberry airline_demo database")

# Cell 2: Data Overview
def get_table_summary():
    query = """
    SELECT 
        'passenger' as table_name, COUNT(*) as row_count, 
        pg_size_pretty(pg_total_relation_size('passenger')) as size
    FROM passenger
    UNION ALL
    SELECT 'flights', COUNT(*), pg_size_pretty(pg_total_relation_size('flights'))
    FROM flights
    UNION ALL
    SELECT 'booking', COUNT(*), pg_size_pretty(pg_total_relation_size('booking'))
    FROM booking;
    """
    return pd.read_sql_query(query, engine)

summary = get_table_summary()
print("Database Summary:")
display(summary)

# Cell 3: Query Plan Visualization
def analyze_query_plan(query, title="Query Plan Analysis"):
    explain_query = f"EXPLAIN (ANALYZE, COSTS, VERBOSE) {query}"
    
    with engine.connect() as conn:
        # Get ORCA plan
        conn.execute("SET optimizer = on")
        orca_result = conn.execute(explain_query)
        orca_plan = [row[0] for row in orca_result]
        
        # Get PostgreSQL plan
        conn.execute("SET optimizer = off")
        pg_result = conn.execute(explain_query)
        pg_plan = [row[0] for row in pg_result]
    
    print(f"\n{title}")
    print("="*50)
    print("ORCA Optimizer Plan:")
    for line in orca_plan:
        print(line)
    
    print("\nPostgreSQL Planner Plan:")
    for line in pg_plan:
        print(line)

# Analyze a complex query
complex_query = """
SELECT 
    f.origin,
    f.destination,
    COUNT(*) as bookings,
    AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_hours
FROM flights f
JOIN booking b ON f.flight_id = b.flight_id
GROUP BY f.origin, f.destination
HAVING COUNT(*) > 20
ORDER BY bookings DESC
LIMIT 10
"""

analyze_query_plan(complex_query, "Route Analysis Query")

# Cell 4: Performance Comparison Visualization
def compare_optimizer_performance():
    test_queries = [
        ("Simple Join", "SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id"),
        ("Complex Aggregation", """
            SELECT f.origin, COUNT(*), AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time))
            FROM flights f JOIN booking b ON f.flight_id = b.flight_id
            GROUP BY f.origin
        """),
        ("Window Function", """
            SELECT passenger_id, COUNT(*),
                   RANK() OVER (ORDER BY COUNT(*) DESC)
            FROM booking GROUP BY passenger_id
        """)
    ]
    
    results = []
    for name, query in test_queries:
        with engine.connect() as conn:
            # Test ORCA
            conn.execute("SET optimizer = on")
            start = time.time()
            conn.execute(query)
            orca_time = time.time() - start
            
            # Test PostgreSQL
            conn.execute("SET optimizer = off")
            start = time.time()
            conn.execute(query)
            pg_time = time.time() - start
            
            results.append({
                'Query': name,
                'ORCA (ms)': orca_time * 1000,
                'PostgreSQL (ms)': pg_time * 1000
            })
    
    df = pd.DataFrame(results)
    return df

perf_results = compare_optimizer_performance()
display(perf_results)

# Visualize performance comparison
plt.figure(figsize=(12, 6))
x = range(len(perf_results))
width = 0.35

plt.bar([i - width/2 for i in x], perf_results['ORCA (ms)'], 
        width, label='ORCA', alpha=0.8, color='blue')
plt.bar([i + width/2 for i in x], perf_results['PostgreSQL (ms)'], 
        width, label='PostgreSQL', alpha=0.8, color='orange')

plt.xlabel('Query Type')
plt.ylabel('Execution Time (ms)')
plt.title('Apache Cloudberry: ORCA vs PostgreSQL Planner Performance')
plt.xticks(x, perf_results['Query'])
plt.legend()
plt.grid(axis='y', alpha=0.3)
plt.show()
```

### Monitoring & Alerting Integration

#### Prometheus Metrics Export
```python
# cloudberry_metrics.py
from prometheus_client import Gauge, Counter, start_http_server
import psycopg2
import time
import threading

class CloudberryMetricsExporter:
    def __init__(self, connection_params):
        self.connection_params = connection_params
        
        # Define Prometheus metrics
        self.total_bookings = Gauge('cloudberry_total_bookings', 'Total number of bookings')
        self.total_flights = Gauge('cloudberry_total_flights', 'Total number of flights')
        self.query_duration = Gauge('cloudberry_query_duration_seconds', 'Query execution time', ['query_type'])
        self.segment_count = Gauge('cloudberry_active_segments', 'Number of active segments')
        self.connection_count = Gauge('cloudberry_connections', 'Number of active connections')
        
    def collect_metrics(self):
        """Collect metrics from Apache Cloudberry"""
        try:
            conn = psycopg2.connect(**self.connection_params)
            cursor = conn.cursor()
            
            # Collect basic counts
            cursor.execute("SELECT COUNT(*) FROM booking")
            self.total_bookings.set(cursor.fetchone()[0])
            
            cursor.execute("SELECT COUNT(*) FROM flights")
            self.total_flights.set(cursor.fetchone()[0])
            
            # Collect segment information
            cursor.execute("SELECT COUNT(DISTINCT gp_segment_id) FROM gp_configuration WHERE content >= 0")
            self.segment_count.set(cursor.fetchone()[0])
            
            # Collect connection count
            cursor.execute("SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active'")
            self.connection_count.set(cursor.fetchone()[0])
            
            # Performance metrics
            test_queries = {
                'simple_count': "SELECT COUNT(*) FROM booking",
                'join_query': "SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id",
                'aggregation': "SELECT origin, COUNT(*) FROM flights GROUP BY origin"
            }
            
            for query_type, query in test_queries.items():
                start_time = time.time()
                cursor.execute(query)
                cursor.fetchall()
                duration = time.time() - start_time
                self.query_duration.labels(query_type=query_type).set(duration)
            
            conn.close()
            
        except Exception as e:
            print(f"Error collecting metrics: {e}")
    
    def start_collection(self, interval=30):
        """Start metrics collection in background thread"""
        def collect_loop():
            while True:
                self.collect_metrics()
                time.sleep(interval)
        
        thread = threading.Thread(target=collect_loop, daemon=True)
        thread.start()

# Usage
if __name__ == "__main__":
    connection_params = {
        'host': 'localhost',
        'port': 7000,
        'database': 'airline_demo',
        'user': 'cbadmin'
    }
    
    exporter = CloudberryMetricsExporter(connection_params)
    exporter.start_collection()
    
    # Start Prometheus metrics server
    start_http_server(8000)
    print("Metrics server started on port 8000")
    
    # Keep running
    while True:
        time.sleep(60)
```

## Production Deployment Guidelines

### Configuration Tuning for Different Workloads

#### Memory Allocation Recommendations
```sql
-- OLAP/Analytics Workload (default for airline demo)
ALTER SYSTEM SET work_mem = '256MB';  -- Per segment operator memory
ALTER SYSTEM SET shared_buffers = '25% of total RAM';  -- Buffer cache
ALTER SYSTEM SET max_connections = 200;  -- Conservative for analytics
ALTER SYSTEM SET effective_cache_size = '75% of total RAM';  -- OS cache estimate

-- Mixed OLTP/OLAP Workload  
ALTER SYSTEM SET work_mem = '64MB';  -- Lower per-query memory
ALTER SYSTEM SET shared_buffers = '15% of total RAM';  -- More conservative
ALTER SYSTEM SET max_connections = 500;  -- Higher connection count
ALTER SYSTEM SET random_page_cost = 1.1;  -- Assume SSD storage

-- High-Concurrency OLTP Workload
ALTER SYSTEM SET work_mem = '32MB';  -- Minimize per-query footprint  
ALTER SYSTEM SET shared_buffers = '10% of total RAM';  -- Conservative buffering
ALTER SYSTEM SET max_connections = 1000;  -- High concurrency
ALTER SYSTEM SET checkpoint_completion_target = 0.9;  -- Smooth checkpoints

-- Apply changes
SELECT pg_reload_conf();
```

#### Segment Configuration for Hardware Profiles
```bash
# Small Development Environment (1-2 CPU cores, 8GB RAM)
gpconfig -c gp_vmem_protect_limit -v 4096  # 4GB per segment
gpconfig -c max_connections -v 100
gpconfig -c shared_buffers -v '128MB'
gpconfig -c work_mem -v '32MB'

# Medium Production Environment (4-8 CPU cores, 32GB RAM)  
gpconfig -c gp_vmem_protect_limit -v 8192  # 8GB per segment
gpconfig -c max_connections -v 200
gpconfig -c shared_buffers -v '512MB'
gpconfig -c work_mem -v '128MB'

# Large Analytics Environment (16+ CPU cores, 128GB+ RAM)
gpconfig -c gp_vmem_protect_limit -v 16384  # 16GB per segment
gpconfig -c max_connections -v 300
gpconfig -c shared_buffers -v '2GB'
gpconfig -c work_mem -v '512MB'

# Apply configuration changes
gpstop -ar  # Restart cluster to apply changes
```

#### Optimizer Settings for Production
```sql
-- Analytics-Optimized Settings
SET optimizer = on;  -- Use ORCA for complex queries
SET enable_hashagg = on;  -- Efficient aggregations
SET enable_hashjoin = on;  -- Scalable joins
SET enable_mergejoin = off;  -- Prefer hash joins in MPP
SET random_page_cost = 1.0;  -- Assume fast storage
SET cpu_tuple_cost = 0.001;  -- Faster modern CPUs

-- Memory-Constrained Settings
SET work_mem = '64MB';  -- Conservative memory usage
SET enable_material = off;  -- Reduce materialization
SET enable_sort = on;  -- Allow disk-based sorts
SET temp_file_limit = '2GB';  -- Limit temp file growth

-- High-Concurrency Settings  
SET optimizer = off;  -- PostgreSQL planner for simple queries
SET enable_nestloop = on;  -- Efficient for small result sets
SET enable_indexscan = on;  -- Utilize indexes for selective queries
SET random_page_cost = 4.0;  -- Conservative I/O cost
```

### Security Considerations for Airline Data

#### Role-Based Access Control
```sql
-- Create role hierarchy for airline data access
CREATE ROLE airline_readonly;
CREATE ROLE airline_analyst;
CREATE ROLE airline_admin;
CREATE ROLE airline_etl;

-- Grant base permissions
GRANT CONNECT ON DATABASE airline_demo TO airline_readonly;
GRANT USAGE ON SCHEMA public TO airline_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO airline_readonly;

-- Analyst permissions (read + temp tables)
GRANT airline_readonly TO airline_analyst;
GRANT CREATE ON SCHEMA public TO airline_analyst;
GRANT TEMP ON DATABASE airline_demo TO airline_analyst;

-- Admin permissions (DDL operations)
GRANT airline_analyst TO airline_admin;
GRANT CREATE, DROP ON ALL TABLES IN SCHEMA public TO airline_admin;
GRANT ALTER ON ALL TABLES IN SCHEMA public TO airline_admin;

-- ETL permissions (bulk loading)
GRANT airline_analyst TO airline_etl;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO airline_etl;
GRANT TRUNCATE ON ALL TABLES IN SCHEMA public TO airline_etl;

-- Create specific users
CREATE USER analyst_user WITH ENCRYPTED PASSWORD 'secure_password';
CREATE USER etl_service WITH ENCRYPTED PASSWORD 'etl_service_password';
CREATE USER dashboard_app WITH ENCRYPTED PASSWORD 'dashboard_password';

-- Assign roles
GRANT airline_analyst TO analyst_user;
GRANT airline_etl TO etl_service;
GRANT airline_readonly TO dashboard_app;
```

#### Data Masking Strategies for PII
```sql
-- Create masked views for sensitive data
CREATE VIEW passenger_masked AS
SELECT 
  passenger_id,
  LEFT(first_name, 1) || '***' as first_name_masked,
  LEFT(last_name, 1) || '***' as last_name_masked,
  REGEXP_REPLACE(email, '(.{2}).*@', '\1***@') as email_masked,
  REGEXP_REPLACE(phone, '(\+1-)(\d{3})-.*', '\1\2-***-****') as phone_masked
FROM passenger;

-- Grant access to masked view instead of raw table
REVOKE SELECT ON passenger FROM airline_readonly;
GRANT SELECT ON passenger_masked TO airline_readonly;

-- Function-based masking for dynamic access control
CREATE OR REPLACE FUNCTION get_passenger_data(
  p_user_role TEXT,
  p_passenger_id INT DEFAULT NULL
) RETURNS TABLE (
  passenger_id INT,
  first_name TEXT,
  last_name TEXT, 
  email TEXT,
  phone TEXT
) AS $$
BEGIN
  IF p_user_role = 'admin' THEN
    -- Full access for admins
    RETURN QUERY
    SELECT p.passenger_id, p.first_name, p.last_name, p.email, p.phone
    FROM passenger p
    WHERE (p_passenger_id IS NULL OR p.passenger_id = p_passenger_id);
    
  ELSIF p_user_role = 'analyst' THEN
    -- Masked access for analysts
    RETURN QUERY  
    SELECT p.passenger_id, 
           LEFT(p.first_name, 1) || '***',
           LEFT(p.last_name, 1) || '***',
           REGEXP_REPLACE(p.email, '(.{2}).*@', '\1***@'),
           REGEXP_REPLACE(p.phone, '(\+1-)(\d{3})-.*', '\1\2-***-****')
    FROM passenger p
    WHERE (p_passenger_id IS NULL OR p.passenger_id = p_passenger_id);
    
  ELSE
    -- No access for other roles
    RETURN;
  END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

#### Audit Logging for Query Performance
```sql
-- Enable comprehensive audit logging
ALTER SYSTEM SET log_statement = 'all';  -- Log all SQL statements
ALTER SYSTEM SET log_duration = on;      -- Log query execution time
ALTER SYSTEM SET log_min_duration_statement = 1000;  -- Log slow queries (>1s)
ALTER SYSTEM SET log_checkpoints = on;   -- Log checkpoint activity
ALTER SYSTEM SET log_connections = on;   -- Log connections
ALTER SYSTEM SET log_disconnections = on; -- Log disconnections

-- Custom audit table for application-level tracking
CREATE TABLE query_audit_log (
  audit_id SERIAL PRIMARY KEY,
  user_name TEXT NOT NULL,
  query_text TEXT NOT NULL,
  query_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  execution_time_ms INT,
  rows_affected BIGINT,
  client_ip INET,
  application_name TEXT
) DISTRIBUTED BY (audit_id);

-- Audit trigger function
CREATE OR REPLACE FUNCTION audit_query_execution() 
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO query_audit_log (
    user_name, query_text, execution_time_ms, rows_affected, client_ip, application_name
  ) VALUES (
    current_user,
    current_query(),
    0,  -- Execution time filled by application
    0,  -- Rows affected filled by application
    inet_client_addr(),
    current_setting('application_name', true)
  );
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

### Monitoring and Alerting Setup

#### Essential System Views for Monitoring
```sql
-- Create monitoring views for production oversight
CREATE VIEW production_health_dashboard AS
SELECT 
  'Segment Health' as metric_category,
  gp_segment_id,
  hostname,
  port,
  status,
  role
FROM gp_configuration
WHERE content >= 0
UNION ALL
SELECT 
  'Query Performance',
  NULL::int,
  query,
  NULL::int,
  state,
  EXTRACT(epoch FROM (now() - query_start))::text
FROM pg_stat_activity 
WHERE state != 'idle' AND pid != pg_backend_pid();

-- Table size monitoring
CREATE VIEW table_size_monitor AS
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
  pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
  (SELECT COUNT(*) FROM gp_dist_random(tablename)) as estimated_rows
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Statistics freshness monitoring
CREATE VIEW statistics_freshness AS
SELECT 
  schemaname,
  tablename,
  last_analyze,
  last_autoanalyze,
  analyze_count,
  autoanalyze_count,
  CASE 
    WHEN last_analyze IS NULL THEN 'NEVER_ANALYZED'
    WHEN age(now(), last_analyze) > interval '1 day' THEN 'STALE'
    WHEN age(now(), last_analyze) > interval '6 hours' THEN 'AGING'
    ELSE 'CURRENT'
  END as freshness_status
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY last_analyze DESC NULLS LAST;
```

#### Automated Performance Monitoring
```sql
-- Performance baseline collection
CREATE TABLE performance_baselines (
  baseline_date DATE PRIMARY KEY,
  avg_query_time_ms NUMERIC,
  total_bookings BIGINT,
  total_flights BIGINT,
  segment_utilization NUMERIC,
  compression_ratio NUMERIC,
  largest_table_size_mb NUMERIC
);

-- Daily baseline capture procedure
CREATE OR REPLACE FUNCTION capture_daily_baseline() 
RETURNS VOID AS $$
DECLARE
  baseline_record performance_baselines%ROWTYPE;
BEGIN
  -- Calculate performance metrics
  SELECT 
    CURRENT_DATE,
    COALESCE(AVG(execution_time_ms), 0),
    (SELECT COUNT(*) FROM booking),
    (SELECT COUNT(*) FROM flights),
    (SELECT COUNT(DISTINCT gp_segment_id) FROM gp_dist_random('booking')),
    85.0,  -- Estimated compression ratio
    (SELECT pg_total_relation_size('booking') / 1024 / 1024)
  INTO baseline_record
  FROM query_performance_log 
  WHERE log_timestamp >= CURRENT_DATE - INTERVAL '1 day';
  
  -- Insert or update baseline
  INSERT INTO performance_baselines VALUES (baseline_record.*)
  ON CONFLICT (baseline_date) 
  DO UPDATE SET 
    avg_query_time_ms = EXCLUDED.avg_query_time_ms,
    total_bookings = EXCLUDED.total_bookings,
    total_flights = EXCLUDED.total_flights,
    segment_utilization = EXCLUDED.segment_utilization,
    compression_ratio = EXCLUDED.compression_ratio,
    largest_table_size_mb = EXCLUDED.largest_table_size_mb;
    
  -- Cleanup old baselines (keep 90 days)
  DELETE FROM performance_baselines 
  WHERE baseline_date < CURRENT_DATE - INTERVAL '90 days';
END;
$$ LANGUAGE plpgsql;

-- Schedule baseline collection (call from cron or scheduler)
-- SELECT capture_daily_baseline();
```

#### Resource Queue Management
```sql
-- Create resource queues for workload management
CREATE RESOURCE QUEUE analytics_queue WITH (
  ACTIVE_STATEMENTS=10,
  MEMORY_LIMIT='2GB',
  COST_OVERCOMMIT=FALSE,
  MIN_COST=100.0
);

CREATE RESOURCE QUEUE etl_queue WITH (
  ACTIVE_STATEMENTS=3,
  MEMORY_LIMIT='4GB', 
  COST_OVERCOMMIT=FALSE,
  MIN_COST=1000.0
);

CREATE RESOURCE QUEUE dashboard_queue WITH (
  ACTIVE_STATEMENTS=50,
  MEMORY_LIMIT='1GB',
  COST_OVERCOMMIT=TRUE,
  MIN_COST=10.0
);

-- Assign users to appropriate queues
ALTER ROLE airline_analyst SET RESOURCE_QUEUE analytics_queue;
ALTER ROLE airline_etl SET RESOURCE_QUEUE etl_queue;
ALTER ROLE airline_readonly SET RESOURCE_QUEUE dashboard_queue;

-- Monitor resource queue usage
CREATE VIEW resource_queue_monitor AS
SELECT 
  rsqname as queue_name,
  rsqcountlimit as max_active_statements,
  rsqcostlimit as cost_limit,
  rsqmemorylimit as memory_limit,
  rsqwaiters as current_waiters,
  rsqholders as current_active
FROM pg_resqueue_status
ORDER BY rsqwaiters DESC;
```

### Backup and Recovery Strategies

#### Continuous WAL Archiving Setup
```bash
# Configure WAL archiving for point-in-time recovery
gpconfig -c wal_level -v replica
gpconfig -c archive_mode -v on
gpconfig -c archive_command -v 'rsync %p backup_server:/backup/wal_archive/%f'
gpconfig -c max_wal_senders -v 5
gpconfig -c wal_keep_segments -v 64

# Restart cluster to apply WAL settings
gpstop -ar
```

#### Automated Backup Procedures
```bash
#!/bin/bash
# cloudberry_backup.sh - Automated backup script for airline demo

BACKUP_DIR="/backup/cloudberry/airline_demo"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="airline_demo_${TIMESTAMP}"

# Create backup directory
mkdir -p ${BACKUP_DIR}/${BACKUP_NAME}

# Full database backup
echo "Starting full backup at $(date)"
pg_dump -h localhost -p 7000 -U cbadmin -d airline_demo \
  --format=custom \
  --compress=9 \
  --file=${BACKUP_DIR}/${BACKUP_NAME}/airline_demo.backup

# Backup table-specific data for fast restoration
pg_dump -h localhost -p 7000 -U cbadmin -d airline_demo \
  --format=custom \
  --table=booking \
  --file=${BACKUP_DIR}/${BACKUP_NAME}/booking_only.backup

# Export schema only for quick recreation
pg_dump -h localhost -p 7000 -U cbadmin -d airline_demo \
  --schema-only \
  --file=${BACKUP_DIR}/${BACKUP_NAME}/schema_only.sql

# Compress backup directory
tar -czf ${BACKUP_DIR}/${BACKUP_NAME}.tar.gz -C ${BACKUP_DIR} ${BACKUP_NAME}
rm -rf ${BACKUP_DIR}/${BACKUP_NAME}

# Cleanup old backups (keep 30 days)
find ${BACKUP_DIR} -name "airline_demo_*.tar.gz" -mtime +30 -delete

echo "Backup completed at $(date): ${BACKUP_DIR}/${BACKUP_NAME}.tar.gz"
```

### Performance Testing & Validation

#### Benchmarking Scripts for Production Validation
```sql
-- Performance validation test suite
CREATE OR REPLACE FUNCTION run_performance_benchmark() 
RETURNS TABLE (
  test_name TEXT,
  execution_time_ms NUMERIC,
  rows_processed BIGINT,
  status TEXT
) AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  row_count BIGINT;
BEGIN
  -- Test 1: Simple COUNT query
  start_time := clock_timestamp();
  SELECT COUNT(*) INTO row_count FROM booking;
  end_time := clock_timestamp();
  
  RETURN QUERY SELECT 
    'Simple Count'::TEXT,
    EXTRACT(epoch FROM (end_time - start_time)) * 1000,
    row_count,
    CASE WHEN EXTRACT(epoch FROM (end_time - start_time)) < 1.0 THEN 'PASS' ELSE 'SLOW' END;
  
  -- Test 2: Complex join query
  start_time := clock_timestamp();
  SELECT COUNT(*) INTO row_count 
  FROM booking b 
  JOIN flights f ON b.flight_id = f.flight_id 
  JOIN passenger p ON b.passenger_id = p.passenger_id;
  end_time := clock_timestamp();
  
  RETURN QUERY SELECT 
    'Complex Join'::TEXT,
    EXTRACT(epoch FROM (end_time - start_time)) * 1000,
    row_count,
    CASE WHEN EXTRACT(epoch FROM (end_time - start_time)) < 5.0 THEN 'PASS' ELSE 'SLOW' END;
    
  -- Test 3: Aggregation query
  start_time := clock_timestamp();
  SELECT COUNT(DISTINCT origin) INTO row_count FROM flights;
  end_time := clock_timestamp();
  
  RETURN QUERY SELECT 
    'Aggregation'::TEXT,
    EXTRACT(epoch FROM (end_time - start_time)) * 1000,
    row_count,
    CASE WHEN EXTRACT(epoch FROM (end_time - start_time)) < 2.0 THEN 'PASS' ELSE 'SLOW' END;
END;
$$ LANGUAGE plpgsql;

-- Run benchmark and review results
SELECT * FROM run_performance_benchmark();
```

## Extended Query Gallery

### Time-Series Analytics

#### Rolling Window Calculations for Booking Trends
```sql
-- 7-day rolling average of bookings with trend analysis
WITH daily_bookings AS (
  SELECT 
    DATE(booking_date) as booking_day,
    COUNT(*) as daily_count
  FROM booking
  WHERE booking_date >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY DATE(booking_date)
),
rolling_metrics AS (
  SELECT 
    booking_day,
    daily_count,
    AVG(daily_count) OVER (
      ORDER BY booking_day 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7day_avg,
    LAG(daily_count, 7) OVER (ORDER BY booking_day) as same_day_last_week,
    FIRST_VALUE(daily_count) OVER (
      ORDER BY booking_day 
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as period_min,
    LAST_VALUE(daily_count) OVER (
      ORDER BY booking_day 
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as period_max
  FROM daily_bookings
)
SELECT 
  booking_day,
  daily_count,
  ROUND(rolling_7day_avg, 2) as rolling_average,
  ROUND(
    CASE 
      WHEN same_day_last_week > 0 
      THEN ((daily_count - same_day_last_week) * 100.0 / same_day_last_week) 
      ELSE NULL 
    END, 2
  ) as week_over_week_pct,
  CASE 
    WHEN daily_count > rolling_7day_avg * 1.2 THEN 'HIGH'
    WHEN daily_count < rolling_7day_avg * 0.8 THEN 'LOW' 
    ELSE 'NORMAL'
  END as volume_category
FROM rolling_metrics
ORDER BY booking_day DESC;
```

#### Seasonal Analysis of Flight Patterns
```sql
-- Comprehensive seasonal analysis with multiple time dimensions
WITH flight_seasonality AS (
  SELECT 
    EXTRACT(month FROM departure_time) as month,
    EXTRACT(dow FROM departure_time) as day_of_week,  -- 0=Sunday
    EXTRACT(hour FROM departure_time) as hour_of_day,
    origin,
    destination,
    COUNT(*) as flight_count,
    COUNT(DISTINCT DATE(departure_time)) as days_operated,
    AVG(EXTRACT(epoch FROM arrival_time - departure_time)/3600) as avg_duration_hours
  FROM flights f
  JOIN booking b ON f.flight_id = b.flight_id
  GROUP BY EXTRACT(month FROM departure_time),
           EXTRACT(dow FROM departure_time),
           EXTRACT(hour FROM departure_time),
           origin, destination
),
seasonal_trends AS (
  SELECT 
    month,
    CASE 
      WHEN month IN (12, 1, 2) THEN 'Winter'
      WHEN month IN (3, 4, 5) THEN 'Spring'
      WHEN month IN (6, 7, 8) THEN 'Summer'
      ELSE 'Fall'
    END as season,
    CASE 
      WHEN day_of_week IN (0, 6) THEN 'Weekend'
      ELSE 'Weekday'
    END as day_type,
    CASE 
      WHEN hour_of_day BETWEEN 6 AND 11 THEN 'Morning'
      WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon'
      WHEN hour_of_day BETWEEN 18 AND 22 THEN 'Evening'
      ELSE 'Night'
    END as time_period,
    origin,
    destination,
    SUM(flight_count) as total_flights,
    AVG(avg_duration_hours) as avg_duration
  FROM flight_seasonality
  GROUP BY month, day_of_week, hour_of_day, origin, destination
)
SELECT 
  season,
  day_type,
  time_period,
  origin || ' → ' || destination as route,
  total_flights,
  ROUND(avg_duration, 2) as avg_hours,
  RANK() OVER (PARTITION BY season, day_type ORDER BY total_flights DESC) as popularity_rank
FROM seasonal_trends
WHERE total_flights >= 5
ORDER BY season, day_type, popularity_rank
LIMIT 50;
```

#### Predictive Analytics Using Window Functions
```sql
-- Booking lead time prediction and passenger behavior analysis
WITH passenger_behavior AS (
  SELECT 
    p.passenger_id,
    p.first_name || ' ' || p.last_name as passenger_name,
    f.departure_time,
    b.booking_date,
    EXTRACT(epoch FROM f.departure_time - b.booking_date)/86400 as lead_days,
    f.origin,
    f.destination,
    ROW_NUMBER() OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as booking_sequence,
    LAG(EXTRACT(epoch FROM f.departure_time - b.booking_date)/86400) 
      OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as previous_lead_days,
    LAG(f.destination) 
      OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as previous_destination
  FROM passenger p
  JOIN booking b ON p.passenger_id = b.passenger_id
  JOIN flights f ON b.flight_id = f.flight_id
),
behavior_patterns AS (
  SELECT 
    passenger_id,
    passenger_name,
    COUNT(*) as total_bookings,
    AVG(lead_days) as avg_lead_days,
    STDDEV(lead_days) as lead_days_stddev,
    MIN(lead_days) as min_lead_days,
    MAX(lead_days) as max_lead_days,
    COUNT(CASE WHEN origin = previous_destination THEN 1 END) as connecting_flights,
    COUNT(DISTINCT origin) as origins_used,
    COUNT(DISTINCT destination) as destinations_used,
    -- Predict next booking lead time using linear regression approximation
    CASE 
      WHEN STDDEV(lead_days) > 10 THEN 'Unpredictable'
      WHEN AVG(lead_days) > 21 THEN 'Planner'
      WHEN AVG(lead_days) < 7 THEN 'Last-Minute'
      ELSE 'Moderate'
    END as booking_personality
  FROM passenger_behavior
  WHERE booking_sequence > 1  -- Only analyze repeat customers
  GROUP BY passenger_id, passenger_name
)
SELECT 
  booking_personality,
  COUNT(*) as passenger_count,
  ROUND(AVG(avg_lead_days), 1) as avg_lead_time,
  ROUND(AVG(total_bookings), 1) as avg_bookings_per_passenger,
  ROUND(AVG(connecting_flights::numeric / total_bookings * 100), 1) as connection_rate_pct,
  ROUND(AVG(destinations_used), 1) as avg_destinations_visited
FROM behavior_patterns
GROUP BY booking_personality
ORDER BY passenger_count DESC;
```

### Graph-Style Queries

#### Route Network Analysis
```sql
-- Advanced graph analysis of airline route network
WITH route_graph AS (
  SELECT 
    f.origin as source_airport,
    f.destination as dest_airport,
    COUNT(*) as flight_frequency,
    COUNT(DISTINCT b.passenger_id) as passenger_count,
    AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_duration
  FROM flights f
  JOIN booking b ON f.flight_id = b.flight_id
  GROUP BY f.origin, f.destination
),
airport_metrics AS (
  -- Calculate hub metrics for each airport
  SELECT 
    source_airport as airport,
    COUNT(*) as outbound_routes,
    SUM(flight_frequency) as outbound_flights,
    SUM(passenger_count) as outbound_passengers,
    AVG(avg_duration) as avg_outbound_duration
  FROM route_graph
  GROUP BY source_airport
  
  UNION ALL
  
  SELECT 
    dest_airport as airport,
    COUNT(*) as inbound_routes,
    SUM(flight_frequency) as inbound_flights, 
    SUM(passenger_count) as inbound_passengers,
    AVG(avg_duration) as avg_inbound_duration
  FROM route_graph
  GROUP BY dest_airport
),
hub_analysis AS (
  SELECT 
    airport,
    SUM(outbound_routes + inbound_routes) as total_connectivity,
    SUM(outbound_flights + inbound_flights) as total_traffic,
    SUM(outbound_passengers + inbound_passengers) as total_passengers,
    AVG(avg_outbound_duration) as avg_flight_duration
  FROM airport_metrics
  GROUP BY airport
),
route_importance AS (
  SELECT 
    rg.*,
    ha_source.total_connectivity as source_hub_score,
    ha_dest.total_connectivity as dest_hub_score,
    rg.flight_frequency * rg.passenger_count as route_importance_score
  FROM route_graph rg
  JOIN hub_analysis ha_source ON rg.source_airport = ha_source.airport
  JOIN hub_analysis ha_dest ON rg.dest_airport = ha_dest.airport
)
SELECT 
  source_airport || ' ↔ ' || dest_airport as route,
  flight_frequency,
  passenger_count,
  ROUND(avg_duration, 2) as avg_hours,
  route_importance_score,
  CASE 
    WHEN source_hub_score > 1000 AND dest_hub_score > 1000 THEN 'Hub-to-Hub'
    WHEN source_hub_score > 1000 OR dest_hub_score > 1000 THEN 'Hub-to-Spoke'
    ELSE 'Spoke-to-Spoke'
  END as route_type,
  RANK() OVER (ORDER BY route_importance_score DESC) as importance_rank
FROM route_importance
ORDER BY route_importance_score DESC
LIMIT 20;
```

#### Passenger Journey Mapping with Path Analysis
```sql
-- Multi-city journey analysis with path finding
WITH passenger_paths AS (
  SELECT 
    p.passenger_id,
    p.first_name || ' ' || p.last_name as passenger_name,
    f.departure_time,
    f.origin,
    f.destination,
    LAG(f.destination) OVER (
      PARTITION BY p.passenger_id 
      ORDER BY f.departure_time
    ) as previous_destination,
    LAG(f.departure_time) OVER (
      PARTITION BY p.passenger_id 
      ORDER BY f.departure_time  
    ) as previous_departure,
    LEAD(f.origin) OVER (
      PARTITION BY p.passenger_id 
      ORDER BY f.departure_time
    ) as next_origin,
    ROW_NUMBER() OVER (
      PARTITION BY p.passenger_id 
      ORDER BY f.departure_time
    ) as leg_number
  FROM passenger p
  JOIN booking b ON p.passenger_id = b.passenger_id
  JOIN flights f ON b.flight_id = f.flight_id
),
journey_segments AS (
  SELECT 
    passenger_id,
    passenger_name,
    leg_number,
    origin,
    destination,
    departure_time,
    -- Detect connecting flights (same destination as next origin within 24 hours)
    CASE 
      WHEN origin = previous_destination 
      AND departure_time - previous_departure <= INTERVAL '24 hours'
      THEN 'CONNECTING'
      ELSE 'NEW_JOURNEY'
    END as segment_type,
    -- Calculate layover time for connections
    CASE 
      WHEN origin = previous_destination 
      THEN EXTRACT(epoch FROM departure_time - previous_departure)/3600
      ELSE NULL
    END as layover_hours
  FROM passenger_paths
),
complete_journeys AS (
  SELECT 
    passenger_id,
    passenger_name,
    COUNT(*) as total_segments,
    COUNT(CASE WHEN segment_type = 'CONNECTING' THEN 1 END) as connecting_segments,
    MIN(departure_time) as journey_start,
    MAX(departure_time) as journey_end,
    STRING_AGG(
      CASE WHEN segment_type = 'NEW_JOURNEY' THEN '| ' ELSE '' END ||
      origin || '→' || destination, 
      ' ' ORDER BY leg_number
    ) as full_journey_path,
    AVG(layover_hours) as avg_layover_hours,
    COUNT(DISTINCT origin) as unique_origins,
    COUNT(DISTINCT destination) as unique_destinations
  FROM journey_segments
  GROUP BY passenger_id, passenger_name
)
SELECT 
  passenger_name,
  total_segments,
  connecting_segments,
  full_journey_path,
  ROUND(avg_layover_hours, 1) as avg_layover_hours,
  unique_origins,
  unique_destinations,
  CASE 
    WHEN connecting_segments > 0 THEN 'Multi-City Traveler'
    WHEN total_segments > 2 THEN 'Frequent Traveler'
    WHEN total_segments = 2 THEN 'Round-Trip Traveler'
    ELSE 'One-Way Traveler'
  END as traveler_type,
  EXTRACT(epoch FROM journey_end - journey_start)/86400 as total_journey_days
FROM complete_journeys
WHERE total_segments > 1
ORDER BY connecting_segments DESC, total_segments DESC
LIMIT 30;
```

#### Hub Connectivity Analysis
```sql
-- Advanced hub analysis with centrality measures
WITH airport_connections AS (
  SELECT 
    f.origin as airport,
    f.destination as connected_airport,
    COUNT(*) as direct_flights,
    COUNT(DISTINCT b.passenger_id) as passengers_served,
    AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_flight_time
  FROM flights f
  JOIN booking b ON f.flight_id = b.flight_id
  GROUP BY f.origin, f.destination
  
  UNION
  
  SELECT 
    f.destination as airport,
    f.origin as connected_airport,
    COUNT(*) as direct_flights,
    COUNT(DISTINCT b.passenger_id) as passengers_served,
    AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_flight_time
  FROM flights f
  JOIN booking b ON f.flight_id = b.flight_id
  GROUP BY f.destination, f.origin
),
airport_centrality AS (
  SELECT 
    airport,
    COUNT(DISTINCT connected_airport) as direct_connections,
    SUM(direct_flights) as total_flight_volume,
    SUM(passengers_served) as total_passengers,
    AVG(avg_flight_time) as avg_connection_time,
    -- Degree centrality (number of direct connections)
    COUNT(DISTINCT connected_airport) as degree_centrality,
    -- Weighted centrality (connections weighted by passenger volume)
    SUM(passengers_served * direct_flights) as weighted_centrality
  FROM airport_connections
  GROUP BY airport
),
two_hop_connections AS (
  -- Calculate airports reachable in 2 hops (hub efficiency measure)
  SELECT 
    ac1.airport,
    COUNT(DISTINCT ac2.connected_airport) as two_hop_reachable
  FROM airport_connections ac1
  JOIN airport_connections ac2 ON ac1.connected_airport = ac2.airport
  WHERE ac1.airport != ac2.connected_airport
  GROUP BY ac1.airport
)
SELECT 
  ac.airport,
  ac.direct_connections,
  ac.total_flight_volume,
  ac.total_passengers,
  ROUND(ac.avg_connection_time, 2) as avg_hours,
  thc.two_hop_reachable,
  -- Hub efficiency score
  ROUND(
    (ac.degree_centrality * 0.4 + 
     thc.two_hop_reachable * 0.3 + 
     (ac.total_passengers / 1000.0) * 0.3), 2
  ) as hub_efficiency_score,
  CASE 
    WHEN ac.degree_centrality >= 15 THEN 'Major Hub'
    WHEN ac.degree_centrality >= 8 THEN 'Regional Hub'
    WHEN ac.degree_centrality >= 4 THEN 'Focus City'
    ELSE 'Spoke'
  END as airport_classification
FROM airport_centrality ac
LEFT JOIN two_hop_connections thc ON ac.airport = thc.airport
ORDER BY hub_efficiency_score DESC;
```

### Advanced Analytical Patterns

#### Customer Lifetime Value Analysis
```sql
-- CLV calculation with MPP-optimized window functions
WITH passenger_metrics AS (
  SELECT 
    p.passenger_id,
    p.first_name || ' ' || p.last_name as passenger_name,
    COUNT(*) as total_bookings,
    MIN(b.booking_date) as first_booking,
    MAX(b.booking_date) as last_booking,
    AVG(EXTRACT(epoch FROM f.departure_time - b.booking_date)/86400) as avg_lead_time,
    COUNT(DISTINCT f.origin) as origins_used,
    COUNT(DISTINCT f.destination) as destinations_used,
    COUNT(DISTINCT DATE(f.departure_time)) as travel_days,
    SUM(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as total_flight_hours
  FROM passenger p
  JOIN booking b ON p.passenger_id = b.passenger_id
  JOIN flights f ON b.flight_id = f.flight_id
  GROUP BY p.passenger_id, p.first_name, p.last_name
),
value_calculations AS (
  SELECT 
    *,
    EXTRACT(epoch FROM last_booking - first_booking)/86400 as customer_lifetime_days,
    total_bookings::numeric / NULLIF(EXTRACT(epoch FROM last_booking - first_booking)/86400, 0) as booking_frequency,
    -- Estimated revenue per booking (airline industry average: $200-400)
    total_bookings * 300 as estimated_revenue,
    CASE 
      WHEN total_bookings >= 5 THEN 'High Value'
      WHEN total_bookings >= 3 THEN 'Medium Value'
      WHEN total_bookings = 2 THEN 'Developing'
      ELSE 'One-Time'
    END as customer_segment,
    NTILE(5) OVER (ORDER BY total_bookings DESC) as value_quintile
  FROM passenger_metrics
  WHERE EXTRACT(epoch FROM last_booking - first_booking)/86400 > 0
)
SELECT 
  customer_segment,
  COUNT(*) as customer_count,
  ROUND(AVG(total_bookings), 2) as avg_bookings,
  ROUND(AVG(customer_lifetime_days), 0) as avg_lifetime_days,
  ROUND(AVG(booking_frequency) * 365, 2) as avg_annual_booking_rate,
  ROUND(AVG(estimated_revenue), 0) as avg_estimated_revenue,
  ROUND(AVG(total_flight_hours), 1) as avg_total_flight_hours,
  ROUND(AVG(origins_used), 1) as avg_origins_per_customer,
  ROUND(AVG(destinations_used), 1) as avg_destinations_per_customer,
  -- Customer value score
  ROUND(
    AVG(total_bookings) * AVG(booking_frequency) * 100, 2
  ) as customer_value_score
FROM value_calculations
GROUP BY customer_segment
ORDER BY customer_value_score DESC;
```

#### Market Basket Analysis for Route Combinations
```sql
-- Route combination analysis (which routes are frequently booked together)
WITH passenger_routes AS (
  SELECT 
    p.passenger_id,
    f.origin || '→' || f.destination as route,
    COUNT(*) as route_frequency
  FROM passenger p
  JOIN booking b ON p.passenger_id = b.passenger_id
  JOIN flights f ON b.flight_id = f.flight_id
  GROUP BY p.passenger_id, f.origin, f.destination
),
route_combinations AS (
  SELECT 
    pr1.route as route_a,
    pr2.route as route_b,
    COUNT(*) as combination_frequency,
    COUNT(DISTINCT pr1.passenger_id) as unique_passengers
  FROM passenger_routes pr1
  JOIN passenger_routes pr2 ON pr1.passenger_id = pr2.passenger_id
  WHERE pr1.route < pr2.route  -- Avoid duplicate combinations
  GROUP BY pr1.route, pr2.route
),
market_basket_analysis AS (
  SELECT 
    route_a,
    route_b,
    combination_frequency,
    unique_passengers,
    -- Support: How often the combination appears
    combination_frequency::numeric / (SELECT COUNT(DISTINCT passenger_id) FROM passenger_routes) as support,
    -- Calculate confidence: P(route_b | route_a)
    combination_frequency::numeric / (
      SELECT COUNT(DISTINCT passenger_id) 
      FROM passenger_routes 
      WHERE route = route_a
    ) as confidence_a_to_b,
    -- Calculate confidence: P(route_a | route_b)  
    combination_frequency::numeric / (
      SELECT COUNT(DISTINCT passenger_id)
      FROM passenger_routes
      WHERE route = route_b  
    ) as confidence_b_to_a
  FROM route_combinations
)
SELECT 
  route_a,
  route_b,
  combination_frequency,
  unique_passengers,
  ROUND(support * 100, 3) as support_pct,
  ROUND(confidence_a_to_b * 100, 2) as confidence_a_to_b_pct,
  ROUND(confidence_b_to_a * 100, 2) as confidence_b_to_a_pct,
  -- Lift: How much more likely the combination is than random
  ROUND(
    support / (
      (SELECT COUNT(DISTINCT passenger_id) FROM passenger_routes WHERE route = route_a)::numeric / 
      (SELECT COUNT(DISTINCT passenger_id) FROM passenger_routes) *
      (SELECT COUNT(DISTINCT passenger_id) FROM passenger_routes WHERE route = route_b)::numeric /
      (SELECT COUNT(DISTINCT passenger_id) FROM passenger_routes)
    ), 2
  ) as lift_score
FROM market_basket_analysis
WHERE combination_frequency >= 3  -- Minimum support threshold
ORDER BY lift_score DESC, combination_frequency DESC
LIMIT 25;
```

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