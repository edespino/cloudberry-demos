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
psql -h localhost -p 5432 -d your_database
\i airline-reservations-demo.sql

# Load the realistic data  
\i load_passengers.sql
\i load_flights.sql
\i load_bookings.sql
```

#### Method 2: Self-Contained SQL Demo
```bash
# Connect to your Apache Cloudberry instance
psql -h localhost -p 5432 -d your_database

# Run the complete demo with generated data
\i airline-reservations-demo.sql
```

#### Method 3: Basic CSV Generation
```bash
# Generate simple CSV files
python data-generator.py

# Load data into Cloudberry
psql -h localhost -p 5432 -d your_database
\COPY passenger FROM 'passengers.csv' CSV HEADER;
\COPY flights FROM 'flights.csv' CSV HEADER; 
\COPY booking FROM 'bookings.csv' CSV HEADER;
```

### Environment Configuration
```bash
# Optional: Set connection parameters
export CLOUDBERRY_HOST=your-host
export CLOUDBERRY_PORT=5432
export CLOUDBERRY_DB=your-database
export CLOUDBERRY_USER=your-user

# Then run demo
./run-demo.sh enhanced
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

### 4. Optimizer Control
```sql
-- Demonstrate ORCA optimizer hints
SET enable_indexscan = OFF;  -- Force sequential scan
SET enable_seqscan = OFF;    -- Force index scan
EXPLAIN SELECT * FROM passenger WHERE last_name = 'Smith';
```

## Apache Cloudberry MPP Features Highlighted

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

## Next Steps

After running this demo, explore:
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