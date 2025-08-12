-- ============================================================================
-- Apache Cloudberry (Incubating) - Airline Reservations System Demo
-- ============================================================================
-- This demo showcases Apache Cloudberry's MPP query processing capabilities
-- using a fictional airline reservations schema with realistic data patterns.
--
-- Key Features Demonstrated:
-- - MPP table design with appropriate distribution keys
-- - ORCA optimizer behavior and parallel execution
-- - Join operations with Motion operations
-- - Aggregation and window functions across segments
-- - Query plan analysis and optimization hints
-- ============================================================================

-- Clean up any existing objects
DROP TABLE IF EXISTS booking CASCADE;
DROP TABLE IF EXISTS flights CASCADE;
DROP TABLE IF EXISTS passenger CASCADE;

\echo '===================='
\echo 'SCHEMA SETUP'
\echo '===================='

-- Create passenger table
-- DISTRIBUTED BY passenger_id for even distribution across segments
-- Uses SERIAL for auto-incrementing primary key
CREATE TABLE passenger (
    passenger_id SERIAL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
DISTRIBUTED BY (passenger_id);

\echo 'Created passenger table with appendonly storage, zstd compression level 5'
\echo 'Distributed by passenger_id for optimal MPP parallelism'

-- Create flights table
-- DISTRIBUTED BY flight_id for balanced segment distribution
-- Includes realistic airline schedule fields
CREATE TABLE flights (
    flight_id SERIAL PRIMARY KEY,
    flight_number TEXT NOT NULL,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    departure_time TIMESTAMP NOT NULL,
    arrival_time TIMESTAMP NOT NULL
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
DISTRIBUTED BY (flight_id);

\echo 'Created flights table with appendonly storage, zstd compression level 5'
\echo 'Distributed by flight_id for optimal join performance'

-- Create booking table
-- DISTRIBUTED BY booking_id for even distribution
-- Links passengers to flights with booking metadata
CREATE TABLE booking (
    booking_id SERIAL,
    passenger_id INT NOT NULL,
    flight_id INT NOT NULL,
    booking_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    seat_number TEXT NOT NULL
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=5)
DISTRIBUTED BY (booking_id);

\echo 'Created booking table with appendonly storage, zstd compression level 5'
\echo 'Distributed by booking_id for balanced workload distribution'

-- Add foreign key constraints (informational - not enforced in Cloudberry)
-- These help the ORCA optimizer understand relationships
ALTER TABLE booking ADD CONSTRAINT fk_passenger 
    FOREIGN KEY (passenger_id) REFERENCES passenger(passenger_id);
ALTER TABLE booking ADD CONSTRAINT fk_flight 
    FOREIGN KEY (flight_id) REFERENCES flights(flight_id);

\echo '===================='
\echo 'DATA LOADING'
\echo '===================='

-- Generate synthetic passenger data (10,000 passengers)
INSERT INTO passenger (first_name, last_name, email, phone)
SELECT 
    (ARRAY['John', 'Jane', 'Michael', 'Sarah', 'David', 'Lisa', 'Robert', 'Emily', 'James', 'Jessica', 
           'William', 'Ashley', 'Christopher', 'Amanda', 'Daniel', 'Melissa', 'Matthew', 'Deborah',
           'Anthony', 'Dorothy', 'Mark', 'Amy', 'Donald', 'Angela', 'Steven', 'Helen', 'Paul', 'Brenda',
           'Andrew', 'Emma', 'Joshua', 'Olivia', 'Kenneth', 'Cynthia', 'Kevin', 'Marie', 'Brian', 'Janet',
           'George', 'Catherine', 'Timothy', 'Frances', 'Ronald', 'Christine', 'Jason', 'Samantha',
           'Edward', 'Debra', 'Jeffrey', 'Rachel'])[1 + (random() * 49)::int] as first_name,
    (ARRAY['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
           'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
           'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
           'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
           'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell', 'Carter', 'Roberts'])[1 + (random() * 49)::int] as last_name,
    'user' || generate_series || '@airline-demo.com' as email,
    '+1-' || (100 + (random() * 899)::int)::text || '-' || 
    (100 + (random() * 899)::int)::text || '-' || 
    (1000 + (random() * 8999)::int)::text as phone
FROM generate_series(1, 10000);

\echo 'Loaded 10,000 synthetic passengers with realistic names and contact info'

-- Generate flights data (750 flights over 30 days)
-- Uses major US airport codes for realistic flight patterns
INSERT INTO flights (flight_number, origin, destination, departure_time, arrival_time)
SELECT 
    'AA' || (1000 + (random() * 8999)::int)::text as flight_number,
    (ARRAY['JFK', 'LAX', 'ORD', 'DFW', 'DEN', 'ATL', 'SFO', 'SEA', 'LAS', 'MCO',
           'EWR', 'CLT', 'PHX', 'IAH', 'MIA', 'BOS', 'MSP', 'DTW', 'PHL', 'LGA',
           'FLL', 'BWI', 'IAD', 'MDW', 'TPA', 'SAN', 'HNL', 'PDX', 'STL', 'AUS'])[1 + (random() * 29)::int] as origin,
    (ARRAY['JFK', 'LAX', 'ORD', 'DFW', 'DEN', 'ATL', 'SFO', 'SEA', 'LAS', 'MCO',
           'EWR', 'CLT', 'PHX', 'IAH', 'MIA', 'BOS', 'MSP', 'DTW', 'PHL', 'LGA',
           'FLL', 'BWI', 'IAD', 'MDW', 'TPA', 'SAN', 'HNL', 'PDX', 'STL', 'AUS'])[1 + (random() * 29)::int] as destination,
    CURRENT_DATE + (random() * 30)::int + (random() * 24)::int * INTERVAL '1 hour' as departure_time,
    CURRENT_DATE + (random() * 30)::int + (random() * 24)::int * INTERVAL '1 hour' + 
    (1 + random() * 8) * INTERVAL '1 hour' as arrival_time
FROM generate_series(1, 750)
WHERE origin != destination; -- Ensure no same-city flights

\echo 'Loaded 750 flights across major US airports over 30-day window'

-- Generate booking data (15,000 bookings, 1-3 per passenger)
INSERT INTO booking (passenger_id, flight_id, booking_date, seat_number)
SELECT 
    p.passenger_id,
    f.flight_id,
    f.departure_time - (random() * 30)::int * INTERVAL '1 day' as booking_date,
    (ARRAY['A', 'B', 'C', 'D', 'E', 'F'])[1 + (random() * 5)::int] || 
    (1 + (random() * 35)::int)::text as seat_number
FROM passenger p
CROSS JOIN LATERAL (
    SELECT flight_id 
    FROM flights 
    ORDER BY random() 
    LIMIT 1 + (random() * 2)::int  -- 1-3 bookings per passenger
) f;

\echo 'Generated realistic booking data linking passengers to flights'
\echo 'Each passenger has 1-3 random flight bookings with seat assignments'

-- Display data summary
\echo '===================='
\echo 'DATA SUMMARY'
\echo '===================='

SELECT 'Passengers' as table_name, count(*) as row_count FROM passenger
UNION ALL
SELECT 'Flights', count(*) FROM flights
UNION ALL  
SELECT 'Bookings', count(*) FROM booking
ORDER BY table_name;

\echo '===================='
\echo 'APACHE CLOUDBERRY MPP QUERY DEMONSTRATIONS'
\echo '===================='

\echo 'Query 1: Basic JOIN with EXPLAIN - Shows Motion operations'
\echo 'This demonstrates how Cloudberry redistributes data across segments for joins'

EXPLAIN (COSTS OFF, VERBOSE)
SELECT 
    b.booking_id,
    p.first_name || ' ' || p.last_name as passenger_name,
    f.flight_number,
    f.origin,
    f.destination,
    f.departure_time,
    b.seat_number
FROM booking b
JOIN passenger p ON b.passenger_id = p.passenger_id
JOIN flights f ON b.flight_id = f.flight_id
ORDER BY f.departure_time
LIMIT 10;

\echo 'Notice the Motion operations above - these show data redistribution between segments'
\echo 'The ORCA optimizer automatically determines optimal join strategies'

\echo '===================='
\echo 'Sample Query Results:'

SELECT 
    b.booking_id,
    p.first_name || ' ' || p.last_name as passenger_name,
    f.flight_number,
    f.origin,
    f.destination,
    f.departure_time,
    b.seat_number
FROM booking b
JOIN passenger p ON b.passenger_id = p.passenger_id
JOIN flights f ON b.flight_id = f.flight_id
ORDER BY f.departure_time
LIMIT 10;

\echo '===================='
\echo 'Query 2: Aggregation Demo - Parallel execution across segments'
\echo 'Shows how Cloudberry distributes GROUP BY operations'

EXPLAIN (COSTS OFF, VERBOSE)
SELECT 
    f.destination,
    COUNT(*) as total_bookings,
    COUNT(DISTINCT b.passenger_id) as unique_passengers,
    MIN(f.departure_time) as earliest_flight,
    MAX(f.departure_time) as latest_flight
FROM flights f
JOIN booking b ON f.flight_id = b.flight_id
GROUP BY f.destination
HAVING COUNT(*) > 10
ORDER BY total_bookings DESC;

\echo 'Sample Results:'
SELECT 
    f.destination,
    COUNT(*) as total_bookings,
    COUNT(DISTINCT b.passenger_id) as unique_passengers,
    MIN(f.departure_time) as earliest_flight,
    MAX(f.departure_time) as latest_flight
FROM flights f
JOIN booking b ON f.flight_id = b.flight_id
GROUP BY f.destination
HAVING COUNT(*) > 10
ORDER BY total_bookings DESC
LIMIT 10;

\echo '===================='
\echo 'Query 3: CTE with Window Function - Advanced MPP Processing'
\echo 'Demonstrates Common Table Expressions and analytic functions'

EXPLAIN (COSTS OFF, VERBOSE)
WITH passenger_flight_summary AS (
    SELECT 
        p.passenger_id,
        p.first_name || ' ' || p.last_name as passenger_name,
        COUNT(*) as flight_count,
        MIN(f.departure_time) as first_flight,
        MAX(f.departure_time) as last_flight
    FROM passenger p
    JOIN booking b ON p.passenger_id = b.passenger_id
    JOIN flights f ON b.flight_id = f.flight_id
    GROUP BY p.passenger_id, p.first_name, p.last_name
)
SELECT 
    passenger_name,
    flight_count,
    first_flight,
    last_flight,
    RANK() OVER (ORDER BY flight_count DESC) as booking_rank,
    PERCENT_RANK() OVER (ORDER BY flight_count) as percentile_rank
FROM passenger_flight_summary
WHERE flight_count > 1
ORDER BY flight_count DESC;

\echo 'Sample Results - Top Frequent Travelers:'
WITH passenger_flight_summary AS (
    SELECT 
        p.passenger_id,
        p.first_name || ' ' || p.last_name as passenger_name,
        COUNT(*) as flight_count,
        MIN(f.departure_time) as first_flight,
        MAX(f.departure_time) as last_flight
    FROM passenger p
    JOIN booking b ON p.passenger_id = b.passenger_id
    JOIN flights f ON b.flight_id = f.flight_id
    GROUP BY p.passenger_id, p.first_name, p.last_name
)
SELECT 
    passenger_name,
    flight_count,
    first_flight,
    last_flight,
    RANK() OVER (ORDER BY flight_count DESC) as booking_rank,
    ROUND(PERCENT_RANK() OVER (ORDER BY flight_count)::numeric, 3) as percentile_rank
FROM passenger_flight_summary
WHERE flight_count > 1
ORDER BY flight_count DESC
LIMIT 15;

\echo '===================='
\echo 'Query 4: Optimizer Hints Demo - Force Different Scan Types'
\echo 'Shows how to influence Cloudberry ORCA optimizer behavior'

\echo 'Force Sequential Scan:'
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;

EXPLAIN (COSTS OFF)
SELECT * FROM passenger WHERE last_name = 'Smith';

\echo 'Enable Index Scans:'
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;
SET enable_seqscan = OFF;

EXPLAIN (COSTS OFF)
SELECT * FROM passenger WHERE last_name = 'Smith';

-- Reset to defaults
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET enable_seqscan;

\echo '===================='
\echo 'Query 5: Distribution Key Impact Analysis'
\echo 'Shows how distribution affects data movement in joins'

\echo 'Current join (distributed by different keys - requires redistribution):'
EXPLAIN (COSTS OFF, VERBOSE)
SELECT COUNT(*)
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id;

\echo 'Segment-level statistics to show data distribution:'
SELECT 
    schemaname,
    tablename,
    attname as distribution_key,
    n_distinct,
    correlation
FROM pg_stats 
WHERE schemaname = 'public' 
AND tablename IN ('passenger', 'flights', 'booking')
AND attname IN ('passenger_id', 'flight_id', 'booking_id')
ORDER BY tablename, attname;

\echo '===================='
\echo 'Query 6: Complex Route Analysis - Multi-table Join with Aggregation'
\echo 'Demonstrates Cloudberry handling complex analytical workloads'

SELECT 
    f.origin || ' → ' || f.destination as route,
    COUNT(*) as total_bookings,
    COUNT(DISTINCT DATE(f.departure_time)) as days_operated,
    AVG(EXTRACT(EPOCH FROM (f.arrival_time - f.departure_time))/3600) as avg_flight_hours,
    COUNT(DISTINCT b.passenger_id) as unique_passengers,
    ROUND(COUNT(*)::numeric / COUNT(DISTINCT b.passenger_id), 2) as avg_bookings_per_passenger
FROM flights f
JOIN booking b ON f.flight_id = b.flight_id
GROUP BY f.origin, f.destination
HAVING COUNT(*) >= 5
ORDER BY total_bookings DESC
LIMIT 10;

\echo '===================='
\echo 'PERFORMANCE INSIGHTS'
\echo '===================='

\echo 'Table sizes and compression effectiveness:'
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size
FROM pg_tables 
WHERE schemaname = 'public' 
AND tablename IN ('passenger', 'flights', 'booking')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

\echo 'Distribution quality (should be relatively even across segments):'
SELECT 
    'passenger' as table_name,
    gp_segment_id,
    count(*) as row_count
FROM gp_dist_random('passenger')
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

\echo '===================='
\echo 'DEMO SUMMARY'
\echo '===================='
\echo 'This Apache Cloudberry (Incubating) demo showcased:'
\echo '1. MPP table design with optimal distribution keys'
\echo '2. Appendonly tables with zstd compression for analytics workloads'
\echo '3. ORCA optimizer behavior with Motion operations'
\echo '4. Parallel execution of complex joins and aggregations'
\echo '5. Window functions and CTEs in distributed environment'
\echo '6. Query optimization techniques and hints'
\echo '7. Data distribution impact on join performance'
\echo ''
\echo 'Key Apache Cloudberry MPP Features Demonstrated:'
\echo '- Automatic data redistribution via Motion operations'
\echo '- Segment-parallel query execution'
\echo '- Advanced query optimization with ORCA'
\echo '- Columnar storage options for analytical workloads'
\echo '- Distributed aggregation and window functions'
\echo ''
\echo 'For more information about Apache Cloudberry (Incubating):'
\echo 'https://cloudberry.apache.org/'