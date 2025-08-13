-- ============================================================================
-- ORCA Optimizer Demonstrations  
-- ============================================================================
-- Examples of working with Apache Cloudberry's ORCA optimizer for
-- performance tuning and query plan analysis.

\echo 'ORCA Optimizer Demonstrations'
\echo '============================='

-- Check current optimizer setting
\echo 'Current optimizer configuration:'
SHOW optimizer;

\echo ''
\echo 'Query Plan Comparison: ORCA vs PostgreSQL Planner'
\echo '================================================='

-- Enable ORCA (default in Cloudberry)
SET optimizer = on;
\echo 'ORCA optimizer enabled - analyzing join query:'
EXPLAIN (ANALYZE OFF, COSTS OFF)
SELECT 
    p.first_name || ' ' || p.last_name as passenger_name,
    f.flight_number,
    f.origin,
    f.destination,
    b.seat_number
FROM booking b 
JOIN passenger p ON b.passenger_id = p.passenger_id
JOIN flights f ON b.flight_id = f.flight_id
WHERE f.origin = 'JFK'
LIMIT 10;

\echo ''
\echo 'PostgreSQL planner - same query:'
SET optimizer = off;
EXPLAIN (ANALYZE OFF, COSTS OFF)
SELECT 
    p.first_name || ' ' || p.last_name as passenger_name,
    f.flight_number,
    f.origin,
    f.destination,
    b.seat_number
FROM booking b 
JOIN passenger p ON b.passenger_id = p.passenger_id
JOIN flights f ON b.flight_id = f.flight_id
WHERE f.origin = 'JFK'
LIMIT 10;

-- Reset to ORCA
SET optimizer = on;

\echo ''
\echo 'Motion Operations Analysis:'
\echo '=========================='

-- Query that demonstrates Motion operations
\echo 'Analyzing data redistribution patterns:'
EXPLAIN (ANALYZE OFF, COSTS OFF, VERBOSE)
SELECT 
    f.destination,
    COUNT(*) as booking_count,
    AVG(EXTRACT(days FROM (f.departure_time - b.booking_date))) as avg_lead_time
FROM flights f
JOIN booking b ON f.flight_id = b.flight_id  
GROUP BY f.destination
HAVING COUNT(*) > 100
ORDER BY booking_count DESC;

\echo ''
\echo 'Statistics Quality Check:'
\echo '========================'

-- Check if tables have been analyzed
\echo 'Table analyze status:'
SELECT 
    schemaname,
    relname as tablename,
    last_analyze,
    CASE 
        WHEN last_analyze IS NULL THEN 'NEEDS ANALYZE'
        WHEN last_analyze < (CURRENT_TIMESTAMP - INTERVAL '1 day') THEN 'STALE STATS'
        ELSE 'CURRENT'
    END as status
FROM pg_stat_user_tables 
WHERE schemaname = 'public'
ORDER BY relname;

\echo ''
\echo 'Running ANALYZE to update statistics:'
ANALYZE passenger;
ANALYZE flights;
ANALYZE booking;

\echo ''
\echo 'ORCA Optimizer Best Practices:'
\echo '============================='

\echo 'Best Practice 1: Simple aggregation with ORCA'
EXPLAIN (ANALYZE OFF, COSTS OFF)
SELECT 
    origin,
    destination,
    COUNT(*) as flight_count,
    MIN(departure_time) as earliest_departure,
    MAX(departure_time) as latest_departure
FROM flights
GROUP BY origin, destination
ORDER BY flight_count DESC
LIMIT 20;

\echo ''
\echo 'Best Practice 2: Window function optimization'
EXPLAIN (ANALYZE OFF, COSTS OFF)
SELECT 
    passenger_id,
    booking_id,
    booking_date,
    ROW_NUMBER() OVER (PARTITION BY passenger_id ORDER BY booking_date) as booking_sequence
FROM booking
WHERE passenger_id <= 1000
ORDER BY passenger_id, booking_sequence;

\echo ''
\echo 'Best Practice 3: Efficient filtering with statistics'
EXPLAIN (ANALYZE OFF, COSTS OFF)
SELECT COUNT(*)
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id
WHERE f.departure_time >= CURRENT_DATE
  AND f.departure_time < CURRENT_DATE + INTERVAL '7 days';

\echo ''
\echo 'ORCA Configuration Settings:'
\echo '==========================='

-- Show ORCA-related configuration
\echo 'Key ORCA settings:'
SELECT name, setting, short_desc
FROM pg_settings 
WHERE name LIKE '%optimizer%' 
   OR name LIKE '%orca%'
ORDER BY name;

\echo ''
\echo 'Segment Distribution Analysis:'
\echo '============================'

-- Analyze how data is distributed for joins
\echo 'Join performance analysis - passenger distribution:'
SELECT gp_segment_id, COUNT(*) as passenger_count
FROM gp_dist_random('passenger') 
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

\echo ''
\echo 'Flight distribution across segments:'
SELECT gp_segment_id, COUNT(*) as flight_count
FROM gp_dist_random('flights') 
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

\echo ''
\echo 'Performance Tuning Tips:'
\echo '======================='
\echo '1. ORCA relies heavily on table statistics - run ANALYZE regularly'
\echo '2. Distribution keys should align with common join patterns'
\echo '3. Use EXPLAIN to understand Motion operations'
\echo '4. Monitor segment balance for optimal parallelism'
\echo '5. Consider partition pruning for time-series data'
\echo ''
\echo 'Query completed - ORCA optimizer analysis finished'