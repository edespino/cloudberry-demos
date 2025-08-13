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
\echo 'SQL: SELECT p.first_name || '' '' || p.last_name as passenger_name, f.flight_number, f.origin, f.destination, b.seat_number'
\echo '     FROM booking b JOIN passenger p ON b.passenger_id = p.passenger_id JOIN flights f ON b.flight_id = f.flight_id'
\echo '     WHERE f.origin = ''JFK'' LIMIT 10;'
\echo ''

-- Enable ORCA (default in Cloudberry)
SET optimizer = on;
\echo 'WITH ORCA optimizer:'
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
\echo 'WITH PostgreSQL planner (same query):'
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
\echo 'SQL: SELECT f.destination, COUNT(*) as booking_count, AVG(EXTRACT(days FROM (f.departure_time - b.booking_date))) as avg_lead_time'
\echo '     FROM flights f JOIN booking b ON f.flight_id = b.flight_id GROUP BY f.destination'  
\echo '     HAVING COUNT(*) > 100 ORDER BY booking_count DESC;'
\echo ''

-- Query that demonstrates Motion operations
\echo 'Analyzing data redistribution patterns with VERBOSE output:'
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
\echo 'SQL: SELECT origin, destination, COUNT(*) as flight_count, MIN(departure_time), MAX(departure_time)'
\echo '     FROM flights GROUP BY origin, destination ORDER BY flight_count DESC LIMIT 20;'
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
\echo 'SQL: SELECT passenger_id, booking_id, booking_date, ROW_NUMBER() OVER (PARTITION BY passenger_id ORDER BY booking_date) as booking_sequence'
\echo '     FROM booking WHERE passenger_id <= 1000 ORDER BY passenger_id, booking_sequence;'
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
\echo 'SQL: SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id'
\echo '     WHERE f.departure_time >= CURRENT_DATE AND f.departure_time < CURRENT_DATE + INTERVAL ''7 days'';'
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
\echo 'Practical ORCA Optimization Examples:'
\echo '==================================='

\echo 'Example 1: Complex analytical query - ORCA excels here'
\echo 'SQL: Hub analysis with passenger rankings'
EXPLAIN (ANALYZE OFF, COSTS OFF)
WITH hub_stats AS (
    SELECT 
        f.origin as hub,
        COUNT(*) as total_flights,
        COUNT(DISTINCT f.destination) as destinations_served,
        AVG(EXTRACT(epoch FROM (f.arrival_time - f.departure_time))/3600) as avg_flight_hours
    FROM flights f
    GROUP BY f.origin
),
passenger_rankings AS (
    SELECT 
        p.passenger_id,
        p.first_name || ' ' || p.last_name as name,
        COUNT(b.booking_id) as total_bookings,
        ROW_NUMBER() OVER (ORDER BY COUNT(b.booking_id) DESC) as booking_rank
    FROM passenger p
    JOIN booking b ON p.passenger_id = b.passenger_id
    GROUP BY p.passenger_id, p.first_name, p.last_name
    HAVING COUNT(b.booking_id) >= 3
)
SELECT 
    h.hub,
    h.total_flights,
    h.destinations_served,
    ROUND(h.avg_flight_hours, 2) as avg_hours,
    COUNT(pr.passenger_id) as frequent_travelers
FROM hub_stats h
JOIN flights f ON h.hub = f.origin
JOIN booking b ON f.flight_id = b.flight_id
JOIN passenger_rankings pr ON b.passenger_id = pr.passenger_id AND pr.booking_rank <= 100
GROUP BY h.hub, h.total_flights, h.destinations_served, h.avg_flight_hours
ORDER BY h.total_flights DESC
LIMIT 10;

\echo ''
\echo 'Example 2: Time-based analysis - ORCA handles complex conditions'
\echo 'SQL: Peak booking times and lead time analysis'
EXPLAIN (ANALYZE OFF, COSTS OFF)
SELECT 
    EXTRACT(hour FROM b.booking_date) as booking_hour,
    EXTRACT(dow FROM f.departure_time) as departure_day_of_week,
    COUNT(*) as booking_count,
    AVG(EXTRACT(days FROM (f.departure_time - b.booking_date))) as avg_lead_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(days FROM (f.departure_time - b.booking_date))) as median_lead_days
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id
WHERE f.departure_time >= CURRENT_DATE
  AND f.departure_time < CURRENT_DATE + INTERVAL '30 days'
GROUP BY EXTRACT(hour FROM b.booking_date), EXTRACT(dow FROM f.departure_time)
HAVING COUNT(*) >= 5
ORDER BY booking_count DESC;

\echo ''
\echo 'Example 3: Multi-table join optimization - ORCA Motion analysis'
\echo 'SQL: Route profitability with passenger preferences'
EXPLAIN (ANALYZE OFF, COSTS OFF, VERBOSE)
SELECT 
    f.origin,
    f.destination,
    COUNT(b.booking_id) as bookings,
    COUNT(DISTINCT b.passenger_id) as unique_passengers,
    STRING_AGG(DISTINCT SUBSTRING(p.email FROM '@(.*)'), ', ') as email_domains,
    AVG(CASE 
        WHEN EXTRACT(days FROM (f.departure_time - b.booking_date)) <= 7 THEN 1.5
        WHEN EXTRACT(days FROM (f.departure_time - b.booking_date)) <= 30 THEN 1.0  
        ELSE 0.8
    END) as booking_premium_factor
FROM flights f
JOIN booking b ON f.flight_id = b.flight_id
JOIN passenger p ON b.passenger_id = p.passenger_id
WHERE f.origin IN ('JFK', 'LAX', 'ORD', 'ATL', 'DFW')
GROUP BY f.origin, f.destination
HAVING COUNT(b.booking_id) >= 10
ORDER BY bookings DESC, booking_premium_factor DESC;

\echo ''
\echo 'Example 4: Advanced window functions - ORCA analytical strength'  
\echo 'SQL: Passenger journey analysis with rankings using CTE'
EXPLAIN (ANALYZE OFF, COSTS OFF)
WITH passenger_trips AS (
    SELECT 
        p.passenger_id,
        p.first_name || ' ' || p.last_name as passenger_name,
        f.origin,
        f.destination,
        f.departure_time,
        b.booking_date,
        EXTRACT(days FROM (f.departure_time - b.booking_date)) as lead_days,
        ROW_NUMBER() OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as trip_sequence,
        LAG(f.destination) OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as previous_destination,
        COUNT(*) OVER (PARTITION BY p.passenger_id) as total_trips
    FROM passenger p
    JOIN booking b ON p.passenger_id = b.passenger_id
    JOIN flights f ON b.flight_id = f.flight_id
    WHERE p.passenger_id <= 1000  -- Limit for demo purposes
)
SELECT 
    passenger_id,
    passenger_name,
    origin,
    destination,
    departure_time,
    booking_date,
    lead_days,
    trip_sequence,
    previous_destination,
    CASE 
        WHEN previous_destination = origin 
        THEN 'CONNECTING_FLIGHT'
        ELSE 'NEW_JOURNEY'
    END as trip_type,
    total_trips,
    RANK() OVER (ORDER BY total_trips DESC) as passenger_activity_rank
FROM passenger_trips
ORDER BY passenger_activity_rank, passenger_id, trip_sequence;

\echo ''
\echo 'Example 5: ORCA vs PostgreSQL planner comparison on complex query'
\echo 'SQL: SELECT DATE_TRUNC(''week'', f.departure_time) as week_start, f.origin, COUNT(*) as flights,'
\echo '     AVG(COUNT(*)) OVER (PARTITION BY f.origin ORDER BY DATE_TRUNC(''week'', f.departure_time) ROWS 2 PRECEDING) as moving_avg'
\echo '     FROM flights f WHERE f.departure_time >= CURRENT_DATE - INTERVAL ''60 days'''
\echo '     GROUP BY DATE_TRUNC(''week'', f.departure_time), f.origin HAVING COUNT(*) >= 3;'
\echo 'Same complex query with both optimizers:'

SET optimizer = on;
\echo 'WITH ORCA optimizer:'
EXPLAIN (ANALYZE OFF, COSTS OFF)
SELECT 
    DATE_TRUNC('week', f.departure_time) as week_start,
    f.origin,
    COUNT(*) as flights,
    AVG(COUNT(*)) OVER (PARTITION BY f.origin ORDER BY DATE_TRUNC('week', f.departure_time) ROWS 2 PRECEDING) as moving_avg
FROM flights f
WHERE f.departure_time >= CURRENT_DATE - INTERVAL '60 days'
GROUP BY DATE_TRUNC('week', f.departure_time), f.origin
HAVING COUNT(*) >= 3
ORDER BY week_start, f.origin;

\echo ''
SET optimizer = off; 
\echo 'WITH PostgreSQL planner:'
EXPLAIN (ANALYZE OFF, COSTS OFF)
SELECT 
    DATE_TRUNC('week', f.departure_time) as week_start,
    f.origin,
    COUNT(*) as flights,
    AVG(COUNT(*)) OVER (PARTITION BY f.origin ORDER BY DATE_TRUNC('week', f.departure_time) ROWS 2 PRECEDING) as moving_avg
FROM flights f
WHERE f.departure_time >= CURRENT_DATE - INTERVAL '60 days'
GROUP BY DATE_TRUNC('week', f.departure_time), f.origin
HAVING COUNT(*) >= 3
ORDER BY week_start, f.origin;

-- Reset to ORCA
SET optimizer = on;

\echo ''
\echo 'Performance Tuning Tips:'
\echo '======================='
\echo '1. ORCA relies heavily on table statistics - run ANALYZE regularly'
\echo '2. Distribution keys should align with common join patterns'
\echo '3. Use EXPLAIN to understand Motion operations'
\echo '4. Monitor segment balance for optimal parallelism'
\echo '5. Consider partition pruning for time-series data'
\echo '6. Complex analytical queries are ORCA strengths'
\echo '7. Window functions perform excellently with ORCA'
\echo '8. Multi-table joins benefit from ORCA cost-based decisions'
\echo ''
\echo 'Query completed - ORCA optimizer analysis finished'