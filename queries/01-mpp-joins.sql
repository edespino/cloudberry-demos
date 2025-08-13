-- ============================================================================
-- Apache Cloudberry MPP Join Processing Demonstrations
-- ============================================================================
-- This file demonstrates how Apache Cloudberry handles distributed joins
-- with Motion operations and parallel execution across segments.

-- Show execution plan to see Motion operations
\echo 'MPP Join Processing with Motion Operations'
\echo '=========================================='

-- Basic 3-table join showing Motion operations
EXPLAIN (COSTS OFF, VERBOSE)
SELECT b.booking_id, p.first_name || ' ' || p.last_name as passenger_name,
       f.flight_number, f.origin, f.destination, f.departure_time
FROM booking b
JOIN passenger p ON b.passenger_id = p.passenger_id  
JOIN flights f ON b.flight_id = f.flight_id
ORDER BY f.departure_time LIMIT 10;

\echo ''
\echo 'Running the actual query:'
\echo '========================='

-- Execute the actual query
SELECT b.booking_id, p.first_name || ' ' || p.last_name as passenger_name,
       f.flight_number, f.origin, f.destination, f.departure_time
FROM booking b
JOIN passenger p ON b.passenger_id = p.passenger_id  
JOIN flights f ON b.flight_id = f.flight_id
ORDER BY f.departure_time LIMIT 10;

\echo ''
\echo 'Aggregation with distributed GROUP BY:'
\echo '======================================'

-- Show how GROUP BY is processed across segments
EXPLAIN (COSTS OFF)
SELECT f.origin, f.destination, COUNT(*) as total_bookings,
       COUNT(DISTINCT b.passenger_id) as unique_passengers,
       AVG(EXTRACT(epoch FROM (f.arrival_time - f.departure_time))/3600) as avg_flight_hours
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id
GROUP BY f.origin, f.destination
ORDER BY COUNT(*) DESC;

-- Execute the aggregation
SELECT f.origin, f.destination, COUNT(*) as total_bookings,
       COUNT(DISTINCT b.passenger_id) as unique_passengers,
       AVG(EXTRACT(epoch FROM (f.arrival_time - f.departure_time))/3600) as avg_flight_hours
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id
GROUP BY f.origin, f.destination
ORDER BY COUNT(*) DESC;