-- ============================================================================
-- Apache Cloudberry Window Functions and Analytics
-- ============================================================================
-- Demonstrates advanced analytical processing with window functions
-- across distributed segments.

\echo 'Window Functions and Analytical Queries'
\echo '======================================='

-- Passenger booking patterns with ranking
\echo 'Passenger Booking Rankings:'
\echo '---------------------------'

WITH passenger_flight_summary AS (
    SELECT p.passenger_id, 
           p.first_name || ' ' || p.last_name as passenger_name,
           COUNT(*) as flight_count
    FROM passenger p 
    JOIN booking b ON p.passenger_id = b.passenger_id
    GROUP BY p.passenger_id, p.first_name, p.last_name
)
SELECT passenger_name, flight_count,
       RANK() OVER (ORDER BY flight_count DESC) as booking_rank,
       PERCENT_RANK() OVER (ORDER BY flight_count) as percentile_rank
FROM passenger_flight_summary
WHERE flight_count > 1
ORDER BY flight_count DESC
LIMIT 15;

\echo ''
\echo 'Route Popularity Analysis:'
\echo '-------------------------'

-- Route rankings with window functions
SELECT origin, destination, flight_count, 
       DENSE_RANK() OVER (ORDER BY flight_count DESC) as popularity_rank,
       PERCENT_RANK() OVER (ORDER BY flight_count) as percentile
FROM (
    SELECT f.origin, f.destination, COUNT(*) as flight_count
    FROM flights f
    JOIN booking b ON f.flight_id = b.flight_id  
    GROUP BY f.origin, f.destination
) route_stats
ORDER BY popularity_rank, flight_count DESC
LIMIT 20;

\echo ''
\echo 'Advanced Window Functions - Running Totals:'
\echo '============================================'

-- Running totals and moving averages
SELECT DATE(b.booking_date) as booking_day,
       COUNT(*) as daily_bookings,
       SUM(COUNT(*)) OVER (ORDER BY DATE(b.booking_date)) as running_total,
       AVG(COUNT(*)) OVER (ORDER BY DATE(b.booking_date) ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as seven_day_avg
FROM booking b
GROUP BY DATE(b.booking_date)
ORDER BY booking_day
LIMIT 30;

\echo ''
\echo 'Passenger Journey Analysis:'
\echo '==========================='

-- Complex window functions for journey analysis
WITH passenger_journeys AS (
    SELECT p.passenger_id,
           p.first_name || ' ' || p.last_name as passenger_name,
           f.departure_time,
           f.origin,
           f.destination,
           COUNT(*) OVER (PARTITION BY p.passenger_id) as total_flights,
           ROW_NUMBER() OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as flight_sequence,
           LAG(f.destination) OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as previous_destination
    FROM passenger p
    JOIN booking b ON p.passenger_id = b.passenger_id
    JOIN flights f ON b.flight_id = f.flight_id
)
SELECT passenger_name, origin, destination, departure_time,
       flight_sequence, total_flights,
       CASE WHEN origin = previous_destination THEN 'CONNECTING' ELSE 'ORIGIN' END as journey_type
FROM passenger_journeys
WHERE total_flights > 1
ORDER BY passenger_name, flight_sequence;