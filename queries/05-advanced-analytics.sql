-- ============================================================================
-- Advanced Analytics and Business Intelligence Queries
-- ============================================================================
-- Complex analytical queries demonstrating Apache Cloudberry's capabilities
-- for business intelligence and data analytics workloads.

\echo 'Advanced Analytics for Airline Business Intelligence'
\echo '==================================================='

\echo 'Flight Network Analysis:'
\echo '======================='

-- Hub airport analysis
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
    -- Outbound traffic
    SELECT 
        origin as airport,
        'outbound' as direction,
        COUNT(*) as route_count,
        SUM(flight_frequency) as total_flights,
        SUM(passenger_count) as total_passengers,
        AVG(avg_duration_hours) as avg_flight_duration
    FROM route_metrics
    GROUP BY origin
    
    UNION ALL
    
    -- Inbound traffic  
    SELECT 
        destination as airport,
        'inbound' as direction,
        COUNT(*) as route_count,
        SUM(flight_frequency) as total_flights,
        SUM(passenger_count) as total_passengers,
        AVG(avg_duration_hours) as avg_flight_duration
    FROM route_metrics
    GROUP BY destination
)
SELECT 
    airport,
    SUM(route_count) as total_routes,
    SUM(total_flights) as total_traffic,
    SUM(total_passengers) as passengers_served,
    AVG(avg_flight_duration) as avg_duration
FROM hub_analysis
GROUP BY airport
HAVING SUM(total_flights) > 50
ORDER BY total_traffic DESC
LIMIT 15;

\echo ''
\echo 'Passenger Behavior Analysis:'
\echo '============================'

-- Advanced passenger segmentation
WITH passenger_profiles AS (
    SELECT 
        p.passenger_id,
        p.first_name || ' ' || p.last_name as passenger_name,
        COUNT(*) as total_bookings,
        COUNT(DISTINCT f.origin) as origins_visited,
        COUNT(DISTINCT f.destination) as destinations_visited,
        MIN(b.booking_date) as first_booking,
        MAX(b.booking_date) as last_booking,
        AVG(EXTRACT(epoch FROM f.departure_time - b.booking_date)/86400) as avg_booking_lead_days,
        AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_flight_duration
    FROM passenger p
    JOIN booking b ON p.passenger_id = b.passenger_id
    JOIN flights f ON b.flight_id = f.flight_id
    GROUP BY p.passenger_id, p.first_name, p.last_name
),
passenger_segments AS (
    SELECT *,
        CASE 
            WHEN total_bookings >= 4 THEN 'Frequent Flyer'
            WHEN total_bookings = 3 THEN 'Regular Traveler'  
            WHEN total_bookings = 2 THEN 'Occasional Traveler'
            ELSE 'One-time Traveler'
        END as traveler_segment,
        CASE
            WHEN avg_booking_lead_days >= 30 THEN 'Planner'
            WHEN avg_booking_lead_days >= 7 THEN 'Advance Booker'
            ELSE 'Last Minute'
        END as booking_behavior
    FROM passenger_profiles
)
SELECT 
    traveler_segment,
    booking_behavior,
    COUNT(*) as passenger_count,
    ROUND(AVG(total_bookings), 1) as avg_bookings,
    ROUND(AVG(avg_booking_lead_days), 1) as avg_lead_days,
    ROUND(AVG(origins_visited), 1) as avg_origins,
    ROUND(AVG(destinations_visited), 1) as avg_destinations
FROM passenger_segments
GROUP BY traveler_segment, booking_behavior
ORDER BY passenger_count DESC;

\echo ''
\echo 'Route Profitability Analysis:'
\echo '============================='

-- Route performance metrics
WITH route_performance AS (
    SELECT 
        f.origin,
        f.destination,
        f.origin || ' -> ' || f.destination as route,
        COUNT(DISTINCT f.flight_id) as scheduled_flights,
        COUNT(b.booking_id) as total_bookings,
        COUNT(DISTINCT b.passenger_id) as unique_passengers,
        ROUND(COUNT(b.booking_id)::numeric / COUNT(DISTINCT f.flight_id), 1) as avg_bookings_per_flight,
        AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_flight_hours,
        MIN(f.departure_time) as first_flight,
        MAX(f.departure_time) as last_flight
    FROM flights f
    LEFT JOIN booking b ON f.flight_id = b.flight_id
    GROUP BY f.origin, f.destination
),
route_rankings AS (
    SELECT *,
        RANK() OVER (ORDER BY total_bookings DESC) as booking_rank,
        RANK() OVER (ORDER BY avg_bookings_per_flight DESC) as efficiency_rank,
        CASE 
            WHEN avg_bookings_per_flight >= 25 THEN 'High Demand'
            WHEN avg_bookings_per_flight >= 15 THEN 'Medium Demand'
            WHEN avg_bookings_per_flight >= 5 THEN 'Low Demand'
            ELSE 'Very Low Demand'
        END as demand_category
    FROM route_performance
    WHERE scheduled_flights > 0
)
SELECT 
    route,
    scheduled_flights,
    total_bookings,
    avg_bookings_per_flight,
    demand_category,
    booking_rank,
    efficiency_rank,
    ROUND(avg_flight_hours, 1) as flight_hours
FROM route_rankings
ORDER BY total_bookings DESC
LIMIT 20;

\echo ''
\echo 'Time-based Booking Patterns:'
\echo '============================'

-- Seasonal and daily patterns
SELECT 
    EXTRACT(hour FROM booking_date) as booking_hour,
    EXTRACT(dow FROM booking_date) as day_of_week,
    COUNT(*) as booking_count,
    ROUND(AVG(EXTRACT(epoch FROM f.departure_time - b.booking_date)/86400), 1) as avg_lead_days
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id
GROUP BY EXTRACT(hour FROM booking_date), EXTRACT(dow FROM booking_date)
HAVING COUNT(*) > 10
ORDER BY booking_count DESC
LIMIT 25;

\echo ''
\echo 'Multi-leg Journey Analysis:'
\echo '=========================='

-- Identify connecting passengers
WITH passenger_trips AS (
    SELECT 
        p.passenger_id,
        p.first_name || ' ' || p.last_name as passenger_name,
        f.departure_time,
        f.arrival_time,
        f.origin,
        f.destination,
        LAG(f.destination) OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as prev_destination,
        LAG(f.arrival_time) OVER (PARTITION BY p.passenger_id ORDER BY f.departure_time) as prev_arrival,
        COUNT(*) OVER (PARTITION BY p.passenger_id) as total_flights
    FROM passenger p
    JOIN booking b ON p.passenger_id = b.passenger_id
    JOIN flights f ON b.flight_id = f.flight_id
),
connections AS (
    SELECT 
        passenger_name,
        origin,
        destination, 
        departure_time,
        total_flights,
        CASE 
            WHEN origin = prev_destination 
            AND departure_time - prev_arrival <= INTERVAL '24 hours'
            THEN 'CONNECTING'
            ELSE 'ORIGIN'
        END as trip_type,
        CASE 
            WHEN origin = prev_destination 
            THEN EXTRACT(epoch FROM departure_time - prev_arrival)/3600
            ELSE NULL
        END as connection_hours
    FROM passenger_trips
    WHERE total_flights > 1
)
SELECT 
    trip_type,
    COUNT(*) as segment_count,
    ROUND(AVG(connection_hours), 1) as avg_connection_hours,
    MIN(connection_hours) as min_connection_hours,
    MAX(connection_hours) as max_connection_hours
FROM connections
WHERE connection_hours IS NOT NULL OR trip_type = 'ORIGIN'
GROUP BY trip_type
ORDER BY segment_count DESC;