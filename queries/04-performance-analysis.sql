-- ============================================================================
-- Performance Analysis and Statistics
-- ============================================================================
-- Queries for analyzing Apache Cloudberry performance, statistics,
-- and data distribution across segments.

\echo 'Apache Cloudberry Performance Analysis'
\echo '======================================'

\echo 'Table Row Counts:'
\echo '================='

-- Basic table row counts
SELECT 'passenger' as table_name, COUNT(*) as row_count FROM passenger
UNION ALL
SELECT 'flights', COUNT(*) FROM flights  
UNION ALL
SELECT 'booking', COUNT(*) FROM booking
ORDER BY table_name;

\echo ''
\echo 'Table Statistics and Maintenance:'
\echo '================================='

-- Check table statistics (simplified for Cloudberry compatibility)
SELECT 
    schemaname,
    relname as tablename,
    n_tup_ins as rows_inserted,
    n_tup_upd as rows_updated,
    n_tup_del as rows_deleted,
    last_analyze,
    analyze_count
FROM pg_stat_user_tables 
WHERE schemaname = 'public'
ORDER BY relname;

\echo ''
\echo 'Data Distribution Across Segments:'
\echo '=================================='

-- Check data distribution for each table
\echo 'Booking table distribution:'
SELECT gp_segment_id, count(*) as row_count
FROM gp_dist_random('booking') 
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

\echo ''
\echo 'Flights table distribution:'
SELECT gp_segment_id, count(*) as row_count
FROM gp_dist_random('flights') 
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

\echo ''
\echo 'Passenger table distribution:'
SELECT gp_segment_id, count(*) as row_count
FROM gp_dist_random('passenger') 
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

\echo ''
\echo 'Column Statistics Quality:'
\echo '========================='

-- Check statistics quality for key columns (simplified for Cloudberry)
SELECT tablename, attname, n_distinct, correlation
FROM pg_stats 
WHERE schemaname = 'public'
  AND tablename IN ('passenger', 'flights', 'booking') 
  AND attname IN ('passenger_id', 'flight_id', 'booking_id')
ORDER BY tablename, attname;

\echo ''
\echo 'Query Performance Benchmarks:'
\echo '============================'

-- Benchmark common query patterns
\timing on

\echo 'Simple count query:'
SELECT COUNT(*) FROM booking;

\echo ''
\echo 'Two-table join:'
SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id;

\echo ''
\echo 'Three-table join with aggregation:'
SELECT COUNT(*), AVG(EXTRACT(epoch FROM f.arrival_time - f.departure_time)/3600) as avg_hours
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id 
JOIN passenger p ON b.passenger_id = p.passenger_id;

\echo ''
\echo 'Complex aggregation:'
SELECT f.origin, COUNT(*) as bookings, COUNT(DISTINCT b.passenger_id) as passengers
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id 
GROUP BY f.origin 
ORDER BY COUNT(*) DESC 
LIMIT 10;

\timing off

\echo ''
\echo 'Memory and Resource Usage:'
\echo '=========================='

-- Show current resource settings
SELECT name, setting, unit, source 
FROM pg_settings 
WHERE name IN ('work_mem', 'shared_buffers', 'max_connections', 'gp_vmem_protect_limit')
ORDER BY name;

\echo ''
\echo 'Compression Effectiveness:'
\echo '========================='

-- Estimate compression ratios
SELECT 
  tablename,
  pg_size_pretty(pg_total_relation_size('public.'||tablename)) as compressed_size,
  CASE 
    WHEN tablename = 'passenger' THEN '~25MB uncompressed'
    WHEN tablename = 'flights' THEN '~8MB uncompressed'  
    WHEN tablename = 'booking' THEN '~45MB uncompressed'
    ELSE 'unknown'
  END as estimated_uncompressed,
  CASE 
    WHEN tablename = 'passenger' THEN '60-70%'
    WHEN tablename = 'flights' THEN '65-75%'
    WHEN tablename = 'booking' THEN '50-60%'
    ELSE 'unknown'
  END as compression_ratio
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size('public.'||tablename) DESC;