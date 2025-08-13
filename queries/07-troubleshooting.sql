-- ============================================================================
-- Troubleshooting and Diagnostics
-- ============================================================================
-- Queries for diagnosing performance issues, checking system health,
-- and troubleshooting common Apache Cloudberry problems.

\echo 'Apache Cloudberry Troubleshooting Queries'
\echo '=========================================='

\echo 'System Health Check:'
\echo '==================='

-- Check segment configuration and status
\echo 'Segment configuration:'
SELECT content, hostname, port, 
       CASE WHEN role = 'p' THEN 'primary' ELSE 'mirror' END as role_type,
       CASE WHEN status = 'u' THEN 'up' ELSE 'down' END as status,
       mode
FROM gp_segment_configuration
WHERE content >= 0  -- Exclude master (-1)
ORDER BY content;

\echo ''
\echo 'Database Statistics Currency:'
\echo '============================'

-- Check when tables were last analyzed
SELECT 
    schemaname,
    relname as tablename,
    last_analyze,
    analyze_count,
    CASE 
        WHEN last_analyze IS NULL THEN 'NEVER ANALYZED'
        WHEN age(now(), last_analyze) > interval '1 day' THEN 'STALE (>1 day)'
        WHEN age(now(), last_analyze) > interval '1 hour' THEN 'OLD (>1 hour)'
        ELSE 'CURRENT'
    END as statistics_status
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY last_analyze DESC NULLS LAST;

\echo ''
\echo 'Data Distribution Balance:'
\echo '========================='

-- Check for data skew across segments
WITH segment_counts AS (
    SELECT 'booking' as table_name, gp_segment_id, count(*) as row_count
    FROM gp_dist_random('booking') 
    GROUP BY gp_segment_id
    
    UNION ALL
    
    SELECT 'flights' as table_name, gp_segment_id, count(*) as row_count
    FROM gp_dist_random('flights') 
    GROUP BY gp_segment_id
    
    UNION ALL
    
    SELECT 'passenger' as table_name, gp_segment_id, count(*) as row_count
    FROM gp_dist_random('passenger') 
    GROUP BY gp_segment_id
),
distribution_stats AS (
    SELECT 
        table_name,
        MIN(row_count) as min_rows,
        MAX(row_count) as max_rows,
        AVG(row_count) as avg_rows,
        STDDEV(row_count) as stddev_rows
    FROM segment_counts
    GROUP BY table_name
)
SELECT 
    table_name,
    min_rows,
    max_rows,
    ROUND(avg_rows) as avg_rows,
    ROUND(stddev_rows) as stddev_rows,
    CASE 
        WHEN max_rows - min_rows > avg_rows * 0.5 THEN 'SIGNIFICANT SKEW'
        WHEN max_rows - min_rows > avg_rows * 0.2 THEN 'MODERATE SKEW'
        ELSE 'WELL BALANCED'
    END as distribution_status
FROM distribution_stats
ORDER BY table_name;

\echo ''
\echo 'Memory and Resource Settings:'
\echo '============================'

-- Key performance settings
SELECT 
    name,
    setting,
    unit,
    context,
    source,
    CASE 
        WHEN name = 'work_mem' AND setting::int < 32768 THEN 'Consider increasing for complex queries'
        WHEN name = 'shared_buffers' AND setting::int < 131072 THEN 'Consider increasing for better caching'
        WHEN name = 'gp_vmem_protect_limit' AND setting::int < 8192 THEN 'May be too low for large queries'
        ELSE 'OK'
    END as recommendation
FROM pg_settings 
WHERE name IN (
    'work_mem', 'shared_buffers', 'max_connections',
    'gp_vmem_protect_limit', 'optimizer', 'enable_nestloop'
)
ORDER BY name;

\echo ''
\echo 'Query Performance Diagnosis:'
\echo '==========================='

-- Sample problematic query patterns
\echo 'Testing common performance patterns:'

\echo ''
\echo '1. Large table scan performance:'
\timing on
SELECT COUNT(*) FROM booking;
\timing off

\echo ''
\echo '2. Join performance without distribution key alignment:'
\timing on
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*) 
FROM booking b 
JOIN passenger p ON b.passenger_id = p.passenger_id;
\timing off

\echo ''
\echo '3. Aggregation performance:'
\timing on
SELECT origin, COUNT(*) 
FROM flights f 
JOIN booking b ON f.flight_id = b.flight_id 
GROUP BY origin 
ORDER BY COUNT(*) DESC;
\timing off

\echo ''
\echo 'Common Performance Issues and Solutions:'
\echo '======================================='

-- Check for common performance problems
\echo 'Checking for potential issues:'

-- 1. Missing statistics
WITH missing_stats AS (
    SELECT relname as tablename 
    FROM pg_stat_user_tables 
    WHERE schemaname = 'public' 
    AND (last_analyze IS NULL OR analyze_count = 0)
)
SELECT 
    'Missing Statistics' as issue_type,
    tablename as affected_object,
    'Run ANALYZE ' || tablename as solution
FROM missing_stats;

-- 2. Check for very old statistics
WITH stale_stats AS (
    SELECT relname as tablename 
    FROM pg_stat_user_tables 
    WHERE schemaname = 'public' 
    AND last_analyze < now() - interval '1 day'
)
SELECT 
    'Stale Statistics' as issue_type,
    tablename as affected_object,
    'Run ANALYZE ' || tablename as solution
FROM stale_stats;

\echo ''
\echo 'Optimizer Behavior Analysis:'
\echo '============================'

-- Test optimizer choices
\echo 'Current optimizer setting:'
SHOW optimizer;

\echo ''
\echo 'Testing optimizer decision for complex query:'
EXPLAIN (COSTS OFF)
WITH complex_query AS (
    SELECT 
        f.origin,
        f.destination,
        COUNT(*) as flight_count,
        COUNT(DISTINCT b.passenger_id) as passenger_count
    FROM flights f
    JOIN booking b ON f.flight_id = b.flight_id
    GROUP BY f.origin, f.destination
)
SELECT * FROM complex_query 
WHERE flight_count > 5
ORDER BY passenger_count DESC;

\echo ''
\echo 'Memory Usage Analysis:'
\echo '====================='

-- Check for memory pressure indicators
SELECT 
    name,
    setting,
    CASE name 
        WHEN 'work_mem' THEN 'Per-operation memory limit'
        WHEN 'shared_buffers' THEN 'Shared cache size'
        WHEN 'gp_vmem_protect_limit' THEN 'Per-segment memory limit'
        WHEN 'max_connections' THEN 'Maximum concurrent connections'
        WHEN 'effective_cache_size' THEN 'Planner cache size estimate'
        ELSE 'Memory-related setting'
    END as purpose
FROM pg_settings 
WHERE name LIKE '%mem%' 
   OR name LIKE '%buffer%'
ORDER BY name;

\echo ''
\echo 'Quick Performance Fixes:'
\echo '======================'
\echo '1. Update statistics: ANALYZE passenger; ANALYZE flights; ANALYZE booking;'
\echo '2. For slow joins: Check distribution key alignment'
\echo '3. For memory errors: Increase work_mem or simplify queries'
\echo '4. For plan instability: Use pg_hint_plan or lock optimizer choice'
\echo '5. For data skew: Consider redistribution by different columns'

\echo ''
\echo 'Monitoring Queries for Ongoing Health:'
\echo '====================================='
\echo 'Run these periodically to monitor system health:'
\echo ''
\echo '-- Check segment status:'
\echo 'SELECT content, status FROM gp_configuration WHERE content >= 0;'
\echo ''
\echo '-- Monitor statistics age:'
\echo 'SELECT tablename, age(now(), last_analyze) FROM pg_stat_user_tables;'
\echo ''
\echo '-- Check data distribution:'
\echo 'SELECT gp_segment_id, count(*) FROM gp_dist_random(''your_table'') GROUP BY 1;'