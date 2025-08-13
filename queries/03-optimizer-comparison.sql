-- ============================================================================
-- ORCA vs PostgreSQL Optimizer Comparison
-- ============================================================================
-- Demonstrates the differences between ORCA and PostgreSQL query planners
-- in Apache Cloudberry.

\echo 'ORCA vs PostgreSQL Optimizer Comparison'
\echo '========================================'

\echo 'First, ensure statistics are up to date:'
ANALYZE passenger;
ANALYZE flights; 
ANALYZE booking;

\echo ''
\echo 'Test Query: Complex join with aggregation'
\echo '========================================='

-- Store the test query
\set test_query 'SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id'

\echo 'ORCA Optimizer Plan:'
\echo '-------------------'
SET optimizer = on;
EXPLAIN (COSTS OFF) :test_query;

\echo ''
\echo 'PostgreSQL Optimizer Plan:'
\echo '---------------------------'
SET optimizer = off;
EXPLAIN (COSTS OFF) :test_query;

\echo ''
\echo 'Performance Comparison:'
\echo '======================'

-- Test with timing
\timing on

\echo 'ORCA Performance:'
SET optimizer = on;
:test_query;

\echo 'PostgreSQL Performance:'
SET optimizer = off;
:test_query;

\timing off

\echo ''
\echo 'Complex Analytical Query Comparison:'
\echo '===================================='

-- More complex query for comparison
\set complex_query 'WITH passenger_metrics AS (SELECT p.passenger_id, COUNT(*) as flight_count, AVG(EXTRACT(epoch FROM f.departure_time - b.booking_date)/86400) as avg_lead_days FROM passenger p JOIN booking b ON p.passenger_id = b.passenger_id JOIN flights f ON b.flight_id = f.flight_id GROUP BY p.passenger_id) SELECT flight_count, COUNT(*) as passenger_count, AVG(avg_lead_days) as avg_planning_days FROM passenger_metrics GROUP BY flight_count ORDER BY flight_count DESC'

\echo 'ORCA Plan for Complex Query:'
SET optimizer = on;
EXPLAIN (ANALYZE, COSTS OFF) :complex_query;

\echo ''
\echo 'PostgreSQL Plan for Complex Query:'
SET optimizer = off;
EXPLAIN (ANALYZE, COSTS OFF) :complex_query;

-- Reset to ORCA (recommended for analytical workloads)
SET optimizer = on;

\echo ''
\echo 'Optimizer Recommendations:'
\echo '=========================='
\echo '- ORCA: Best for complex analytics, multi-table joins, window functions'
\echo '- PostgreSQL: Good for simple OLTP queries, more predictable for basic operations'
\echo '- Always run ANALYZE after data loads for optimal ORCA performance'