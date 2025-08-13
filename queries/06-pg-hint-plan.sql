-- ============================================================================
-- pg_hint_plan Demonstrations
-- ============================================================================
-- Examples of using pg_hint_plan extension to control query execution
-- in Apache Cloudberry for performance tuning and troubleshooting.

\echo 'pg_hint_plan Extension Demonstrations'
\echo '====================================='

-- Check if pg_hint_plan is available
\echo 'Checking pg_hint_plan availability:'
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_hint_plan';

\echo ''
\echo 'Hint Examples - Join Methods:'
\echo '============================='

-- Force specific join algorithms
\echo 'Hash Join hint:'
/*+ HashJoin(b f) */
EXPLAIN (COSTS OFF)
SELECT COUNT(*) 
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id;

\echo ''
\echo 'Nested Loop hint:'
/*+ NestLoop(b f) */
EXPLAIN (COSTS OFF)
SELECT b.booking_id, f.flight_number
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id
LIMIT 5;

\echo ''
\echo 'Join Order Control:'
\echo '=================='

-- Control join order in multi-table queries
\echo 'Leading hint to control join order:'
/*+ Leading((b f) p) */
EXPLAIN (COSTS OFF)
SELECT COUNT(*)
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id
JOIN passenger p ON b.passenger_id = p.passenger_id;

\echo ''
\echo 'Scan Method Control:'
\echo '==================='

-- Force specific scan methods
\echo 'Sequential scan hint:'
/*+ SeqScan(f) */
EXPLAIN (COSTS OFF)
SELECT * FROM flights f WHERE f.origin = 'LAX';

\echo ''
\echo 'Index scan hint (if indexes exist):'
/*+ IndexScan(f) */
EXPLAIN (COSTS OFF)
SELECT * FROM flights f WHERE f.flight_id = 100;

\echo ''
\echo 'Multiple Hints Combined:'
\echo '======================='

-- Combine multiple hints for complex control
\echo 'Multiple hints in one query:'
/*+ HashJoin(b f) Leading((b f) p) SeqScan(p) */
EXPLAIN (COSTS OFF)
SELECT p.first_name, p.last_name, f.flight_number
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id
JOIN passenger p ON b.passenger_id = p.passenger_id
WHERE p.first_name LIKE 'John%';

\echo ''
\echo 'Performance Comparison - With and Without Hints:'
\echo '==============================================='

\timing on

\echo 'Without hints:'
SELECT COUNT(*) 
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id;

\echo ''
\echo 'With hash join hint:'
/*+ HashJoin(b f) */
SELECT COUNT(*) 
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id;

\timing off

\echo ''
\echo 'Subquery Hints:'
\echo '==============='

-- Hints for subqueries
\echo 'Subquery optimization:'
/*+ HashJoin(outer inner) */
SELECT b.booking_id, b.booking_date
FROM booking b
WHERE b.flight_id IN (
    /*+ SeqScan(f) */
    SELECT f.flight_id 
    FROM flights f 
    WHERE f.origin = 'LAX'
);

\echo ''
\echo 'Invalid Hint Handling:'
\echo '====================='

-- Show how invalid hints are handled
\echo 'Invalid hint example (should be ignored):'
/*+ InvalidHint(b f) */
EXPLAIN (COSTS OFF)
SELECT COUNT(*) 
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id;

\echo ''
\echo 'Best Practices for pg_hint_plan:'
\echo '==============================='
\echo '1. Use hints sparingly - let ORCA optimize automatically'
\echo '2. Always test performance with and without hints'  
\echo '3. Document why specific hints are needed'
\echo '4. Re-evaluate hints after ANALYZE or schema changes'
\echo '5. Consider hints as temporary solutions for specific issues'

\echo ''
\echo 'When to Use Hints:'
\echo '=================='
\echo '- Forcing specific execution plans for testing'
\echo '- Working around temporary optimizer issues'
\echo '- Performance troubleshooting and plan comparison'
\echo '- Critical production queries requiring consistent plans'

\echo ''
\echo 'Hint Syntax Reference:'
\echo '====================='
\echo '/*+ HashJoin(table1 table2) */     - Force hash join'
\echo '/*+ NestLoop(table1 table2) */     - Force nested loop'
\echo '/*+ Leading((table1 table2) table3) */ - Control join order'
\echo '/*+ SeqScan(table) */              - Force sequential scan'
\echo '/*+ IndexScan(table) */            - Force index scan'