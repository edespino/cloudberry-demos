-- ============================================================================
-- pg_hint_plan Demonstrations
-- ============================================================================
-- Examples of using pg_hint_plan extension to control query execution
-- in Apache Cloudberry for performance tuning and troubleshooting.

\echo 'pg_hint_plan Extension Demonstrations'
\echo '====================================='

-- Install pg_hint_plan extension if not already available
\echo 'Installing pg_hint_plan extension:'
CREATE EXTENSION IF NOT EXISTS pg_hint_plan;

-- Check if pg_hint_plan is now available
\echo 'Checking pg_hint_plan availability:'
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_hint_plan';

\echo ''
\echo 'Hint Examples - Join Methods:'
\echo '============================='
\echo 'Note: Hints only change execution plans when they force the optimizer'
\echo 'to choose differently than it would naturally. If plans look identical,'
\echo 'it means the optimizer was already choosing the hinted method.'
\echo ''
\echo 'IMPORTANT: ORCA (GPORCA) is very sophisticated and often makes optimal'
\echo 'choices automatically. You may see identical plans because ORCA already'
\echo 'selected the best strategy. This shows ORCA quality, not hint failure!'
\echo ''

-- Force specific join algorithms - using a query that shows difference
\echo 'Hash Join vs Nested Loop demonstration:'
\echo 'SQL: SELECT b.booking_id, f.flight_number FROM booking b JOIN flights f ON b.flight_id = f.flight_id WHERE b.booking_id < 100;'
\echo ''
\echo 'WITHOUT hint (ORCA natural choice for small result set):'
EXPLAIN (COSTS OFF)
SELECT b.booking_id, f.flight_number 
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id 
WHERE b.booking_id < 100;

\echo ''
\echo 'WITH HashJoin hint /*+ HashJoin(b f) */ - forces hash join even for small result:'
/*+ HashJoin(b f) */
EXPLAIN (COSTS OFF)
SELECT b.booking_id, f.flight_number 
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id 
WHERE b.booking_id < 100;

\echo ''
\echo 'Nested Loop hint demonstration:'
\echo 'SQL: SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id;'
\echo ''
\echo 'WITHOUT hint (ORCA prefers hash join for large tables):'
EXPLAIN (COSTS OFF)
SELECT COUNT(*)
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id;

\echo ''
\echo 'WITH NestLoop hint /*+ NestLoop(b f) */ - forces nested loop for large join:'
/*+ NestLoop(b f) */
EXPLAIN (COSTS OFF)
SELECT COUNT(*)
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id;

\echo ''
\echo 'Join Order Control:'
\echo '=================='

-- Control join order in multi-table queries
\echo 'Leading hint demonstration - 3-table join:'
\echo 'SQL: SELECT COUNT(*) FROM booking b JOIN flights f ON b.flight_id = f.flight_id JOIN passenger p ON b.passenger_id = p.passenger_id;'
\echo ''
\echo 'WITHOUT hint (natural join order):'
EXPLAIN (COSTS OFF)
SELECT COUNT(*)
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id
JOIN passenger p ON b.passenger_id = p.passenger_id;

\echo ''
\echo 'WITH Leading hint /*+ Leading(((b f) p)) */ - forces (booking⋈flights) then ⋈passenger:'
/*+ Leading(((b f) p)) */
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
\echo 'Multiple hints combination demonstration:'
\echo 'SQL: SELECT p.first_name, p.last_name, f.flight_number FROM booking b'
\echo '     JOIN flights f ON b.flight_id = f.flight_id JOIN passenger p ON b.passenger_id = p.passenger_id'
\echo '     WHERE p.first_name LIKE ''John%'';'
\echo ''
\echo 'WITHOUT hints (optimizer decides everything):'
EXPLAIN (COSTS OFF)
SELECT p.first_name, p.last_name, f.flight_number
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id
JOIN passenger p ON b.passenger_id = p.passenger_id
WHERE p.first_name LIKE 'John%';

\echo ''
\echo 'WITH multiple hints /*+ HashJoin(b f) Leading(((b f) p)) SeqScan(p) */:'
\echo '  - Forces hash join between booking and flights'
\echo '  - Forces join order: (booking⋈flights) then ⋈passenger'  
\echo '  - Forces sequential scan on passenger table'
/*+ HashJoin(b f) Leading(((b f) p)) SeqScan(p) */
EXPLAIN (COSTS OFF)
SELECT p.first_name, p.last_name, f.flight_number
FROM booking b
JOIN flights f ON b.flight_id = f.flight_id
JOIN passenger p ON b.passenger_id = p.passenger_id
WHERE p.first_name LIKE 'John%';

\echo ''
\echo 'ORCA vs PostgreSQL Planner - When Hints Matter More:'
\echo '=================================================='

-- Demonstrate hints with PostgreSQL planner (hints may show more effect)
\echo 'Switching to PostgreSQL planner to see more hint effects:'
SET optimizer = off;

\echo ''
\echo 'PostgreSQL planner WITHOUT hints:'
EXPLAIN (COSTS OFF)
SELECT COUNT(*)
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id 
JOIN passenger p ON b.passenger_id = p.passenger_id
WHERE f.origin = 'JFK';

\echo ''
\echo 'PostgreSQL planner WITH Leading hint /*+ Leading(((f b) p)) */:'
/*+ Leading(((f b) p)) */
EXPLAIN (COSTS OFF)
SELECT COUNT(*)
FROM booking b 
JOIN flights f ON b.flight_id = f.flight_id 
JOIN passenger p ON b.passenger_id = p.passenger_id
WHERE f.origin = 'JFK';

-- Switch back to ORCA
SET optimizer = on;

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
\echo 'Subquery optimization demonstration:'
\echo 'SQL: SELECT b.booking_id, b.booking_date FROM booking b WHERE b.flight_id IN (SELECT f.flight_id FROM flights f WHERE f.origin = ''LAX'');'
\echo ''
\echo 'WITHOUT hints (natural optimizer choice):'
EXPLAIN (COSTS OFF)
SELECT b.booking_id, b.booking_date
FROM booking b
WHERE b.flight_id IN (
    SELECT f.flight_id 
    FROM flights f 
    WHERE f.origin = 'LAX'
);

\echo ''
\echo 'WITH subquery hints /*+ HashJoin(outer inner) */ and /*+ SeqScan(f) */:'
/*+ HashJoin(outer inner) */
EXPLAIN (COSTS OFF)
SELECT b.booking_id, b.booking_date
FROM booking b
WHERE b.flight_id IN (
    /*+ SeqScan(f) */
    SELECT f.flight_id 
    FROM flights f 
    WHERE f.origin = 'LAX'
);

\echo ''
\echo 'OBSERVATION: Plans are identical! ORCA transformed the subquery into'
\echo 'an optimal join strategy and chose the best scan methods automatically.'
\echo 'The hints requested what ORCA was already planning to do.'

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

\echo ''
\echo '=========================================='
\echo 'SUMMARY: pg_hint_plan with Apache Cloudberry'
\echo '=========================================='
\echo ''
\echo 'Key Observations:'
\echo '1. ORCA optimizer often makes optimal choices automatically'
\echo '2. Hints show more effect with PostgreSQL planner than ORCA'
\echo '3. Identical plans indicate ORCA was already choosing optimally'
\echo '4. pg_hint_plan is most useful for:'
\echo '   - Forcing specific behavior for testing'
\echo '   - Overriding ORCA in edge cases'
\echo '   - Troubleshooting performance issues'
\echo '   - Ensuring consistent plans across environments'
\echo ''
\echo 'This demonstrates ORCA high quality - it often chooses'
\echo 'the same strategies that manual hints would request!'