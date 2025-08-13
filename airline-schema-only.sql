-- ============================================================================
-- Apache Cloudberry (Incubating) - Airline Demo Schema
-- ============================================================================
-- Schema-only version for use with enhanced data loader
-- This creates tables without embedded data generation
-- ============================================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS booking CASCADE;
DROP TABLE IF EXISTS flights CASCADE;
DROP TABLE IF EXISTS passenger CASCADE;

-- ============================================================================
-- TABLE DEFINITIONS
-- ============================================================================

-- Passenger table
CREATE TABLE passenger (
    passenger_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL, 
    email VARCHAR(100) NOT NULL,
    phone VARCHAR(50)
) WITH (appendonly=true, compresslevel=5, compresstype=zstd)
DISTRIBUTED BY (passenger_id);

-- Flights table  
CREATE TABLE flights (
    flight_id SERIAL PRIMARY KEY,
    flight_number VARCHAR(10) NOT NULL,
    origin VARCHAR(3) NOT NULL,
    destination VARCHAR(3) NOT NULL,
    departure_time TIMESTAMP NOT NULL,
    arrival_time TIMESTAMP NOT NULL
) WITH (appendonly=true, compresslevel=5, compresstype=zstd)
DISTRIBUTED BY (flight_id);

-- Booking table
CREATE TABLE booking (
    booking_id SERIAL PRIMARY KEY,
    passenger_id INTEGER NOT NULL,
    flight_id INTEGER NOT NULL,
    booking_date TIMESTAMP NOT NULL,
    seat_number VARCHAR(5) NOT NULL
) WITH (appendonly=true, compresslevel=5, compresstype=zstd)
DISTRIBUTED BY (booking_id);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Passenger indexes (email uniqueness enforced at application level)
CREATE INDEX idx_passenger_email ON passenger(email);
CREATE INDEX idx_passenger_name ON passenger(last_name, first_name);

-- Flight indexes
CREATE INDEX idx_flights_route ON flights(origin, destination);
CREATE INDEX idx_flights_departure ON flights(departure_time);
CREATE INDEX idx_flights_number ON flights(flight_number);

-- Booking indexes
CREATE INDEX idx_booking_passenger ON booking(passenger_id);
CREATE INDEX idx_booking_flight ON booking(flight_id);
CREATE INDEX idx_booking_date ON booking(booking_date);

-- ============================================================================
-- SCHEMA COMPLETE
-- ============================================================================

-- Tables created:
--   • passenger: Passenger information with contact details
--   • flights: Flight schedules and routes  
--   • booking: Reservations linking passengers to flights
--
-- Storage: Appendonly tables with zstd compression level 5
-- Distribution: Hash distributed by primary key for optimal parallelism
-- Indexes: Performance indexes on common query patterns
-- Note: Email uniqueness enforced at application level (data generation ensures uniqueness)